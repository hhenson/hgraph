#include <hgraph/types/temporal.h>

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/value/value.h>

#if defined(HGRAPH_TIME_ZONE_BACKEND_STD)
#include <chrono>
#elif defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
#include <date/tz.h>
#else
#error "No hgraph time-zone backend was configured"
#endif

#include <array>
#include <atomic>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::int64_t microseconds_per_second = 1'000'000;

        struct TransparentStringHash
        {
            using is_transparent = void;

            [[nodiscard]] std::size_t operator()(
                std::string_view value) const noexcept
            {
                return std::hash<std::string_view>{}(value);
            }

            [[nodiscard]] std::size_t operator()(
                const std::string &value) const noexcept
            {
                return (*this)(std::string_view{value});
            }
        };

#if defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
        using ZoneAliasMap = std::unordered_map<
            std::string, std::string, TransparentStringHash, std::equal_to<>>;

        [[nodiscard]] ZoneAliasMap load_zone_aliases()
        {
            // date/tz deliberately omits Link records when USE_OS_TZDB is
            // enabled and expects links to exist as files.  Some otherwise
            // complete system installations (including Ubuntu's tzdata)
            // retain the links only in tzdata.zi.  Read that authoritative
            // source once so accepted IANA names behave consistently across
            // the standard and fallback providers.
            constexpr std::array<std::string_view, 3> candidates{
                "/usr/share/zoneinfo/tzdata.zi",
                "/usr/share/zoneinfo/uclibc/tzdata.zi",
                "/var/db/timezone/zoneinfo/tzdata.zi",
            };
            for (const auto path : candidates)
            {
                std::ifstream input{std::string{path}};
                if (!input) { continue; }

                ZoneAliasMap aliases;
                std::string line;
                while (std::getline(input, line))
                {
                    std::istringstream fields{line};
                    std::string kind;
                    std::string target;
                    std::string alias;
                    if (fields >> kind >> target >> alias &&
                        (kind == "L" || kind == "Link"))
                    {
                        aliases.insert_or_assign(
                            std::move(alias), std::move(target));
                    }
                }
                return aliases;
            }
            return {};
        }
#endif

        template <typename TimePoint>
        [[nodiscard]] Instant to_instant(TimePoint value)
        {
            const long double count =
                std::chrono::duration<long double, std::micro>{
                    value.time_since_epoch()}
                    .count();
            if (count <= static_cast<long double>(
                             std::numeric_limits<std::int64_t>::min()))
            {
                return Instant::min();
            }
            if (count >= static_cast<long double>(
                             std::numeric_limits<std::int64_t>::max()))
            {
                return Instant::max();
            }
            return Instant{Duration{static_cast<std::int64_t>(count)}};
        }

        template <typename DurationType>
        [[nodiscard]] std::int32_t offset_seconds(DurationType value)
        {
            const auto count =
                std::chrono::duration_cast<std::chrono::seconds>(value).count();
            if (count < std::numeric_limits<std::int32_t>::min() ||
                count > std::numeric_limits<std::int32_t>::max())
            {
                throw std::overflow_error("time-zone offset exceeds int32");
            }
            return static_cast<std::int32_t>(count);
        }

        [[nodiscard]] ResolvedInstant local_candidate(
            CivilDateTime local, std::int32_t offset)
        {
            const Instant coordinate{
                Duration{local.epoch_microseconds()}};
            return ResolvedInstant{
                checked_subtract(
                    coordinate,
                    Duration{static_cast<std::int64_t>(offset) *
                             microseconds_per_second}),
                offset};
        }

        template <typename OffsetAt>
        [[nodiscard]] Instant gap_transition(
            CivilDateTime local, std::int32_t before_offset,
            std::int32_t after_offset, OffsetAt &&offset_at)
        {
            const auto before_candidate =
                local_candidate(local, before_offset).instant;
            const auto after_candidate =
                local_candidate(local, after_offset).instant;
            auto lower = after_candidate.time_since_epoch().count();
            auto upper = before_candidate.time_since_epoch().count();
            if (lower > upper) { std::swap(lower, upper); }

            const Instant lower_instant{Duration{lower}};
            const Instant upper_instant{Duration{upper}};
            if (offset_at(lower_instant) != before_offset ||
                offset_at(upper_instant) != after_offset)
            {
                throw std::runtime_error(
                    "time-zone provider returned inconsistent gap boundaries");
            }

            // Search on the system timeline rather than trusting local_info's
            // interval endpoint.  libstdc++ 15 can report that endpoint one
            // daylight-saving adjustment late when a zone changes its base
            // UTC offset while DST is active (Pacific/Apia, 2011).
            while (lower < upper)
            {
                const auto midpoint = lower + (upper - lower) / 2;
                if (offset_at(Instant{Duration{midpoint}}) == after_offset)
                {
                    upper = midpoint;
                }
                else
                {
                    lower = midpoint + 1;
                }
            }
            return Instant{Duration{lower}};
        }

        struct TimeZoneProviderHolder
        {
            std::shared_ptr<const TimeZoneProvider> provider;
        };

#if defined(HGRAPH_TIME_ZONE_BACKEND_STD)
        using NativeTimeZone = std::chrono::time_zone;
#elif defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
        using NativeTimeZone = date::time_zone;
#endif

        struct BoundNativeTimeZone
        {
            std::uint64_t key{0};
            const NativeTimeZone *record{nullptr};
        };

        [[nodiscard]] constexpr std::uint64_t zone_cache_key(
            ZoneId zone) noexcept
        {
            const ZoneIdPayload payload = zone.payload();
            return static_cast<std::uint64_t>(payload.slot) |
                   (static_cast<std::uint64_t>(payload.generation) << 32U) |
                   (static_cast<std::uint64_t>(payload.name_tag) << 48U);
        }

        class NativeTimeZoneCache
        {
          public:
            template <typename Resolve>
            [[nodiscard]] const NativeTimeZone *locate(
                ZoneId zone, Resolve &&resolve)
            {
                const std::uint64_t key = zone_cache_key(zone);
                const std::size_t index = cache_index(key);
                if (const BoundNativeTimeZone *cached =
                        front_[index].load(std::memory_order_acquire);
                    cached != nullptr && cached->key == key)
                {
                    return cached->record;
                }

                const BoundNativeTimeZone &bound = records_.intern(
                    key,
                    [&] {
                        return BoundNativeTimeZone{
                            key, resolve(zone.name())};
                    });
                front_[index].store(
                    &bound, std::memory_order_release);
                return bound.record;
            }

            void clear() noexcept
            {
                // reset_all_registries() is test-only and cannot race an
                // evaluation.  Withdraw every lock-free borrower before the
                // intern table releases its stable records.
                for (auto &entry : front_)
                {
                    entry.store(nullptr, std::memory_order_relaxed);
                }
                records_.clear();
            }

          private:
            static constexpr std::size_t front_size = 4096;

            [[nodiscard]] static constexpr std::size_t cache_index(
                std::uint64_t key) noexcept
            {
                key ^= key >> 33U;
                key *= 0xff51afd7ed558ccdULL;
                key ^= key >> 33U;
                return static_cast<std::size_t>(key) &
                       (front_size - 1U);
            }

            static_assert(
                std::atomic<const BoundNativeTimeZone *>::is_always_lock_free,
                "supported 64-bit targets require lock-free pointer atomics");

            InternTable<std::uint64_t, BoundNativeTimeZone> records_;
            std::array<
                std::atomic<const BoundNativeTimeZone *>, front_size>
                front_{};
        };

        [[nodiscard]] NativeTimeZoneCache &native_time_zone_cache()
        {
            static NativeTimeZoneCache cache;
            return cache;
        }

#if defined(HGRAPH_TIME_ZONE_BACKEND_STD)
        class StdChronoTimeZoneProvider final : public TimeZoneProvider
        {
          public:
            StdChronoTimeZoneProvider()
                : version_(std::chrono::get_tzdb().version)
            {
            }

            [[nodiscard]] std::string_view version() const noexcept override
            {
                return version_;
            }

            [[nodiscard]] bool contains(ZoneId zone) const noexcept override
            {
                try { return locate(zone) != nullptr; }
                catch (...) { return false; }
            }

            [[nodiscard]] OffsetInfo at(
                Instant instant, ZoneId zone) const override
            {
                const auto *record = locate(zone);
                const auto info = record->get_info(
                    std::chrono::sys_time<std::chrono::microseconds>{
                        instant.time_since_epoch()});
                return OffsetInfo{offset_seconds(info.offset),
                                  to_instant(info.begin),
                                  to_instant(info.end)};
            }

            [[nodiscard]] LocalResolution resolve(
                CivilDateTime local, ZoneId zone) const override
            {
                const auto *record = locate(zone);
                const auto info = record->get_info(
                    std::chrono::local_time<std::chrono::microseconds>{
                        Duration{local.epoch_microseconds()}});
                const auto first_offset = offset_seconds(info.first.offset);
                if (info.result == std::chrono::local_info::unique)
                {
                    return LocalResolution{
                        LocalResolutionKind::Unique,
                        local_candidate(local, first_offset), {}};
                }
                const auto second_offset = offset_seconds(info.second.offset);
                if (info.result == std::chrono::local_info::ambiguous)
                {
                    return LocalResolution{
                        LocalResolutionKind::Ambiguous,
                        local_candidate(local, first_offset),
                        local_candidate(local, second_offset)};
                }
                const Instant transition = gap_transition(
                    local, first_offset, second_offset,
                    [record](Instant instant) {
                        return offset_seconds(record->get_info(
                            std::chrono::sys_time<std::chrono::microseconds>{
                                instant.time_since_epoch()})
                                                  .offset);
                    });
                return LocalResolution{
                    LocalResolutionKind::Nonexistent,
                    ResolvedInstant{
                        checked_subtract(transition, Duration{1}),
                        first_offset},
                    ResolvedInstant{transition, second_offset}};
            }

          private:
            [[nodiscard]] const std::chrono::time_zone *locate(
                ZoneId zone) const
            {
                return native_time_zone_cache().locate(
                    zone, [](std::string_view name) {
                        try
                        {
                            return std::chrono::get_tzdb().locate_zone(
                                std::string{name});
                        }
                        catch (const std::exception &error)
                        {
                            throw std::invalid_argument(
                                "unknown time-zone '" +
                                std::string{name} + "': " +
                                error.what());
                        }
                    });
            }

            std::string version_;
        };
#elif defined(HGRAPH_TIME_ZONE_BACKEND_DATE)
        class DateTzTimeZoneProvider final : public TimeZoneProvider
        {
          public:
            DateTzTimeZoneProvider()
                : version_(date::get_tzdb().version),
                  aliases_(load_zone_aliases())
            {
            }

            [[nodiscard]] std::string_view version() const noexcept override
            {
                return version_;
            }

            [[nodiscard]] bool contains(ZoneId zone) const noexcept override
            {
                try
                {
                    return locate(zone) != nullptr;
                }
                catch (...)
                {
                    return false;
                }
            }

            [[nodiscard]] OffsetInfo at(
                Instant instant, ZoneId zone) const override
            {
                const auto *record = locate(zone);
                const auto info = record->get_info(
                    date::sys_time<std::chrono::microseconds>{
                        instant.time_since_epoch()});
                return OffsetInfo{offset_seconds(info.offset),
                                  to_instant(info.begin),
                                  to_instant(info.end)};
            }

            [[nodiscard]] LocalResolution resolve(
                CivilDateTime local, ZoneId zone) const override
            {
                const auto *record = locate(zone);
                const auto info = record->get_info(
                    date::local_time<std::chrono::microseconds>{
                        Duration{local.epoch_microseconds()}});
                const auto first_offset = offset_seconds(info.first.offset);
                if (info.result == date::local_info::unique)
                {
                    return LocalResolution{
                        LocalResolutionKind::Unique,
                        local_candidate(local, first_offset), {}};
                }
                const auto second_offset = offset_seconds(info.second.offset);
                if (info.result == date::local_info::ambiguous)
                {
                    return LocalResolution{
                        LocalResolutionKind::Ambiguous,
                        local_candidate(local, first_offset),
                        local_candidate(local, second_offset)};
                }
                const Instant transition = gap_transition(
                    local, first_offset, second_offset,
                    [record](Instant instant) {
                        return offset_seconds(record->get_info(
                            date::sys_time<std::chrono::microseconds>{
                                instant.time_since_epoch()})
                                                  .offset);
                    });
                return LocalResolution{
                    LocalResolutionKind::Nonexistent,
                    ResolvedInstant{
                        checked_subtract(transition, Duration{1}),
                        first_offset},
                    ResolvedInstant{transition, second_offset}};
            }

          private:
            [[nodiscard]] const date::time_zone *locate(ZoneId zone) const
            {
                return native_time_zone_cache().locate(
                    zone, [this](std::string_view name) {
                        std::string candidate{name};
                        std::string direct_error;
                        constexpr std::size_t max_alias_depth = 16;
                        for (std::size_t depth = 0;
                             depth <= max_alias_depth; ++depth)
                        {
                            try
                            {
                                return date::get_tzdb().locate_zone(
                                    candidate);
                            }
                            catch (const std::exception &error)
                            {
                                if (depth == 0)
                                {
                                    direct_error = error.what();
                                }
                            }

                            const auto alias = aliases_.find(candidate);
                            if (alias == aliases_.end()) { break; }
                            candidate = alias->second;
                        }
                        throw std::invalid_argument(
                            "unknown time-zone '" + std::string{name} +
                            "': " + direct_error);
                    });
            }

            std::string version_;
            const ZoneAliasMap aliases_;
        };
#endif
    }  // namespace

    TimeZoneBackend configured_time_zone_backend() noexcept
    {
#if defined(HGRAPH_TIME_ZONE_BACKEND_STD)
        return TimeZoneBackend::Standard;
#else
        return TimeZoneBackend::DateTz;
#endif
    }

    std::shared_ptr<const TimeZoneProvider> make_time_zone_provider()
    {
#if defined(HGRAPH_TIME_ZONE_BACKEND_STD)
        return std::make_shared<StdChronoTimeZoneProvider>();
#else
        return std::make_shared<DateTzTimeZoneProvider>();
#endif
    }

    void clear_time_zone_provider_cache() noexcept
    {
        native_time_zone_cache().clear();
    }

    void set_time_zone_provider(
        GlobalStateView state,
        std::shared_ptr<const TimeZoneProvider> provider)
    {
        if (!provider)
        {
            throw std::invalid_argument(
                "time-zone provider must not be null");
        }
        (void)TypeRegistry::instance().register_scalar<TimeZoneProviderHolder>(
            "__time_zone_provider_holder__");
        state.set(TIME_ZONE_PROVIDER_STATE_KEY,
                  Value{TimeZoneProviderHolder{std::move(provider)}});
    }

    const TimeZoneProvider &time_zone_provider(GlobalStateView state)
    {
        const ValueView value = state.get(TIME_ZONE_PROVIDER_STATE_KEY);
        if (!value)
        {
            throw std::logic_error(
                "no time-zone provider is installed in GlobalState");
        }
        const auto &holder = value.checked_as<TimeZoneProviderHolder>();
        if (!holder.provider)
        {
            throw std::logic_error(
                "GlobalState contains a null time-zone provider");
        }
        return *holder.provider;
    }
}  // namespace hgraph
