#include <hgraph/types/metadata/type_names.h>

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/counted_mutex.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

#include <charconv>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::string_view trim(std::string_view text) noexcept
        {
            while (!text.empty() && text.front() == ' ') { text.remove_prefix(1); }
            while (!text.empty() && text.back() == ' ') { text.remove_suffix(1); }
            return text;
        }

        [[noreturn]] void unresolved(std::string_view what, std::string_view name)
        {
            throw std::invalid_argument(fmt::format("type name: '{}' is not a {} the registry knows", name, what));
        }

        /** ``Family<open>args<close>`` split into its family and its args. */
        [[nodiscard]] bool split_family(std::string_view text, char open, char close, std::string_view &family,
                                        std::string_view &args) noexcept
        {
            const auto at = text.find(open);
            if (at == std::string_view::npos || text.empty() || text.back() != close) { return false; }
            family = text.substr(0, at);
            args   = text.substr(at + 1, text.size() - at - 2);
            return true;
        }

        /** Split at the commas outside any bracket or brace. */
        [[nodiscard]] std::vector<std::string_view> split_top(std::string_view args)
        {
            std::vector<std::string_view> parts;
            if (trim(args).empty()) { return parts; }  // Tuple[] and Bundle{}
            int                           depth = 0;
            std::size_t                   start = 0;
            for (std::size_t index = 0; index < args.size(); ++index)
            {
                const char c = args[index];
                if (c == '[' || c == '{') { ++depth; }
                else if (c == ']' || c == '}') { --depth; }
                else if (c == ',' && depth == 0)
                {
                    parts.push_back(trim(args.substr(start, index - start)));
                    start = index + 1;
                }
            }
            parts.push_back(trim(args.substr(start)));
            return parts;
        }

        /** ``name:type`` split at the first top-level colon. */
        [[nodiscard]] std::pair<std::string_view, std::string_view> split_field(std::string_view field,
                                                                                std::string_view whole)
        {
            int depth = 0;
            for (std::size_t index = 0; index < field.size(); ++index)
            {
                const char c = field[index];
                if (c == '[' || c == '{') { ++depth; }
                else if (c == ']' || c == '}') { --depth; }
                else if (c == ':' && depth == 0 && (index + 1 >= field.size() || field[index + 1] != ':') &&
                         (index == 0 || field[index - 1] != ':'))
                {
                    return {trim(field.substr(0, index)), trim(field.substr(index + 1))};
                }
            }
            unresolved("field list", whole);
        }

        [[nodiscard]] std::uint64_t parse_count(std::string_view text, std::string_view whole)
        {
            text = trim(text);
            std::uint64_t value{};
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (error != std::errc{} || end != text.data() + text.size()) { unresolved("size", whole); }
            return value;
        }

        /** ``key=count`` (the duration window's labelled arguments). */
        [[nodiscard]] std::int64_t parse_labelled(std::string_view text, std::string_view key, std::string_view whole)
        {
            text = trim(text);
            if (!text.starts_with(key) || text.size() <= key.size() || text[key.size()] != '=')
            {
                unresolved("time-series type", whole);
            }
            return static_cast<std::int64_t>(parse_count(text.substr(key.size() + 1), whole));
        }
    }  // namespace

    const ValueTypeMetaData *parse_value_type_name(std::string_view name)
    {
        auto &registry = TypeRegistry::instance();
        name           = trim(name);
        // Every place a named schema lives: the alias table, then the nominal
        // caches that do not publish an alias, then the Any singleton.
        if (const auto *meta = registry.value_type(name); meta != nullptr) { return meta; }
        if (const auto *meta = registry.named_bundle(name); meta != nullptr) { return meta; }
        if (const auto *meta = registry.named_enum(name); meta != nullptr) { return meta; }
        if (const auto *meta = registry.named_opaque_python(name); meta != nullptr) { return meta; }
        if (const auto *any = registry.any(); name == any->name()) { return any; }

        std::string_view family;
        std::string_view args;
        if (split_family(name, '{', '}', family, args) && family == "Bundle")
        {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            for (const auto field : split_top(args))
            {
                const auto [field_name, field_type] = split_field(field, name);
                fields.emplace_back(std::string{field_name}, parse_value_type_name(field_type));
            }
            return registry.un_named_bundle(fields);
        }
        if (!split_family(name, '[', ']', family, args)) { unresolved("value type", name); }
        const auto parts = split_top(args);
        const auto one   = [&]() {
            if (parts.size() != 1) { unresolved("value type", name); }
            return parse_value_type_name(parts[0]);
        };
        const auto two = [&]() {
            if (parts.size() != 2) { unresolved("value type", name); }
            return std::pair{parse_value_type_name(parts[0]), parse_value_type_name(parts[1])};
        };
        const auto sized = [&](auto build) {
            if (parts.empty() || parts.size() > 2) { unresolved("value type", name); }
            const auto *element = parse_value_type_name(parts[0]);
            return build(element, parts.size() == 2 ? static_cast<std::size_t>(parse_count(parts[1], name))
                                                    : std::size_t{0},
                         parts.size() == 2);
        };

        if (family == "Tuple")
        {
            std::vector<const ValueTypeMetaData *> elements;
            for (const auto part : parts) { elements.push_back(parse_value_type_name(part)); }
            return registry.tuple(elements);
        }
        if (family == "VariadicTuple") { return registry.list(one(), 0, true); }
        if (family == "NullableTuple") { return registry.nullable_tuple(one()); }
        if (family == "List")
        {
            // A shown size is a fixed extent (``List[T,0]`` is the fixed empty list).
            return sized([&](const ValueTypeMetaData *element, std::size_t size, bool shown) {
                return shown ? registry.fixed_list(element, size) : registry.list(element);
            });
        }
        if (family == "MutableList") { return registry.mutable_list(one()); }
        if (family == "Set") { return registry.set(one()); }
        if (family == "MutableSet") { return registry.mutable_set(one()); }
        if (family == "Map")
        {
            const auto [key, value] = two();
            return registry.map(key, value);
        }
        if (family == "MutableMap")
        {
            const auto [key, value] = two();
            return registry.mutable_map(key, value);
        }
        if (family == "CyclicBuffer")
        {
            return sized([&](const ValueTypeMetaData *element, std::size_t size, bool) {
                return registry.cyclic_buffer(element, size);
            });
        }
        if (family == "Queue")
        {
            return sized([&](const ValueTypeMetaData *element, std::size_t size, bool) {
                return registry.queue(element, size);
            });
        }
        if (family == "Array")
        {
            if (parts.size() != 2) { unresolved("value type", name); }
            const auto *element = parse_value_type_name(parts[0]);
            return registry.array(element, parts[1] == "*" ? std::size_t{0}
                                                           : static_cast<std::size_t>(parse_count(parts[1], name)));
        }
        if (family == "Owned") { return registry.owned(one()); }
        if (family == "Shared") { return registry.shared(one()); }
        if (const auto *base = registry.value_type(family); base != nullptr && registry.is_frame(base))
        {
            if (parts.size() == 1) { return registry.frame(parse_value_type_name(parts[0])); }
            const auto [columns, metadata] = two();
            return registry.frame(columns, metadata);
        }
        if (const auto *base = registry.value_type(family); base != nullptr && family == "series")
        {
            return registry.series(one());
        }
        unresolved("value type", name);
    }

    const TSValueTypeMetaData *parse_ts_type_name(std::string_view name)
    {
        auto &registry = TypeRegistry::instance();
        name           = trim(name);
        if (const auto *meta = registry.time_series_type(name); meta != nullptr) { return meta; }

        std::string_view family;
        std::string_view args;
        if (split_family(name, '{', '}', family, args) && family == "TSB")
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            for (const auto field : split_top(args))
            {
                const auto [field_name, field_type] = split_field(field, name);
                fields.emplace_back(std::string{field_name}, parse_ts_type_name(field_type));
            }
            return registry.un_named_tsb(fields);
        }
        if (!split_family(name, '[', ']', family, args)) { unresolved("time-series type", name); }
        const auto parts = split_top(args);

        if (family == "TS" && parts.size() == 1) { return registry.ts(parse_value_type_name(parts[0])); }
        if (family == "TSS" && parts.size() == 1) { return registry.tss(parse_value_type_name(parts[0])); }
        if (family == "TSD" && parts.size() == 2)
        {
            return registry.tsd(parse_value_type_name(parts[0]), parse_ts_type_name(parts[1]));
        }
        if (family == "TSL" && (parts.size() == 1 || parts.size() == 2))
        {
            // The registry leaves the extent out only for the unbounded list;
            // ``TSL[T,0]`` is the fixed empty one.
            return registry.tsl(parse_ts_type_name(parts[0]),
                                parts.size() == 2 ? static_cast<std::size_t>(parse_count(parts[1], name))
                                                  : unbounded_tsl_size);
        }
        if (family == "TSW" && parts.size() == 3)
        {
            const auto *value = parse_value_type_name(parts[0]);
            if (parts[1].starts_with("duration="))
            {
                return registry.tsw_duration(value, TimeDelta{parse_labelled(parts[1], "duration", name)},
                                             TimeDelta{parse_labelled(parts[2], "min", name)});
            }
            return registry.tsw(value, static_cast<std::size_t>(parse_count(parts[1], name)),
                                static_cast<std::size_t>(parse_count(parts[2], name)));
        }
        if (family == "REF" && parts.size() == 1) { return registry.ref(parse_ts_type_name(parts[0])); }
        unresolved("time-series type", name);
    }

    std::string serialise_type_value(const TypeCarrier &type)
    {
        switch (type.kind())
        {
            case ResolutionKind::TimeSeries:
                if (type.ts() == nullptr) { throw std::invalid_argument("type value: a null time-series type"); }
                return "ts:" + std::string{type.ts()->name()};
            case ResolutionKind::Scalar:
                if (type.scalar() == nullptr) { throw std::invalid_argument("type value: a null scalar type"); }
                return "scalar:" + std::string{type.scalar()->name()};
            default:
                // The unbounded sentinel is written as Python writes it: -1.
                if (*type.size() == unbounded_tsl_size) { return "size:-1"; }
                return "size:" + std::to_string(*type.size());
        }
    }

    TypeCarrier parse_type_value(std::string_view serialised)
    {
        // Cold path, cached by form. The cache compares the registry's reset
        // generation and drops itself when it moves: the schemas it points at
        // are registry-owned.
        struct Cache
        {
            TypeSystemMutex                                       mutex;
            std::uint64_t                                         generation{0};
            ankerl::unordered_dense::map<std::string, TypeCarrier> entries;
        };
        static Cache cache;
        auto        &registry   = TypeRegistry::instance();
        const auto   generation = registry.reset_generation();
        {
            const std::lock_guard lock(cache.mutex);
            if (cache.generation != generation)
            {
                cache.entries.clear();
                cache.generation = generation;
            }
            if (const auto found = cache.entries.find(std::string{serialised}); found != cache.entries.end())
            {
                return found->second;
            }
        }

        TypeCarrier type;
        if (serialised.starts_with("ts:")) { type = TypeCarrier::of_ts(parse_ts_type_name(serialised.substr(3))); }
        else if (serialised.starts_with("scalar:"))
        {
            type = TypeCarrier::of_scalar(parse_value_type_name(serialised.substr(7)));
        }
        else if (serialised.starts_with("size:"))
        {
            const auto count = serialised.substr(5);
            type = TypeCarrier::of_size(count == "-1" ? unbounded_tsl_size
                                                      : static_cast<std::size_t>(parse_count(count, serialised)));
        }
        else
        {
            throw std::invalid_argument(
                fmt::format("type value: '{}' is not a serialised type (ts:, scalar: or size:)", serialised));
        }

        const std::lock_guard lock(cache.mutex);
        if (cache.generation == generation) { cache.entries.emplace(std::string{serialised}, type); }
        return type;
    }
}  // namespace hgraph
