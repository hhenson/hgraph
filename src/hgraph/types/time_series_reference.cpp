#include <hgraph/types/time_series_reference.h>

#include <hgraph/types/time_series/ts_input.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t kHashMix = 0x9e3779b97f4a7c15ULL;

        std::size_t hash_combine(std::size_t seed, std::size_t value) noexcept
        {
            return seed ^ (value + kHashMix + (seed << 6U) + (seed >> 2U));
        }

        std::size_t hash_output_handle(const TSOutputHandle &handle) noexcept
        {
            std::size_t seed = std::hash<const void *>{}(static_cast<const void *>(handle.output()));
            seed = hash_combine(seed, std::hash<const TypeRecord *>{}(handle.type_ref().record()));
            seed = hash_combine(seed, std::hash<const void *>{}(handle.data_view().data()));
            return seed;
        }

    }  // namespace

    TimeSeriesReference::TimeSeriesReference() noexcept = default;

    TimeSeriesReference::TimeSeriesReference(const TSValueTypeMetaData *target_schema) noexcept
        : kind_{Kind::EMPTY}, target_schema_{target_schema}
    {
    }

    TimeSeriesReference::TimeSeriesReference(TSOutputHandle target)
    {
        if (!target.bound())
        {
            throw std::invalid_argument("TimeSeriesReference peered construction requires a bound output handle");
        }
        target_schema_ = target.schema();
        if (target_schema_ == nullptr)
        {
            throw std::invalid_argument("TimeSeriesReference peered construction requires a typed output handle");
        }
        std::construct_at(&storage_.target, std::move(target));
        kind_ = Kind::PEERED;
    }

    TimeSeriesReference::TimeSeriesReference(const TSOutputView &target)
        : TimeSeriesReference{target.handle()}
    {
    }

    TimeSeriesReference::TimeSeriesReference(const TSInputView &source)
        : TimeSeriesReference{source.reference()}
    {
    }

    TimeSeriesReference::TimeSeriesReference(const TSValueTypeMetaData    *target_schema,
                                              std::vector<TimeSeriesReference> items)
        : target_schema_{target_schema}
    {
        std::construct_at(&storage_.items, std::move(items));
        kind_ = Kind::NON_PEERED;
    }

    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other)
    {
        copy_from(other);
    }

    TimeSeriesReference &TimeSeriesReference::operator=(const TimeSeriesReference &other)
    {
        if (this != &other)
        {
            TimeSeriesReference copy{other};
            *this = std::move(copy);
        }
        return *this;
    }

    TimeSeriesReference::TimeSeriesReference(TimeSeriesReference &&other) noexcept
    {
        move_from(std::move(other));
    }

    TimeSeriesReference &TimeSeriesReference::operator=(TimeSeriesReference &&other) noexcept
    {
        if (this != &other)
        {
            destroy();
            move_from(std::move(other));
        }
        return *this;
    }

    TimeSeriesReference::~TimeSeriesReference() noexcept
    {
        destroy();
    }

    void TimeSeriesReference::destroy() noexcept
    {
        switch (kind_)
        {
            case Kind::PEERED:
                std::destroy_at(&storage_.target);
                break;
            case Kind::NON_PEERED:
                std::destroy_at(&storage_.items);
                break;
            case Kind::EMPTY:
                break;
        }
        kind_ = Kind::EMPTY;
        target_schema_ = nullptr;
    }

    void TimeSeriesReference::copy_from(const TimeSeriesReference &other)
    {
        target_schema_ = other.target_schema_;
        switch (other.kind_)
        {
            case Kind::PEERED:
                std::construct_at(&storage_.target, other.storage_.target);
                kind_ = Kind::PEERED;
                break;
            case Kind::NON_PEERED:
                std::construct_at(&storage_.items, other.storage_.items);
                kind_ = Kind::NON_PEERED;
                break;
            case Kind::EMPTY:
                kind_ = Kind::EMPTY;
                break;
        }
    }

    void TimeSeriesReference::move_from(TimeSeriesReference &&other) noexcept
    {
        target_schema_ = other.target_schema_;
        switch (other.kind_)
        {
            case Kind::PEERED:
                std::construct_at(&storage_.target, std::move(other.storage_.target));
                kind_ = Kind::PEERED;
                break;
            case Kind::NON_PEERED:
                std::construct_at(&storage_.items, std::move(other.storage_.items));
                kind_ = Kind::NON_PEERED;
                break;
            case Kind::EMPTY:
                kind_ = Kind::EMPTY;
                break;
        }
        other.destroy();
    }

    TimeSeriesReference TimeSeriesReference::empty(const TSValueTypeMetaData *target_schema) noexcept
    {
        return TimeSeriesReference{target_schema};
    }

    TimeSeriesReference TimeSeriesReference::peered(TSOutputHandle target)
    {
        return TimeSeriesReference{target};
    }

    TimeSeriesReference TimeSeriesReference::peered(const TSOutputView &target)
    {
        return TimeSeriesReference{target};
    }

    TimeSeriesReference TimeSeriesReference::peered_as(const TSValueTypeMetaData *target_schema,
                                                       TSOutputHandle target)
    {
        TimeSeriesReference result{std::move(target)};
        result.target_schema_ = target_schema;
        return result;
    }

    TimeSeriesReference TimeSeriesReference::non_peered(const TSValueTypeMetaData       *target_schema,
                                                        std::vector<TimeSeriesReference> items)
    {
        return TimeSeriesReference{target_schema, std::move(items)};
    }

    bool TimeSeriesReference::has_output() const noexcept
    {
        return kind_ == Kind::PEERED && storage_.target.bound();
    }

    bool TimeSeriesReference::is_valid(DateTime evaluation_time) const
    {
        switch (kind_)
        {
            case Kind::EMPTY: return false;
            case Kind::PEERED: return storage_.target.view(evaluation_time).valid();
            case Kind::NON_PEERED:
                return std::ranges::any_of(storage_.items,
                                           [](const TimeSeriesReference &item) { return !item.is_empty(); });
        }
        return false;
    }

    const TSOutputHandle &TimeSeriesReference::target_output() const
    {
        if (!has_output())
        {
            throw std::logic_error("TimeSeriesReference::target_output() requires a PEERED reference with output");
        }
        return storage_.target;
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const
    {
        if (kind_ != Kind::NON_PEERED)
        {
            throw std::logic_error("TimeSeriesReference::items() requires a NON_PEERED reference");
        }
        return storage_.items;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t index) const
    {
        if (kind_ != Kind::NON_PEERED)
        {
            throw std::logic_error("TimeSeriesReference::operator[] requires a NON_PEERED reference");
        }
        if (index >= storage_.items.size())
        {
            throw std::out_of_range("TimeSeriesReference::operator[] index out of range");
        }
        return storage_.items[index];
    }

    TimeSeriesReference TimeSeriesReference::with_target_schema_unchecked(
        const TSValueTypeMetaData *target_schema) const
    {
        TimeSeriesReference result{*this};
        result.target_schema_ = target_schema;
        return result;
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const noexcept
    {
        if (kind_ != other.kind_) { return false; }
        if (target_schema_ != other.target_schema_) { return false; }
        if (kind_ == Kind::PEERED) { return storage_.target.same_as(other.storage_.target); }
        if (kind_ == Kind::NON_PEERED) { return storage_.items == other.storage_.items; }
        return true;
    }

    std::size_t TimeSeriesReference::hash() const noexcept
    {
        std::size_t seed = static_cast<std::size_t>(kind_);
        seed = hash_combine(seed, std::hash<const void *>{}(static_cast<const void *>(target_schema_)));
        if (kind_ == Kind::PEERED) { seed = hash_combine(seed, hash_output_handle(storage_.target)); }
        if (kind_ == Kind::NON_PEERED)
        {
            for (const auto &item : storage_.items) { seed = hash_combine(seed, item.hash()); }
        }
        return seed;
    }

    std::string TimeSeriesReference::to_string() const
    {
        switch (kind_)
        {
            case Kind::EMPTY:  return "TimeSeriesReference{empty}";
            case Kind::PEERED: return "TimeSeriesReference{peered}";
            case Kind::NON_PEERED:
            {
                std::ostringstream out;
                out << "TimeSeriesReference{[";
                for (size_t i = 0; i < storage_.items.size(); ++i)
                {
                    if (i != 0) { out << ", "; }
                    out << storage_.items[i].to_string();
                }
                out << "]}";
                return out.str();
            }
        }
        return "TimeSeriesReference{?}";
    }

    const TimeSeriesReference &TimeSeriesReference::empty_reference() noexcept
    {
        static const TimeSeriesReference empty{};
        return empty;
    }
}  // namespace hgraph
