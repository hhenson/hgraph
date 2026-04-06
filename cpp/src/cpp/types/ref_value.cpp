#include <hgraph/types/ref.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace hgraph
{
    TimeSeriesReference atomic_default_value(std::type_identity<TimeSeriesReference>)
    {
        return TimeSeriesReference::make();
    }

    size_t atomic_hash(const TimeSeriesReference &value)
    {
        return std::hash<std::string>{}(value.to_string());
    }

    std::partial_ordering atomic_compare(const TimeSeriesReference &lhs, const TimeSeriesReference &rhs)
    {
        return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
    }

    std::string to_string(const TimeSeriesReference &value)
    {
        return value.to_string();
    }

    TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {}

    TimeSeriesReference::TimeSeriesReference(time_series_output_s_ptr output) : _kind(Kind::BOUND)
    {
        new (&_storage.bound) time_series_output_s_ptr(std::move(output));
    }

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items) : _kind(Kind::UNBOUND)
    {
        new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(items));
    }

    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other) : _kind(other._kind) { copy_from(other); }

    TimeSeriesReference::TimeSeriesReference(TimeSeriesReference &&other) noexcept : _kind(other._kind)
    {
        move_from(std::move(other));
    }

    TimeSeriesReference &TimeSeriesReference::operator=(const TimeSeriesReference &other)
    {
        if (this != &other) {
            destroy();
            _kind = other._kind;
            copy_from(other);
        }
        return *this;
    }

    TimeSeriesReference &TimeSeriesReference::operator=(TimeSeriesReference &&other) noexcept
    {
        if (this != &other) {
            destroy();
            _kind = other._kind;
            move_from(std::move(other));
        }
        return *this;
    }

    TimeSeriesReference::~TimeSeriesReference() { destroy(); }

    void TimeSeriesReference::destroy() noexcept
    {
        switch (_kind) {
            case Kind::EMPTY: break;
            case Kind::BOUND: _storage.bound.~shared_ptr(); break;
            case Kind::UNBOUND: _storage.unbound.~vector(); break;
        }
    }

    void TimeSeriesReference::copy_from(const TimeSeriesReference &other)
    {
        switch (other._kind) {
            case Kind::EMPTY: break;
            case Kind::BOUND: new (&_storage.bound) time_series_output_s_ptr(other._storage.bound); break;
            case Kind::UNBOUND: new (&_storage.unbound) std::vector<TimeSeriesReference>(other._storage.unbound); break;
        }
    }

    void TimeSeriesReference::move_from(TimeSeriesReference &&other) noexcept
    {
        switch (other._kind) {
            case Kind::EMPTY: break;
            case Kind::BOUND: new (&_storage.bound) time_series_output_s_ptr(std::move(other._storage.bound)); break;
            case Kind::UNBOUND: new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(other._storage.unbound)); break;
        }
    }

    const time_series_output_s_ptr &TimeSeriesReference::output() const
    {
        if (_kind != Kind::BOUND) { throw std::runtime_error("TimeSeriesReference::output() called on non-bound reference"); }
        return _storage.bound;
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const
    {
        if (_kind != Kind::UNBOUND) { throw std::runtime_error("TimeSeriesReference::items() called on non-unbound reference"); }
        return _storage.unbound;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t ndx) const { return items()[ndx]; }

    void TimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const
    {
        switch (_kind) {
            case Kind::EMPTY:
                try {
                    ts_input.un_bind_output(false);
                } catch (const std::exception &e) {
                    throw std::runtime_error(std::string("Error in EmptyTimeSeriesReference::bind_input: ") + e.what());
                } catch (...) {
                    throw std::runtime_error("Unknown error in EmptyTimeSeriesReference::bind_input");
                }
                break;

            case Kind::BOUND:
                {
                    bool reactivate = false;
                    if (ts_input.bound() && !ts_input.has_peer()) {
                        reactivate = ts_input.active();
                        ts_input.un_bind_output(false);
                    }
                    ts_input.bind_output(_storage.bound);
                    if (reactivate) { ts_input.make_active(); }
                    break;
                }

            case Kind::UNBOUND:
                {
                    bool reactivate = false;
                    if (ts_input.bound() && ts_input.has_peer()) {
                        reactivate = ts_input.active();
                        ts_input.un_bind_output(false);
                    }

                    for (size_t i = 0; i < _storage.unbound.size(); ++i) {
                        auto item = ts_input.get_input(i);
                        _storage.unbound[i].bind_input(*item);
                    }

                    if (reactivate) { ts_input.make_active(); }
                    break;
                }
        }
    }

    bool TimeSeriesReference::has_output() const
    {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND: return true;
            case Kind::UNBOUND: return false;
        }
        return false;
    }

    bool TimeSeriesReference::is_valid() const
    {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND: return _storage.bound && _storage.bound->valid();
            case Kind::UNBOUND:
                return std::any_of(
                    _storage.unbound.begin(), _storage.unbound.end(), [](const auto &item) { return item.is_valid(); });
        }
        return false;
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const
    {
        if (_kind != other._kind) { return false; }

        switch (_kind) {
            case Kind::EMPTY: return true;
            case Kind::BOUND: return _storage.bound == other._storage.bound;
            case Kind::UNBOUND: return _storage.unbound == other._storage.unbound;
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const
    {
        switch (_kind) {
            case Kind::EMPTY: return "REF[<UnSet>]";
            case Kind::BOUND:
                return fmt::format("REF[output@{:p}]", static_cast<const void *>(_storage.bound.get()));
            case Kind::UNBOUND:
                {
                    std::vector<std::string> string_items;
                    string_items.reserve(_storage.unbound.size());
                    for (const auto &item : _storage.unbound) { string_items.push_back(item.to_string()); }
                    return fmt::format("REF[{}]", fmt::join(string_items, ", "));
                }
        }
        return "REF[?]";
    }

    TimeSeriesReference TimeSeriesReference::make() { return TimeSeriesReference(); }

    const TimeSeriesReference &TimeSeriesReference::empty()
    {
        static const TimeSeriesReference empty_ref;
        return empty_ref;
    }

    TimeSeriesReference TimeSeriesReference::make(time_series_output_s_ptr output)
    {
        if (output == nullptr) { return make(); }
        return TimeSeriesReference(std::move(output));
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items)
    {
        if (items.empty()) { return make(); }
        return TimeSeriesReference(std::move(items));
    }
}  // namespace hgraph
