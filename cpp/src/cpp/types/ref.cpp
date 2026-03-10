#include <hgraph/types/constants.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>

#include <algorithm>

namespace hgraph
{
    namespace {
        bool view_data_equals(const ViewData& lhs, const ViewData& rhs) {
            return lhs.path_indices() == rhs.path_indices() &&
                   lhs.value_data == rhs.value_data &&
                   lhs.time_data == rhs.time_data &&
                   lhs.observer_data == rhs.observer_data &&
                   lhs.delta_data == rhs.delta_data &&
                   lhs.link_data == rhs.link_data &&
                   lhs.projection == rhs.projection &&
                   lhs.ops == rhs.ops &&
                   lhs.meta == rhs.meta;
        }

        void unbind_view_recursive(const TSView& root_view) {
            TSView view = root_view;
            if (!view) {
                return;
            }

            view.unbind();
            const size_t count = view.child_count();
            for (size_t i = 0; i < count; ++i) {
                unbind_view_recursive(view.child_at(i));
            }
        }
    }

    // ============================================================
    // TimeSeriesReference Implementation
    // ============================================================

    // Private constructors
    TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {}

    TimeSeriesReference::TimeSeriesReference(ViewData bound_view) : _kind(Kind::BOUND), _bound_view(std::move(bound_view)) {}

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items)
        : _kind(Kind::UNBOUND), _unbound_items(std::move(items)) {}

    // Copy constructor
    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other)
        : _kind(other._kind), _bound_view(other._bound_view), _unbound_items(other._unbound_items) {}

    // Move constructor
    TimeSeriesReference::TimeSeriesReference(TimeSeriesReference &&other) noexcept
        : _kind(other._kind), _bound_view(std::move(other._bound_view)), _unbound_items(std::move(other._unbound_items)) {
        other._kind = Kind::EMPTY;
    }

    // Copy assignment
    TimeSeriesReference &TimeSeriesReference::operator=(const TimeSeriesReference &other) {
        if (this != &other) {
            _kind = other._kind;
            _bound_view = other._bound_view;
            _unbound_items = other._unbound_items;
        }
        return *this;
    }

    // Move assignment
    TimeSeriesReference &TimeSeriesReference::operator=(TimeSeriesReference &&other) noexcept {
        if (this != &other) {
            _kind = other._kind;
            _bound_view = std::move(other._bound_view);
            _unbound_items = std::move(other._unbound_items);
            other._kind = Kind::EMPTY;
        }
        return *this;
    }

    // Destructor
    TimeSeriesReference::~TimeSeriesReference() { destroy(); }

    void TimeSeriesReference::destroy() noexcept {
        _bound_view.reset();
        _unbound_items.clear();
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const {
        if (_kind != Kind::UNBOUND) { throw std::runtime_error("TimeSeriesReference::items() called on non-unbound reference"); }
        return _unbound_items;
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t ndx) const { return items()[ndx]; }

    const ViewData* TimeSeriesReference::bound_view() const noexcept {
        return _bound_view.has_value() ? &*_bound_view : nullptr;
    }

    void TimeSeriesReference::bind_input(TSInputView &ts_input) const {
        switch (_kind) {
            case Kind::EMPTY:
                unbind_view_recursive(ts_input.as_ts_view());
                return;

            case Kind::BOUND:
                {
                    if (!_bound_view.has_value()) {
                        throw std::runtime_error("Cannot bind TSInputView: no bound view data");
                    }

                    const bool reactivate = ts_input.active();
                    if (ts_input.is_bound()) {
                        if (reactivate) {
                            ts_input.make_passive();
                        }
                        ts_input.unbind();
                    }

                    TSView target(*_bound_view, ts_input.as_ts_view().view_data().engine_time_ptr);
                    ts_input.as_ts_view().bind(target);

                    if (reactivate) {
                        ts_input.make_active();
                    }
                    return;
                }

            case Kind::UNBOUND:
                {
                    const bool reactivate = ts_input.active();
                    if (ts_input.is_bound()) {
                        if (reactivate) {
                            ts_input.make_passive();
                        }
                        ts_input.unbind();
                    }

                    const TSKind input_kind = ts_input.as_ts_view().kind();
                    const bool is_indexed = input_kind == TSKind::TSL || input_kind == TSKind::TSB;
                    if (is_indexed) {
                        TSIndexedInputView indexed_input{ts_input};
                        for (size_t i = 0; i < _unbound_items.size(); ++i) {
                            TSInputView item = indexed_input.at(i);
                            _unbound_items[i].bind_input(item);
                        }
                    } else if (input_kind == TSKind::REF) {
                        for (size_t i = 0; i < _unbound_items.size(); ++i) {
                            TSInputView item(nullptr, ts_input.as_ts_view().child_at(i));
                            _unbound_items[i].bind_input(item);
                        }
                    } else {
                        for (size_t i = 0; i < _unbound_items.size(); ++i) {
                            TSInputView item{};
                            _unbound_items[i].bind_input(item);
                        }
                    }

                    if (reactivate) {
                        ts_input.make_active();
                    }
                    return;
                }
        }
    }

    bool TimeSeriesReference::has_output() const {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND: return _bound_view.has_value();
            case Kind::UNBOUND: return false;
        }
        return false;
    }

    bool TimeSeriesReference::is_valid() const {
        switch (_kind) {
            case Kind::EMPTY: return false;
            case Kind::BOUND:
                if (_bound_view.has_value()) {
                    const ViewData& vd = *_bound_view;
                    return vd.ops != nullptr ? vd.ops->valid(vd) : false;
                }
                return false;
            case Kind::UNBOUND:
                return std::any_of(_unbound_items.begin(), _unbound_items.end(),
                                   [](const auto &item) { return item.is_valid(); });
        }
        return false;
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const {
        if (_kind != other._kind) return false;

        switch (_kind) {
            case Kind::EMPTY: return true;
            case Kind::BOUND:
                return _bound_view.has_value() && other._bound_view.has_value() &&
                       view_data_equals(*_bound_view, *other._bound_view);
            case Kind::UNBOUND: return _unbound_items == other._unbound_items;
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const {
        switch (_kind) {
            case Kind::EMPTY: return "REF[<UnSet>]";
            case Kind::BOUND:
                if (_bound_view.has_value()) {
                    return fmt::format("REF[TSView:{}]", _bound_view->to_short_path().to_string());
                }
                return "REF[<UnSet>]";
            case Kind::UNBOUND:
                {
                    std::vector<std::string> string_items;
                    string_items.reserve(_unbound_items.size());
                    for (const auto &item : _unbound_items) { string_items.push_back(item.to_string()); }
                    return fmt::format("REF[{}]", fmt::join(string_items, ", "));
                }
        }
        return "REF[?]";
    }

    // Factory methods
    TimeSeriesReference TimeSeriesReference::make() { return TimeSeriesReference(); }

    TimeSeriesReference TimeSeriesReference::make(const ViewData& bound_view) {
        if (bound_view.meta == nullptr || bound_view.ops == nullptr) {
            return make();
        }
        return TimeSeriesReference(bound_view);
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
        if (items.empty()) { return make(); }
        return TimeSeriesReference(std::move(items));
    }

}  // namespace hgraph
