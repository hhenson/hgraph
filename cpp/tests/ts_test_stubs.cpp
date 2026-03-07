#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>

#include <stdexcept>
#include <utility>

namespace hgraph {

TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {}

TimeSeriesReference::TimeSeriesReference(ViewData bound_view)
    : _kind(Kind::BOUND), _bound_view(std::move(bound_view)) {}

TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items)
    : _kind(Kind::UNBOUND), _unbound_items(std::move(items)) {}

TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference& other) = default;
TimeSeriesReference::TimeSeriesReference(TimeSeriesReference&& other) noexcept = default;
TimeSeriesReference& TimeSeriesReference::operator=(const TimeSeriesReference& other) = default;
TimeSeriesReference& TimeSeriesReference::operator=(TimeSeriesReference&& other) noexcept = default;
TimeSeriesReference::~TimeSeriesReference() = default;

bool TimeSeriesReference::has_output() const {
    return _kind == Kind::BOUND && _bound_view.has_value();
}

bool TimeSeriesReference::is_valid() const {
    return has_output();
}

const std::vector<TimeSeriesReference>& TimeSeriesReference::items() const {
    if (_kind != Kind::UNBOUND) {
        throw std::runtime_error("TimeSeriesReference::items called on non-unbound reference");
    }
    return _unbound_items;
}

const TimeSeriesReference& TimeSeriesReference::operator[](size_t ndx) const {
    return items().at(ndx);
}

const ViewData* TimeSeriesReference::bound_view() const noexcept {
    return _bound_view.has_value() ? &*_bound_view : nullptr;
}

void TimeSeriesReference::bind_input(TSInputView&) const {}

bool TimeSeriesReference::operator==(const TimeSeriesReference& other) const {
    if (_kind != other._kind) {
        return false;
    }

    switch (_kind) {
        case Kind::EMPTY:
            return true;
        case Kind::BOUND:
            if (_bound_view.has_value() != other._bound_view.has_value()) {
                return false;
            }
            if (!_bound_view.has_value()) {
                return true;
            }
            return _bound_view->meta == other._bound_view->meta &&
                   _bound_view->path.indices == other._bound_view->path.indices;
        case Kind::UNBOUND:
            return _unbound_items == other._unbound_items;
    }

    return false;
}

std::string TimeSeriesReference::to_string() const {
    switch (_kind) {
        case Kind::EMPTY:
            return "REF[<UnSet>]";
        case Kind::BOUND:
            return "REF[Bound]";
        case Kind::UNBOUND:
            return "REF[Unbound]";
    }
    return "REF[?]";
}

TimeSeriesReference TimeSeriesReference::make() {
    return TimeSeriesReference();
}

TimeSeriesReference TimeSeriesReference::make(const ViewData& bound_view) {
    return TimeSeriesReference(bound_view);
}

TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
    return TimeSeriesReference(std::move(items));
}

void TimeSeriesReference::destroy() noexcept {
    _bound_view.reset();
    _unbound_items.clear();
}

void register_ts_link_observer(LinkTarget&) {}
void unregister_ts_link_observer(LinkTarget&) {}
void register_ts_ref_link_observer(REFLink&) {}
void unregister_ts_ref_link_observer(REFLink&) {}
void register_ts_active_link_observer(LinkTarget&) {}
void unregister_ts_active_link_observer(LinkTarget&) {}
void register_ts_active_ref_link_observer(REFLink&) {}
void unregister_ts_active_ref_link_observer(REFLink&) {}
void reset_ts_link_observers() {}

}  // namespace hgraph
