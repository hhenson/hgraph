#include <hgraph/types/ref.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph {

TimeSeriesReference::TimeSeriesReference() noexcept : _kind(Kind::EMPTY) {}

TimeSeriesReference::TimeSeriesReference(time_series_output_s_ptr output) : _kind(Kind::BOUND) {
    new (&_storage.bound) time_series_output_s_ptr(std::move(output));
}

TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items) : _kind(Kind::UNBOUND) {
    new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(items));
}

TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference& other) : _kind(other._kind) {
    copy_from(other);
}

TimeSeriesReference::TimeSeriesReference(TimeSeriesReference&& other) noexcept : _kind(other._kind) {
    move_from(std::move(other));
}

TimeSeriesReference& TimeSeriesReference::operator=(const TimeSeriesReference& other) {
    if (this != &other) {
        destroy();
        _kind = other._kind;
        copy_from(other);
    }
    return *this;
}

TimeSeriesReference& TimeSeriesReference::operator=(TimeSeriesReference&& other) noexcept {
    if (this != &other) {
        destroy();
        _kind = other._kind;
        move_from(std::move(other));
    }
    return *this;
}

TimeSeriesReference::~TimeSeriesReference() {
    destroy();
}

bool TimeSeriesReference::has_output() const {
    return _kind == Kind::BOUND && static_cast<bool>(_storage.bound);
}

bool TimeSeriesReference::is_valid() const {
    if (_kind == Kind::BOUND) {
        return static_cast<bool>(_storage.bound);
    }
    if (_kind == Kind::UNBOUND) {
        return !_storage.unbound.empty();
    }
    return false;
}

const time_series_output_s_ptr& TimeSeriesReference::output() const {
    if (_kind != Kind::BOUND) {
        throw std::runtime_error("TimeSeriesReference::output() called on a non-bound reference");
    }
    return _storage.bound;
}

const std::vector<TimeSeriesReference>& TimeSeriesReference::items() const {
    if (_kind != Kind::UNBOUND) {
        throw std::runtime_error("TimeSeriesReference::items() called on a non-unbound reference");
    }
    return _storage.unbound;
}

const TimeSeriesReference& TimeSeriesReference::operator[](size_t ndx) const {
    return items().at(ndx);
}

void TimeSeriesReference::bind_input(TimeSeriesInput&) const {}

bool TimeSeriesReference::operator==(const TimeSeriesReference& other) const {
    if (_kind != other._kind) {
        return false;
    }

    switch (_kind) {
        case Kind::EMPTY:
            return true;
        case Kind::BOUND:
            return _storage.bound == other._storage.bound;
        case Kind::UNBOUND:
            return _storage.unbound == other._storage.unbound;
    }

    return false;
}

std::string TimeSeriesReference::to_string() const {
    switch (_kind) {
        case Kind::EMPTY:
            return "REF[<empty>]";
        case Kind::BOUND:
            return "REF[<bound>]";
        case Kind::UNBOUND:
            return "REF[<unbound>]";
    }

    return "REF[<?>]";
}

TimeSeriesReference TimeSeriesReference::make() {
    return TimeSeriesReference();
}

TimeSeriesReference TimeSeriesReference::make(time_series_output_s_ptr output) {
    return output ? TimeSeriesReference(std::move(output)) : TimeSeriesReference();
}

TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items) {
    return items.empty() ? TimeSeriesReference() : TimeSeriesReference(std::move(items));
}

TimeSeriesReference TimeSeriesReference::make(const std::vector<TimeSeriesReferenceInput*>& items) {
    std::vector<TimeSeriesReference> refs;
    refs.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        refs.push_back(TimeSeriesReference::make());
    }
    return make(std::move(refs));
}

TimeSeriesReference TimeSeriesReference::make(const std::vector<std::shared_ptr<TimeSeriesReferenceInput>>& items) {
    std::vector<TimeSeriesReference> refs;
    refs.reserve(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
        refs.push_back(TimeSeriesReference::make());
    }
    return make(std::move(refs));
}

const TimeSeriesReference& TimeSeriesReference::empty() {
    static const TimeSeriesReference empty_ref;
    return empty_ref;
}

void TimeSeriesReference::destroy() noexcept {
    switch (_kind) {
        case Kind::EMPTY:
            break;
        case Kind::BOUND:
            _storage.bound.~shared_ptr();
            break;
        case Kind::UNBOUND:
            _storage.unbound.~vector();
            break;
    }
}

void TimeSeriesReference::copy_from(const TimeSeriesReference& other) {
    switch (other._kind) {
        case Kind::EMPTY:
            break;
        case Kind::BOUND:
            new (&_storage.bound) time_series_output_s_ptr(other._storage.bound);
            break;
        case Kind::UNBOUND:
            new (&_storage.unbound) std::vector<TimeSeriesReference>(other._storage.unbound);
            break;
    }
}

void TimeSeriesReference::move_from(TimeSeriesReference&& other) noexcept {
    switch (other._kind) {
        case Kind::EMPTY:
            break;
        case Kind::BOUND:
            new (&_storage.bound) time_series_output_s_ptr(std::move(other._storage.bound));
            break;
        case Kind::UNBOUND:
            new (&_storage.unbound) std::vector<TimeSeriesReference>(std::move(other._storage.unbound));
            break;
    }
}

} // namespace hgraph
