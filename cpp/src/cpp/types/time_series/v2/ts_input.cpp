//
// TsInput - Read View Implementation (New V2 Architecture)
//

#include <hgraph/types/time_series/v2/ts_input.h>
#include <hgraph/types/time_series/v2/ts_output.h>
#include <hgraph/types/node.h>
#include <stdexcept>

namespace hgraph {
namespace ts {

TsInput::TsInput(node_ptr parent, const TSTypeMeta* meta)
    : _ctx(parent)
    , _meta(meta) {}

TsInput::TsInput(TimeSeriesInput* parent, const TSTypeMeta* meta)
    : _ctx(parent)
    , _meta(meta) {}

TsInput::~TsInput() {
    // Unsubscribe if active
    if (_active && _state) {
        _state->unsubscribe(this);
    }
}

nb::object TsInput::py_value() const {
    return _state ? _state->value : nb::none();
}

nb::object TsInput::py_delta_value() const {
    return _state ? _state->value : nb::none();
}

engine_time_t TsInput::last_modified_time() const {
    return _state ? _state->last_modified : MIN_DT;
}

bool TsInput::modified() const {
    if (!_state) return false;
    auto current = _ctx.current_time();
    // Modified if state was modified this tick or we were sampled this tick
    return _state->modified(current) || _sample_time == current;
}

bool TsInput::valid() const {
    return _state && _state->valid();
}

void TsInput::builder_release_cleanup() {
    un_bind_output(true);
    _active = false;
    _sample_time = MIN_DT;
}

bool TsInput::is_same_type(const TimeSeriesType* other) const {
    if (!other) return false;
    if (auto* ts = dynamic_cast<const TsInput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

TimeSeriesInput::s_ptr TsInput::parent_input() const {
    if (_ctx.is_parent_owner()) {
        auto* parent = std::get<TimeSeriesType*>(_ctx.owner);
        // Cast to TimeSeriesInput first, then use its shared_from_this
        if (auto* input_parent = dynamic_cast<TimeSeriesInput*>(parent)) {
            return input_parent->shared_from_this();
        }
    }
    return nullptr;
}

void TsInput::make_active() {
    if (!_active) {
        _active = true;
        if (_state) {
            _state->subscribe(this);
        }
    }
}

void TsInput::make_passive() {
    if (_active) {
        _active = false;
        if (_state) {
            _state->unsubscribe(this);
        }
    }
}

bool TsInput::bind_output(time_series_output_s_ptr output) {
    // Unsubscribe from old state
    if (_active && _state) {
        _state->unsubscribe(this);
    }

    // Get shared state from output - must be V2 TsOutput (no V1/V2 mixing)
    auto* ts = dynamic_cast<TsOutput*>(output.get());
    if (!ts) {
        throw std::runtime_error(
            "TsInput cannot bind to output - must be V2 TsOutput. "
            "V1/V2 mixing is not allowed. Ensure the graph is built with matching API version.");
    }

    _state = ts->shared_state();
    _bound_output = output;

    // Subscribe to new state if active
    if (_active && _state) {
        _state->subscribe(this);
    }

    // Check if we should mark as sampled on bind (during start)
    auto node = owning_node();
    if (node && (node->is_started() || node->is_starting()) && valid()) {
        _sample_time = _ctx.current_time();
        if (_active) {
            notify(_sample_time);
        }
    }

    return true;
}

void TsInput::un_bind_output(bool unbind_refs) {
    if (_active && _state) {
        _state->unsubscribe(this);
    }
    _state.reset();
    _bound_output.reset();
}

void TsInput::notify(engine_time_t modified_time) {
    // Propagate notification up the hierarchy
    if (has_parent_input()) {
        parent_input()->notify(modified_time);
    } else if (has_owning_node()) {
        owning_node()->notify(modified_time);
    }
}

} // namespace ts
} // namespace hgraph
