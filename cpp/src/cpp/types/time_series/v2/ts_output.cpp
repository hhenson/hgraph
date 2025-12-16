//
// TsOutput - Write View Implementation (New V2 Architecture)
//

#include <hgraph/types/time_series/v2/ts_output.h>
#include <hgraph/types/node.h>

namespace hgraph {
namespace ts {

TsOutput::TsOutput(node_ptr parent, const TSTypeMeta* meta)
    : _state(std::make_shared<TSValue>(meta))
    , _ctx(parent)
    , _meta(meta) {}

TsOutput::TsOutput(TimeSeriesOutput* parent, const TSTypeMeta* meta)
    : _state(std::make_shared<TSValue>(meta))
    , _ctx(parent)
    , _meta(meta) {}

void TsOutput::builder_release_cleanup() {
    _state->clear();
    _state->subscribers.clear();
}

bool TsOutput::is_same_type(const TimeSeriesType* other) const {
    if (!other) return false;
    // For now, check if both are TsOutput with same meta
    if (auto* ts = dynamic_cast<const TsOutput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

TimeSeriesOutput::s_ptr TsOutput::parent_output() const {
    if (_ctx.is_parent_owner()) {
        auto* parent = std::get<TimeSeriesType*>(_ctx.owner);
        // Cast to TimeSeriesOutput first, then use its shared_from_this
        if (auto* output_parent = dynamic_cast<TimeSeriesOutput*>(parent)) {
            return output_parent->shared_from_this();
        }
    }
    return nullptr;
}

TimeSeriesOutput::s_ptr TsOutput::parent_output() {
    return const_cast<const TsOutput*>(this)->parent_output();
}

void TsOutput::apply_result(const nb::object& value) {
    if (!value.is_none()) {
        py_set_value(value);
    }
}

void TsOutput::py_set_value(const nb::object& value) {
    if (value.is_none()) {
        invalidate();
    } else {
        _state->set_value(value, _ctx.current_time());
    }
}

void TsOutput::copy_from_output(const TimeSeriesOutput& output) {
    py_set_value(output.py_value());
}

void TsOutput::copy_from_input(const TimeSeriesInput& input) {
    py_set_value(input.py_value());
}

void TsOutput::clear() {
    _state->clear();
}

void TsOutput::invalidate() {
    _state->invalidate(_ctx.current_time());
}

void TsOutput::mark_invalid() {
    _state->value = nb::none();
}

void TsOutput::mark_modified() {
    _state->mark_modified(_ctx.current_time());
}

void TsOutput::mark_modified(engine_time_t modified_time) {
    _state->mark_modified(modified_time);
}

void TsOutput::mark_child_modified(TimeSeriesOutput& child, engine_time_t modified_time) {
    // Scalar output has no children, but propagate to parent if any
    if (has_parent_output()) {
        parent_output()->mark_child_modified(*this, modified_time);
    }
}

bool TsOutput::can_apply_result(const nb::object& value) {
    // Scalar output can always apply a result
    return true;
}

} // namespace ts
} // namespace hgraph
