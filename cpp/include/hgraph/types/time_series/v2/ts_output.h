//
// TsOutput - Write View for Scalar Time-Series (New V2 Architecture)
//
// This is the output view that owns TSValue. It provides write access
// to the shared state and implements the TimeSeriesOutput interface.
//
// NOTE: Temporarily in hgraph::ts namespace to avoid conflict with
// existing TsOutput in ts_v2_types.h. Will be moved to hgraph namespace
// when the old types are removed.
//

#ifndef HGRAPH_TS_OUTPUT_V2_NEW_H
#define HGRAPH_TS_OUTPUT_V2_NEW_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/v2/ts_value.h>
#include <hgraph/types/time_series/v2/ts_context.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph {
namespace ts {  // Temporary namespace to avoid conflict with old TsOutput

/**
 * TsOutput - Write view for scalar time-series (TS[T]).
 *
 * This is the output implementation that:
 * - Creates and owns the TSValue shared state
 * - Provides write access to the value
 * - Manages subscriber notifications
 * - Implements TimeSeriesOutput interface
 *
 * Inputs bind to this by getting the shared_state() pointer.
 */
struct TsOutput final : TimeSeriesOutput {
    using s_ptr = std::shared_ptr<TsOutput>;

    // Construction with node owner
    TsOutput(node_ptr parent, const TSTypeMeta* meta);

    // Construction with parent time-series owner
    TsOutput(TimeSeriesOutput* parent, const TSTypeMeta* meta);

    // Access to shared state (for input binding)
    [[nodiscard]] TSValue::ptr shared_state() const { return _state; }

    // === TimeSeriesType interface (delegates to context) ===
    [[nodiscard]] node_ptr owning_node() override { return _ctx.owning_node(); }
    [[nodiscard]] node_ptr owning_node() const override { return _ctx.owning_node(); }
    [[nodiscard]] graph_ptr owning_graph() override { return _ctx.owning_graph(); }
    [[nodiscard]] graph_ptr owning_graph() const override { return _ctx.owning_graph(); }
    [[nodiscard]] bool has_parent_or_node() const override { return _ctx.has_owner(); }
    [[nodiscard]] bool has_owning_node() const override { return owning_node() != nullptr; }

    // === Value access (delegates to state) ===
    [[nodiscard]] nb::object py_value() const override { return _state->value; }
    [[nodiscard]] nb::object py_delta_value() const override { return _state->value; }
    [[nodiscard]] engine_time_t last_modified_time() const override { return _state->last_modified; }
    [[nodiscard]] bool modified() const override { return _state->modified(_ctx.current_time()); }
    [[nodiscard]] bool valid() const override { return _state->valid(); }
    [[nodiscard]] bool all_valid() const override { return valid(); }

    // === Re-parenting ===
    void re_parent(node_ptr parent) override { _ctx.re_parent(parent); }
    void re_parent(time_series_type_ptr parent) override { _ctx.re_parent(parent); }
    void reset_parent_or_node() override { _ctx.reset(); }
    void builder_release_cleanup() override;

    // === Type checking ===
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool is_reference() const override { return false; }
    [[nodiscard]] bool has_reference() const override { return false; }

    // === TimeSeriesOutput interface ===
    [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() const override;
    [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() override;
    [[nodiscard]] bool has_parent_output() const override { return _ctx.is_parent_owner(); }

    void subscribe(Notifiable* n) override { _state->subscribe(n); }
    void un_subscribe(Notifiable* n) override { _state->unsubscribe(n); }

    // === Mutation (write access) ===
    void apply_result(const nb::object& value) override;
    void py_set_value(const nb::object& value) override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;

    void clear() override;
    void invalidate() override;
    void mark_invalid() override;
    void mark_modified() override;
    void mark_modified(engine_time_t modified_time) override;
    void mark_child_modified(TimeSeriesOutput& child, engine_time_t modified_time) override;
    bool can_apply_result(const nb::object& value) override;

    VISITOR_SUPPORT()

private:
    TSValue::ptr _state;    // Owned shared state
    TSContext _ctx;         // Navigation context
    const TSTypeMeta* _meta;
};

} // namespace ts
} // namespace hgraph

#endif // HGRAPH_TS_OUTPUT_V2_NEW_H
