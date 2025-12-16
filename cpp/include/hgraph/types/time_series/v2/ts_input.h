//
// TsInput - Read View for Scalar Time-Series (New V2 Architecture)
//
// This is the input view that binds to TSValue. It provides read access
// to the shared state and implements the TimeSeriesInput interface.
//
// NOTE: Temporarily in hgraph::ts namespace to avoid conflict with
// existing TsInput in ts_v2_types.h. Will be moved to hgraph namespace
// when the old types are removed.
//

#ifndef HGRAPH_TS_INPUT_V2_NEW_H
#define HGRAPH_TS_INPUT_V2_NEW_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/v2/ts_value.h>
#include <hgraph/types/time_series/v2/ts_context.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph {
namespace ts {  // Temporary namespace to avoid conflict with old TsInput

// Forward declaration
struct TsOutput;

/**
 * TsInput - Read view for scalar time-series (TS[T]).
 *
 * This is the input implementation that:
 * - Binds to an output's TSValue shared state
 * - Provides read-only access to the value
 * - Manages active/passive subscription
 * - Implements TimeSeriesInput interface
 *
 * When binding to an output, it gets the TSValue pointer from TsOutput::shared_state().
 */
struct TsInput final : TimeSeriesInput {
    using s_ptr = std::shared_ptr<TsInput>;

    // Construction with node owner
    TsInput(node_ptr parent, const TSTypeMeta* meta);

    // Construction with parent time-series owner
    TsInput(TimeSeriesInput* parent, const TSTypeMeta* meta);

    // Destructor - unsubscribe if active
    ~TsInput() override;

    // === TimeSeriesType interface (delegates to context) ===
    [[nodiscard]] node_ptr owning_node() override { return _ctx.owning_node(); }
    [[nodiscard]] node_ptr owning_node() const override { return _ctx.owning_node(); }
    [[nodiscard]] graph_ptr owning_graph() override { return _ctx.owning_graph(); }
    [[nodiscard]] graph_ptr owning_graph() const override { return _ctx.owning_graph(); }
    [[nodiscard]] bool has_parent_or_node() const override { return _ctx.has_owner(); }
    [[nodiscard]] bool has_owning_node() const override { return owning_node() != nullptr; }

    // === Value access (reads from bound state) ===
    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
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

    // === TimeSeriesInput interface ===
    [[nodiscard]] TimeSeriesInput::s_ptr parent_input() const override;
    [[nodiscard]] bool has_parent_input() const override { return _ctx.is_parent_owner(); }

    [[nodiscard]] bool active() const override { return _active; }
    void make_active() override;
    void make_passive() override;

    [[nodiscard]] bool bound() const override { return _state != nullptr; }
    [[nodiscard]] bool has_peer() const override { return _bound_output != nullptr; }
    [[nodiscard]] time_series_output_s_ptr output() const override { return _bound_output; }
    [[nodiscard]] bool has_output() const override { return _bound_output != nullptr; }
    bool bind_output(time_series_output_s_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;

    [[nodiscard]] time_series_reference_output_s_ptr reference_output() const override { return nullptr; }
    [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override { return nullptr; }

    // === Notifiable interface ===
    void notify(engine_time_t modified_time) override;

    // Note: VISITOR_SUPPORT() removed temporarily until types are registered
    // in the visitor system. Will be added when old V2 types are removed.

private:
    TSValue::ptr _state;                      // Shared state (from bound output)
    TSContext _ctx;                           // Navigation context
    const TSTypeMeta* _meta;
    time_series_output_s_ptr _bound_output;   // Keep output alive
    bool _active{false};
    engine_time_t _sample_time{MIN_DT};       // When we were sampled
};

} // namespace ts
} // namespace hgraph

#endif // HGRAPH_TS_INPUT_V2_NEW_H
