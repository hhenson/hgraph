//
// Created by Howard Henson on 26/12/2024.
//

#ifndef HGRAPH_FORWARD_DECLARATIONS_H
#define HGRAPH_FORWARD_DECLARATIONS_H

#include <functional>
#include <memory>
#include <nanobind/intrusive/ref.h>
#include <nanobind/nanobind.h>
#include <string>
#include <tp/tpack.h>
#include <hgraph/util/date_time.h>

namespace nb = nanobind;

namespace hgraph {
    // NodeSignature - keeps nb::ref
    struct NodeSignature;
    using node_signature_ptr = NodeSignature*;
    using node_signature_s_ptr = nb::ref<NodeSignature>;

    // Node - converted to shared_ptr
    struct Node;
    using node_ptr = Node*;
    using node_s_ptr = std::shared_ptr<Node>;

    // Traits - converted to shared_ptr
    struct Traits;
    using traits_ptr = Traits*;
    using const_traits_ptr = const Traits*;
    using traits_s_ptr = std::shared_ptr<Traits>;

    // Graph - converted to shared_ptr
    struct Graph;
    using graph_ptr = Graph*;
    using const_graph_ptr = const Graph*;
    using graph_s_ptr = std::shared_ptr<Graph>;

    // SenderReceiverState - raw pointer only
    struct SenderReceiverState;
    using sender_receiver_state_ptr = SenderReceiverState*;

    // GraphBuilder - keeps nb::ref
    struct GraphBuilder;
    using graph_builder_ptr = GraphBuilder*;
    using graph_builder_s_ptr = nb::ref<GraphBuilder>;

    // NodeBuilder - keeps nb::ref
    struct NodeBuilder;
    using node_builder_ptr = NodeBuilder*;
    using node_builder_s_ptr = nb::ref<NodeBuilder>;

    // EngineEvaluationClock - converted to shared_ptr
    struct EngineEvaluationClock;
    using engine_evaluation_clock_ptr = EngineEvaluationClock*;
    using engine_evaluation_clock_s_ptr = std::shared_ptr<EngineEvaluationClock>;

    // InputBuilder - keeps nb::ref
    struct InputBuilder;
    using input_builder_ptr = InputBuilder*;
    using input_builder_s_ptr = nb::ref<InputBuilder>;

    // OutputBuilder - keeps nb::ref
    struct OutputBuilder;
    using output_builder_ptr = OutputBuilder*;
    using output_builder_s_ptr = nb::ref<OutputBuilder>;

    // ============================================================================
    // LEGACY TIME-SERIES TYPES (DEPRECATED)
    // These types are being phased out in favor of TSValue-based storage.
    // They are kept temporarily for backwards compatibility during migration.
    // TODO: Remove once all code is migrated to TSValue
    // ============================================================================

    // TimeSeriesType - legacy abstract interface
    struct TimeSeriesType;
    using time_series_type_ptr = TimeSeriesType*;

    // TimeSeriesInput - legacy input interface
    struct TimeSeriesInput;
    using time_series_input_ptr = TimeSeriesInput*;
    using time_series_input_s_ptr = std::shared_ptr<TimeSeriesInput>;

    // TimeSeriesOutput - legacy output interface
    struct TimeSeriesOutput;
    using time_series_output_ptr = TimeSeriesOutput*;
    using time_series_output_s_ptr = std::shared_ptr<TimeSeriesOutput>;

    // TimeSeriesReferenceOutput - still used for start_inputs and feature_extension
    struct TimeSeriesReferenceOutput;
    struct TimeSeriesReferenceInput;
    using time_series_reference_input_ptr = TimeSeriesReferenceInput*;
    using time_series_reference_input_s_ptr = std::shared_ptr<TimeSeriesReferenceInput>;
    using time_series_reference_output_s_ptr = std::shared_ptr<TimeSeriesReferenceOutput>;

    // Legacy concept for type constraints (still used by tss.h)
    template<typename T_TS>
    concept TimeSeriesT = std::is_base_of_v<TimeSeriesInput, T_TS> || std::is_base_of_v<TimeSeriesOutput, T_TS>;

    using c_string_ref = std::reference_wrapper<const std::string>;

    // ============================================================================
    // LEGACY CONCRETE TIME-SERIES TYPES (DEPRECATED)
    // These are forward declarations for types being phased out.
    // The visitor patterns in time_series_type.h still reference some of these.
    // TODO: Remove once visitor patterns are also migrated
    // ============================================================================

    // Legacy base types (for visitor patterns)
    struct BaseTimeSeriesInput;
    struct BaseTimeSeriesOutput;
    struct TimeSeriesValueInputBase;
    struct TimeSeriesValueOutputBase;

    // Legacy concrete input types
    struct TimeSeriesSignalInput;
    struct IndexedTimeSeriesInput;
    struct TimeSeriesDictInput;
    struct TimeSeriesValueReferenceInput;
    struct TimeSeriesWindowReferenceInput;
    struct TimeSeriesListReferenceInput;
    struct TimeSeriesSetReferenceInput;
    struct TimeSeriesDictReferenceInput;
    struct TimeSeriesBundleReferenceInput;
    struct TimeSeriesValueInput;
    struct TimeSeriesListInput;
    using time_series_list_input_s_ptr = std::shared_ptr<TimeSeriesListInput>;
    struct TimeSeriesSetInput;
    struct TimeSeriesBundleInput;
    struct TimeSeriesDictInputImpl;
    struct TimeSeriesWindowInput;

    // Legacy schema type (used by builders)
    struct TimeSeriesSchema;
    using time_series_schema_ptr = TimeSeriesSchema*;
    using time_series_schema_s_ptr = nb::ref<TimeSeriesSchema>;

    // Legacy concrete output types
    struct IndexedTimeSeriesOutput;
    struct TimeSeriesDictOutput;
    struct TimeSeriesValueReferenceOutput;
    struct TimeSeriesWindowReferenceOutput;
    struct TimeSeriesListReferenceOutput;
    struct TimeSeriesSetReferenceOutput;
    struct TimeSeriesDictReferenceOutput;
    struct TimeSeriesBundleReferenceOutput;
    struct TimeSeriesValueOutput;
    using time_series_value_output_ptr = TimeSeriesValueOutput*;
    using time_series_value_output_s_ptr = std::shared_ptr<TimeSeriesValueOutput>;
    struct TimeSeriesListOutput;
    using time_series_list_output_s_ptr = std::shared_ptr<TimeSeriesListOutput>;
    struct TimeSeriesSetOutput;
    struct TimeSeriesBundleOutput;
    using time_series_bundle_output_s_ptr = std::shared_ptr<TimeSeriesBundleOutput>;
    struct TimeSeriesDictOutputImpl;
    struct TimeSeriesFixedWindowOutput;
    struct TimeSeriesTimeWindowOutput;
    using time_series_bundle_input_s_ptr = std::shared_ptr<TimeSeriesBundleInput>;

    struct ContextStubSourceNode;
    struct NestedNode;
    struct PushQueueNode;
    struct LastValuePullNode;
    struct BasePythonNode;
    struct TsdNonAssociativeReduceNode;
    struct PythonGeneratorNode;
    struct PythonNode;
    struct ReduceNode;
    struct SwitchNode;
    struct NestedGraphNode;
    struct TsdMapNode;
    struct ComponentNode;
    struct TryExceptNode;
    struct MeshNode;

    using ts_payload_types = tp::tpack<bool, int64_t, double, engine_date_t, engine_time_t, engine_time_delta_t, nb::object>;
    inline constexpr auto ts_payload_types_v = ts_payload_types{};


} // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H