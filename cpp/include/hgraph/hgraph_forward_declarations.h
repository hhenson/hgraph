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

    // TimeSeriesSchema is defined purely in Python (subclass of AbstractSchema).
    // No C++ forward declaration needed.

    using c_string_ref = std::reference_wrapper<const std::string>;

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

    // TSOutput / TSInput - new owning time-series types
    class TSOutput;
    class TSInput;
    class TSOutputView;
    class TSInputView;

    // Legacy type aliases - these map to the new TSOutput/TSInput types.
    // Used by builder interfaces and feature_extension until fully migrated.
    using time_series_output_ptr   = TSOutput*;
    using time_series_output_s_ptr = std::shared_ptr<TSOutput>;
    using time_series_input_ptr    = TSInput*;
    using time_series_input_s_ptr  = std::shared_ptr<TSInput>;

    using ts_payload_types = tp::tpack<bool, int64_t, double, engine_date_t, engine_time_t, engine_time_delta_t, nb::object>;
    inline constexpr auto ts_payload_types_v = ts_payload_types{};


} // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H
