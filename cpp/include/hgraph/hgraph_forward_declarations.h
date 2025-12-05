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

    // TimeSeriesType - converted to shared_ptr
    struct TimeSeriesType;
    using time_series_type_ptr = TimeSeriesType*;
    using time_series_type_s_ptr = std::shared_ptr<TimeSeriesType>;

    // TimeSeriesInput - converted to shared_ptr
    struct TimeSeriesInput;
    struct BaseTimeSeriesInput;
    using time_series_input_ptr = TimeSeriesInput*;
    using time_series_input_s_ptr = std::shared_ptr<TimeSeriesInput>;

    // TimeSeriesBundleInput - converted to shared_ptr
    struct TimeSeriesBundleInput;
    using time_series_bundle_input_ptr = TimeSeriesBundleInput*;
    using time_series_bundle_input_s_ptr = std::shared_ptr<TimeSeriesBundleInput>;

    // TimeSeriesBundleOutput - converted to shared_ptr
    struct TimeSeriesBundleOutput;
    using time_series_bundle_output_ptr = TimeSeriesBundleOutput*;
    using time_series_bundle_output_s_ptr = std::shared_ptr<TimeSeriesBundleOutput>;

    // TimeSeriesOutput - converted to shared_ptr
    struct TimeSeriesOutput;
    struct BaseTimeSeriesOutput;
    using time_series_output_ptr = TimeSeriesOutput*;
    using const_time_series_output_ptr = const TimeSeriesOutput*;
    using time_series_output_s_ptr = std::shared_ptr<TimeSeriesOutput>;

    // TimeSeriesReference - converted to shared_ptr
    struct TimeSeriesReference;
    using time_series_reference_ptr = TimeSeriesReference*;
    using time_series_reference_s_ptr = std::shared_ptr<TimeSeriesReference>;

    // TimeSeriesReferenceInput - converted to shared_ptr
    struct TimeSeriesReferenceInput;
    using time_series_reference_input_ptr = TimeSeriesReferenceInput*;
    using time_series_reference_input_s_ptr = std::shared_ptr<TimeSeriesReferenceInput>;

    // TimeSeriesReferenceOutput - converted to shared_ptr
    struct TimeSeriesReferenceOutput;
    using time_series_reference_output_ptr = TimeSeriesReferenceOutput*;
    using time_series_reference_output_s_ptr = std::shared_ptr<TimeSeriesReferenceOutput>;

    // TimeSeriesListInput - converted to shared_ptr
    struct TimeSeriesListInput;
    using time_series_list_input_ptr = TimeSeriesListInput*;
    using time_series_list_input_s_ptr = std::shared_ptr<TimeSeriesListInput>;

    // TimeSeriesListOutput - converted to shared_ptr
    struct TimeSeriesListOutput;
    using time_series_list_output_ptr = TimeSeriesListOutput*;
    using time_series_list_output_s_ptr = std::shared_ptr<TimeSeriesListOutput>;

    // TimeSeriesSetInput - converted to shared_ptr
    struct TimeSeriesSetInput;
    using time_series_set_input_ptr = TimeSeriesSetInput*;
    using time_series_set_input_s_ptr = std::shared_ptr<TimeSeriesSetInput>;

    // TimeSeriesSetOutput - converted to shared_ptr
    struct TimeSeriesSetOutput;
    using time_series_set_output_ptr = TimeSeriesSetOutput*;
    using time_series_set_output_s_ptr = std::shared_ptr<TimeSeriesSetOutput>;

    // TimeSeriesSchema - keeps nb::ref
    struct TimeSeriesSchema;
    using time_series_schema_ptr = TimeSeriesSchema*;
    using time_series_schema_s_ptr = nb::ref<TimeSeriesSchema>;

    using c_string_ref = std::reference_wrapper<const std::string>;

    template<typename T_TS>
    concept TimeSeriesT = std::is_base_of_v<TimeSeriesInput, T_TS> || std::is_base_of_v<TimeSeriesOutput, T_TS>;

    struct TimeSeriesValueInputBase;
    struct TimeSeriesSignalInput;
    struct IndexedTimeSeriesInput;
    struct TimeSeriesDictInput;
    struct TimeSeriesValueReferenceInput;
    struct TimeSeriesWindowReferenceInput;
    struct TimeSeriesListReferenceInput;
    struct TimeSeriesSetReferenceInput;
    struct TimeSeriesDictReferenceInput;
    struct TimeSeriesBundleReferenceInput;
    struct TimeSeriesValueOutputBase;
    struct IndexedTimeSeriesOutput;
    struct TimeSeriesDictOutput;
    struct TimeSeriesValueReferenceOutput;
    struct TimeSeriesWindowReferenceOutput;
    struct TimeSeriesListReferenceOutput;
    struct TimeSeriesSetReferenceOutput;
    struct TimeSeriesDictReferenceOutput;
    struct TimeSeriesBundleReferenceOutput;

    template<typename> struct TimeSeriesValueInput;
    template<typename> struct TimeSeriesDictInput_T;
    template<typename> struct TimeSeriesSetInput_T;
    template<typename> struct TimeSeriesWindowInput;
    template<typename> struct TimeSeriesValueOutput;
    template<typename> struct TimeSeriesDictOutput_T;
    template<typename> struct TimeSeriesSetOutput_T;
    template<typename> struct TimeSeriesFixedWindowOutput;
    template<typename> struct TimeSeriesTimeWindowOutput;

    struct ContextStubSourceNode;
    struct NestedNode;
    struct PushQueueNode;
    struct LastValuePullNode;
    struct BasePythonNode;
    struct TsdNonAssociativeReduceNode;
    struct PythonGeneratorNode;
    struct PythonNode;
    template<typename> struct ReduceNode;
    template<typename> struct SwitchNode;
    struct NestedGraphNode;
    template<typename> struct TsdMapNode;
    struct ComponentNode;
    struct TryExceptNode;
    template <typename> struct MeshNode;

    using ts_payload_types = tp::tpack<bool, int64_t, double, engine_date_t, engine_time_t, engine_time_delta_t, nb::object>;
    inline constexpr auto ts_payload_types_v = ts_payload_types{};


} // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H