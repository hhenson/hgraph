//
// Created by Howard Henson on 26/12/2024.
//

#ifndef HGRAPH_FORWARD_DECLARATIONS_H
#define HGRAPH_FORWARD_DECLARATIONS_H

// Just in case this get's imported naked, ensure we have nanobind loaded to get the additional template behaviour from ref.
#include <functional>
#include <memory>
#include <nanobind/intrusive/ref.h>
#include <nanobind/nanobind.h>
#include <string>

namespace nb = nanobind;

namespace hgraph {
    struct NodeSignature;
    using node_signature_ptr = nb::ref<NodeSignature>;  // NodeSignature uses intrusive_base

    struct Node;
    using node_ptr = std::shared_ptr<Node>;

    struct Traits;
    using traits_ptr = std::shared_ptr<Traits>;

    struct Graph;
    using graph_ptr = std::shared_ptr<Graph>;

    struct SenderReceiverState;
    using sender_receiver_state_ptr = SenderReceiverState *;

    struct GraphBuilder;
    using graph_builder_ptr = nb::ref<GraphBuilder>;

    struct NodeBuilder;

    struct EngineEvaluationClock;
    using engine_evalaution_clock_ptr = std::shared_ptr<EngineEvaluationClock>;  // Clock is wrapped

    struct InputBuilder;
    using input_builder_ptr = nb::ref<InputBuilder>;

    struct OutputBuilder;
    using output_builder_ptr = nb::ref<OutputBuilder>;

    struct NodeBuilder;
    using node_builder_ptr = nb::ref<NodeBuilder>;

    struct TimeSeriesType;
    using time_series_type_ptr = std::shared_ptr<TimeSeriesType>;

    struct TimeSeriesInput;
    struct BaseTimeSeriesInput;
    using time_series_input_ptr = std::shared_ptr<TimeSeriesInput>;

    struct TimeSeriesBundleInput;
    using time_series_bundle_input_ptr = std::shared_ptr<TimeSeriesBundleInput>;

    struct TimeSeriesBundleOutput;
    using time_series_bundle_output_ptr = std::shared_ptr<TimeSeriesBundleOutput>;

    struct TimeSeriesOutput;
    struct BaseTimeSeriesOutput;
    using time_series_output_ptr = std::shared_ptr<TimeSeriesOutput>;

    struct TimeSeriesReference;
    using time_series_reference_ptr = std::shared_ptr<TimeSeriesReference>;

    struct TimeSeriesReferenceInput;
    using time_series_reference_input_ptr = std::shared_ptr<TimeSeriesReferenceInput>;

    struct TimeSeriesReferenceOutput;
    using time_series_reference_output_ptr = std::shared_ptr<TimeSeriesReferenceOutput>;

    struct TimeSeriesListInput;
    using time_series_list_input_ptr = std::shared_ptr<TimeSeriesListInput>;

    struct TimeSeriesListOutput;
    using time_series_list_output_ptr = std::shared_ptr<TimeSeriesListOutput>;

    struct TimeSeriesSetInput;
    using time_series_set_input_ptr = std::shared_ptr<TimeSeriesSetInput>;

    struct TimeSeriesSetOutput;
    using time_series_set_output_ptr = std::shared_ptr<TimeSeriesSetOutput>;

    struct TimeSeriesSchema;
    using time_series_schema_ptr = nb::ref<TimeSeriesSchema>;

    using c_string_ref = std::reference_wrapper<const std::string>;

    template<typename T_TS>
    concept TimeSeriesT = std::is_base_of_v<TimeSeriesInput, T_TS> || std::is_base_of_v<TimeSeriesOutput, T_TS>;

    struct OutputBuilder;
    struct InputBuilder;

    // Note: output_builder_ptr and input_builder_ptr are already defined above
} // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H