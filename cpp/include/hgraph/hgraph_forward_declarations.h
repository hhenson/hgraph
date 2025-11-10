//
// Created by Howard Henson on 26/12/2024.
//

#ifndef HGRAPH_FORWARD_DECLARATIONS_H
#define HGRAPH_FORWARD_DECLARATIONS_H

// Just in case this get's imported naked, ensure we have nanobind loaded to get the additional template behaviour from ref.
#include <functional>
#include <nanobind/intrusive/ref.h>
#include <nanobind/nanobind.h>
#include <string>

namespace hgraph {
    struct NodeSignature;
    using node_signature_ptr = nanobind::ref<NodeSignature>;

    struct Node;
    using node_ptr = nanobind::ref<Node>;

    struct Traits;
    using traits_ptr = nanobind::ref<Traits>;

    struct Graph;
    using graph_ptr = nanobind::ref<Graph>;

    struct SenderReceiverState;
    using sender_receiver_state_ptr = SenderReceiverState *;

    struct GraphBuilder;
    using graph_builder_ptr = nanobind::ref<GraphBuilder>;

    struct NodeBuilder;

    struct EngineEvaluationClock;
    using engine_evalaution_clock_ptr = nanobind::ref<EngineEvaluationClock>;

    struct InputBuilder;
    using input_builder_ptr = nanobind::ref<InputBuilder>;

    struct OutputBuilder;
    using output_builder_ptr = nanobind::ref<OutputBuilder>;

    struct NodeBuilder;
    using node_builder_ptr = nanobind::ref<NodeBuilder>;

    struct TimeSeriesType;
    using time_series_type_ptr = nanobind::ref<TimeSeriesType>;

    struct TimeSeriesInput;
    struct BaseTimeSeriesInput;
    using time_series_input_ptr = nanobind::ref<TimeSeriesInput>;

    struct TimeSeriesBundleInput;
    using time_series_bundle_input_ptr = nanobind::ref<TimeSeriesBundleInput>;

    struct TimeSeriesBundleOutput;
    using time_series_bundle_output_ptr = nanobind::ref<TimeSeriesBundleOutput>;

    struct TimeSeriesOutput;
    struct BaseTimeSeriesOutput;
    using time_series_output_ptr = nanobind::ref<TimeSeriesOutput>;

    struct TimeSeriesReference;
    using time_series_reference_ptr = nanobind::ref<TimeSeriesReference>;

    struct TimeSeriesReferenceInput;
    using time_series_reference_input_ptr = nanobind::ref<TimeSeriesReferenceInput>;

    struct TimeSeriesReferenceOutput;
    using time_series_reference_output_ptr = nanobind::ref<TimeSeriesReferenceOutput>;

    struct TimeSeriesListInput;
    using time_series_list_input_ptr = nanobind::ref<TimeSeriesListInput>;

    struct TimeSeriesListOutput;
    using time_series_list_output_ptr = nanobind::ref<TimeSeriesListOutput>;

    struct TimeSeriesSetInput;
    using time_series_set_input_ptr = nanobind::ref<TimeSeriesSetInput>;

    struct TimeSeriesSetOutput;
    using time_series_set_output_ptr = nanobind::ref<TimeSeriesSetOutput>;

    struct TimeSeriesSchema;
    using time_series_schema_ptr = nanobind::ref<TimeSeriesSchema>;

    using c_string_ref = std::reference_wrapper<const std::string>;

    template<typename T_TS>
    concept TimeSeriesT = std::is_base_of_v<TimeSeriesInput, T_TS> || std::is_base_of_v<TimeSeriesOutput, T_TS>;

    struct OutputBuilder;
    struct InputBuilder;

    using output_builder_ptr = nanobind::ref<OutputBuilder>;
    using input_builder_ptr = nanobind::ref<InputBuilder>;
} // namespace hgraph

#endif  // HGRAPH_FORWARD_DECLARATIONS_H