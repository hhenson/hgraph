//
// Wrapper Factory - Creates appropriate wrapper based on runtime type inspection
//

#ifndef HGRAPH_WRAPPER_FACTORY_H
#define HGRAPH_WRAPPER_FACTORY_H

#include "py_time_series.h"
// Ensure Nanobind core types are available for iterator helpers below
#include <nanobind/nanobind.h>

#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/types/node.h>
#include <memory>
#include <type_traits>

namespace hgraph
{
    // Forward declarations
    struct Node;
    struct Graph;
    struct Traits;
    struct NodeScheduler;
    struct EvaluationEngineApi;
    struct EvaluationClock;

    /**
     * Wrap a Node in a PyNode.
     * Creates appropriate specialized wrapper based on runtime type.
     */
    nb::object wrap_node(PyNode::api_ptr impl);
    nb::object wrap_node(const node_s_ptr &impl);

    /**
     * Wrap a Graph in a PyGraph.
     */
    nb::object wrap_graph(const graph_s_ptr &impl);

    /**
     * Wrap a Traits pointer in a PyTraits.
     */
    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block);

    /**
     * Wrap a NodeScheduler pointer in a PyNodeScheduler.
     */
    nb::object wrap_node_scheduler(const NodeScheduler::s_ptr &impl);

    // =========================================================================
    // Time-Series Wrapper Functions
    // =========================================================================
    // These wrap ts::TSOutput and ts::TSInput using Node's shared_ptr for lifetime.
    // The returned wrapper holds: node_s_ptr + view + meta

    /**
     * Wrap a ts::TSOutput in the appropriate PyTimeSeriesOutput wrapper.
     *
     * @param output Pointer to the output (owned by Node as std::optional)
     * @param node Shared pointer to the owning Node (provides lifetime management)
     * @return Python wrapper object (PyTimeSeriesOutput or subclass)
     */
    nb::object wrap_output(ts::TSOutput* output, const node_s_ptr& node);

    /**
     * Wrap a ts::TSInput in the appropriate PyTimeSeriesInput wrapper.
     *
     * @param input Pointer to the input (owned by Node as std::optional)
     * @param node Shared pointer to the owning Node (provides lifetime management)
     * @return Python wrapper object (PyTimeSeriesInput or subclass)
     */
    nb::object wrap_input(ts::TSInput* input, const node_s_ptr& node);

    /**
     * Wrap a field of a bundle input as a Python wrapper.
     *
     * @param input Pointer to the bundle input
     * @param field_name Name of the field within the bundle
     * @param node Shared pointer to the owning Node (provides lifetime management)
     * @return Python wrapper object (PyTimeSeriesInput) for the field
     */
    nb::object wrap_input_field(ts::TSInput* input, const std::string& field_name, const node_s_ptr& node);

    // ---------------------------------------------------------------------
    // List-based helpers for time series wrapping
    // ---------------------------------------------------------------------
    // These helpers convert C++ containers/ranges to Python lists.
    // All functions return nb::list - wrap with nb::iter() in __iter__ methods.

    // Convert range keys to a Python list (for map-like iterators)
    template <typename Iterator> nb::list keys_to_list(Iterator begin, Iterator end) {
        nb::list result;
        for (auto it = begin; it != end; ++it) { result.append(nb::cast(it->first)); }
        return result;
    }

    // Convert map/range keys to a Python list (takes by value to handle views)
    template <typename Range> nb::list keys_to_list(Range range) {
        nb::list result;
        for (const auto &[key, _] : range) { result.append(nb::cast(key)); }
        return result;
    }

    // Convert a set/collection to a Python list
    template <typename Set> nb::list set_to_list(const Set &set) {
        nb::list result;
        for (const auto &item : set) { result.append(nb::cast(item)); }
        return result;
    }

    /**
     * Extract shared_ptr<Node> from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    node_s_ptr unwrap_node(const nb::handle &obj);

    /**
     * Extract shared_ptr<Graph> from PyGraph wrapper.
     * Returns nullptr if obj is not a PyGraph.
     */
    graph_s_ptr unwrap_graph(const nb::handle &obj);

    /**
     * Extract raw ts::TSInput* from PyTimeSeriesInput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesInput.
     */
    ts::TSInput* unwrap_input(const nb::handle &obj);
    ts::TSInput* unwrap_input(const PyTimeSeriesInput &input_);

    /**
     * Extract raw ts::TSOutput* from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    ts::TSOutput* unwrap_output(const nb::handle &obj);
    ts::TSOutput* unwrap_output(const PyTimeSeriesOutput &output_);

    /**
     * Extract the owning Node from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    node_s_ptr unwrap_output_node(const nb::handle &obj);
    node_s_ptr unwrap_output_node(const PyTimeSeriesOutput &output_);

    /**
     * Legacy helper - wrap a time series (output) using the node context.
     * Nodes calling this should have access to their shared_ptr via shared_from_this().
     * For now, tries to get node from wrapper if possible.
     */
    nb::object wrap_time_series(ts::TSOutput* output);
    inline nb::object wrap_time_series(ts::TSOutput* output, const node_s_ptr& node) {
        return wrap_output(output, node);
    }

    /**
     * V1 compatibility: Wrap a time_series_output_s_ptr in a Python wrapper.
     * Uses the Python API wrapper classes from V1.
     * This is kept for V1 types that still exist in the codebase.
     */
    nb::object wrap_time_series(const time_series_output_s_ptr& output);

    /**
     * Wrap an EvaluationEngineApi shared_ptr in a PyEvaluationEngineApi.
     */
    nb::object wrap_evaluation_engine_api(hgraph::EvaluationEngineApi::s_ptr impl);

    /**
     * Wrap an EvaluationClock shared_ptr in a PyEvaluationClock.
     */
    nb::object wrap_evaluation_clock(hgraph::EvaluationClock::s_ptr impl);

}  // namespace hgraph

#endif  // HGRAPH_WRAPPER_FACTORY_H
