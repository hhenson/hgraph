//
// Wrapper Factory - Creates appropriate wrapper based on runtime type inspection
//

#ifndef HGRAPH_WRAPPER_FACTORY_H
#define HGRAPH_WRAPPER_FACTORY_H

#include "py_time_series.h"
// Ensure Nanobind core types are available for iterator helpers below
#include <nanobind/nanobind.h>

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/types/node.h>
#include <hgraph/types/value/value.h>
#include <memory>
#include <type_traits>

namespace hgraph
{
    // Forward declarations
    struct Node;
    struct Graph;
    class TSOutputView;
    class TSInputView;

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

    /**
     * Wrap a TSOutputView in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses TSMeta::kind to determine the correct wrapper type.
     */
    nb::object wrap_output_view(TSOutputView view);

    /**
     * Wrap a TSInputView in the appropriate PyTimeSeriesXxxInput wrapper.
     * Uses TSMeta::kind to determine the correct wrapper type.
     */
    nb::object wrap_input_view(TSInputView view);

    /**
     * Wrap a TSInputView using an explicit effective_meta for wrapper dispatch.
     * Used for cross-graph wiring where the input's own meta (e.g., REF) differs
     * from the bound target's meta (e.g., TSValue). Falls back to the standard
     * overload if effective_meta is null.
     */
    nb::object wrap_input_view(TSInputView view, const TSMeta* effective_meta);

    // ---------------------------------------------------------------------
    // List-based helpers for time series wrapping (view-based)
    // ---------------------------------------------------------------------

    // Helper to convert a key to Python object - handles PlainValue specially
    template <typename K>
    nb::object key_to_python(const K& key) {
        if constexpr (std::is_same_v<std::decay_t<K>, value::PlainValue>) {
            return key.to_python();
        } else {
            return nb::cast(key);
        }
    }

    // Convert range keys to a Python list (for map-like iterators)
    template <typename Iterator> nb::list keys_to_list(Iterator begin, Iterator end) {
        nb::list result;
        for (auto it = begin; it != end; ++it) { result.append(key_to_python(it->first)); }
        return result;
    }

    // Convert map/range keys to a Python list (takes by value to handle views)
    template <typename Range> nb::list keys_to_list(Range range) {
        nb::list result;
        for (const auto &[key, _] : range) { result.append(key_to_python(key)); }
        return result;
    }

    // Convert a set/collection to a Python list
    template <typename Set> nb::list set_to_list(const Set &set) {
        nb::list result;
        for (const auto &item : set) { result.append(nb::cast(item)); }
        return result;
    }

    /**
     * Extract raw Node pointer from PyNode wrapper.
     */
    node_s_ptr unwrap_node(const nb::handle &obj);

    /**
     * Extract raw Graph pointer from PyGraph wrapper.
     */
    graph_s_ptr unwrap_graph(const nb::handle &obj);

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
