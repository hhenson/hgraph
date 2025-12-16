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
#include <memory>
#include <type_traits>

namespace hgraph
{
    // Forward declarations
    struct Node;
    struct Graph;
    struct TimeSeriesInput;
    struct TimeSeriesOutput;

    /**
     * Wrap a Node in a PyNode.
     * Creates appropriate specialized wrapper based on runtime type.
     */
    nb::object wrap_node(PyNode::api_ptr impl);
    // Hard-ban raw pointer wrapping for Nodes: prefer shared_ptr
    // nb::object wrap_node(const hgraph::Node *impl, const control_block_ptr &control_block);
    // nb::object wrap_node(const Node *impl);
    nb::object wrap_node(const node_s_ptr &impl);

    /**
     * Wrap a Graph in a PyGraph.
     * Hard-ban raw pointer wrapping for Graph: prefer shared_ptr
     */
    nb::object wrap_graph(const graph_s_ptr &impl);

    /**
     * Wrap a Traits pointer in a PyTraits.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_traits(const Traits *impl, const control_block_ptr &control_block);

    /**
     * Wrap a NodeScheduler pointer in a PyNodeScheduler.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
   // nb::object wrap_node_scheduler(const hgraph::NodeScheduler *impl, const control_block_ptr &control_block);
    nb::object wrap_node_scheduler(const NodeScheduler::s_ptr &impl);

    /**
     * Wrap a TimeSeriesInput pointer in the appropriate PyTimeSeriesXxxInput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    // Internal implementation uses ApiPtr<T>, but public API only exposes
    // shared_ptr and (T*, control_block_ptr) forms. Keep declaration private to cpp.
    nb::object wrap_input(ApiPtr<TimeSeriesInput> impl);

    /**
     * Wrap a TimeSeriesOutput pointer in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    // See note above on public API forms
    nb::object wrap_output(ApiPtr<TimeSeriesOutput> impl);

    // See note above on public API forms
    nb::object wrap_time_series(ApiPtr<TimeSeriesInput> impl);
    nb::object wrap_time_series(ApiPtr<TimeSeriesOutput> impl);

    // Overloads for shared_ptr - the shared_ptr provides both the pointer and lifetime
    inline nb::object wrap_input(const time_series_input_s_ptr &impl) {
        return wrap_input(ApiPtr<TimeSeriesInput>(impl));
    }
    inline nb::object wrap_time_series(const time_series_input_s_ptr &impl) {
        return wrap_time_series(ApiPtr<TimeSeriesInput>(impl));
    }
    inline nb::object wrap_time_series(const time_series_output_s_ptr &impl) {
        return wrap_time_series(ApiPtr<TimeSeriesOutput>(impl));
    }

    // Hard-ban raw pointer wrapping for time-series values (Inputs/Outputs): prefer shared_ptr

    // NOTE: Raw pointer-only overloads (deriving control block) are intentionally removed.
    // Callers must provide either shared_ptr or (T*, control_block_ptr).

    // Overload for shared_ptr - avoids redundant shared_from_this() call
    nb::object wrap_output(const time_series_output_s_ptr &impl);

    // ---------------------------------------------------------------------
    // List-based helpers for time series wrapping
    // ---------------------------------------------------------------------
    // These helpers convert C++ containers/ranges to Python lists, wrapping
    // time series values appropriately using the provided control block.
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

    // Convert range values to a Python list, wrapping time series values
    // Values are expected to be shared_ptr types
    template <typename Iterator> nb::list values_to_list(Iterator begin, Iterator end) {
        nb::list result;
        for (auto it = begin; it != end; ++it) { result.append(wrap_time_series(it->second)); }
        return result;
    }

    // Convert map/range values to a Python list, wrapping time series values (takes by value to handle views)
    // Values are expected to be shared_ptr types
    template <typename Range> nb::list values_to_list(Range range) {
        nb::list result;
        for (const auto &[_, value] : range) { result.append(wrap_time_series(value)); }
        return result;
    }

    // Convert range items to a Python list of (key, wrapped_value) tuples
    // Values are expected to be shared_ptr types
    template <typename Iterator> nb::list items_to_list(Iterator begin, Iterator end) {
        nb::list result;
        for (auto it = begin; it != end; ++it) {
            result.append(nb::make_tuple(nb::cast(it->first), wrap_time_series(it->second)));
        }
        return result;
    }

    // Convert map/range items to a Python list of (key, wrapped_value) tuples (takes by value to handle views)
    // Values are expected to be shared_ptr types
    template <typename Range> nb::list items_to_list(Range range) {
        nb::list result;
        for (const auto &[key, value] : range) { result.append(nb::make_tuple(nb::cast(key), wrap_time_series(value))); }
        return result;
    }

    // Convert a set/collection to a Python list
    template <typename Set> nb::list set_to_list(const Set &set) {
        nb::list result;
        for (const auto &item : set) { result.append(nb::cast(item)); }
        return result;
    }

    // Convert a list/vector of time series to a Python list, wrapping each element
    // Items are expected to be shared_ptr types
    template <typename Collection> nb::list list_to_list(const Collection &collection) {
        nb::list result;
        for (const auto &item : collection) { result.append(wrap_time_series(item)); }
        return result;
    }

    /**
     * Extract raw Node pointer from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    // Unwrap to shared_ptr only
    node_s_ptr unwrap_node(const nb::handle &obj);

    /**
     * Extract raw Node pointer from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    graph_s_ptr unwrap_graph(const nb::handle &obj);

    /**
     * Extract raw TimeSeriesInput pointer from PyTimeSeriesInput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesInput.
     */
    time_series_input_s_ptr unwrap_input(const nb::handle &obj);
    time_series_input_s_ptr unwrap_input(const PyTimeSeriesInput &input_);
    
    template <typename T> requires std::is_base_of_v<TimeSeriesInput, T>
    std::shared_ptr<T> unwrap_input_as(const nb::handle &obj) {
        return std::dynamic_pointer_cast<T>(unwrap_input(obj));
    }

    /**
     * Extract raw TimeSeriesOutput pointer from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    time_series_output_s_ptr unwrap_output(const nb::handle &obj);
    time_series_output_s_ptr unwrap_output(const PyTimeSeriesOutput &output_);

    template <typename T> requires std::is_base_of_v<TimeSeriesOutput, T>
    std::shared_ptr<T> unwrap_output_as(const nb::handle &obj) {
        return std::dynamic_pointer_cast<T>(unwrap_output(obj));
    }

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
