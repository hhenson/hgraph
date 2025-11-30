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
#include <memory>
#include <type_traits>

namespace hgraph
{
    // Forward declarations
    struct Node;
    struct Graph;
    struct NodeScheduler;
    struct TimeSeriesInput;
    struct TimeSeriesOutput;

    /**
     * Wrap a Node pointer in a PyNode.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_node(const hgraph::Node *impl, const control_block_ptr &control_block);
    nb::object wrap_node(const Node *impl);

    /**
     * Wrap a Graph pointer in a PyGraph.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_graph(const hgraph::Graph *impl, const control_block_ptr &control_block);

    /**
     * Wrap a Traits pointer in a PyTraits.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_traits(const hgraph::Traits *impl, const control_block_ptr &control_block);

    /**
     * Wrap a NodeScheduler pointer in a PyNodeScheduler.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_node_scheduler(const hgraph::NodeScheduler *impl, const control_block_ptr &control_block);

    /**
     * Wrap a TimeSeriesInput pointer in the appropriate PyTimeSeriesXxxInput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     * Caches the created wrapper for future use.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    nb::object wrap_input(const hgraph::TimeSeriesInput *impl, const control_block_ptr &control_block);
    nb::object wrap_input(const TimeSeriesInput *impl);

    /**
     * Wrap a TimeSeriesOutput pointer in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     * Caches the created wrapper for future use.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl, const control_block_ptr &control_block);

    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl);

    nb::object wrap_time_series(const TimeSeriesInput *impl, const control_block_ptr &control_block);
    nb::object wrap_time_series(const TimeSeriesOutput *impl, const control_block_ptr &control_block);
    nb::object wrap_time_series(const TimeSeriesInput *impl);
    nb::object wrap_time_series(const TimeSeriesOutput *impl);

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
    template <typename Iterator> nb::list values_to_list(Iterator begin, Iterator end, const control_block_ptr &cb) {
        nb::list result;
        for (auto it = begin; it != end; ++it) { result.append(wrap_time_series(it->second, cb)); }
        return result;
    }

    // Convert map/range values to a Python list, wrapping time series values (takes by value to handle views)
    template <typename Range> nb::list values_to_list(Range range, const control_block_ptr &cb) {
        nb::list result;
        for (const auto &[_, value] : range) { result.append(wrap_time_series(value, cb)); }
        return result;
    }

    // Convert range items to a Python list of (key, wrapped_value) tuples
    template <typename Iterator> nb::list items_to_list(Iterator begin, Iterator end, const control_block_ptr &cb) {
        nb::list result;
        for (auto it = begin; it != end; ++it) {
            result.append(nb::make_tuple(nb::cast(it->first), wrap_time_series(it->second, cb)));
        }
        return result;
    }

    // Convert map/range items to a Python list of (key, wrapped_value) tuples (takes by value to handle views)
    template <typename Range> nb::list items_to_list(Range range, const control_block_ptr &cb) {
        nb::list result;
        for (const auto &[key, value] : range) { result.append(nb::make_tuple(nb::cast(key), wrap_time_series(value, cb))); }
        return result;
    }

    // Convert a set/collection to a Python list
    template <typename Set> nb::list set_to_list(const Set &set) {
        nb::list result;
        for (const auto &item : set) { result.append(nb::cast(item)); }
        return result;
    }

    // Convert a list/vector of time series to a Python list, wrapping each element
    template <typename Collection> nb::list list_to_list(const Collection &collection, const control_block_ptr &cb) {
        nb::list result;
        for (const auto &item : collection) { result.append(wrap_time_series(item, cb)); }
        return result;
    }

    /**
     * Extract raw Node pointer from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    Node *unwrap_node(const nb::handle &obj);

    /**
     * Extract raw TimeSeriesInput pointer from PyTimeSeriesInput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesInput.
     */
    TimeSeriesInput *unwrap_input(const nb::handle &obj);

    template <typename T> requires std::is_base_of_v<TimeSeriesInput, T> T *unwrap_input_as(const nb::handle &obj) { return dynamic_cast<T *>(unwrap_input(obj)); }

    TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_);

    /**
     * Extract raw TimeSeriesOutput pointer from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    TimeSeriesOutput *unwrap_output(const nb::handle &obj);

    TimeSeriesOutput *unwrap_output(const PyTimeSeriesOutput &output_);

    template <typename T> requires std::is_base_of_v<TimeSeriesOutput, T> T *unwrap_output_as(const nb::handle &obj) { return dynamic_cast<T *>(unwrap_output(obj)); }

    /**
     * Wrap an EvaluationEngineApi pointer in a PyEvaluationEngineApi.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_evaluation_engine_api(const hgraph::EvaluationEngineApi* impl, control_block_ptr control_block);

    /**
     * Wrap an EvaluationClock pointer in a PyEvaluationClock.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_evaluation_clock(const hgraph::EvaluationClock* impl, control_block_ptr control_block);

}  // namespace hgraph

#endif  // HGRAPH_WRAPPER_FACTORY_H
