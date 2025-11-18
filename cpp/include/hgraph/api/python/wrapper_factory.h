//
// Wrapper Factory - Creates appropriate wrapper based on runtime type inspection
//

#ifndef HGRAPH_WRAPPER_FACTORY_H
#define HGRAPH_WRAPPER_FACTORY_H

#include "py_time_series.h"

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>

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
    nb::object wrap_input(const hgraph::TimeSeriesInput *impl, control_block_ptr control_block);
    nb::object wrap_input(const TimeSeriesInput *impl);

    /**
     * Wrap a TimeSeriesOutput pointer in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     * Caches the created wrapper for future use.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl, control_block_ptr control_block);

    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl);

    /**
     * Extract raw Node pointer from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    Node *unwrap_node(const nb::object &obj);

    /**
     * Extract raw TimeSeriesInput pointer from PyTimeSeriesInput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesInput.
     */
    TimeSeriesInput *unwrap_input(const nb::object &obj);

    TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_);

    /**
     * Extract raw TimeSeriesOutput pointer from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    TimeSeriesOutput *unwrap_output(const nb::object &obj);

    TimeSeriesOutput *unwrap_output(const PyTimeSeriesOutput &output_);
    //
    // /**
    //  * Wrap an EvaluationEngineApi pointer in a PyEvaluationEngineApi.
    //  * Uses cached Python wrapper if available (via intrusive_base::self_py()).
    //  * Creates and caches new wrapper if not.
    //  */
    // nb::object wrap_evaluation_engine_api(const hgraph::EvaluationEngineApi* impl, control_block_ptr control_block);
    //
    // /**
    //  * Wrap an EvaluationClock pointer in a PyEvaluationClock.
    //  * Uses cached Python wrapper if available (via intrusive_base::self_py()).
    //  * Creates and caches new wrapper if not.
    //  */
    // nb::object wrap_evaluation_clock(const hgraph::EvaluationClock* impl, control_block_ptr control_block);

}  // namespace hgraph

#endif  // HGRAPH_WRAPPER_FACTORY_H
