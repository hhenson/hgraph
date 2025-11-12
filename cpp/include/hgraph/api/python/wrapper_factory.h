//
// Wrapper Factory - Creates appropriate wrapper based on runtime type inspection
//

#ifndef HGRAPH_WRAPPER_FACTORY_H
#define HGRAPH_WRAPPER_FACTORY_H

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_node.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node_scheduler.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/py_ts_types.h>

namespace hgraph {
    // Forward declarations
    struct Node;
    struct Graph;
    struct NodeScheduler;
    struct TimeSeriesInput;
    struct TimeSeriesOutput;
}

namespace hgraph::api {
    
    /**
     * Wrap a Node pointer in a PyNode.
     * Always returns PyNode (no polymorphism for Node).
     */
    PyNode wrap_node(const hgraph::Node* impl, control_block_ptr control_block);
    
    /**
     * Wrap a Graph pointer in a PyGraph.
     * Always returns PyGraph (no polymorphism for Graph).
     */
    PyGraph wrap_graph(const hgraph::Graph* impl, control_block_ptr control_block);
    
    /**
     * Wrap a NodeScheduler pointer in a PyNodeScheduler.
     * Always returns PyNodeScheduler (no polymorphism for NodeScheduler).
     */
    PyNodeScheduler wrap_node_scheduler(const hgraph::NodeScheduler* impl, control_block_ptr control_block);
    
    /**
     * Wrap a TimeSeriesInput pointer in the appropriate PyTimeSeriesXxxInput wrapper.
     * Uses dynamic_cast to determine actual runtime type and returns specialized wrapper.
     * 
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    PyTimeSeriesInput wrap_input(const hgraph::TimeSeriesInput* impl, control_block_ptr control_block);
    
    /**
     * Wrap a TimeSeriesOutput pointer in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses dynamic_cast to determine actual runtime type and returns specialized wrapper.
     * 
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    PyTimeSeriesOutput wrap_output(const hgraph::TimeSeriesOutput* impl, control_block_ptr control_block);
    
} // namespace hgraph::api

#endif // HGRAPH_WRAPPER_FACTORY_H

