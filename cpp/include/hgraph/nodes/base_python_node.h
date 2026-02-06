//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H
#define HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H

#include <hgraph/types/node.h>

namespace hgraph {
    /**
     * BasePythonNode - Base class for Python-based compute nodes
     *
     * This handles the common functionality for nodes implemented in Python:
     * - Managing Python callable functions (eval, start, stop)
     * - Initializing kwargs from scalars and inputs
     * - Managing context inputs (Python context managers)
     * - Initializing recordable state
     *
     * Concrete implementations:
     * - PythonNode: Standard Python compute nodes
     * - PythonGeneratorNode: Generator-based nodes
     */
    struct BasePythonNode : Node {
        /// Constructor - creates TSInput/TSOutput from TSMeta
        BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn,
                       const TSMeta* input_meta, const TSMeta* output_meta,
                       const TSMeta* error_output_meta = nullptr, const TSMeta* recordable_state_meta = nullptr);

        void _initialise_kwargs();

        void _initialise_kwarg_inputs();

        void _initialise_state();

        VISITOR_SUPPORT()

    protected:
        void do_eval() override;

        void do_start() override;

        void do_stop() override;

        void initialise() override;

        void start() override;

        void dispose() override;

        /// Collect TSView pointers from a wrapped Python time-series object
        /// so they can be updated with the current tick time before each eval.
        void _cache_view_pointers(const nb::object& wrapped);

        /// Update all cached TSView times to the current evaluation time.
        void _update_cached_view_times();

        nb::callable _eval_fn;
        nb::callable _start_fn;
        nb::callable _stop_fn;

        nb::kwargs _kwargs;
        std::vector<TSView*> _cached_views;  ///< Pointers into kwarg wrappers for fast time update
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H