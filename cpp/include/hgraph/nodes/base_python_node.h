//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H
#define HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H

#include <hgraph/types/node.h>
#include <vector>

namespace hgraph {
    class TSInputView;
    class TSOutputView;
    class TSView;

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
        struct WrappedInputRef {
            nb::object owner;
        };

        struct WrappedOutputRef {
            nb::object owner;
        };

        BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                       const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                       nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn);

        void _initialise_kwargs();

        void _initialise_kwarg_inputs();

        void _initialise_state();

        VISITOR_SUPPORT()

    protected:
        void _index_kwarg_time_views();

        void do_eval() override;

        void do_start() override;

        void do_stop() override;

        void initialise() override;

        void start() override;

        void dispose() override;

        void _refresh_kwarg_time_views();

        nb::callable _eval_fn;
        nb::callable _start_fn;
        nb::callable _stop_fn;

        nb::kwargs _kwargs;
        std::vector<WrappedInputRef> _kwarg_wrapped_inputs;
        std::vector<WrappedOutputRef> _kwarg_wrapped_outputs;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_BASE_PYTHON_NODE_H
