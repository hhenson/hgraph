#ifndef CONTEXT_NODE_H
#define CONTEXT_NODE_H

#include <hgraph/types/node.h>
#include <hgraph/python/global_state.h>

namespace hgraph {
    struct ContextStubSourceNode : Node {
        using Node::Node;

        ~ContextStubSourceNode() override = default;

    protected:
        void initialise() override {
        } // No-op initialisation
        void dispose() override {
        } // No-op disposal
        void do_start() override;

        void do_stop() override;

        void do_eval() override;

    private:
        time_series_output_ptr _subscribed_output{};
    };

    void register_context_node_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif // CONTEXT_NODE_H