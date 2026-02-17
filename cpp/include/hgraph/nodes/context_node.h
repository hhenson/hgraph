#ifndef CONTEXT_NODE_H
#define CONTEXT_NODE_H

#include <hgraph/types/node.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/python/global_state.h>

namespace hgraph {
    struct ContextStubSourceNode final : Node {
        using Node::Node;

        ~ContextStubSourceNode() override = default;

        VISITOR_SUPPORT()

    protected:
        void initialise() override {
        } // No-op initialisation
        void dispose() override {
        } // No-op disposal
        void do_start() override;

        void do_stop() override;

        void do_eval() override;

    private:
        LinkTarget _subscribed_link{};
        engine_time_t _owner_time{MIN_DT};
    };

    void register_context_node_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif // CONTEXT_NODE_H
