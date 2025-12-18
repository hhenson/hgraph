#ifndef NESTED_NODE_H
#define NESTED_NODE_H

#include <hgraph/types/node.h>
#include <chrono>
#include <functional>
#include <memory>
#include <vector>

namespace hgraph {

    struct NestedNode : Node {
        using ptr = NestedNode*;
        using s_ptr = std::shared_ptr<NestedNode>;
        using Node::Node;

        void start() override;

        engine_time_t last_evaluation_time() const;

        void mark_evaluated();

        virtual void enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const = 0;

        VISITOR_SUPPORT()

    private:
        engine_time_t _last_evaluation_time{MIN_DT};
    };
} // namespace hgraph

#endif  // NESTED_NODE_H