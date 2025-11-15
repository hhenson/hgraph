#ifndef NESTED_NODE_H
#define NESTED_NODE_H

#include <hgraph/types/node.h>
#include <chrono>
#include <functional>
#include <memory>
#include <vector>

namespace hgraph {
    struct Graph;
    using graph_ptr = nb::ref<Graph>;

    struct NestedNode : Node {
        using ptr = nb::ref<NestedNode>;
        using Node::Node;

        void start() override;

        engine_time_t last_evaluation_time() const;

        void mark_evaluated();

        virtual void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const = 0;

        static void register_with_nanobind(nb::module_ &m);

    private:
        engine_time_t _last_evaluation_time{MIN_DT};
    };
} // namespace hgraph

#endif  // NESTED_NODE_H