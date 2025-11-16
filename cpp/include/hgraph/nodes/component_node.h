#ifndef COMPONENT_NODE_H
#define COMPONENT_NODE_H

#include <hgraph/nodes/nested_node.h>
#include <optional>

namespace hgraph {
    struct ComponentNode : NestedNode {
        ComponentNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                      nb::dict scalars,
                      graph_builder_ptr nested_graph_builder,
                      const std::unordered_map<std::string, int> &input_node_ids,
                      int output_node_id);

        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void do_eval() override;

        std::unordered_map<int, graph_ptr> nested_graphs() const;

        void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const override;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        void wire_graph();

        void write_inputs();

        virtual void wire_outputs();

        std::pair<std::string, bool> recordable_id();

        graph_builder_ptr m_nested_graph_builder_;
        std::unordered_map<std::string, int> m_input_node_ids_;
        int m_output_node_id_;
        graph_ptr m_active_graph_;
        std::optional<engine_time_t> m_last_evaluation_time_;
    };
} // namespace hgraph

#endif  // COMPONENT_NODE_H