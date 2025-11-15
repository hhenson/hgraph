#ifndef NON_ASSOCIATIVE_REDUCE_NODE_H
#define NON_ASSOCIATIVE_REDUCE_NODE_H

#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/tsd.h>

namespace hgraph {
    void register_non_associative_reduce_node_with_nanobind(nb::module_ & m);

    /**
     * C++ implementation of PythonTsdNonAssociativeReduceNodeImpl.
     * Non-associative reduce node over a TSD[int, TIME_SERIES_TYPE] input.
     * This makes the assumption that the TSD is in fact representing a dynamically sized list.
     * The reduction is performed by constructing a linear sequence of nodes, with the zero
     * as the input into the first node lhs and the 0th element as the rhs element. From that point on the
     * chain is lhs = prev_node output with rhs being the nth element of the tsd input.
     */
    struct TsdNonAssociativeReduceNode : NestedNode {
        TsdNonAssociativeReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id,
                                    NodeSignature::ptr signature,
                                    nb::dict scalars, graph_builder_ptr nested_graph_builder,
                                    const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id);

        std::unordered_map<int, graph_ptr> &nested_graphs();

    protected:
        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        void do_eval() override {
        };

        void update_changes();

        void extend_nodes_to(int64_t sz);

        void erase_nodes_from(int64_t ndx);

        void bind_output();

        nb::object last_output_value();

        int64_t node_size() const;

        int64_t node_count() const;

        std::vector<node_ptr> get_node(int64_t ndx);

        void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const override;

    private:
        graph_builder_ptr nested_graph_builder_;
        std::tuple<int64_t, int64_t> input_node_ids_; // LHS index, RHS index
        [[maybe_unused]] int64_t output_node_id_; // TODO: Use in implementation
        graph_ptr nested_graph_;
        [[maybe_unused]] int64_t cached_node_size_{-1}; // TODO: Use in implementation
    };
} // namespace hgraph

#endif  // NON_ASSOCIATIVE_REDUCE_NODE_H