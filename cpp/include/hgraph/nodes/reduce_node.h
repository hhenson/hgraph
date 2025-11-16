#ifndef REDUCE_NODE_H
#define REDUCE_NODE_H

#include <deque>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/tsd.h>

namespace hgraph {
    void register_reduce_node_with_nanobind(nb::module_ & m);

    template<typename K>
    struct ReduceNode;
    template<typename K>
    using reduce_node_ptr = nb::ref<ReduceNode<K> >;

    /**
     * C++ implementation of PythonReduceNodeImpl.
     * This implements TSD reduction using an inverted binary tree with inputs at the leaves
     * and the result at the root. The inputs bound to the leaves can be moved as nodes come and go.
     */
    template<typename K>
    struct ReduceNode : NestedNode {
        ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                   nb::dict scalars,
                   graph_builder_ptr nested_graph_builder, const std::tuple<int64_t, int64_t> &input_node_ids,
                   int64_t output_node_id);

        std::unordered_map<int, graph_ptr> &nested_graphs();

        TimeSeriesDictInput_T<K>::ptr ts();

        time_series_reference_input_ptr zero();

        // Expose attributes to allow us to more easily inspect the state in Python
        // Can make debugging easier.
        const graph_ptr &nested_graph() const;

        const std::tuple<int64_t, int64_t> &input_node_ids() const;

        int64_t output_node_id() const;

        const std::unordered_map<K, std::tuple<int64_t, int64_t> > &bound_node_indexes() const;

        const std::vector<std::tuple<int64_t, int64_t> > &free_node_indexes() const;

        void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const override;

    protected:
        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        void do_eval() override {
        };

        TimeSeriesOutput::ptr last_output();

        void add_nodes(const std::unordered_set<K> &keys);

        void remove_nodes(const std::unordered_set<K> &keys);

        void re_balance_nodes();

        void grow_tree();

        void shrink_tree();

        void bind_key_to_node(const K &key, const std::tuple<int64_t, int64_t> &ndx);

        void zero_node(const std::tuple<int64_t, int64_t> &ndx);

        void swap_node(const std::tuple<int64_t, int64_t> &src_ndx, const std::tuple<int64_t, int64_t> &dst_ndx);

        int64_t node_size() const;

        int64_t node_count() const;

        std::vector<node_ptr> get_node(int64_t ndx);

    private:
        graph_ptr nested_graph_;
        graph_builder_ptr nested_graph_builder_;
        std::tuple<int64_t, int64_t> input_node_ids_; // LHS index, RHS index
        int64_t output_node_id_;
        std::unordered_map<K, std::tuple<int64_t, int64_t> > bound_node_indexes_;
        std::vector<std::tuple<int64_t, int64_t> > free_node_indexes_; // List of (ndx, 0(lhs)|1(rhs)) tuples

        // The python code uses the fact that you can randomly add properties to a python object and tracks
        // if an input is bound to a key or not using _bound_to_key.
        // C++ does not do that, so we can track if the ts is bound to a key using a set.
        std::unordered_set<TimeSeriesInput *> bound_to_key_flags_;
    };
} // namespace hgraph

#endif  // REDUCE_NODE_H