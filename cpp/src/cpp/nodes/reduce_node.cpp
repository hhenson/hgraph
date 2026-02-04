#include <hgraph/types/tss.h>
#include <hgraph/types/value/value.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <algorithm>
#include <deque>
#include <utility>

namespace hgraph {
    ReduceNode::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {
    }

    std::unordered_map<int, graph_s_ptr> &ReduceNode::nested_graphs() {
        static std::unordered_map<int, graph_s_ptr> graphs;
        graphs[0] = nested_graph_;
        return graphs;
    }

    void ReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    TimeSeriesDictInputImpl::ptr ReduceNode::ts() {
        // TODO: Convert to TSInput-based approach
        // This should access the TSD field from TSInputView
        // For now, stub with exception
        throw std::runtime_error("ReduceNode::ts needs TSInput conversion - TODO");
    }

    time_series_reference_input_ptr ReduceNode::zero() {
        // TODO: Convert to TSInput-based approach
        // This should access the zero REF field from TSInputView
        // For now, stub with exception
        throw std::runtime_error("ReduceNode::zero needs TSInput conversion - TODO");
    }

    void ReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void ReduceNode::do_start() {
        // TODO: Convert to TSInput-based approach
        // This method needs to:
        // 1. Access the TSD input via TSInputView
        // 2. Check if TSD is valid
        // 3. Get keys from the TSD key_set and iterate
        // 4. Call add_nodes_from_views or grow_tree
        // For now, just grow tree and start
        grow_tree();
        start_component(*nested_graph_);
    }

    void ReduceNode::do_stop() { stop_component(*nested_graph_); }

    void ReduceNode::dispose() {
        if (nested_graph_ == nullptr) { return; }
        dispose_component(*nested_graph_);
        nested_graph_ = nullptr;
    }

    void ReduceNode::eval() {
        mark_evaluated();

        // TODO: Convert to TSInput/TSOutput-based approach
        // This method needs to:
        // 1. Access the TSD input via TSInputView to get key_set changes
        // 2. Process removed keys and added keys
        // 3. Re-balance tree
        // 4. Evaluate nested graph
        // 5. Propagate output using TSOutput

        // Evaluate the nested graph
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        nested_graph_->evaluate_graph();

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        // TODO: Propagate output - needs TSOutput-based approach for REF types
    }

    TimeSeriesOutput::s_ptr ReduceNode::last_output() {
        // TODO: Convert to TSOutput-based approach
        // This returns the output of the last (root) node in the reduction tree
        // Needs inner node TSOutput access
        throw std::runtime_error("ReduceNode::last_output needs TSOutput conversion - TODO");
    }

    void ReduceNode::add_nodes_from_views(const std::vector<value::View> &keys) {
        // Grow the tree upfront if needed, to avoid growing while binding
        // This ensures the tree structure is consistent before we start binding keys
        while (free_node_indexes_.size() < keys.size()) { grow_tree(); }

        // Note: free_node_indexes_ is sorted in descending order, so .back() gives the LOWEST position
        // This maintains the left-based invariant: keys are always added to leftmost available positions
        for (const auto &key: keys) {
            auto ndx = free_node_indexes_.back();
            free_node_indexes_.pop_back();
            bind_key_to_node(key, ndx);
        }
    }

    void ReduceNode::remove_nodes_from_views(const std::vector<value::View> &keys) {
        for (const auto &key: keys) {
            if (auto it = bound_node_indexes_.find(key); it != bound_node_indexes_.end()) {
                auto ndx = it->second;
                bound_node_indexes_.erase(it);

                if (!bound_node_indexes_.empty()) {
                    // Find the largest bound index (comparing entire tuple lexicographically)
                    auto max_it = std::max_element(bound_node_indexes_.begin(), bound_node_indexes_.end(),
                                                   [](const auto &a, const auto &b) { return a.second < b.second; });

                    // CRITICAL: Save the key and position BEFORE modifying the map, as modifying the map
                    // may invalidate the iterator or cause a rehash
                    value::PlainValue max_key = max_it->first.const_view().clone();  // Clone the key (PlainValue is move-only)
                    auto max_ndx = max_it->second;

                    // Match Python: only swap if max is in a HIGHER layer
                    // Python: if next_largest[1][0] > ndx[0]
                    if (std::get<0>(max_ndx) > std::get<0>(ndx)) {
                        swap_node(ndx, max_ndx);
                        bound_node_indexes_[std::move(max_key)] = ndx;
                        ndx = max_ndx;
                    }
                }
                free_node_indexes_.push_back(ndx);
                zero_node(ndx);
            }
        }
    }

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t> &src_ndx,
                               const std::tuple<int64_t, int64_t> &dst_ndx) {
        // TODO: Convert to TSInput-based approach for cross-graph wiring
        // This method swaps input bindings between two positions in the reduction tree
        // Requires:
        // - Inner node TSInput access
        // - Re-binding/re-parenting semantics
        throw std::runtime_error("ReduceNode::swap_node needs TSInput conversion - TODO");
    }

    void ReduceNode::re_balance_nodes() {
        if (node_count() > 8 && (free_node_indexes_.size() * 0.75) > bound_node_indexes_.size()) { shrink_tree(); }
    }

    void ReduceNode::grow_tree() {
        // TODO: Convert to TSInput/TSOutput-based approach for cross-graph wiring
        // This method:
        // 1. Extends the nested graph with new nodes
        // 2. Sets up leaf nodes with zero bindings
        // 3. Wires interior nodes to their children's outputs
        // Requires:
        // - Inner node TSInput/TSOutput access
        // - bind_output semantics for inner nodes
        throw std::runtime_error("ReduceNode::grow_tree needs TSInput/TSOutput conversion - TODO");
    }

    void ReduceNode::shrink_tree() {
        int64_t capacity = bound_node_indexes_.size() + free_node_indexes_.size();
        if (capacity <= 8) { return; }

        int64_t halved_capacity = capacity / 2;
        int64_t active_count = bound_node_indexes_.size();
        if (halved_capacity < active_count) { return; }

        int64_t last_node = (node_count() - 1) / 2;
        int64_t start = last_node;

        // Delete the high nodes - the left-based invariant ensures no bound keys are in nodes >= start
        nested_graph_->reduce_graph(start * node_size());

        // Keep only the low free positions (first halved_capacity - active_count when sorted)
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a < b; });

        int64_t to_keep = halved_capacity - active_count;
        if (static_cast<size_t>(to_keep) < free_node_indexes_.size()) { free_node_indexes_.resize(to_keep); }

        // Reverse sort so .pop_back() gives lowest positions (maintains left-based invariant)
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a > b; });
    }

    void ReduceNode::bind_key_to_node(const value::View &key, const std::tuple<int64_t, int64_t> &ndx) {
        // TODO: Convert to TSInput-based approach
        // This method binds a TSD key's time series to a leaf node position
        // Requires:
        // - TSInputView access to TSD elements by key
        // - Inner node TSInput access for re-binding
        // - make_active semantics
        throw std::runtime_error("ReduceNode::bind_key_to_node needs TSInput conversion - TODO");
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        // TODO: Convert to TSInput-based approach
        // This method resets a leaf node position to use the zero reference
        // Requires:
        // - Inner node TSInput access
        // - make_passive/make_active semantics
        // - Re-binding to zero reference
        throw std::runtime_error("ReduceNode::zero_node needs TSInput conversion - TODO");
    }

    int64_t ReduceNode::node_size() const { return nested_graph_builder_->node_builders.size(); }

    int64_t ReduceNode::node_count() const { return nested_graph_->nodes().size() / node_size(); }

    std::vector<node_s_ptr> ReduceNode::get_node(int64_t ndx) {
        // This should be cleaned up to return a view over the existing nodes.
        auto &all_nodes = nested_graph_->nodes();
        int64_t ns = node_size();
        int64_t start = ndx * ns;
        int64_t end = start + ns;
        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    const graph_s_ptr &ReduceNode::nested_graph() const { return nested_graph_; }

    const std::tuple<int64_t, int64_t> &ReduceNode::input_node_ids() const { return input_node_ids_; }

    int64_t ReduceNode::output_node_id() const { return output_node_id_; }

    nb::dict ReduceNode::py_bound_node_indexes() const {
        nb::dict result;
        auto* tsd = const_cast<ReduceNode*>(this)->ts();
        const auto* key_schema = tsd->key_type_meta();
        for (const auto& [key, ndx] : bound_node_indexes_) {
            // Convert PlainValue key to Python using TypeMeta
            nb::object py_key = key_schema->ops->to_python(key.data(), key_schema);
            result[py_key] = nb::make_tuple(std::get<0>(ndx), std::get<1>(ndx));
        }
        return result;
    }

    const std::vector<std::tuple<int64_t, int64_t> > &ReduceNode::free_node_indexes() const {
        return free_node_indexes_;
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
                // Constructor not exposed to Python - nodes are created via builders
                .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
                // ts and zero removed - need TSInput conversion
                .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
} // namespace hgraph
