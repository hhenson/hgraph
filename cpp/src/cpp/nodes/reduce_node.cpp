#include <hgraph/types/tss.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <algorithm>
#include <deque>
#include <utility>

namespace hgraph {
    // Helper function for key comparison
    template<typename K>
    inline bool keys_equal(const K &a, const K &b) { return a == b; }

    // Specialization for nb::object
    template<>
    inline bool keys_equal<nb::object>(const nb::object &a, const nb::object &b) { return a.equal(b); }

    template<typename K>
    ReduceNode<K>::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                              nb::dict scalars, graph_builder_ptr nested_graph_builder,
                              const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {
    }

    template<typename K>
    std::unordered_map<int, graph_ptr> &ReduceNode<K>::nested_graphs() {
        static std::unordered_map<int, graph_ptr> graphs;
        graphs[0] = nested_graph_;
        return graphs;
    }

    template<typename K>
    void ReduceNode<K>::enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    template<typename K>
    typename TimeSeriesDictInput_T<K>::ptr ReduceNode<K>::ts() {
        return dynamic_cast<TimeSeriesDictInput_T<K> *>((*input())[0].get());
    }

    template<typename K>
    time_series_reference_input_ptr ReduceNode<K>::zero() {
        return dynamic_cast<TimeSeriesReferenceInput *>((*input())[1].get());
    }

    template<typename K>
    void ReduceNode<K>::initialise() {
        nested_graph_ = new Graph(std::vector<int64_t>{node_ndx()}, std::vector<node_ptr>{}, this, "", new Traits());
        nested_graph_->set_evaluation_engine(new NestedEvaluationEngine(
            graph()->evaluation_engine(), new NestedEngineEvaluationClock(graph()->evaluation_engine_clock(), this)));
        initialise_component(*nested_graph_);
    }

    template<typename K>
    void ReduceNode<K>::do_start() {
        auto tsd{ts()};
        if (tsd->valid()) {
            // Get all keys that are valid but NOT added (i.e., keys present before start)
            // This matches Python: keys = key_set.valid - key_set.added
            std::unordered_set<K> keys;
            const auto &key_set{tsd->key_set_t()};
            for (const auto &key: key_set.value()) {
                if (!key_set.was_added(key)) { keys.insert(key); }
            }

            if (!keys.empty()) {
                add_nodes(keys); // If there are already inputs, then add the keys.
            } else {
                grow_tree();
            }
        } else {
            grow_tree();
        }
        start_component(*nested_graph_);
    }

    template<typename K>
    void ReduceNode<K>::do_stop() { stop_component(*nested_graph_); }

    template<typename K>
    void ReduceNode<K>::dispose() {
        if (nested_graph_ == nullptr) { return; }
        dispose_component(*nested_graph_);
        nested_graph_ = nullptr;
    }

    template<typename K>
    void ReduceNode<K>::eval() {
        mark_evaluated();

        auto &key_set = ts()->key_set_t();

        // Process removals first, then additions
        remove_nodes(key_set.removed());
        add_nodes(key_set.added());

        // Re-balance the tree if required
        re_balance_nodes();

        // Evaluate the nested graph
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        nested_graph_->evaluate_graph();

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        // Propagate output if changed
        auto l = dynamic_cast<TimeSeriesReferenceOutput *>(last_output().get());
        auto o = dynamic_cast<TimeSeriesReferenceOutput *>(output().get());

        // Since l is the last output and o is the main output, they are different TimeSeriesReferenceOutput objects
        // We need to compare their values (both are TimeSeriesReference values)
        bool values_equal = o->valid() && l->valid() && o->has_value() && l->has_value() && (o->value() == l->value());
        // Python reference (reference/hgraph/src/hgraph/_impl/_runtime/_reduce_node.py lines 109-112):
        // if (not o.valid and l.valid) or (l.valid and o.value != l.value): o.value = l.value
        // Do not propagate solely on l->modified(); only propagate when pointer changes or initial assignment.
        if ((l->valid() && !o->valid()) || (l->valid() && !values_equal)) { o->set_value(l->value()); }
    }

    template<typename K>
    TimeSeriesOutput::ptr ReduceNode<K>::last_output() {
        auto root_ndx = node_count() - 1;
        auto sub_graph = get_node(root_ndx);
        auto out_node = sub_graph[output_node_id_];
        return out_node->output();
    }

    template<typename K>
    void ReduceNode<K>::add_nodes(const std::unordered_set<K> &keys) {
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

    template<typename K>
    void ReduceNode<K>::remove_nodes(const std::unordered_set<K> &keys) {
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
                    K max_key = max_it->first;
                    auto max_ndx = max_it->second;

                    // Match Python: only swap if max is in a HIGHER layer
                    // Python: if next_largest[1][0] > ndx[0]
                    if (std::get < 0 > (max_ndx) > std::get < 0 > (ndx)) {
                        swap_node(ndx, max_ndx);
                        bound_node_indexes_[max_key] = ndx;
                        ndx = max_ndx;
                    }
                }
                free_node_indexes_.push_back(ndx);
                zero_node(ndx);
            }
        }
    }

    template<typename K>
    void ReduceNode<K>::swap_node(const std::tuple<int64_t, int64_t> &src_ndx,
                                  const std::tuple<int64_t, int64_t> &dst_ndx) {
        auto [src_node_id, src_side] = src_ndx;
        auto [dst_node_id, dst_side] = dst_ndx;

        auto src_nodes = get_node(src_node_id);
        auto dst_nodes = get_node(dst_node_id);

        auto src_node = src_nodes[src_side];
        auto dst_node = dst_nodes[dst_side];

        // Get the old inputs before swapping
        auto src_input = (*src_node->input())[0];
        auto dst_input = (*dst_node->input())[0];

        // Swap the inputs by creating new input bundles
        src_node->reset_input(src_node->input()->copy_with(src_node.get(), {dst_input.get()}));
        dst_node->reset_input(dst_node->input()->copy_with(dst_node.get(), {src_input.get()}));

        // Re-parent the inputs to their new parent bundles (CRITICAL FIX - Python lines 159-160)
        // Cast to TimeSeriesType::ptr for re_parent
        dst_input->re_parent(src_node->input().get());
        src_input->re_parent(dst_node->input().get());

        src_node->notify();
        dst_node->notify();
    }

    template<typename K>
    void ReduceNode<K>::re_balance_nodes() {
        if (node_count() > 8 && (free_node_indexes_.size() * 0.75) > bound_node_indexes_.size()) { shrink_tree(); }
    }

    template<typename K>
    void ReduceNode<K>::grow_tree() {
        int64_t count = node_count();
        int64_t end = 2 * count + 1;
        int64_t top_layer_length = (end + 1) / 4;
        int64_t top_layer_end = std::max(count + top_layer_length, static_cast<int64_t>(1));
        int64_t last_node = end - 1;

        std::deque<int64_t> un_bound_outputs;
        std::vector<int64_t> wiring_info; // Nodes that need wiring after start

        for (int64_t i = count; i < end; ++i) {
            un_bound_outputs.push_back(i);
            nested_graph_->extend_graph(*nested_graph_builder_, true);

            if (i < top_layer_end) {
                auto ndx_lhs = std::make_tuple(i, std::get < 0 > (input_node_ids_));
                free_node_indexes_.push_back(ndx_lhs);
                zero_node(ndx_lhs);

                auto ndx_rhs = std::make_tuple(i, std::get < 1 > (input_node_ids_));
                free_node_indexes_.push_back(ndx_rhs);
                zero_node(ndx_rhs);
            } else {
                // Defer wiring until after nodes are started
                wiring_info.push_back(i);
            }
        }

        // Wire the nodes that need wiring
        for (auto i: wiring_info) {
            TimeSeriesOutput::ptr left_parent;
            TimeSeriesOutput::ptr right_parent;

            if (i < last_node) {
                auto left_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                left_parent = get_node(left_idx)[output_node_id_]->output();

                auto right_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                right_parent = get_node(right_idx)[output_node_id_]->output();
            } else {
                auto old_root = get_node(count - 1)[output_node_id_];
                left_parent = old_root->output();

                auto new_root_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                auto new_root = get_node(new_root_idx)[output_node_id_];
                right_parent = new_root->output();
            }

            auto sub_graph = get_node(i);
            auto lhs_input = sub_graph[std::get < 0 > (input_node_ids_)];
            auto rhs_input = sub_graph[std::get < 1 > (input_node_ids_)];

            dynamic_cast<TimeSeriesInput &>(*(*lhs_input->input())[0]).bind_output(left_parent.get());
            dynamic_cast<TimeSeriesInput &>(*(*rhs_input->input())[0]).bind_output(right_parent.get());

            lhs_input->notify();
            rhs_input->notify();
        }

        // Start the newly added nodes AFTER wiring them (matches Python line 272-273)
        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            int64_t start_idx = count * node_size();
            int64_t end_idx = nested_graph_->nodes().size();
            nested_graph_->start_subgraph(start_idx, end_idx);
        }

        // Sort free list in descending order so .pop_back() gives LOWEST positions first
        // This maintains the left-based invariant: keys are always added to leftmost available positions
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a > b; });
    }

    template<typename K>
    void ReduceNode<K>::shrink_tree() {
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

    template<typename K>
    void ReduceNode<K>::bind_key_to_node(const K &key, const std::tuple<int64_t, int64_t> &ndx) {
        bound_node_indexes_[key] = ndx;
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        auto node = nodes[side];

        // Get the time series input from the TSD for this key
        auto ts_ = (*ts())[key];

        // Check what's currently at this position before binding
        auto old_input = (*node->input())[0];

        // If the old input is in bound_to_key_flags_, we need to remove it
        // This can happen if we're re-binding to a position that was swapped with another bound position
        if (bound_to_key_flags_.contains(old_input.get()) && old_input.get() != ts_.get()) {
            bound_to_key_flags_.erase(old_input.get());
        }

        // Create new input bundle with the ts (Python line 198)
        node->reset_input(node->input()->copy_with(node.get(), {ts_.get()}));

        // Re-parent the ts to the node's input (CRITICAL FIX - Python line 200)
        ts_->re_parent(node->input().get());

        // Make the time series active (CRITICAL FIX - Python line 201)
        ts_->make_active();

        // Track that this input is bound to a key (Python: ts._bound_to_key = True)
        // Note: insert() is idempotent - if already present, it does nothing
        bound_to_key_flags_.insert(ts_.get());

        node->notify();
    }

    template<typename K>
    void ReduceNode<K>::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        auto node = nodes[side];

        // Get the current input
        auto inner_input = (*node->input())[0];

        // Check if this input is bound to a key (from TSD) by checking if it's in bound_to_key_flags_
        // If not in flags, it's just an unbound reference that we created, so we can reuse it
        if (bound_to_key_flags_.contains(inner_input.get())) {
            // This input was bound to a key, so we need to:
            // 1. Remove it from our tracking set
            // 2. Re-parent it back to the TSD for cleanup
            // 3. Create a new unbound reference input for this node
            bound_to_key_flags_.erase(inner_input.get());
            inner_input->re_parent(ts().get());

            auto new_ref_input = new TimeSeriesReferenceInput(node.get());
            node->reset_input(node->input()->copy_with(node.get(), {new_ref_input}));
            new_ref_input->re_parent(node->input().get());
            new_ref_input->clone_binding(zero());
        } else {
            // This input is not bound to a key (it's an unbound reference we created),
            // so we can just clone the zero binding without creating a new input
            auto inner_ref = dynamic_cast<TimeSeriesReferenceInput *>(inner_input.get());
            inner_ref->clone_binding(zero());
        }

        node->notify();
    }

    template<typename K>
    int64_t ReduceNode<K>::node_size() const { return nested_graph_builder_->node_builders.size(); }

    template<typename K>
    int64_t ReduceNode<K>::node_count() const { return nested_graph_->nodes().size() / node_size(); }

    template<typename K>
    std::vector<node_ptr> ReduceNode<K>::get_node(int64_t ndx) {
        // This should be cleaned up to return a view over the existing nodes.
        auto &all_nodes = nested_graph_->nodes();
        int64_t ns = node_size();
        int64_t start = ndx * ns;
        int64_t end = start + ns;
        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    template<typename K>
    const graph_ptr &ReduceNode<K>::nested_graph() const { return nested_graph_; }

    template<typename K>
    const std::tuple<int64_t, int64_t> &ReduceNode<K>::input_node_ids() const { return input_node_ids_; }

    template<typename K>
    int64_t ReduceNode<K>::output_node_id() const { return output_node_id_; }

    template<typename K>
    const std::unordered_map<K, std::tuple<int64_t, int64_t> > &ReduceNode<K>::bound_node_indexes() const {
        return bound_node_indexes_;
    }

    template<typename K>
    const std::vector<std::tuple<int64_t, int64_t> > &ReduceNode<K>::free_node_indexes() const {
        return free_node_indexes_;
    }

    // Explicit template instantiations for supported key types
    template struct ReduceNode<bool>;
    template struct ReduceNode<int64_t>;
    template struct ReduceNode<double>;
    template struct ReduceNode<engine_date_t>;
    template struct ReduceNode<engine_time_t>;
    template struct ReduceNode<engine_time_delta_t>;
    template struct ReduceNode<nb::object>;

    // Template function to register ReduceNode<K> with nanobind
    template<typename K>
    void register_reduce_node_type(nb::module_ &m, const char *class_name) {
        nb::class_<ReduceNode<K>, NestedNode>(m, class_name)
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::tuple<int64_t, int64_t> &, int64_t>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a)
                .def_prop_ro("nested_graph", &ReduceNode<K>::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode<K>::nested_graphs)
                .def_prop_ro("ts", &ReduceNode<K>::ts)
                .def_prop_ro("zero", &ReduceNode<K>::zero)
                .def_prop_ro("input_node_ids", &ReduceNode<K>::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode<K>::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode<K>::bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode<K>::free_node_indexes);
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        register_reduce_node_type<bool>(m, "ReduceNode_bool");
        register_reduce_node_type<int64_t>(m, "ReduceNode_int");
        register_reduce_node_type<double>(m, "ReduceNode_float");
        register_reduce_node_type<engine_date_t>(m, "ReduceNode_date");
        register_reduce_node_type<engine_time_t>(m, "ReduceNode_datetime");
        register_reduce_node_type<engine_time_delta_t>(m, "ReduceNode_timedelta");
        register_reduce_node_type<nb::object>(m, "ReduceNode_object");
    }
} // namespace hgraph