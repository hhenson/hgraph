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
                           nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
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
        return dynamic_cast<TimeSeriesDictInputImpl *>((*input())[0].get());
    }

    time_series_reference_input_ptr ReduceNode::zero() {
        return dynamic_cast<TimeSeriesReferenceInput *>((*input())[1].get());
    }

    void ReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void ReduceNode::do_start() {
        auto tsd{ts()};
        if (tsd->valid()) {
            // Get all keys that are valid but NOT added (i.e., keys present before start)
            // This matches Python: keys = key_set.valid - key_set.added
            std::vector<value::View> keys;
            auto &key_set = tsd->key_set();
            for (auto elem : key_set.value_view()) {
                if (!key_set.was_added(elem)) {
                    keys.push_back(elem);
                }
            }

            if (!keys.empty()) {
                add_nodes_from_views(keys); // If there are already inputs, then add the keys.
            } else {
                grow_tree();
            }
        } else {
            grow_tree();
        }
        start_component(*nested_graph_);
    }

    void ReduceNode::do_stop() { stop_component(*nested_graph_); }

    void ReduceNode::dispose() {
        if (nested_graph_ == nullptr) { return; }
        nested_graph_builder_->release_instance(nested_graph_);
    }

    void ReduceNode::eval() {
        mark_evaluated();

        auto tsd = ts();
        auto &key_set = tsd->key_set();

        // Process removals first, then additions (matches Python: remove then add to reduce
        // the possibility of growing the tree just to tear it down again)
        auto removed_keys = key_set.collect_removed();
        remove_nodes_from_views(removed_keys);

        // When the upstream REF chain becomes empty (e.g. if_ switches to False),
        // the cascading un_bind_output may not perfectly report all key removals
        // through the key_set. Detect this by checking if the accessor's input is
        // empty/unbound, and if so remove all bound keys.
        // NOTE: We do NOT check individual key ref values - empty refs on individual
        // keys are valid (e.g. when reduce lambda uses default() to handle them).
        bool all_keys_stale = false;
        if (!tsd->has_output()) {
            all_keys_stale = true;
        } else if (tsd->output()) {
            auto tsd_output = tsd->output();
            if (tsd_output->has_owning_node()) {
                auto accessor_node = tsd_output->owning_node();
                const auto& accessor_input = accessor_node->input();
                if (accessor_input) {
                    for (size_t i = 0; i < accessor_input->size(); ++i) {
                        auto input_item = (*accessor_input)[i];
                        if (auto ref_input = dynamic_cast<TimeSeriesReferenceInput*>(input_item.get())) {
                            if (ref_input->value().is_empty()) {
                                all_keys_stale = true;
                                break;
                            }
                        }
                        if (auto tsd_input = dynamic_cast<TimeSeriesDictInput*>(input_item.get())) {
                            if (!tsd_input->has_output()) {
                                all_keys_stale = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (!bound_node_indexes_.empty()) {
            std::vector<value::View> stale_keys;
            if (all_keys_stale) {
                for (const auto &[key, ndx] : bound_node_indexes_) {
                    stale_keys.push_back(key.view());
                }
            } else {
                // Also catch keys individually missing from the TSD
                for (const auto &[key, ndx] : bound_node_indexes_) {
                    if (!tsd->contains(key.view())) {
                        stale_keys.push_back(key.view());
                    }
                }
            }
            if (!stale_keys.empty()) {
                remove_nodes_from_views(stale_keys);
            }
        }

        auto added_keys = key_set.collect_added();
        add_nodes_from_views(added_keys);

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
        auto l_output = last_output();
        auto l = dynamic_cast<TimeSeriesReferenceOutput *>(l_output.get());
        auto o = dynamic_cast<TimeSeriesReferenceOutput *>(output().get());

        bool values_equal = o->valid() && l->valid() && o->has_value() && l->has_value() && (o->value() == l->value());

        // Python: if (not o.valid and l.valid) or (l.valid and o.value != l.value): o.value = l.value
        if ((l->valid() && !o->valid()) || (l->valid() && !values_equal)) {
            o->set_value(l->value());
        }
    }

    TimeSeriesOutput::s_ptr ReduceNode::last_output() {
        auto root_ndx = node_count() - 1;
        auto sub_graph = get_node(root_ndx);
        auto out_node = sub_graph[output_node_id_];
        return out_node->output();
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
                    value::Value max_key = max_it->first.view().clone();  // Clone the key (Value is move-only)
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
        src_node->reset_input(src_node->input()->copy_with(src_node.get(), {dst_input}));
        dst_node->reset_input(dst_node->input()->copy_with(dst_node.get(), {src_input}));

        // Re-parent the inputs to their new parent bundles (CRITICAL FIX - Python lines 159-160)
        dst_input->re_parent(static_cast<time_series_input_ptr>(src_node->input().get()));
        src_input->re_parent(static_cast<time_series_input_ptr>(dst_node->input().get()));

        src_node->notify();
        dst_node->notify();
    }

    void ReduceNode::re_balance_nodes() {
        if (node_count() > 8 && (free_node_indexes_.size() * 0.75) > bound_node_indexes_.size()) { shrink_tree(); }
    }

    void ReduceNode::grow_tree() {
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
                auto ndx_lhs = std::make_tuple(i, std::get<0>(input_node_ids_));
                free_node_indexes_.push_back(ndx_lhs);
                zero_node(ndx_lhs);

                auto ndx_rhs = std::make_tuple(i, std::get<1>(input_node_ids_));
                free_node_indexes_.push_back(ndx_rhs);
                zero_node(ndx_rhs);
            } else {
                // Defer wiring until after nodes are started
                wiring_info.push_back(i);
            }
        }

        // Wire the nodes that need wiring
        for (auto i: wiring_info) {
            time_series_output_s_ptr left_parent;
            time_series_output_s_ptr right_parent;

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
            auto lhs_input = sub_graph[std::get<0>(input_node_ids_)];
            auto rhs_input = sub_graph[std::get<1>(input_node_ids_)];

            dynamic_cast<TimeSeriesInput &>(*(*lhs_input->input())[0]).bind_output(left_parent);
            dynamic_cast<TimeSeriesInput &>(*(*rhs_input->input())[0]).bind_output(right_parent);

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

    void ReduceNode::shrink_tree() {
        int64_t capacity = bound_node_indexes_.size() + free_node_indexes_.size();
        if (capacity <= 8) { return; }

        int64_t halved_capacity = capacity / 2;
        int64_t active_count = bound_node_indexes_.size();
        if (halved_capacity < active_count) { return; }

        int64_t last_node = (node_count() - 1) / 2;
        int64_t start = last_node;

        // Delete the high nodes - the left-based invariant ensures no bound keys are in nodes >= start
        nested_graph_->reduce_graph(*nested_graph_builder_, start * node_size());

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
        // Store key as Value (owned copy)
        bound_node_indexes_[value::Value(key)] = ndx;

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
        node->reset_input(node->input()->copy_with(node.get(), {ts_}));

        // Re-parent the ts to the node's input (CRITICAL FIX - Python line 200)
        ts_->re_parent(static_cast<time_series_input_ptr>(node->input().get()));

        // Make the time series active (CRITICAL FIX - Python line 201)
        ts_->make_active();

        // Track that this input is bound to a key (Python: ts._bound_to_key = True)
        // Note: insert() is idempotent - if already present, it does nothing
        bound_to_key_flags_.insert(ts_.get());

        node->notify();
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        auto node = nodes[side];

        // Get the current input
        auto inner_input = (*node->input())[0];

        // Check if this input is bound to a key (from TSD) by checking if it's in bound_to_key_flags_
        // If not in flags, it's just an unbound reference that we created, so we can reuse it
        if (bound_to_key_flags_.contains(inner_input.get())) {
            // This input was bound to a key, so we need to:
            // 1. Make it passive to unsubscribe from the output (CRITICAL: must do this before re-parenting)
            // 2. Remove it from our tracking set
            // 3. Re-parent it back to the TSD for cleanup
            // 4. Create a new unbound reference input for this node with the same specialized type as zero()
            inner_input->make_passive();  // Unsubscribe from output to prevent dangling subscriber
            bound_to_key_flags_.erase(inner_input.get());
            inner_input->re_parent(static_cast<time_series_input_ptr>(ts()));

            // Clone the specialized type from zero() instead of creating a base type
            auto zero_ref = zero();
            auto new_ref_input = zero_ref->clone_blank_ref_instance();
            node->reset_input(node->input()->copy_with(node.get(), {new_ref_input}));
            auto new_ref_input_ptr = dynamic_cast<TimeSeriesReferenceInput*>(new_ref_input.get());
            new_ref_input_ptr->re_parent(static_cast<time_series_input_ptr>(node->input().get()));
            new_ref_input_ptr->clone_binding(zero_ref);
        } else {
            // This input is not bound to a key (it's an unbound reference we created),
            // so we can just clone the zero binding without creating a new input
            auto inner_ref = dynamic_cast<TimeSeriesReferenceInput *>(inner_input.get());
            inner_ref->clone_binding(zero());
        }

        node->notify();
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
            // Convert Value key to Python using TypeMeta
            nb::object py_key = key_schema->ops().to_python(key.data(), key_schema);
            result[py_key] = nb::make_tuple(std::get<0>(ndx), std::get<1>(ndx));
        }
        return result;
    }

    const std::vector<std::tuple<int64_t, int64_t> > &ReduceNode::free_node_indexes() const {
        return free_node_indexes_;
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                         const std::tuple<int64_t, int64_t> &, int64_t>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a, "output_node_id"_a)
                .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
                .def_prop_ro("ts", &ReduceNode::ts)
                .def_prop_ro("zero", &ReduceNode::zero)
                .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
} // namespace hgraph
