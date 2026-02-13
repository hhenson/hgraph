#include <hgraph/nodes/reduce_node.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <deque>
#include <algorithm>
#include <stdexcept>
#include <cstdio>

// Debug flag - set to true to enable tracing
static constexpr bool REDUCE_DEBUG = false;

namespace hgraph {

    // Helper: Write a TSReference into a REF input's own value_data and mark modified.
    // This is the C++ equivalent of Python's clone_binding/bind_output for REF inputs.
    // Instead of binding the input via LinkTarget (which stores raw output data),
    // we write a TSReference::peered(path) into the input's value storage.
    // When the stub evaluates, ref_value() reads this TSReference from value_data.
    static void set_ref_input_value(TSView& ref_field_view, const ShortPath& target_path, engine_time_t time) {
        auto& vd = ref_field_view.view_data();
        if (!vd.value_data || !vd.meta || !vd.meta->value_type) return;

        // Write TSReference::peered directly into the REF input's value storage
        auto* ref_ptr = static_cast<TSReference*>(vd.value_data);
        *ref_ptr = TSReference::peered(target_path);

        // Mark as modified so the stub picks it up
        if (vd.time_data) {
            *static_cast<engine_time_t*>(vd.time_data) = time;
        }

        // Notify observers on this input (so the stub gets scheduled)
        if (vd.observer_data) {
            auto* obs = static_cast<ObserverList*>(vd.observer_data);
            obs->notify_modified(time);
        }
    }

    // Helper: Get the upstream output's ShortPath from an outer input field's LinkTarget.
    static ShortPath get_upstream_path(const TSView& input_field_view) {
        auto& vd = input_field_view.view_data();
        if (vd.uses_link_target && vd.link_data) {
            auto* lt = static_cast<LinkTarget*>(vd.link_data);
            if (lt->is_linked && lt->target_path.valid()) {
                return lt->target_path;
            }
        }
        // Fallback: resolve through link and use the resolved path
        ViewData resolved = resolve_through_link(vd);
        return resolved.path;
    }

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

    // ========== Simple accessors ==========

    const graph_s_ptr& ReduceNode::nested_graph() const {
        return nested_graph_;
    }

    const std::tuple<int64_t, int64_t>& ReduceNode::input_node_ids() const {
        return input_node_ids_;
    }

    int64_t ReduceNode::output_node_id() const {
        return output_node_id_;
    }

    const std::vector<std::tuple<int64_t, int64_t>>& ReduceNode::free_node_indexes() const {
        return free_node_indexes_;
    }

    nb::dict ReduceNode::py_bound_node_indexes() const {
        nb::dict result;
        for (const auto& [key, ndx] : bound_node_indexes_) {
            nb::object py_key = key.schema()->ops->to_python(key.const_view().data(), key.schema());
            result[py_key] = nb::make_tuple(std::get<0>(ndx), std::get<1>(ndx));
        }
        return result;
    }

    std::unordered_map<int, graph_s_ptr>& ReduceNode::nested_graphs() {
        // Thread-local cache for returning by reference
        thread_local std::unordered_map<int, graph_s_ptr> result;
        result.clear();
        if (nested_graph_) result[0] = nested_graph_;
        return result;
    }

    void ReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    void* ReduceNode::ts() { return nullptr; }
    void* ReduceNode::zero() { return nullptr; }
    void* ReduceNode::last_output() { return nullptr; }

    int64_t ReduceNode::node_size() const {
        return static_cast<int64_t>(nested_graph_builder_->node_builders.size());
    }

    int64_t ReduceNode::node_count() const {
        if (!nested_graph_) return 0;
        auto ns = node_size();
        return ns > 0 ? static_cast<int64_t>(nested_graph_->nodes().size()) / ns : 0;
    }

    std::vector<node_s_ptr> ReduceNode::get_node(int64_t ndx) {
        auto& nodes = nested_graph_->nodes();
        auto ns = node_size();
        auto start = ndx * ns;
        auto end = start + ns;
        return std::vector<node_s_ptr>(nodes.begin() + start, nodes.begin() + end);
    }

    // ========== Lifecycle ==========

    void ReduceNode::initialise() {
        // Create an empty graph (tree starts with no nodes, grows dynamically)
        nested_graph_ = arena_make_shared<Graph>(
            node_id(), Graph::node_list{}, std::optional<node_ptr>(static_cast<node_ptr>(this)),
            std::string("reduce"), nullptr);
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(),
            std::make_shared<NestedEngineEvaluationClock>(
                graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void ReduceNode::do_start() {
        if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] do_start()\n");
        auto time = graph()->evaluation_time();
        auto outer_input = ts_input()->view(time);
        auto tsd_field = outer_input.field("ts").ts_view();
        ViewData tsd_resolved = resolve_through_link(tsd_field.view_data());

        if (tsd_resolved.valid() && tsd_resolved.ops && tsd_resolved.ops->valid(tsd_resolved)) {
            TSDView tsd(tsd_resolved, time);
            TSSView key_set = tsd.key_set();

            // Collect pre-existing keys (valid but not added this tick)
            std::vector<value::View> pre_existing;
            for (auto key_view : key_set.values()) {
                if (!key_set.was_added(key_view)) {
                    pre_existing.push_back(key_view);
                }
            }

            if (!pre_existing.empty()) {
                if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] do_start: %zu pre-existing keys\n", pre_existing.size());
                add_nodes_from_views(pre_existing);
            } else {
                if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] do_start: no pre-existing keys, grow_tree\n");
                grow_tree();
            }
        } else {
            if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] do_start: TSD not valid, grow_tree\n");
            grow_tree();
        }

        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] do_start: node_count=%lld, node_size=%lld, free=%zu\n",
                node_count(), node_size(), free_node_indexes_.size());
            auto [lhs_id, rhs_id] = input_node_ids_;
            fprintf(stderr, "[REDUCE] input_node_ids=(%lld,%lld), output_node_id=%lld\n",
                lhs_id, rhs_id, output_node_id_);
            // Dump edges
            fprintf(stderr, "[REDUCE] nested_graph_builder has %zu edges:\n", nested_graph_builder_->edges.size());
            for (const auto& e : nested_graph_builder_->edges) {
                fprintf(stderr, "[REDUCE]   edge: src=%lld, output_path=[", e.src_node);
                for (auto p : e.output_path) fprintf(stderr, "%lld,", p);
                fprintf(stderr, "] → dst=%lld, input_path=[", e.dst_node);
                for (auto p : e.input_path) fprintf(stderr, "%lld,", p);
                fprintf(stderr, "]\n");
            }
        }
        start_component(*nested_graph_);
        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] do_start: inner graph started, nodes=%zu\n",
                nested_graph_->nodes().size());
            auto time = graph()->evaluation_time();
            // Check subscription state of each inner node's input
            for (size_t ni = 0; ni < nested_graph_->nodes().size(); ++ni) {
                auto n = nested_graph_->nodes()[ni];
                if (n->ts_input()) {
                    auto iv = n->ts_input()->view(time);
                    auto meta = n->ts_input()->meta();
                    fprintf(stderr, "[REDUCE]   node %zu input: kind=%d, field_count=%zu\n",
                        ni, meta ? (int)meta->kind : -1, meta ? meta->field_count : 0);
                    if (meta && meta->kind == TSKind::TSB) {
                        for (size_t fi = 0; fi < meta->field_count; ++fi) {
                            auto fv = iv[fi].ts_view();
                            auto& fvd = fv.view_data();
                            fprintf(stderr, "[REDUCE]     field %zu: kind=%d, has_link=%d",
                                fi, fvd.meta ? (int)fvd.meta->kind : -1, fvd.uses_link_target ? 1 : 0);
                            if (fvd.uses_link_target && fvd.link_data) {
                                auto* lt = static_cast<LinkTarget*>(fvd.link_data);
                                fprintf(stderr, ", linked=%d, lt_meta_kind=%d, lt_obs=%p",
                                    lt->is_linked ? 1 : 0,
                                    lt->meta ? (int)lt->meta->kind : -1,
                                    lt->observer_data);
                                fprintf(stderr, ", owning_input=%p", (void*)lt->active_notifier.owning_input);
                            }
                            fprintf(stderr, "\n");
                        }
                    }
                }
            }
        }
    }

    void ReduceNode::do_stop() {
        if (nested_graph_) {
            stop_component(*nested_graph_);
        }
    }

    void ReduceNode::dispose() {
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    // ========== Core evaluation ==========

    void ReduceNode::eval() {
        mark_evaluated();
        if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] eval()\n");

        auto time = graph()->evaluation_time();
        auto outer_input = ts_input()->view(time);
        auto tsd_field = outer_input.field("ts").ts_view();
        ViewData tsd_resolved = resolve_through_link(tsd_field.view_data());
        TSDView tsd(tsd_resolved, time);
        TSSView key_set = tsd.key_set();

        // Collect removed and added keys
        std::vector<value::View> removed_keys;
        for (auto key : key_set.removed()) {
            removed_keys.push_back(key);
        }

        std::vector<value::View> added_keys;
        for (auto key : key_set.added()) {
            added_keys.push_back(key);
        }

        if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] eval: added=%zu, removed=%zu\n",
            added_keys.size(), removed_keys.size());

        // Process removals first (reduce chance of unnecessary tree growth)
        remove_nodes_from_views(removed_keys);
        add_nodes_from_views(added_keys);

        // Rebalance if needed
        re_balance_nodes();

        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] eval: after rebalance, node_count=%lld, bound=%zu, free=%zu\n",
                node_count(), bound_node_indexes_.size(), free_node_indexes_.size());
            // Dump inner node states + stub input values
            for (size_t ni = 0; ni < nested_graph_->nodes().size(); ++ni) {
                auto n = nested_graph_->nodes()[ni];
                auto& sched = nested_graph_->schedule();
                auto inner_now = *nested_graph_->cached_evaluation_time_ptr();
                bool scheduled = (sched[ni] == inner_now);
                fprintf(stderr, "[REDUCE]   inner node %zu: has_input=%d, has_output=%d, scheduled=%d\n",
                    ni, n->has_input() ? 1 : 0, n->ts_output() != nullptr ? 1 : 0, scheduled ? 1 : 0);
                if (n->ts_output()) {
                    auto oview = n->ts_output()->view(time);
                    fprintf(stderr, "[REDUCE]     output: valid=%d, modified=%d\n",
                        oview.ts_view().valid() ? 1 : 0, oview.ts_view().modified() ? 1 : 0);
                }
                // Check stub input REF value_data (nodes 0 and 1)
                if ((ni == 0 || ni == 1) && n->ts_input()) {
                    auto iv = n->ts_input()->view(time);
                    auto meta = n->ts_input()->meta();
                    if (meta && meta->kind == TSKind::TSB && meta->field_count > 0) {
                        auto fv = iv[0].ts_view();
                        auto& fvd = fv.view_data();
                        if (fvd.value_data && fvd.meta && fvd.meta->value_type) {
                            auto* ref_ptr = static_cast<const TSReference*>(fvd.value_data);
                            fprintf(stderr, "[REDUCE]     input[0] ref value: %s (kind=%d, valid_path=%d)\n",
                                ref_ptr->to_string().c_str(),
                                (int)ref_ptr->kind(),
                                ref_ptr->is_peered() ? (ref_ptr->path().valid() ? 1 : 0) : -1);
                            // Check time_data
                            if (fvd.time_data) {
                                auto t = *static_cast<engine_time_t*>(fvd.time_data);
                                fprintf(stderr, "[REDUCE]     input[0] time=%lld, valid=%d\n",
                                    (long long)t.time_since_epoch().count(), t != engine_time_t{} ? 1 : 0);
                            }
                        }
                    }
                }
            }
        }

        // Force-schedule all inner nodes before evaluation.
        // In Python, inner inputs are properly bound to TSD elements with subscriptions,
        // so they get notified when element values change. In C++, the stubs hold
        // TSReferences without subscriptions, so we force-schedule to ensure nodes
        // pick up changed values from the TSD.
        {
            auto eval_time = *nested_graph_->cached_evaluation_time_ptr();
            for (size_t i = 0; i < nested_graph_->nodes().size(); ++i) {
                nested_graph_->schedule_node(static_cast<int64_t>(i), eval_time, true);
            }
        }

        // Evaluate inner graph
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(
                nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        nested_graph_->evaluate_graph();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(
                nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] eval: AFTER evaluate_graph\n");
            for (size_t ni = 0; ni < nested_graph_->nodes().size(); ++ni) {
                auto n = nested_graph_->nodes()[ni];
                if (n->ts_output()) {
                    auto oview = n->ts_output()->view(time);
                    bool valid = oview.ts_view().valid();
                    bool mod = oview.ts_view().modified();
                    fprintf(stderr, "[REDUCE]   node %zu output: valid=%d, modified=%d", ni, valid ? 1 : 0, mod ? 1 : 0);
                    if (valid) {
                        try {
                            nb::object py_val = oview.ts_view().to_python();
                            nb::str s(py_val);
                            fprintf(stderr, ", value=%s", s.c_str());
                        } catch (...) {
                            fprintf(stderr, ", value=<error>");
                        }
                    }
                    fprintf(stderr, "\n");
                }
                // Check input LinkTarget state for node 2 (computation)
                if (ni == 2 && n->ts_input()) {
                    auto iv = n->ts_input()->view(time);
                    auto meta = n->ts_input()->meta();
                    if (meta && meta->kind == TSKind::TSB) {
                        for (size_t fi = 0; fi < meta->field_count; ++fi) {
                            auto fv = iv[fi].ts_view();
                            auto& fvd = fv.view_data();
                            fprintf(stderr, "[REDUCE]   node 2 input[%zu]: kind=%d", fi, fvd.meta ? (int)fvd.meta->kind : -1);
                            if (fvd.uses_link_target && fvd.link_data) {
                                auto* lt = static_cast<LinkTarget*>(fvd.link_data);
                                fprintf(stderr, ", linked=%d, lt_meta=%d, lt_obs=%p, ref_binding=%p, owning=%p",
                                    lt->is_linked ? 1 : 0,
                                    lt->meta ? (int)lt->meta->kind : -1,
                                    lt->observer_data,
                                    lt->ref_binding_,
                                    (void*)lt->active_notifier.owning_input);
                                if (lt->is_linked && lt->value_data && lt->meta && lt->meta->kind != TSKind::REF) {
                                    // Try to read resolved int value
                                    try {
                                        value::View v(lt->value_data, lt->meta->value_type);
                                        if (v.valid()) {
                                            nb::object py_v = lt->meta->value_type->ops->to_python(v.data(), lt->meta->value_type);
                                            nb::str s(py_v);
                                            fprintf(stderr, ", resolved_value=%s", s.c_str());
                                        }
                                    } catch (...) { fprintf(stderr, ", resolved_value=<error>"); }
                                }
                            }
                            fprintf(stderr, "\n");
                        }
                    }
                }
            }
        }

        // Propagate output from last tree node to outer output
        if (node_count() > 0 && ts_output()) {
            auto inner_nodes = get_node(node_count() - 1);
            auto* out_node = inner_nodes[output_node_id_].get();
            if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] eval: checking output, out_node has_output=%d\n",
                out_node->ts_output() != nullptr ? 1 : 0);
            if (out_node->ts_output()) {
                auto inner_out = out_node->ts_output()->view(time);
                auto outer_out = ts_output()->view(time);

                bool inner_valid = inner_out.ts_view().valid();
                bool outer_valid = outer_out.ts_view().valid();

                if constexpr (REDUCE_DEBUG) {
                    auto outer_meta = outer_out.ts_view().view_data().meta;
                    fprintf(stderr, "[REDUCE] eval: inner_valid=%d, outer_valid=%d, outer_meta_kind=%d\n",
                        inner_valid ? 1 : 0, outer_valid ? 1 : 0,
                        outer_meta ? (int)outer_meta->kind : -1);
                }

                if ((!outer_valid && inner_valid) || inner_valid) {
                    nb::object inner_val = inner_out.ts_view().to_python();
                    if constexpr (REDUCE_DEBUG) {
                        try {
                            nb::str s(inner_val);
                            fprintf(stderr, "[REDUCE] eval: inner_val=%s\n", s.c_str());
                        } catch (...) {
                            fprintf(stderr, "[REDUCE] eval: inner_val=<error>\n");
                        }
                    }
                    bool need_update = !outer_valid;
                    if (!need_update) {
                        nb::object outer_val = outer_out.ts_view().to_python();
                        try {
                            need_update = !inner_val.equal(outer_val);
                        } catch (...) {
                            need_update = true;
                        }
                    }
                    if (need_update) {
                        auto& out_vd = outer_out.ts_view().view_data();
                        if constexpr (REDUCE_DEBUG) {
                            fprintf(stderr, "[REDUCE] eval: updating outer output, obs=%p obs_count=%zu forwarded=%d\n",
                                out_vd.observer_data,
                                out_vd.observer_data ? static_cast<ObserverList*>(out_vd.observer_data)->size() : 0,
                                ts_output()->is_forwarded() ? 1 : 0);
                        }
                        out_vd.ops->from_python(out_vd, inner_val, time);
                    }
                }
            }
        }
    }

    // ========== Tree management ==========

    void ReduceNode::add_nodes_from_views(const std::vector<value::View>& keys) {
        for (const auto& key : keys) {
            if (free_node_indexes_.empty()) {
                grow_tree();
            }
            auto ndx = free_node_indexes_.back();
            free_node_indexes_.pop_back();
            bind_key_to_node(key, ndx);
        }
    }

    void ReduceNode::remove_nodes_from_views(const std::vector<value::View>& keys) {
        for (const auto& key : keys) {
            // Find and remove from bound_node_indexes
            auto it = bound_node_indexes_.find(value::PlainValue(key));
            if (it == bound_node_indexes_.end()) continue;

            auto ndx = it->second;
            bound_node_indexes_.erase(it);

            if (!bound_node_indexes_.empty()) {
                // Find the bound node with the largest (node_id, side) tuple
                auto max_it = std::max_element(
                    bound_node_indexes_.begin(), bound_node_indexes_.end(),
                    [](const auto& a, const auto& b) { return a.second < b.second; });

                if (std::get<0>(max_it->second) > std::get<0>(ndx)) {
                    // Swap the max node into the gap left by removed node
                    swap_node(ndx, max_it->second);
                    // Update: the max key now points to the removed node's position
                    // The removed position gets the max node's old position
                    auto old_max_ndx = max_it->second;
                    max_it->second = ndx;
                    ndx = old_max_ndx;
                }
            }

            free_node_indexes_.push_back(ndx);
            zero_node(ndx);
        }
    }

    void ReduceNode::re_balance_nodes() {
        // Shrink if tree has > 8 nodes and free slots are 75%+ of bound slots
        if (node_count() > 8 &&
            (static_cast<int64_t>(free_node_indexes_.size()) * 3 >
             static_cast<int64_t>(bound_node_indexes_.size()) * 4)) {
            shrink_tree();
        }
    }

    void ReduceNode::grow_tree() {
        if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] grow_tree()\n");
        int64_t count = node_count();
        int64_t end = 2 * count + 1;  // Not inclusive
        int64_t top_layer_length = (end + 1) / 4;
        int64_t top_layer_end = std::max(count + top_layer_length, int64_t(1));
        int64_t last_node = end - 1;
        std::deque<int64_t> un_bound_outputs;

        auto [lhs_id, rhs_id] = input_node_ids_;
        auto time = graph()->evaluation_time();

        for (int64_t i = count; i < end; ++i) {
            un_bound_outputs.push_back(i);
            nested_graph_->extend_graph(*nested_graph_builder_, true);  // delay_start=true

            if (i < top_layer_end) {
                // Leaf nodes: add both sides to free list and bind to zero
                auto ndx_lhs = std::make_tuple(i, lhs_id);
                free_node_indexes_.push_back(ndx_lhs);
                zero_node(ndx_lhs);

                auto ndx_rhs = std::make_tuple(i, rhs_id);
                free_node_indexes_.push_back(ndx_rhs);
                zero_node(ndx_rhs);
            } else {
                // Tree-level nodes: connect outputs from lower levels to inputs
                TSView left_parent_view, right_parent_view;

                if (i < last_node) {
                    // Middle node: connect two lower-level outputs
                    auto left_idx = un_bound_outputs.front(); un_bound_outputs.pop_front();
                    auto right_idx = un_bound_outputs.front(); un_bound_outputs.pop_front();

                    auto left_nodes = get_node(left_idx);
                    auto right_nodes = get_node(right_idx);
                    left_parent_view = left_nodes[output_node_id_]->ts_output()->view(time).ts_view();
                    right_parent_view = right_nodes[output_node_id_]->ts_output()->view(time).ts_view();
                } else {
                    // Last node (new root): left=old root output, right=new subtree output
                    auto old_root_nodes = get_node(count - 1);
                    left_parent_view = old_root_nodes[output_node_id_]->ts_output()->view(time).ts_view();

                    auto new_subtree_idx = un_bound_outputs.front(); un_bound_outputs.pop_front();
                    auto new_subtree_nodes = get_node(new_subtree_idx);
                    right_parent_view = new_subtree_nodes[output_node_id_]->ts_output()->view(time).ts_view();
                }

                // Bind LHS and RHS inputs of this tree node to their parent outputs.
                // Python equivalent: lhs_input.input[0].bind_output(left_parent)
                // This writes TSReference::peered(output_path) into the stub's REF input value.
                auto sub_graph = get_node(i);
                auto lhs_node = sub_graph[lhs_id];
                auto rhs_node = sub_graph[rhs_id];

                // Get output paths for left and right parent outputs
                ShortPath left_path = left_parent_view.view_data().path;
                ShortPath right_path = right_parent_view.view_data().path;

                // Write TSReference into lhs and rhs stub input values
                auto lhs_input = lhs_node->ts_input()->view(time);
                auto lhs_ts = lhs_input[0].ts_view();
                set_ref_input_value(lhs_ts, left_path, time);
                lhs_node->notify();

                auto rhs_input = rhs_node->ts_input()->view(time);
                auto rhs_ts = rhs_input[0].ts_view();
                set_ref_input_value(rhs_ts, right_path, time);
                rhs_node->notify();
            }
        }

        // Start newly added nodes if the graph is already running
        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            nested_graph_->start_subgraph(count * node_size(),
                                          static_cast<int64_t>(nested_graph_->nodes().size()));
        }
    }

    void ReduceNode::shrink_tree() {
        int64_t active_count = static_cast<int64_t>(bound_node_indexes_.size());
        int64_t capacity = active_count + static_cast<int64_t>(free_node_indexes_.size());
        if (capacity <= 8) return;

        int64_t halved_capacity = capacity / 2;
        if (halved_capacity < active_count) return;

        int64_t last_node = (node_count() - 1) / 2;
        nested_graph_->reduce_graph(last_node * node_size());

        // Keep only (halved_capacity - active_count) free nodes, sorted ascending
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end());
        int64_t keep_count = halved_capacity - active_count;
        if (static_cast<int64_t>(free_node_indexes_.size()) > keep_count) {
            free_node_indexes_.resize(static_cast<size_t>(keep_count));
        }
        // Sort in descending order so pop_back gives the smallest
        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(), std::greater<>());
    }

    // ========== Node binding operations ==========

    void ReduceNode::bind_key_to_node(const value::View& key, const std::tuple<int64_t, int64_t>& ndx) {
        // Store key → (node_id, side) mapping
        bound_node_indexes_.emplace(value::PlainValue(key), ndx);

        auto [node_id, side] = ndx;
        auto time = graph()->evaluation_time();
        auto inner_node = get_node(node_id)[side];

        // Get TSD element's upstream output ShortPath.
        // Python equivalent: ts = self._tsd[key]; node.input = node.input.copy_with(ts=ts)
        // In C++, we write TSReference::peered(path_to_tsd_element_output) into the stub's REF input.
        auto outer_input = ts_input()->view(time);
        auto tsd_field = outer_input.field("ts").ts_view();
        ViewData tsd_resolved = resolve_through_link(tsd_field.view_data());
        // Override path: resolve_through_link preserves input path but we need the upstream output path
        // so that TSReference::peered(path) creates a resolvable output path (same pattern as switch_node.cpp)
        {
            auto& tsd_vd = tsd_field.view_data();
            if (tsd_vd.uses_link_target && tsd_vd.link_data) {
                auto* lt = static_cast<LinkTarget*>(tsd_vd.link_data);
                if (lt->is_linked && lt->target_path.valid()) {
                    tsd_resolved.path = lt->target_path;
                }
            }
        }
        TSView tsd_element = tsd_resolved.ops->child_by_key(tsd_resolved, key, time);
        ShortPath element_path = tsd_element.view_data().path;

        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] bind_key_to_node: element_path valid=%d, path=%s\n",
                element_path.valid() ? 1 : 0, element_path.to_string().c_str());
            // Also print the intermediate paths
            auto& tsd_vd = tsd_field.view_data();
            fprintf(stderr, "[REDUCE]   tsd_field path=%s\n", tsd_vd.path.to_string().c_str());
            if (tsd_vd.uses_link_target && tsd_vd.link_data) {
                auto* lt = static_cast<LinkTarget*>(tsd_vd.link_data);
                fprintf(stderr, "[REDUCE]   lt->target_path=%s, linked=%d\n",
                    lt->target_path.to_string().c_str(), lt->is_linked ? 1 : 0);
            }
            fprintf(stderr, "[REDUCE]   tsd_resolved path=%s\n", tsd_resolved.path.to_string().c_str());
        }

        // Write TSReference::peered(element_path) into the stub's REF input value
        if (inner_node->ts_input()) {
            auto inner_input_view = inner_node->ts_input()->view(time);
            auto ts_field_view = inner_input_view[0].ts_view();
            set_ref_input_value(ts_field_view, element_path, time);
        }

        // Track as bound to key
        bound_to_key_flags_.insert(static_cast<void*>(inner_node.get()));

        // Schedule for evaluation
        inner_node->notify();
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t>& ndx) {
        auto [node_id, side] = ndx;
        if constexpr (REDUCE_DEBUG) fprintf(stderr, "[REDUCE] zero_node(%lld, %lld)\n", node_id, side);
        auto time = graph()->evaluation_time();
        auto inner_node = get_node(node_id)[side];

        // Remove from bound-to-key tracking
        bound_to_key_flags_.erase(static_cast<void*>(inner_node.get()));

        // Get the upstream zero output's ShortPath.
        // Python equivalent: inner_input.clone_binding(self._zero)
        //   → inner_input.bind_output(self._zero.output)
        //   → creates TSReference::peered(path_to_zero_output)
        auto outer_input = ts_input()->view(time);
        auto zero_field = outer_input.field("zero").ts_view();
        ShortPath upstream_path = get_upstream_path(zero_field);

        if constexpr (REDUCE_DEBUG) {
            fprintf(stderr, "[REDUCE] zero_node: upstream_path valid=%d\n", upstream_path.valid() ? 1 : 0);
        }

        // Write TSReference::peered(upstream_path) into the stub's REF input value.
        // This tells the stub "you reference the upstream zero output".
        // When the stub evaluates, it reads this TSReference and writes it to its REF output.
        // The downstream REFBindingHelper then resolves the reference to actual TS[int] data.
        if (inner_node->ts_input()) {
            auto inner_input_view = inner_node->ts_input()->view(time);
            auto ts_field_view = inner_input_view[0].ts_view();
            set_ref_input_value(ts_field_view, upstream_path, time);
        }

        // Notify to schedule for evaluation
        inner_node->notify();
    }

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t>& src_ndx,
                               const std::tuple<int64_t, int64_t>& dst_ndx) {
        auto [src_node_id, src_side] = src_ndx;
        auto [dst_node_id, dst_side] = dst_ndx;
        auto time = graph()->evaluation_time();

        auto src_node = get_node(src_node_id)[src_side];
        auto dst_node = get_node(dst_node_id)[dst_side];

        if (!src_node->ts_input() || !dst_node->ts_input()) return;

        auto src_input_view = src_node->ts_input()->view(time);
        auto dst_input_view = dst_node->ts_input()->view(time);
        auto src_ts = src_input_view[0].ts_view();
        auto dst_ts = dst_input_view[0].ts_view();

        // Read current TSReference values from each REF input's value_data
        auto& src_vd = src_ts.view_data();
        auto& dst_vd = dst_ts.view_data();

        if (!src_vd.value_data || !dst_vd.value_data) return;

        auto* src_ref = static_cast<TSReference*>(src_vd.value_data);
        auto* dst_ref = static_cast<TSReference*>(dst_vd.value_data);

        // Swap the TSReference values
        TSReference tmp = std::move(*src_ref);
        *src_ref = std::move(*dst_ref);
        *dst_ref = std::move(tmp);

        // Mark both as modified
        if (src_vd.time_data) *static_cast<engine_time_t*>(src_vd.time_data) = time;
        if (dst_vd.time_data) *static_cast<engine_time_t*>(dst_vd.time_data) = time;

        src_node->notify();
        dst_node->notify();
    }

    // ========== nanobind registration ==========

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
                .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
                .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
                .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
                .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
                .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
                .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
} // namespace hgraph
