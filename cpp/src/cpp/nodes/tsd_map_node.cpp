#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <stdexcept>
#include <optional>

namespace hgraph {

    // Helper: Write a TSReference into a REF input's own value_data and mark modified.
    static void map_set_ref_input_value(TSView& ref_field_view, const ShortPath& target_path, engine_time_t time) {
        auto& vd = ref_field_view.view_data();
        if (!vd.value_data || !vd.meta || !vd.meta->value_type) return;

        auto* ref_ptr = static_cast<TSReference*>(vd.value_data);
        *ref_ptr = TSReference::peered(target_path);

        if (vd.time_data) {
            *static_cast<engine_time_t*>(vd.time_data) = time;
        }

        if (vd.observer_data) {
            auto* obs = static_cast<ObserverList*>(vd.observer_data);
            obs->notify_modified(time);
        }
    }

    // Resolve a map input field through input links and one-or-more REF wrappers so scheduling
    // decisions can use the actual underlying collection semantics.
    static ViewData resolve_field_for_scheduling(TSView field_ts_view, engine_time_t time) {
        ViewData resolved = resolve_through_link(field_ts_view.view_data());

        // Preserve upstream output path when this input is bound through LinkTarget.
        auto& input_vd = field_ts_view.view_data();
        if (input_vd.uses_link_target && input_vd.link_data) {
            auto* lt = static_cast<LinkTarget*>(input_vd.link_data);
            if (lt->is_linked && lt->target_path.valid()) {
                resolved.path = lt->target_path;
            }
        }

        // Follow REF chains (up to a small guard depth) to the concrete target.
        for (size_t depth = 0; depth < 8; ++depth) {
            if (!resolved.meta || resolved.meta->kind != TSKind::REF || !resolved.value_data) {
                break;
            }
            auto* ref_ptr = static_cast<TSReference*>(resolved.value_data);
            if (!ref_ptr || ref_ptr->is_empty()) {
                break;
            }
            TSView target = ref_ptr->resolve(time);
            if (!target || !target.view_data().valid()) {
                break;
            }
            resolved = target.view_data();
        }

        return resolved;
    }

    // ========== MapNestedEngineEvaluationClock ==========

    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::PlainValue key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto* node = static_cast<TsdMapNode*>(_nested_node);
        auto let = node->last_evaluation_time();

        if ((let != MIN_DT && let >= next_time) || node->is_stopping()) {
            return;
        }

        // Update per-key scheduling in the map node
        auto key_view = _key.const_view();
        auto it = node->scheduled_keys_.find(key_view);
        if (it == node->scheduled_keys_.end() || it->second > next_time) {
            // Insert or update: need PlainValue key for insertion
            if (it == node->scheduled_keys_.end()) {
                node->scheduled_keys_.emplace(value::PlainValue(key_view), next_time);
            } else {
                it->second = next_time;
            }
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        if (!_key.schema()) return nb::none();
        return _key.schema()->ops->to_python(_key.const_view().data(), _key.schema());
    }

    // ========== TsdMapNode Constructor ==========

    TsdMapNode::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                           const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars,
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id),
          multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    // ========== PendingWiringNotifier ==========

    void TsdMapNode::PendingWiringNotifier::notify(engine_time_t et) {
        if (node && node->graph() && !node->is_stopping()) {
            node->graph()->schedule_node(node->node_ndx(), et);
        }
    }

    void TsdMapNode::clear_pending_wiring_subscriptions() {
        if (pending_wiring_notifier_) {
            for (auto* obs : pending_wiring_subscriptions_) {
                obs->remove_observer(pending_wiring_notifier_.get());
            }
            pending_wiring_subscriptions_.clear();
        }
    }

    // ========== Accessors ==========

    nb::dict TsdMapNode::py_nested_graphs() const {
        nb::dict result;
        for (const auto& [key, graph_] : active_graphs_) {
            nb::object py_key = key.schema()->ops->to_python(key.const_view().data(), key.schema());
            result[py_key] = nb::cast(graph_);
        }
        return result;
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        for (const auto& [key, graph_] : active_graphs_) {
            callback(graph_);
        }
    }

    void* TsdMapNode::tsd_output() {
        return ts_output();
    }

    // ========== Lifecycle ==========

    void TsdMapNode::initialise() {
        // Extract key_type_meta from the output TSD's key_type, or from a multiplexed TSD input's key_type
        if (ts_output() && ts_output()->ts_meta() && ts_output()->ts_meta()->key_type) {
            key_type_meta_ = ts_output()->ts_meta()->key_type;
        } else {
            // Fallback: look for key_type from the first multiplexed TSD input
            auto* input_meta = ts_input()->meta();
            if (input_meta && input_meta->kind == TSKind::TSB) {
                for (size_t i = 0; i < input_meta->field_count; ++i) {
                    auto* field_meta = input_meta->fields[i].ts_type;
                    if (field_meta && field_meta->kind == TSKind::TSD && field_meta->key_type) {
                        key_type_meta_ = field_meta->key_type;
                        break;
                    }
                }
            }
        }
    }

    void TsdMapNode::do_start() {
        if (has_recordable_id_trait(graph()->traits())) {
            auto record_id = signature().record_replay_id;
            recordable_id_ = get_fq_recordable_id(
                graph()->traits(), record_id.has_value() ? record_id.value() : "map_");
        }
    }

    void TsdMapNode::do_stop() {
        for (auto it = active_graphs_.begin(); it != active_graphs_.end(); ) {
            auto key_view = it->first.const_view();
            auto& graph_ = it->second;
            // Unbind inner graph inputs BEFORE un_wire_graph to prevent dangling observers
            for (size_t ni = 0; ni < graph_->nodes().size(); ni++) {
                auto& node = graph_->nodes()[ni];
                if (node->ts_input()) {
                    ViewData vd = node->ts_input()->value().make_view_data();
                    vd.uses_link_target = true;
                    if (vd.ops && vd.ops->unbind) {
                        vd.ops->unbind(vd);
                    }
                }
            }
            un_wire_graph(key_view, graph_);
            try {
                stop_component(*graph_);
            } catch (...) {}
            // Schedule deferred release
            auto builder = nested_graph_builder_;
            auto g = graph_;
            graph()->evaluation_engine()->add_before_evaluation_notification(
                [builder, g]() { builder->release_instance(g); });
            it = active_graphs_.erase(it);
        }
        scheduled_keys_.clear();
        pending_keys_.clear();
        pending_multiplexed_wirings_.clear();
        absent_non_multiplexed_keys_prev_.clear();
        clear_pending_wiring_subscriptions();
    }

    void TsdMapNode::dispose() {
        for (auto& [key, graph_] : active_graphs_) {
            try { dispose_component(*graph_); } catch (...) {}
        }
        active_graphs_.clear();
    }

    // ========== Core evaluation ==========

    void TsdMapNode::eval() {
        mark_evaluated();

        auto time = graph()->evaluation_time();
        auto now = last_evaluation_time();
        key_set_type created_keys_this_tick;
        key_set_type protected_same_tick_schedules;


        // Clear output delta for this tick (lazy clearing).
        // dict_create/dict_remove modify the TSD via MapStorage which notifies MapDelta via SlotObserver,
        // but nothing else triggers the lazy delta clear. We must do it here at the start of each tick.
        if (ts_output()) {
            ts_output()->native_value().delta_value_view(time);
        }

        auto outer_input = ts_input()->view(time);

        // 1. Reconcile active inner graphs against current __keys__ snapshot.
        // The key-set can appear valid without added/removed deltas (for example after
        // REF rebinds or switch branch activation with pre-existing keys). Using only
        // SetDelta misses those cases and leaves active_graphs_ stale.
        auto keys_field = outer_input.field(KEYS_ARG).ts_view();
        ViewData keys_resolved = resolve_through_link(keys_field.view_data());

        bool keys_tss_valid = keys_resolved.valid() && keys_resolved.ops &&
                              keys_resolved.ops->valid(keys_resolved);

        // Detect when the __keys__ binding changed this tick via REFBindingHelper rebind.
        // When the switch changes branches, REFBindingHelper rebinds to the new branch's TSS
        // and calls owner->notify(et), which stamps lt->owner_time_ptr = et. However,
        // keys_field.modified() delegates through the link to the resolved TSS (which may not
        // have produced yet on the new branch), so modified()=false even though the binding
        // changed. We must detect this so orphaned keys from the old branch can be removed.
        // This matches Python switch: when branch changes, __keys__ is "modified" even if
        // the new branch's TSS is initially empty/invalid.
        //
        // NOTE: We deliberately do NOT use keys_field.modified() here. Using it would fire
        // during reduce-tree swaps when the TSS is temporarily invalid (due to a removed key
        // being transiently referenced), causing all active graphs to be cleared with an empty
        // current_keys set, losing valid state. keys_binding_changed_this_tick is more precise:
        // it only fires when the REF binding itself changed (i.e., switch branch change), not
        // for transient invalidity during graph restructuring.
        bool keys_binding_changed_this_tick = false;
        {
            auto& vd = keys_field.view_data();
            if (vd.uses_link_target && vd.link_data) {
                auto* lt = static_cast<LinkTarget*>(vd.link_data);
                if (lt->ref_binding_ && lt->owner_time_ptr && *lt->owner_time_ptr >= time) {
                    keys_binding_changed_this_tick = true;
                }
            }
        }

        if (keys_tss_valid || keys_binding_changed_this_tick) {
            // Compute current key set from TSS (empty if TSS not yet valid).
            std::unordered_set<value::PlainValue, PlainValueHash, PlainValueEqual> current_keys;
            if (keys_tss_valid) {
                TSSView keys_view(keys_resolved, time);
                for (auto key_val : keys_view.values()) {
                    current_keys.emplace(key_val);
                }
            }
            // (if keys_modified but !keys_tss_valid: current_keys stays empty →
            //  all active keys become orphaned, e.g. switch to empty branch)

            // Remove graphs for keys no longer present.
            {
                std::vector<value::PlainValue> orphaned_keys;
                for (auto& [key, _] : active_graphs_) {
                    if (current_keys.find(key.const_view()) == current_keys.end()) {
                        orphaned_keys.emplace_back(key.const_view());
                    }
                }
                for (auto& key : orphaned_keys) {
                    remove_graph(key.const_view());
                    auto sk_it = scheduled_keys_.find(key.const_view());
                    if (sk_it != scheduled_keys_.end()) {
                        scheduled_keys_.erase(sk_it);
                    }
                    auto pw_it = pending_multiplexed_wirings_.find(key.const_view());
                    if (pw_it != pending_multiplexed_wirings_.end()) {
                        pending_multiplexed_wirings_.erase(pw_it);
                    }
                }
            }

            // Add graphs for any pre-existing/new keys that are currently present.
            for (const auto& key_val : current_keys) {
                if (active_graphs_.find(key_val.const_view()) == active_graphs_.end()) {
                    created_keys_this_tick.emplace(value::PlainValue(key_val.const_view()));
                    create_new_graph(key_val.const_view());
                }
            }
        }

        // 1.5 Refresh multiplexed bindings for all active keys.
        // This handles source rebinds and key removals for already-wired graphs.
        if (!multiplexed_args_.empty() && !active_graphs_.empty()) {
            std::vector<value::PlainValue> active_keys_snapshot;
            active_keys_snapshot.reserve(active_graphs_.size());
            for (const auto& [key, _] : active_graphs_) {
                active_keys_snapshot.emplace_back(key.const_view());
            }

            for (const auto& key : active_keys_snapshot) {
                bool binding_changed = false;
                bool all_wired = try_wire_multiplexed_for_key(key.const_view(), time, &binding_changed);
                bool key_needs_eval = false;

                // Schedule when the key's upstream multiplexed element changed even if
                // the binding target path stayed the same.
                for (const auto& [arg, _] : input_node_ids_) {
                    if (!multiplexed_args_.count(arg)) {
                        continue;
                    }

                    auto field_view = outer_input.field(arg).ts_view();
                    ViewData tsd_resolved = resolve_through_link(field_view.view_data());
                    auto& vd = field_view.view_data();
                    if (vd.uses_link_target && vd.link_data) {
                        auto* lt = static_cast<LinkTarget*>(vd.link_data);
                        if (lt->is_linked && lt->target_path.valid()) {
                            tsd_resolved.path = lt->target_path;
                        }
                    }

                    if (!tsd_resolved.meta || tsd_resolved.meta->kind != TSKind::TSD || !tsd_resolved.ops) {
                        continue;
                    }

                    TSDView dict_view(tsd_resolved, time);
                    auto key_view = key.const_view();
                    if (dict_view.was_added(key_view) || dict_view.was_removed(key_view)) {
                        key_needs_eval = true;
                        break;
                    }
                    if (dict_view.contains(key_view)) {
                        TSView elem = dict_view.at(key_view);
                        if (elem.modified()) {
                            key_needs_eval = true;
                            break;
                        }
                    }
                }
                if (key_needs_eval) {
                    force_full_inner_eval_ = true;
                }

                if (binding_changed || key_needs_eval) {
                    protected_same_tick_schedules.emplace(value::PlainValue(key.const_view()));
                    scheduled_keys_.emplace(value::PlainValue(key.const_view()), now);
                }

                if (all_wired) {
                    auto pending_it = pending_multiplexed_wirings_.find(key.const_view());
                    if (pending_it != pending_multiplexed_wirings_.end()) {
                        pending_multiplexed_wirings_.erase(pending_it);
                    }
                } else {
                    pending_multiplexed_wirings_.emplace(value::PlainValue(key.const_view()));
                }
            }
        }

        // 1.6 Try to complete any deferred multiplexed wirings
        try_wire_pending_keys(time);

        // Unsubscribe from upstream TSD observers once all pending wirings are resolved
        if (pending_multiplexed_wirings_.empty()) {
            clear_pending_wiring_subscriptions();
        }

        // 1.7 Evaluate all active keys whenever the map has non-multiplexed TS inputs.
        // Those inputs are shared across every inner graph and can affect output without
        // surfacing as per-key wiring changes. Running all active keys keeps inner
        // graphs coherent with pass-through/non-multiplexed dependencies.
        if (!active_graphs_.empty()) {
            bool has_non_multiplexed_inputs = false;
            bool has_non_multiplexed_collection_inputs = false;
            std::vector<std::pair<std::string, ViewData>> non_multiplexed_inputs;
            for (const auto& [arg, _] : input_node_ids_) {
                if (arg == key_arg_ || arg == KEYS_ARG || multiplexed_args_.count(arg)) {
                    continue;
                }
                has_non_multiplexed_inputs = true;
                ViewData resolved = resolve_field_for_scheduling(outer_input.field(arg).ts_view(), time);
                non_multiplexed_inputs.emplace_back(arg, resolved);
                if (resolved.meta) {
                    switch (resolved.meta->kind) {
                        case TSKind::TSD:
                        case TSKind::TSS:
                        case TSKind::TSL:
                        case TSKind::TSB:
                            has_non_multiplexed_collection_inputs = true;
                            break;
                        default:
                            break;
                    }
                }
            }

            force_full_inner_eval_ = false;

            auto schedule_now = [this, now](const value::View& key_view) {
                auto it = scheduled_keys_.find(key_view);
                if (it == scheduled_keys_.end()) {
                    scheduled_keys_.emplace(value::PlainValue(key_view), now);
                } else if (it->second > now) {
                    it->second = now;
                }
            };

            if (has_non_multiplexed_collection_inputs) {
                key_set_type absent_non_multiplexed_keys_now;
                for (const auto& [key, _] : active_graphs_) {
                    bool key_needs_eval = false;
                    bool saw_tsd_input = false;
                    bool key_missing_in_tsd = false;
                    for (const auto& [arg, resolved_ref] : non_multiplexed_inputs) {
                        (void)arg;
                        ViewData resolved = resolved_ref;
                        if (!resolved.meta || !resolved.ops) {
                            continue;
                        }

                        if (resolved.meta->kind == TSKind::TSD) {
                            saw_tsd_input = true;
                            TSDView dict_view(resolved, time);
                            auto key_view = key.const_view();
                            if (dict_view.was_added(key_view) || dict_view.was_removed(key_view)) {
                                key_needs_eval = true;
                                break;
                            }
                            if (dict_view.contains(key_view)) {
                                TSView elem = dict_view.at(key_view);
                                if (elem.modified()) {
                                    key_needs_eval = true;
                                    break;
                                }
                            } else {
                                key_missing_in_tsd = true;
                            }
                        } else if (resolved.ops->modified(resolved, time)) {
                            key_needs_eval = true;
                            break;
                        }
                    }

                    if (key_needs_eval) {
                        protected_same_tick_schedules.emplace(value::PlainValue(key.const_view()));
                        schedule_now(key.const_view());
                    } else {
                        auto key_view = key.const_view();
                        bool preserve_first_absence_tick =
                            saw_tsd_input &&
                            key_missing_in_tsd &&
                            absent_non_multiplexed_keys_prev_.find(key_view) ==
                                absent_non_multiplexed_keys_prev_.end();

                        if (!preserve_first_absence_tick &&
                            created_keys_this_tick.find(key_view) == created_keys_this_tick.end() &&
                            protected_same_tick_schedules.find(key_view) == protected_same_tick_schedules.end()) {
                            auto scheduled_it = scheduled_keys_.find(key_view);
                            if (scheduled_it != scheduled_keys_.end() && scheduled_it->second == now) {
                                scheduled_keys_.erase(scheduled_it);
                            }
                        }
                    }

                    if (saw_tsd_input && key_missing_in_tsd) {
                        absent_non_multiplexed_keys_now.emplace(value::PlainValue(key.const_view()));
                    }
                }
                absent_non_multiplexed_keys_prev_ = std::move(absent_non_multiplexed_keys_now);
            } else if (has_non_multiplexed_inputs) {
                for (const auto& [key, _] : active_graphs_) {
                    schedule_now(key.const_view());
                }
                absent_non_multiplexed_keys_prev_.clear();
            } else {
                absent_non_multiplexed_keys_prev_.clear();
            }
        } else {
            force_full_inner_eval_ = false;
            absent_non_multiplexed_keys_prev_.clear();
        }

        // 2. Evaluate scheduled graphs
        auto old_scheduled = std::move(scheduled_keys_);
        scheduled_keys_.clear();

        for (auto& [key, dt] : old_scheduled) {
            auto key_view = key.const_view();
            if (dt < last_evaluation_time()) {
                throw std::runtime_error(
                    fmt::format("Scheduled time is in the past; last evaluation time: {:%Y-%m-%d %H:%M:%S}, "
                               "scheduled time: {:%Y-%m-%d %H:%M:%S}",
                               last_evaluation_time(), dt));
            } else if (dt == last_evaluation_time()) {
                dt = evaluate_graph(key_view);
            }

            if (dt != MAX_DT && dt > last_evaluation_time()) {
                // Re-schedule
                scheduled_keys_.emplace(value::PlainValue(key_view), dt);
                graph()->schedule_node(node_ndx(), dt);
            }
        }

        force_full_inner_eval_ = false;

    }

    // ========== Graph management ==========

    void TsdMapNode::create_new_graph(const value::View &key) {
        // Build graph ID: node_id + (-count_)
        auto graph_id = node_id();
        graph_id.push_back(-count_);
        count_++;

        // Convert key to string for label
        std::string key_label = key_type_meta_
            ? key_type_meta_->ops->to_string(key.data(), key_type_meta_)
            : "?";

        auto new_graph = nested_graph_builder_->make_instance(graph_id, this, key_label, /*use_arena=*/false);

        // Create MapNestedEngineEvaluationClock with a copy of the key
        auto clock = std::make_shared<MapNestedEngineEvaluationClock>(
            graph()->evaluation_engine_clock().get(),
            value::PlainValue(key),
            this);

        new_graph->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), clock));

        initialise_component(*new_graph);

        if (!recordable_id_.empty()) {
            std::string recordable_id = recordable_id_ + "[" + key_label + "]";
            set_parent_recordable_id(*new_graph, recordable_id);
        }

        wire_graph(key, new_graph);
        start_component(*new_graph);

        // Store in active graphs
        active_graphs_.emplace(value::PlainValue(key), new_graph);

        // Schedule for immediate evaluation
        scheduled_keys_.emplace(value::PlainValue(key), last_evaluation_time());
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) return;

        auto graph_ = it->second;

        active_graphs_.erase(it);

        // Unbind inputs BEFORE un_wire_graph and stop. un_wire_graph calls dict_remove
        // which notifies the TSD output's observer list. If inner graph nodes have
        // ActiveNotifiers subscribed to the output (e.g., via LinkTarget/TSInput bindings),
        // those must be unsubscribed first to prevent dangling Notifiable* pointers.
        for (size_t ni = 0; ni < graph_->nodes().size(); ni++) {
            auto& node = graph_->nodes()[ni];
            if (node->ts_input()) {
                ViewData vd = node->ts_input()->value().make_view_data();
                vd.uses_link_target = true;
                if (vd.ops && vd.ops->unbind) {
                    vd.ops->unbind(vd);
                }
            }
        }

        un_wire_graph(key, graph_);

        try {
            stop_component(*graph_);
        } catch (...) {}

        // Schedule deferred release
        auto builder = nested_graph_builder_;
        graph()->evaluation_engine()->add_before_evaluation_notification(
            [builder, graph_]() { builder->release_instance(graph_); });
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::View &key) {
        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) {
            return MAX_DT;
        }

        auto& inner_graph = it->second;
        if (auto* nec = dynamic_cast<NestedEngineEvaluationClock*>(
                inner_graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        // Refresh forwarded_target pointers before evaluation.
        // When multiple inner graphs are created in one tick, each dict_create may resize
        // the TSD's internal VarLists (observer, time, link), invalidating pointers that were
        // captured in previously-wired inner graphs' forwarded_targets.
        // Re-lookup the TSD element to get current (post-resize) pointers.
        if (output_node_id_ >= 0 && ts_output()) {
            auto inner_node = inner_graph->nodes()[output_node_id_];
            if (inner_node->ts_output() && inner_node->ts_output()->is_forwarded()) {
                auto time = graph()->evaluation_time();
                ViewData outer_data = ts_output()->native_value().make_view_data();
                // Clear link_data to prevent child_by_key from delegating through REFLink.
                // We want to navigate the local TSD storage, not the linked target.
                outer_data.link_data = nullptr;
                TSView elem_view = outer_data.ops->child_by_key(outer_data, key, time);
                if (elem_view.view_data().valid()) {
                    ViewData elem_vd = elem_view.view_data();
                    LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                    ft.value_data = elem_vd.value_data;
                    ft.time_data = elem_vd.time_data;
                    ft.observer_data = elem_vd.observer_data;
                    ft.delta_data = elem_vd.delta_data;
                    ft.link_data = elem_vd.link_data;
                    ft.ops = elem_vd.ops;
                    ft.meta = elem_vd.meta;
                }
            }
        }

        auto eval_time = *inner_graph->evaluation_engine_clock()->evaluation_time_ptr();
        if (force_full_inner_eval_) {
            // Some pass-through collection inputs update through REF paths that do not reliably
            // propagate scheduling through observers. Force a full inner pass for this tick.
            auto& sched = inner_graph->schedule();
            for (size_t i = 0; i < sched.size(); ++i) {
                if (sched[i] < eval_time) {
                    sched[i] = eval_time;
                }
            }
        } else if (output_node_id_ >= 0) {
            // Explicitly schedule the output stub so it evaluates during this inner graph pass.
            // The output stub's REF input is TS->REF (non-peered, set up by graph_builder's deferred
            // binding path), which does not subscribe to the compute node's output observer list.
            // Without this, the output stub would only run at initial wiring and never again.
            auto& sched = inner_graph->schedule()[output_node_id_];
            if (sched < eval_time) {
                sched = eval_time;
            }
        }

        if (signature().capture_exception) {
            try {
                inner_graph->evaluate_graph();
            } catch (const std::exception& e) {
                // TODO: write to error output TSD element for this key
            }
        } else {
            inner_graph->evaluate_graph();
        }

        engine_time_t next = MAX_DT;
        if (auto* nec = dynamic_cast<NestedEngineEvaluationClock*>(
                inner_graph->evaluation_engine_clock().get())) {
            next = nec->next_scheduled_evaluation_time();
            nec->reset_next_scheduled_evaluation_time();
        }

        return next;
    }

    // ========== Wiring ==========

    void TsdMapNode::wire_graph(const value::View &key, graph_s_ptr &graph_) {
        auto time = graph()->evaluation_time();

        for (const auto& [arg, node_ndx] : input_node_ids_) {
            auto inner_node = graph_->nodes()[node_ndx];
            inner_node->notify();

            if (arg == key_arg_) {
                // Key stub: set eval_fn.key on the PythonNode
                nb::object py_key = key_type_meta_
                    ? key_type_meta_->ops->to_python(key.data(), key_type_meta_)
                    : nb::none();
                auto* py_node = dynamic_cast<PythonNode*>(inner_node.get());
                if (py_node) {
                    nb::object eval_fn_obj = nb::borrow(py_node->eval_fn());
                    eval_fn_obj.attr("key") = py_key;
                }
            } else if (multiplexed_args_.count(arg)) {
                // Multiplexed input: find upstream TSD element for this key and wire inner stub to it.
                // The element may not exist yet (e.g., keys added before data arrives).
                // If missing, we defer wiring and retry in eval() when the input changes.
                auto outer_input_view = ts_input()->view(time);
                auto field_view = outer_input_view.field(arg);
                auto field_ts_view = field_view.ts_view();

                // Resolve through link to get the upstream TSD output data
                ViewData tsd_resolved = resolve_through_link(field_ts_view.view_data());
                // Override path with link target path (upstream output path)
                {
                    auto& vd = field_ts_view.view_data();
                    if (vd.uses_link_target && vd.link_data) {
                        auto* lt = static_cast<LinkTarget*>(vd.link_data);
                        if (lt->is_linked && lt->target_path.valid()) {
                            tsd_resolved.path = lt->target_path;
                        }
                    }
                }

                // Navigate to the upstream TSD element
                TSView tsd_element = tsd_resolved.ops->child_by_key(tsd_resolved, key, time);
                ShortPath element_path = tsd_element.view_data().path;

                if (!element_path.valid()) {
                    // Upstream element doesn't exist yet — defer wiring for this key.
                    // The stub's REF input stays empty; eval() will retry when inputs change.
                    pending_multiplexed_wirings_.emplace(value::PlainValue(key));

                    // Subscribe to the upstream TSD's observer list so we get notified
                    // when new elements are added (triggering re-evaluation and deferred wiring).
                    if (tsd_resolved.observer_data) {
                        if (!pending_wiring_notifier_) {
                            pending_wiring_notifier_ = std::make_unique<PendingWiringNotifier>(this);
                        }
                        auto* obs = static_cast<ObserverList*>(tsd_resolved.observer_data);
                        if (pending_wiring_subscriptions_.find(obs) == pending_wiring_subscriptions_.end()) {
                            obs->add_observer(pending_wiring_notifier_.get());
                            pending_wiring_subscriptions_.insert(obs);
                        }
                    }

                    continue;
                }

                // Write TSReference::peered(element_path) into the inner stub's REF input.
                // This makes the stub's ts.value return a BoundTimeSeriesReference,
                // which triggers the REFBindingHelper to resolve and bind the compute
                // node's input to the actual TSD element data.
                if (inner_node->ts_input()) {
                    auto inner_input_view = inner_node->ts_input()->view(time);
                    auto inner_meta = inner_node->ts_input()->meta();
                    if (inner_meta && inner_meta->kind == TSKind::TSB && inner_meta->field_count > 0) {
                        // Navigate to the "ts" field (first field in the stub's bundle)
                        auto ts_field = inner_input_view[0].ts_view();
                        map_set_ref_input_value(ts_field, element_path, time);
                    } else {
                        auto ts_field = inner_input_view.ts_view();
                        map_set_ref_input_value(ts_field, element_path, time);
                    }
                }
            } else {
                // Non-multiplexed input: bind inner stub to outer input's upstream source (like SwitchNode)
                auto outer_input_view = ts_input()->view(time);
                auto field_view = outer_input_view.field(arg);

                // Resolve outer field's ViewData through its LinkTarget
                ViewData resolved = resolve_through_link(field_view.ts_view().view_data());
                {
                    auto& vd = field_view.ts_view().view_data();
                    if (vd.uses_link_target && vd.link_data) {
                        auto* lt = static_cast<LinkTarget*>(vd.link_data);
                        if (lt->is_linked && lt->target_path.valid()) {
                            resolved.path = lt->target_path;
                        }
                    }
                }
                TSView resolved_target(resolved, time);

                // Bind the stub's input to the resolved outer data
                if (inner_node->ts_input()) {
                    auto inner_input_view = inner_node->ts_input()->view(time);
                    const TSMeta* inner_meta = inner_node->ts_input()->meta();
                    if (inner_meta && inner_meta->kind == TSKind::TSB) {
                        for (size_t fi = 0; fi < inner_meta->field_count; ++fi) {
                            auto inner_field_view = inner_input_view[fi];
                            inner_field_view.ts_view().bind(resolved_target);
                        }
                    } else {
                        inner_input_view.ts_view().bind(resolved_target);
                    }
                }
            }
        }

        // Wire output: create TSD element and forward inner output stub's output to it.
        // The output stub writes a TimeSeriesReference (BoundTimeSeriesReference pointing
        // to the compute node's output). The TSD element has REF kind, so from_python
        // stores the TSReference correctly. The child→container notifier propagates
        // element modifications up to the TSD collection.
        if (output_node_id_ >= 0 && ts_output()) {
            auto inner_node = graph_->nodes()[output_node_id_];
            if (inner_node->ts_output()) {
                // Create element in TSD output for this key
                ViewData outer_data = ts_output()->native_value().make_view_data();
                TSView elem_view = outer_data.ops->dict_create(outer_data, key, time);

                // Set up forwarded_target on the output stub's output
                ViewData elem_vd = elem_view.view_data();
                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = true;
                ft.value_data = elem_vd.value_data;
                ft.time_data = elem_vd.time_data;
                ft.observer_data = elem_vd.observer_data;
                ft.delta_data = elem_vd.delta_data;
                ft.link_data = elem_vd.link_data;
                ft.ops = elem_vd.ops;
                ft.meta = elem_vd.meta;

            }
        }
    }

    void TsdMapNode::un_wire_graph(const value::View &key, graph_s_ptr &graph_) {
        auto time = graph()->evaluation_time();

        // Clear forwarded_target on output stub and remove TSD element
        if (output_node_id_ >= 0 && ts_output()) {
            auto inner_node = graph_->nodes()[output_node_id_];
            if (inner_node->ts_output()) {
                LinkTarget& ft = inner_node->ts_output()->forwarded_target();
                ft.is_linked = false;
            }

            // Remove element from TSD output
            ViewData outer_data = ts_output()->native_value().make_view_data();
            if (outer_data.ops && outer_data.ops->dict_remove) {
                outer_data.ops->dict_remove(outer_data, key, time);
            }
        }
    }

    // ========== Deferred wiring for multiplexed inputs ==========

    bool TsdMapNode::try_wire_multiplexed_for_key(const value::View& key, engine_time_t time, bool* changed) {
        auto graph_it = active_graphs_.find(key);
        if (graph_it == active_graphs_.end()) return false;

        auto& inner_graph = graph_it->second;
        bool all_wired = true;
        bool key_changed = false;

        for (const auto& [arg, node_ndx] : input_node_ids_) {
            if (!multiplexed_args_.count(arg)) continue;

            auto inner_node = inner_graph->nodes()[node_ndx];

            auto outer_input_view = ts_input()->view(time);
            auto field_view = outer_input_view.field(arg);
            auto field_ts_view = field_view.ts_view();

            ViewData tsd_resolved = resolve_through_link(field_ts_view.view_data());
            {
                auto& vd = field_ts_view.view_data();
                if (vd.uses_link_target && vd.link_data) {
                    auto* lt = static_cast<LinkTarget*>(vd.link_data);
                    if (lt->is_linked && lt->target_path.valid()) {
                        tsd_resolved.path = lt->target_path;
                    }
                }
            }

            TSView tsd_element = tsd_resolved.ops->child_by_key(tsd_resolved, key, time);
            ShortPath element_path = tsd_element.view_data().path;

            TSReference new_ref = element_path.valid()
                                  ? TSReference::peered(element_path)
                                  : TSReference::empty();

            // Write the TSReference directly to the stub's REF OUTPUT.
            // This updates downstream REF binding helpers immediately.
            if (inner_node->ts_output()) {
                ViewData out_vd = inner_node->ts_output()->native_value().make_view_data();
                if (out_vd.value_data) {
                    auto* ref_ptr = static_cast<TSReference*>(out_vd.value_data);
                    TSReference old_ref = *ref_ptr;
                    if (old_ref != new_ref) {
                        *ref_ptr = std::move(new_ref);

                        bool binding_requires_eval = false;

                        // Only force eval on binding changes that materially change
                        // target identity (or make previous/new targets unresolved).
                        if (old_ref.is_empty() != ref_ptr->is_empty()) {
                            binding_requires_eval = true;
                        } else if (!old_ref.is_empty() && !ref_ptr->is_empty()) {
                            auto resolve_target = [time](const TSReference& ref) -> std::optional<ViewData> {
                                try {
                                    TSView tv = ref.resolve(time);
                                    if (!tv || !tv.view_data().valid()) {
                                        return std::nullopt;
                                    }
                                    return tv.view_data();
                                } catch (...) {
                                    return std::nullopt;
                                }
                            };

                            auto old_target = resolve_target(old_ref);
                            auto new_target = resolve_target(*ref_ptr);
                            if (!old_target || !new_target) {
                                binding_requires_eval = true;
                            }
                        }

                        key_changed = key_changed || binding_requires_eval;

                        if (out_vd.time_data) {
                            *static_cast<engine_time_t*>(out_vd.time_data) = time;
                        }
                        if (out_vd.observer_data) {
                            auto* obs = static_cast<ObserverList*>(out_vd.observer_data);
                            obs->notify_modified(time);
                        }
                    }
                }
            }

            if (!element_path.valid()) {
                all_wired = false;

                // Subscribe to upstream TSD observer so we retry when key appears.
                if (tsd_resolved.observer_data) {
                    if (!pending_wiring_notifier_) {
                        pending_wiring_notifier_ = std::make_unique<PendingWiringNotifier>(this);
                    }
                    auto* obs = static_cast<ObserverList*>(tsd_resolved.observer_data);
                    if (pending_wiring_subscriptions_.find(obs) == pending_wiring_subscriptions_.end()) {
                        obs->add_observer(pending_wiring_notifier_.get());
                        pending_wiring_subscriptions_.insert(obs);
                    }
                }
            }
        }

        if (changed) {
            *changed = key_changed;
        }
        return all_wired;
    }

    void TsdMapNode::try_wire_pending_keys(engine_time_t time) {
        if (pending_multiplexed_wirings_.empty()) return;

        for (auto it = pending_multiplexed_wirings_.begin(); it != pending_multiplexed_wirings_.end(); ) {
            auto key_view = it->const_view();
            if (try_wire_multiplexed_for_key(key_view, time)) {
                // Schedule the inner graph for evaluation now that it's wired
                scheduled_keys_.emplace(value::PlainValue(key_view), last_evaluation_time());

                // Also schedule the output stub in the inner graph so it evaluates
                // and writes the compute result to the TSD element. The output stub's
                // input is non-peered REF (TS→REF), so it won't be notified when the
                // compute node produces output — we must schedule it explicitly.
                if (output_node_id_ >= 0) {
                    auto graph_it = active_graphs_.find(key_view);
                    if (graph_it != active_graphs_.end()) {
                        auto& inner_graph = graph_it->second;
                        inner_graph->schedule()[output_node_id_] = time;
                    }
                }

                it = pending_multiplexed_wirings_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
