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

        // Clear output delta for this tick (lazy clearing).
        // dict_create/dict_remove modify the TSD via MapStorage which notifies MapDelta via SlotObserver,
        // but nothing else triggers the lazy delta clear. We must do it here at the start of each tick.
        if (ts_output()) {
            ts_output()->native_value().delta_value_view(time);
        }

        auto outer_input = ts_input()->view(time);

        // 1. Process key changes from __keys__ TSS input
        auto keys_field = outer_input.field(KEYS_ARG).ts_view();
        ViewData keys_resolved = resolve_through_link(keys_field.view_data());

        if (keys_resolved.valid() && keys_resolved.ops && keys_resolved.ops->modified(keys_resolved, time)) {
            TSSView keys_view(keys_resolved, time);

            // Process added keys
            for (auto key_val : keys_view.added()) {
                value::PlainValue pv_key(key_val);
                if (active_graphs_.find(key_val) == active_graphs_.end()) {
                    create_new_graph(key_val);
                }
            }

            // Process removed keys
            for (auto key_val : keys_view.removed()) {
                if (active_graphs_.find(key_val) != active_graphs_.end()) {
                    remove_graph(key_val);
                    // Remove from scheduled keys
                    auto sk_it = scheduled_keys_.find(key_val);
                    if (sk_it != scheduled_keys_.end()) {
                        scheduled_keys_.erase(sk_it);
                    }
                    // Remove from pending wirings
                    auto pw_it = pending_multiplexed_wirings_.find(key_val);
                    if (pw_it != pending_multiplexed_wirings_.end()) {
                        pending_multiplexed_wirings_.erase(pw_it);
                    }
                }
            }

            // Reconciliation: when keys are modified but SetDelta is empty (e.g., REF rebind
            // to a new TSS), the delta-based processing above won't catch orphaned active graphs.
            // Compare active_graphs_ against the actual current key set values to remove any
            // graphs whose keys no longer exist, and add any new keys.
            if (!active_graphs_.empty()) {
                // Build set of current keys for quick lookup
                std::unordered_set<value::PlainValue, PlainValueHash, PlainValueEqual> current_keys;
                for (auto kv : keys_view.values()) {
                    current_keys.emplace(kv);
                }

                // Remove active graphs whose keys are not in the current set
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
        } else if (!keys_resolved.valid() && !active_graphs_.empty()) {
            // Keys input became invalid (e.g., switch changed case and invalidated output).
            // In Python, this triggers REF-rebinding which unbinds the TSS input, and the
            // _prev_output mechanism computes removals for all previously-active keys.
            // In C++, we handle this directly: remove all active graphs.
            // Collect keys to remove (can't modify active_graphs_ while iterating)
            std::vector<value::PlainValue> keys_to_remove;
            keys_to_remove.reserve(active_graphs_.size());
            for (auto& [key, _] : active_graphs_) {
                keys_to_remove.emplace_back(key.const_view());
            }
            for (auto& key : keys_to_remove) {
                remove_graph(key.const_view());
            }
            scheduled_keys_.clear();
        }

        // 1.5 Try to complete any deferred multiplexed wirings
        try_wire_pending_keys(time);

        // Unsubscribe from upstream TSD observers once all pending wirings are resolved
        if (pending_multiplexed_wirings_.empty()) {
            clear_pending_wiring_subscriptions();
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

        un_wire_graph(key, graph_);

        try {
            stop_component(*graph_);
        } catch (...) {}

        // Unbind inputs of removed nodes AFTER stop but BEFORE dispose/release.
        // This unsubscribes REFBindingHelpers from observer lists of surviving nodes,
        // preventing dangling Notifiable* pointers. Same pattern as Graph::reduce_graph.
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

    bool TsdMapNode::try_wire_multiplexed_for_key(const value::View& key, engine_time_t time) {
        auto graph_it = active_graphs_.find(key);
        if (graph_it == active_graphs_.end()) return false;

        auto& inner_graph = graph_it->second;
        bool all_wired = true;

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

            if (!element_path.valid()) {
                all_wired = false;
                continue;
            }

            // Deferred wiring: write the TSReference directly to the stub's REF OUTPUT.
            // We bypass the stub's eval function because:
            // 1. The graph is already started — make_active() already ran (with no binding)
            // 2. TSView::bind() on the stub's REF input doesn't establish subscriptions
            // 3. Writing directly to the output triggers the REFBindingHelper which
            //    resolves the reference and updates the downstream node's input
            if (inner_node->ts_output()) {
                ViewData out_vd = inner_node->ts_output()->native_value().make_view_data();
                if (out_vd.value_data) {
                    auto* ref_ptr = static_cast<TSReference*>(out_vd.value_data);
                    *ref_ptr = TSReference::peered(element_path);
                }
                if (out_vd.time_data) {
                    *static_cast<engine_time_t*>(out_vd.time_data) = time;
                }
                if (out_vd.observer_data) {
                    auto* obs = static_cast<ObserverList*>(out_vd.observer_data);
                    obs->notify_modified(time);
                }
            }
            // Don't call inner_node->notify() — we bypassed the stub by writing directly
            // to its output. The REFBindingHelper will schedule the downstream compute node.
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
