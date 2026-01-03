#include <hgraph/types/tss.h>
#include <hgraph/types/value/value.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <hgraph/util/scope.h>

namespace hgraph
{
    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::PlainValue key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto &node_{*static_cast<TsdMapNode *>(node())};
        auto  let{node_.last_evaluation_time()};
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) { return; }

        auto it{node_.scheduled_keys_.find(_key.const_view())};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) {
            node_.scheduled_keys_.insert_or_assign(_key.const_view().clone(), next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<TsdMapNode*>(node());
        const auto* key_schema = node_->key_type_meta();
        return key_schema->ops->to_python(_key.data(), key_schema);
    }

    TsdMapNode::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                           const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                           const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars), nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    nb::dict TsdMapNode::py_nested_graphs() const {
        nb::dict result;
        for (const auto &[key, graph] : active_graphs_) {
            nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
            result[py_key] = nb::cast(graph);
        }
        return result;
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        for (const auto &[key, graph] : active_graphs_) {
            if (graph) { callback(graph); }
        }
    }

    void TsdMapNode::initialise() {
        // Get key type metadata from the keys input (TimeSeriesSetInput)
        auto &keys = dynamic_cast<TimeSeriesSetInput &>(*(*input())[KEYS_ARG]);
        key_type_meta_ = keys.element_type();
    }

    void TsdMapNode::do_start() {
        // Note: In Python, super().do_start() is called here, but in C++ the base Node class
        // do_start() is pure virtual and has no implementation to call.
        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(), recordable_id.has_value() ? recordable_id.value() : "map_");
        }
    }

    void TsdMapNode::do_stop() {
        // Collect all keys first (can't erase while iterating)
        std::vector<value::PlainValue> keys;
        keys.reserve(active_graphs_.size());
        for (const auto &[k, _] : active_graphs_) {
            keys.push_back(k.const_view().clone());
        }
        for (const auto &k : keys) {
            remove_graph(k.const_view());
        }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    void TsdMapNode::dispose() {}

    void TsdMapNode::eval() {
        mark_evaluated();

        auto &keys = dynamic_cast<TimeSeriesSetInput &>(*(*input())[KEYS_ARG]);
        if (keys.modified()) {
            // Use INPUT's collect_added() which handles sampled() case (returns all values when first bound)
            // Process added keys using Value-based iteration
            auto added_keys = keys.collect_added();
            for (size_t i = 0; i < added_keys.size(); ++i) {
                auto& key = added_keys[i];
                // There seems to be a case where a set can show a value as added even though it is not.
                // This protects from accidentally creating duplicate graphs
                if (active_graphs_.find(key.const_view()) == active_graphs_.end()) {
                    create_new_graph(key.const_view());
                }
                // If key already exists, skip it (can happen during startup before reset_prev() is called)
            }
            // Use INPUT's collect_removed() which handles sampled() case (returns empty when first bound)
            for (auto& key : keys.collect_removed()) {
                auto key_view = key.const_view();
                if (auto it = active_graphs_.find(key_view); it != active_graphs_.end()) {
                    remove_graph(key_view);
                    // Use iterator-based erase (heterogeneous erase not available until C++23)
                    if (auto sched_it = scheduled_keys_.find(key_view); sched_it != scheduled_keys_.end()) {
                        scheduled_keys_.erase(sched_it);
                    }
                } else {
                    nb::object py_key = key_type_meta_->ops->to_python(key_view.data(), key_type_meta_);
                    throw std::runtime_error(
                        fmt::format("[{}] Key {} does not exist in active graphs", signature().wiring_path_name,
                                    nb::repr(py_key).c_str()));
                }
            }
        }

        key_time_map_type scheduled_keys;
        std::swap(scheduled_keys, scheduled_keys_);

        for (const auto &[k, dt] : scheduled_keys) {
            if (dt < last_evaluation_time()) {
                nb::object py_key = key_type_meta_->ops->to_python(k.const_view().data(), key_type_meta_);
                throw std::runtime_error(
                    fmt::format("Scheduled time is in the past; last evaluation time: {}, scheduled time: {}, evaluation time: {}",
                                last_evaluation_time(), dt, graph()->evaluation_time()));
            }
            engine_time_t next_dt;
            if (dt == last_evaluation_time()) {
                next_dt = evaluate_graph(k.const_view());
            } else {
                next_dt = dt;
            }
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_.insert_or_assign(k.const_view().clone(), next_dt);
                graph()->schedule_node(node_ndx(), next_dt);
            }
        }
    }

    TimeSeriesDictOutputImpl &TsdMapNode::tsd_output() {
        return dynamic_cast<TimeSeriesDictOutputImpl &>(*output());
    }

    void TsdMapNode::create_new_graph(const value::ConstValueView &key) {
        // Convert key to string for graph label
        nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
        std::string key_str = nb::repr(py_key).c_str();

        // Extend parent's node_id with the new instance counter
        auto child_owning_graph_id = node_id();
        child_owning_graph_id.push_back(-static_cast<int64_t>(count_++));
        auto graph_{
            nested_graph_builder_->make_instance(child_owning_graph_id, this, key_str)
        };

        active_graphs_.emplace(key.clone(), graph_);

        graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(),
            std::make_shared<MapNestedEngineEvaluationClock>(graph()->evaluation_engine()->engine_evaluation_clock().get(),
                                                            key.clone(), this)));

        initialise_component(*graph_);

        if (!recordable_id_.empty()) {
            auto nested_recordable_id = fmt::format("{}[{}]", recordable_id_, key_str);
            set_parent_recordable_id(*graph_, nested_recordable_id);
        }

        wire_graph(key, graph_);
        start_component(*graph_);
        scheduled_keys_.emplace(key.clone(), last_evaluation_time());
    }

    void TsdMapNode::remove_graph(const value::ConstValueView &key) {
        if (signature().capture_exception) {
            // Remove the error output associated to the graph if there is one
            auto &error_output_ = dynamic_cast<TimeSeriesDictOutputImpl &>(*error_output());
            error_output_.erase(key);
        }

        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) { return; }
        auto graph = it->second;
        active_graphs_.erase(it);

        un_wire_graph(key, graph);

        auto cleanup = make_scope_exit([this, graph = graph]() {
            // Release the graph back to the builder pool (which will call dispose)
            nested_graph_builder_->release_instance(graph);
        });
        stop_component(*graph);
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::ConstValueView &key) {
        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) {
            return MAX_DT;
        }
        auto &graph = it->second;
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        if (signature().capture_exception) {
            try {
                graph->evaluate_graph();
            } catch (const std::exception &e) {
                auto &error_tsd  = dynamic_cast<TimeSeriesDictOutputImpl &>(*error_output());
                nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
                auto  msg        = std::string("key: ") + nb::repr(py_key).c_str();
                auto  node_error = NodeError::capture_error(e, *this, msg);
                auto  error_ts   = error_tsd.get_or_create(key);
                // Create a heap-allocated copy managed by nanobind
                auto error_ptr = nb::ref<NodeError>(new NodeError(node_error));
                error_ts->py_set_value(nb::cast(error_ptr));
            }
        } else {
            graph->evaluate_graph();
        }

        auto next = graph->evaluation_engine_clock()->next_scheduled_evaluation_time();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        return next;
    }

    void TsdMapNode::un_wire_graph(const value::ConstValueView &key, graph_s_ptr &graph) {
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            if (arg != key_arg_) {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto  ts{static_cast<TimeSeriesInput *>((*input())[arg].get())};
                    auto &tsd = dynamic_cast<TimeSeriesDictInputImpl &>(*ts);

                    // Make the per-key input passive to unsubscribe from output before re-parenting
                    // CRITICAL: must do this before re-parenting to prevent dangling subscriber pointers
                    auto per_key_input = (*node->input())["ts"];
                    per_key_input->make_passive();

                    // Re-parent the per-key input back to the TSD to detach it from the nested graph
                    per_key_input->re_parent(static_cast<time_series_input_ptr>(ts));

                    // Create a new empty reference input to replace the old one in the node's input bundle
                    // This ensures the per-key input is fully detached before the nested graph is torn down
                    auto empty_ref_owner = std::dynamic_pointer_cast<TimeSeriesReferenceInput>(node->input()->get_input(0));
                    auto empty_ref = empty_ref_owner->clone_blank_ref_instance();
                    node->reset_input(node->input()->copy_with(node.get(), {empty_ref}));
                    dynamic_cast<TimeSeriesReferenceInput *>(empty_ref.get())->re_parent(static_cast<time_series_input_ptr>(node->input().get()));

                    // Align with Python: only clear upstream per-key state when the key is truly absent
                    // from the upstream key set (and that key set is valid). Do NOT clear during startup
                    // when the key set may be invalid, as this breaks re-add semantics.
                    auto &key_set = dynamic_cast<TimeSeriesSetInput &>(tsd.key_set());
                    if (key_set.valid() && !key_set.contains(key)) { tsd.on_key_removed(key); }
                }
            }
        }

        if (output_node_id_ >= 0) {
            tsd_output().erase(key);
        }
    }

    void TsdMapNode::wire_graph(const value::ConstValueView &key, graph_s_ptr &graph) {
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node{graph->nodes()[node_ndx]};
            node->notify();

            if (arg == key_arg_) {
                auto key_node{dynamic_cast<PythonNode &>(*node)};
                // This relies on the current stub binding mechanism with a stub python class to hold the key.
                nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
                nb::setattr(key_node.eval_fn(), "key", py_key);
            } else {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto  ts       = static_cast<TimeSeriesInput *>((*input())[arg].get());
                    auto &tsd      = dynamic_cast<TimeSeriesDictInputImpl &>(*ts);
                    auto  ts_value = tsd.get_or_create(key);

                    node->reset_input(node->input()->copy_with(node.get(), {ts_value->shared_from_this()}));
                    ts_value->re_parent(static_cast<time_series_input_ptr>(node->input().get()));
                } else {
                    auto ts          = dynamic_cast<TimeSeriesReferenceInput *>((*input())[arg].get());
                    auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>((*node->input())["ts"].get());

                    if (ts != nullptr && inner_input != nullptr) { inner_input->clone_binding(ts); }
                }
            }
        }

        if (output_node_id_ >= 0) {
            auto  node       = graph->nodes()[output_node_id_];
            auto &output_tsd = tsd_output();
            auto  output_ts  = output_tsd.get_or_create(key);
            node->set_output(output_ts->shared_from_this());
        }
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        // Register single non-templated MapNestedEngineEvaluationClock
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        // Register single non-templated TsdMapNode
        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
