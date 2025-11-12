#include <hgraph/types/tss.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
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

namespace hgraph {
    template<typename K>
    MapNestedEngineEvaluationClock<K>::MapNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock, K key,
        tsd_map_node_ptr<K> nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node.get())),
          _key(key) {
    }

    template<typename K>
    void MapNestedEngineEvaluationClock<K>::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto &node_{*static_cast<TsdMapNode<K> *>(node().get())};
        auto let{node_.last_evaluation_time()};
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) { return; }

        auto it{node_.scheduled_keys_.find(_key)};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) { node_.scheduled_keys_[_key] = next_time; }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    template<typename K>
    TsdMapNode<K>::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                              nb::dict scalars, graph_builder_ptr nested_graph_builder,
                              const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                              const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars), nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args),
          key_arg_(key_arg) {
    }

    template<typename K>
    std::unordered_map<K, graph_ptr> &TsdMapNode<K>::nested_graphs() { return active_graphs_; }

    template<typename K>
    void TsdMapNode<K>::enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const {
        for (const auto& [key, graph] : active_graphs_) {
            if (graph) {
                callback(graph);
            }
        }
    }

    template<typename K>
    void TsdMapNode<K>::initialise() {
    }

    template<typename K>
    void TsdMapNode<K>::do_start() {
        // Note: In Python, super().do_start() is called here, but in C++ the base Node class
        // do_start() is pure virtual and has no implementation to call.
        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(),
                                                  recordable_id.has_value() ? recordable_id.value() : "map_");
        }
    }

    template<typename K>
    void TsdMapNode<K>::do_stop() {
        std::vector<K> keys;
        keys.reserve(active_graphs_.size());
        for (const auto &[k, _]: active_graphs_) { keys.push_back(k); }
        for (const auto &k: keys) { remove_graph(k); }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    template<typename K>
    void TsdMapNode<K>::dispose() {
    }

    template<typename K>
    void TsdMapNode<K>::eval() {
        mark_evaluated();

        auto &keys = dynamic_cast<TimeSeriesSetInput_T<K> &>(*(*input())[KEYS_ARG]);
        if (keys.modified()) {
            for (const auto &k: keys.added()) {
                // There seems to be a case where a set can show a value as added even though it is not.
                // This protects from accidentally creating duplicate graphs
                if (active_graphs_.find(k) == active_graphs_.end()) {
                    create_new_graph(k);
                }
                // If key already exists, skip it (can happen during startup before reset_prev() is called)
            }
            for (const auto &k: keys.removed()) {
                if (auto it = active_graphs_.find(k); it != active_graphs_.end()) {
                    remove_graph(k);
                    scheduled_keys_.erase(k);
                } else {
                    throw std::runtime_error(
                        fmt::format("[{}] Key {} does not exist in active graphs", signature().wiring_path_name,
                                    to_string(k)));
                }
            }
        }

        std::unordered_map<K, engine_time_t> scheduled_keys;
        std::swap(scheduled_keys, scheduled_keys_);

        for (const auto &[k, dt]: scheduled_keys) {
            if (dt < last_evaluation_time()) {
                throw std::runtime_error(
                    fmt::format(
                        "Scheduled time is in the past; last evaluation time: {}, scheduled time: {}, evaluation time: {}",
                        last_evaluation_time(), dt, graph()->evaluation_clock()->evaluation_time()));
            }
            engine_time_t next_dt;
            if (dt == last_evaluation_time()) {
                next_dt = evaluate_graph(k);
            } else {
                next_dt = dt;
            }
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_[k] = next_dt;
                graph()->schedule_node(node_ndx(), next_dt);
            }
        }
    }

    template<typename K>
    TimeSeriesDictOutput_T<K> &TsdMapNode<K>::tsd_output() {
        return dynamic_cast<TimeSeriesDictOutput_T<K> &>(*output());
    }

    template<typename K>
    void TsdMapNode<K>::create_new_graph(const K &key) {
        // Extend parent's node_id with the new instance counter
        auto child_owning_graph_id = node_id();
        child_owning_graph_id.push_back(-static_cast<int64_t>(count_++));
        auto graph_{
            nested_graph_builder_->make_instance(child_owning_graph_id, this,
                                                 to_string(key)) // This will come back to haunt me :(
        };

        active_graphs_[key] = graph_;

        graph_->set_evaluation_engine(new NestedEvaluationEngine(
            graph()->evaluation_engine(),
            new MapNestedEngineEvaluationClock<K>(graph()->evaluation_engine()->engine_evaluation_clock(), key, this)));

        initialise_component(*graph_);

        if (!recordable_id_.empty()) {
            auto nested_recordable_id = fmt::format("{}[{}]", recordable_id_, to_string(key));
            set_parent_recordable_id(*graph_, nested_recordable_id);
        }

        wire_graph(key, graph_);
        start_component(*graph_);
        scheduled_keys_[key] = last_evaluation_time();
    }

    template<typename K>
    void TsdMapNode<K>::remove_graph(const K &key) {
        if (signature().capture_exception) {
            // Remove the error output associated to the graph if there is one
            auto &error_output_ = dynamic_cast<TimeSeriesDictOutput_T<K> &>(*error_output());
            error_output_.erase(key);
        }

        auto graph{active_graphs_[key]};
        active_graphs_.erase(key);

        un_wire_graph(key, graph);

        auto cleanup = make_scope_exit([this, graph=graph]() {
            // Release the graph back to the builder pool (which will call dispose)
            nested_graph_builder_->release_instance(graph);
        });
        stop_component(*graph);
    }

    template<typename K>
    engine_time_t TsdMapNode<K>::evaluate_graph(const K &key) {
        auto &graph = active_graphs_[key];
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        if (signature().capture_exception) {
            try {
                graph->evaluate_graph();
            } catch (const std::exception &e) {
                auto &error_tsd = dynamic_cast<TimeSeriesDictOutput_T<K> &>(*error_output());
                auto msg = std::string("key: ") + to_string(key);
                auto node_error = NodeError::capture_error(e, *this, msg);
                auto error_ts = error_tsd._get_or_create(key);
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

    template<typename K>
    void TsdMapNode<K>::un_wire_graph(const K &key, Graph::ptr &graph) {
        for (const auto &[arg, node_ndx]: input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            if (arg != key_arg_) {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto ts{static_cast<TimeSeriesInput *>((*input())[arg].get())};
                    auto &tsd =
                            dynamic_cast<TimeSeriesDictInput_T<K> &>(*ts);
                    // Since this is a multiplexed arg it must be of type K

                    // Re-parent the per-key input back to the TSD to detach it from the nested graph
                    (*node->input())["ts"]->re_parent(ts);

                    // Create a new empty reference input to replace the old one in the node's input bundle
                    // This ensures the per-key input is fully detached before the nested graph is torn down
                    auto empty_ref = nb::ref<TimeSeriesReferenceInput>(new TimeSeriesReferenceInput(node));
                    node->reset_input(node->input()->copy_with(node, {empty_ref.get()}));
                    empty_ref->re_parent(node->input().get());

                    // Align with Python: only clear upstream per-key state when the key is truly absent
                    // from the upstream key set (and that key set is valid). Do NOT clear during startup
                    // when the key set may be invalid, as this breaks re-add semantics.
                    auto &key_set = dynamic_cast<TimeSeriesSetInput_T<K> &>(tsd.key_set());
                    if (key_set.valid() && !key_set.contains(key)) {
                        tsd.on_key_removed(key);
                    }
                }
            }
        }

        if (output_node_id_ >= 0) { tsd_output().erase(key); }
    }

    template<typename K>
    void TsdMapNode<K>::wire_graph(const K &key, Graph::ptr &graph) {
        for (const auto &[arg, node_ndx]: input_node_ids_) {
            auto node{graph->nodes()[node_ndx]};
            node->notify();

            if (arg == key_arg_) {
                auto key_node{dynamic_cast<PythonNode &>(*node)};
                // This relies on the current stub binding mechanism with a stub python class to hold the key.
                nb::setattr(key_node.eval_fn(), "key", nb::cast(key));
            } else {
                if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                    auto ts = static_cast<TimeSeriesInput *>((*input())[arg].get());
                    auto &tsd = dynamic_cast<TimeSeriesDictInput_T<K> &>(*ts);
                    auto ts_value = tsd.get_or_create(key);

                    node->reset_input(node->input()->copy_with(node, {ts_value}));
                    ts_value->re_parent(node->input().get());
                } else {
                    auto ts = dynamic_cast<TimeSeriesReferenceInput *>((*input())[arg].get());
                    auto inner_input = dynamic_cast<TimeSeriesReferenceInput *>((*node->input())["ts"].get());

                    if (ts != nullptr && inner_input != nullptr) { inner_input->clone_binding(ts); }
                }
            }
        }

        if (output_node_id_ >= 0) {
            auto node = graph->nodes()[output_node_id_];
            auto &output_tsd = tsd_output();
            auto output_ts = output_tsd._get_or_create(key);
            node->set_output(output_ts.get());
        }
    }

    using TsdMapNode_bool = TsdMapNode<bool>;

    // Explicit template instantiations to ensure symbols are emitted in this TU
    template struct TsdMapNode<bool>;
    template struct TsdMapNode<int64_t>;
    template struct TsdMapNode<double>;
    template struct TsdMapNode<engine_date_t>;
    template struct TsdMapNode<engine_time_t>;
    template struct TsdMapNode<engine_time_delta_t>;
    template struct TsdMapNode<nb::object>;

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_ < MapNestedEngineEvaluationClock<bool>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_bool");
        nb::class_ < MapNestedEngineEvaluationClock<int64_t>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_int");
        nb::class_ < MapNestedEngineEvaluationClock<double>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_float");
        nb::class_ < MapNestedEngineEvaluationClock<engine_date_t>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_date");
        nb::class_ < MapNestedEngineEvaluationClock<engine_time_t>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_datetime");
        nb::class_ < MapNestedEngineEvaluationClock<engine_time_delta_t>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_timedelta");
        nb::class_ < MapNestedEngineEvaluationClock<nb::object>, NestedEngineEvaluationClock > (
            m, "MapNestedEngineEvaluationClock_object");

        nb::class_<TsdMapNode<bool>, NestedNode>(m, "TsdMapNode_bool")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<bool>::nested_graphs);
        nb::class_<TsdMapNode<int64_t>, NestedNode>(m, "TsdMapNode_int")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<int64_t>::nested_graphs);
        nb::class_<TsdMapNode<double>, NestedNode>(m, "TsdMapNode_float")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<double>::nested_graphs);
        nb::class_<TsdMapNode<engine_date_t>, NestedNode>(m, "TsdMapNode_date")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<engine_date_t>::nested_graphs);
        nb::class_<TsdMapNode<engine_time_t>, NestedNode>(m, "TsdMapNode_datetime")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<engine_time_t>::nested_graphs);
        nb::class_<TsdMapNode<engine_time_delta_t>, NestedNode>(m, "TsdMapNode_timedelta")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<engine_time_delta_t>::nested_graphs);
        nb::class_<TsdMapNode<nb::object>, NestedNode>(m, "TsdMapNode_object")
                .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::ptr, nb::dict, graph_builder_ptr,
                         const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<
                             std::string> &,
                         const std::string &>(),
                     "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a,
                     "input_node_ids"_a,
                     "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
                .def_prop_ro("nested_graphs", &TsdMapNode<nb::object>::nested_graphs);
    }
} // namespace hgraph