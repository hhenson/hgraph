#include <cstdlib>
#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/python/hashable.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tss.h>
#include <hgraph/util/string_utils.h>
#include <hgraph/util/scope.h>

namespace hgraph {
    // MeshNestedEngineEvaluationClock implementation
    template<typename K>
    MeshNestedEngineEvaluationClock<K>::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock, K key,
        mesh_node_ptr<K> nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock),
                                      nested_node_ptr(static_cast<NestedNode *>(nested_node.get()))),
          _key(key) {
    }

    template<typename K>
    void MeshNestedEngineEvaluationClock<K>::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        // Cast nested_node_ptr to MeshNode<K> using dynamic_cast
        auto node = dynamic_cast<MeshNode<K> *>(_nested_node.get());
        if (!node) {
            return; // Safety check - should not happen
        }

        // Check if we should skip scheduling
        auto let = node->last_evaluation_time();
        if ((let != MIN_DT && let > next_time) || node->is_stopping()) { return; }

        auto rank = node->active_graphs_rank_[_key];

        // If already scheduled for current time at current rank, skip
        if (next_time == let &&
            (rank == node->current_eval_rank_ ||
             (node->current_eval_graph_.has_value() && std::equal_to<K>()(node->current_eval_graph_.value(), _key)))) {
            return;
        }

        // Check if we need to reschedule
        auto it = node->scheduled_keys_by_rank_[rank].find(_key);
        engine_time_t tm = (it != node->scheduled_keys_by_rank_[rank].end()) ? it->second : MIN_DT;

        if (tm == MIN_DT || tm > next_time || tm < node->graph()->evaluation_clock()->evaluation_time()) {
            node->schedule_graph(_key, next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    template<typename K>
    MeshNode<K>::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                          nb::dict scalars,
                          graph_builder_ptr nested_graph_builder,
                          const std::unordered_map<std::string, int64_t> &input_node_ids,
                          int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                          const std::string &key_arg, const std::string &context_path)
        : TsdMapNode<K>(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                        std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg) {
        // Build full context key using centralized key builder to match Python format
        full_context_path_ = keys::context_output_key(this->owning_graph_id(), context_path);
    }

    template<typename K>
    void MeshNode<K>::do_start() {
        TsdMapNode<K>::do_start();

        // Set up the reference output and register in GlobalState
        if (GlobalState::has_instance()) {
            auto *tsb_output = dynamic_cast<TimeSeriesBundleOutput *>(this->output().get());
            // Get the "out" and "ref" outputs from the output bundle
            auto &tsd_output = dynamic_cast<TimeSeriesDictOutput_T<K> &>(*(*tsb_output)["out"]);
            auto &ref_output = dynamic_cast<TimeSeriesReferenceOutput &>(*(*tsb_output)["ref"]);

            // Create a TimeSeriesReference from the "out" output and set it on the "ref" output
            auto reference = TimeSeriesReference::make(time_series_output_ptr(&tsd_output));
            ref_output.set_value(reference);

            // Store the ref output in GlobalState
            GlobalState::set(full_context_path_, nb::cast(ref_output));
        } else {
            throw std::runtime_error("GlobalState instance required for MeshNode");
        }
    }

    template<typename K>
    void MeshNode<K>::do_stop() {
        // Remove from GlobalState
        if (GlobalState::has_instance()) { GlobalState::remove(full_context_path_); }

        TsdMapNode<K>::do_stop();
    }

    template<typename K>
    void MeshNode<K>::eval() {
        this->mark_evaluated();

        // 1. Process keys input (additions/removals)
        auto &input_bundle = dynamic_cast<TimeSeriesBundleInput &>(*this->input());
        auto &keys = dynamic_cast<TimeSeriesSetInput_T<K> &>(*input_bundle[TsdMapNode<K>::KEYS_ARG]);
        if (keys.modified()) {
            for (const auto &k: keys.added()) {
                if (this->active_graphs_.find(k) == this->active_graphs_.end()) {
                    create_new_graph(k);

                    // If this key was pending (requested as a dependency), re-rank its dependents
                    if (this->pending_keys_.count(k) > 0) {
                        this->pending_keys_.erase(k);
                        for (const auto &d: active_graphs_dependencies_[k]) {
                            re_rank(d, k);
                        }
                    }
                }
            }
            for (const auto &k: keys.removed()) {
                // Only remove if no dependencies
                if (active_graphs_dependencies_[k].empty()) {
                    scheduled_keys_by_rank_[active_graphs_rank_[k]].erase(k);
                    remove_graph(k);
                }
            }
        }

        // 2. Process pending keys (keys added due to dependencies)
        if (!this->pending_keys_.empty()) {
            for (const auto &k: this->pending_keys_) {
                create_new_graph(k, 0);
                for (const auto &d: active_graphs_dependencies_[k]) {
                    re_rank(d, k);
                }
            }
            this->pending_keys_.clear();
        }

        // 3. Process graphs to remove
        if (!graphs_to_remove_.empty()) {
            for (const auto &k: graphs_to_remove_) {
                if (active_graphs_dependencies_[k].empty() && !keys.contains(k)) { remove_graph(k); }
            }
            graphs_to_remove_.clear();
        }

        // 4. Evaluate scheduled graphs by rank
        engine_time_t next_time = MAX_DT;
        int rank = 0;
        while (rank <= max_rank_) {
            current_eval_rank_ = rank;
            auto rank_it = scheduled_ranks_.find(rank);
            engine_time_t dt = (rank_it != scheduled_ranks_.end()) ? rank_it->second : MIN_DT;

            if (dt == this->last_evaluation_time()) {
                scheduled_ranks_.erase(rank);
                auto graphs_it = scheduled_keys_by_rank_.find(rank);
                if (graphs_it != scheduled_keys_by_rank_.end()) {
                    auto graphs = std::move(graphs_it->second);
                    scheduled_keys_by_rank_.erase(rank);

                    for (const auto &[k, dtg]: graphs) {
                        if (dtg == dt) {
                            current_eval_graph_ = k;
                            engine_time_t next_dtg = this->evaluate_graph(k);
                            current_eval_graph_ = std::nullopt;

                            if (next_dtg != MAX_DT && next_dtg > this->last_evaluation_time()) {
                                schedule_graph(k, next_dtg);
                                next_time = std::min(next_time, next_dtg);
                            }
                        } else if (dtg != MAX_DT && dtg > this->last_evaluation_time()) {
                            schedule_graph(k, dtg);
                            next_time = std::min(next_time, dtg);
                        }
                    }
                }
            } else if (dt != MIN_DT && dt > this->last_evaluation_time()) {
                next_time = std::min(next_time, dt);
            }

            rank++;
        }

        current_eval_rank_ = std::nullopt;

        // 5. Process re-ranking requests
        if (!re_rank_requests_.empty()) {
            for (const auto &[k, d]: re_rank_requests_) { re_rank(k, d); }
            re_rank_requests_.clear();
        }

        // 6. Schedule next evaluation if needed
        if (next_time < MAX_DT) { this->graph()->schedule_node(this->node_ndx(), next_time); }
    }

    template<typename K>
    TimeSeriesDictOutput_T<K> &MeshNode<K>::tsd_output() {
        // Access output bundle's "out" member - output() returns smart pointer to TimeSeriesBundleOutput
        auto *output_bundle = dynamic_cast<TimeSeriesBundleOutput *>(this->output().get());
        return dynamic_cast<TimeSeriesDictOutput_T<K> &>(*(*output_bundle)["out"]);
    }

    template<typename K>
    void MeshNode<K>::create_new_graph(const K &key, int rank) {
        // Create new graph instance - concatenate node_id with negative count
        std::vector<int64_t> graph_id = this->node_id();
        graph_id.push_back(-static_cast<int64_t>(this->count_++));
        auto graph = this->nested_graph_builder_->make_instance(graph_id, this, to_string(key));

        this->active_graphs_[key] = graph;
        active_graphs_rank_[key] = (rank == -1) ? max_rank_ : rank;

        // Set up evaluation engine with MeshNestedEngineEvaluationClock
        // Pattern from TsdMapNode: new NestedEvaluationEngine(&eval_engine, new Clock(&clock, key, this))
        graph->set_evaluation_engine(new NestedEvaluationEngine(
            this->graph()->evaluation_engine(),
            new MeshNestedEngineEvaluationClock<K>(this->graph()->evaluation_engine()->engine_evaluation_clock(), key,
                                                   this)));

        initialise_component(*graph);
        this->wire_graph(key, graph);
        current_eval_graph_ = key;
        start_component(*graph);
        current_eval_graph_ = std::nullopt;
        schedule_graph(key, this->last_evaluation_time());
    }

    template<typename K>
    void MeshNode<K>::schedule_graph(const K &key, engine_time_t tm) {
        int rank = active_graphs_rank_[key];
        scheduled_keys_by_rank_[rank][key] = tm;

        // Update scheduled rank time
        auto rank_it = scheduled_ranks_.find(rank);
        engine_time_t current_rank_time = (rank_it != scheduled_ranks_.end()) ? rank_it->second : MAX_DT;
        engine_time_t eval_time = this->graph()->evaluation_clock()->evaluation_time();
        scheduled_ranks_[rank] = std::min(std::max(current_rank_time, eval_time), tm);

        this->graph()->schedule_node(this->node_ndx(), tm);

        // Check for circular dependencies
        if (tm == this->last_evaluation_time() && current_eval_rank_.has_value() && rank <= current_eval_rank_.
            value()) {
            std::string node_label = this->signature().label.has_value()
                                         ? this->signature().label.value()
                                         : this->signature().name;
            throw std::runtime_error(fmt::format("mesh {}.{} has a dependency cycle {} -> {}",
                                                 this->signature().wiring_path_name,
                                                 node_label, to_string(key), to_string(key)));
        }
    }

    template<typename K>
    void MeshNode<K>::remove_graph(const K &key) {
        // Remove error output if using exception capture
        if (this->signature().capture_exception) {
            auto &error_output_ = dynamic_cast<TimeSeriesDictOutput_T<K> &>(*this->error_output());
            error_output_.erase(key);
        }

        auto graph_it = this->active_graphs_.find(key);
        if (graph_it != this->active_graphs_.end()) {
            auto graph = graph_it->second;
            this->active_graphs_.erase(graph_it);

            this->un_wire_graph(key, graph);

            // Ensure cleanup happens even if stop_component throws (matches Python try-finally pattern)
            auto cleanup = make_scope_exit([this, key]() {
                scheduled_keys_by_rank_[active_graphs_rank_[key]].erase(key);
                active_graphs_rank_.erase(key);

                // Remove any re-rank requests involving this key
                re_rank_requests_.erase(std::remove_if(re_rank_requests_.begin(), re_rank_requests_.end(),
                                                       [&key](const auto &pair) {
                                                           return std::equal_to<K>()(pair.first, key);
                                                       }),
                                        re_rank_requests_.end());
            });

            // Use component lifecycle functions like TsdMapNode does
            stop_component(*graph);

            // NOTE: Do not call dispose or release_instance here. The graph is managed by nanobind
            // and will be cleaned up when its reference count reaches zero.
        }
    }

    template<typename K>
    bool MeshNode<K>::add_graph_dependency(const K &key, const K &depends_on) {
        active_graphs_dependencies_[depends_on].insert(key);

        if (this->active_graphs_.find(depends_on) == this->active_graphs_.end()) {
            // Dependency doesn't exist yet, add to pending
            this->pending_keys_.insert(depends_on);
            auto schedule_time = this->last_evaluation_time() + MIN_TD;
            this->graph()->schedule_node(this->node_ndx(), schedule_time);
            return false;
        } else {
            return request_re_rank(key, depends_on);
        }
    }

    template<typename K>
    void MeshNode<K>::remove_graph_dependency(const K &key, const K &depends_on) {
        active_graphs_dependencies_[depends_on].erase(key);

        // Check if we should remove the dependency graph
        if (active_graphs_dependencies_[depends_on].empty()) {
            auto &input_bundle = dynamic_cast<TimeSeriesBundleInput &>(*this->input());
            auto &keys = dynamic_cast<TimeSeriesSetInput_T<K> &>(*input_bundle[TsdMapNode<K>::KEYS_ARG]);
            if (!keys.contains(depends_on)) { graphs_to_remove_.insert(depends_on); }
        }
    }

    template<typename K>
    bool MeshNode<K>::request_re_rank(const K &key, const K &depends_on) {
        if (active_graphs_rank_[key] <= active_graphs_rank_[depends_on]) {
            re_rank_requests_.push_back({key, depends_on});
            return false;
        }
        return true;
    }

    template<typename K>
    void MeshNode<K>::re_rank(const K &key, const K &depends_on, std::vector<K> re_rank_stack) {
        int prev_rank = active_graphs_rank_[key];
        int below_rank = active_graphs_rank_[depends_on];

        if (prev_rank <= below_rank) {
            // Remove from current rank schedule
            auto schedule_it = scheduled_keys_by_rank_[prev_rank].find(key);
            engine_time_t schedule = (schedule_it != scheduled_keys_by_rank_[prev_rank].end())
                                         ? schedule_it->second
                                         : MIN_DT;
            scheduled_keys_by_rank_[prev_rank].erase(key);

            // Assign new rank
            int new_rank = below_rank + 1;
            max_rank_ = std::max(max_rank_, new_rank);
            active_graphs_rank_[key] = new_rank;

            // Reschedule if needed
            if (schedule != MIN_DT) { schedule_graph(key, schedule); }

            // Re-rank dependents
            for (const auto &k: active_graphs_dependencies_[key]) {
                // Check for cycles
                auto it = std::find_if(re_rank_stack.begin(), re_rank_stack.end(),
                                       [&k](const K &item) { return std::equal_to<K>()(item, k); });
                if (it != re_rank_stack.end()) {
                    std::vector<K> cycle = re_rank_stack;
                    cycle.push_back(key);
                    cycle.push_back(k);
                    std::string cycle_str;
                    for (size_t i = 0; i < cycle.size(); ++i) {
                        if (i > 0) cycle_str += " -> ";
                        // Ensure we convert proxy types (like vector<bool>) to actual K before stringifying
                        K v = static_cast<K>(cycle[i]);
                        cycle_str += to_string(v);
                    }
                    std::string node_label =
                            this->signature().label.has_value()
                                ? this->signature().label.value()
                                : this->signature().name;
                    throw std::runtime_error(fmt::format("mesh {}.{} has a dependency cycle: {}",
                                                         this->signature().wiring_path_name, node_label, cycle_str));
                }

                auto new_stack = re_rank_stack;
                new_stack.push_back(key);
                re_rank(k, key, new_stack);
            }
        }
    }

    // Template instantiations
    template struct MeshNode<bool>;
    template struct MeshNode<int64_t>;
    template struct MeshNode<double>;
    template struct MeshNode<engine_date_t>;
    template struct MeshNode<engine_time_t>;
    template struct MeshNode<engine_time_delta_t>;
    template struct MeshNode<nb::object>;

    template struct MeshNestedEngineEvaluationClock<bool>;
    template struct MeshNestedEngineEvaluationClock<int64_t>;
    template struct MeshNestedEngineEvaluationClock<double>;
    template struct MeshNestedEngineEvaluationClock<engine_date_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_delta_t>;
    template struct MeshNestedEngineEvaluationClock<nb::object>;

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        // Register MeshNode specializations
        nb::class_<MeshNode<bool>, TsdMapNode<bool> >(m, "MeshNode_bool")
                .def("_add_graph_dependency", &MeshNode<bool>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<bool>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<int64_t>, TsdMapNode<int64_t> >(m, "MeshNode_int")
                .def("_add_graph_dependency", &MeshNode<int64_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<int64_t>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<double>, TsdMapNode<double> >(m, "MeshNode_float")
                .def("_add_graph_dependency", &MeshNode<double>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<double>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<engine_date_t>, TsdMapNode<engine_date_t> >(m, "MeshNode_date")
                .def("_add_graph_dependency", &MeshNode<engine_date_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_date_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<engine_time_t>, TsdMapNode<engine_time_t> >(m, "MeshNode_date_time")
                .def("_add_graph_dependency", &MeshNode<engine_time_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_time_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<engine_time_delta_t>, TsdMapNode<engine_time_delta_t> >(m, "MeshNode_time_delta")
                .def("_add_graph_dependency", &MeshNode<engine_time_delta_t>::_add_graph_dependency, "key"_a,
                     "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_time_delta_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<nb::object>, TsdMapNode<nb::object> >(m, "MeshNode_object")
                .def("_add_graph_dependency", &MeshNode<nb::object>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<nb::object>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);

        // Register MeshNestedEngineEvaluationClock specializations with 'key' property so Python can discover mesh keys
        nb::class_<MeshNestedEngineEvaluationClock<bool>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_bool")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<bool>::key);
        nb::class_<MeshNestedEngineEvaluationClock<int64_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_int")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<int64_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<double>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_float")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<double>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_date_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_date")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_date_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_time_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_date_time")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_time_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_time_delta_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_time_delta")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_time_delta_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<nb::object>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_object")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<nb::object>::key);
    }
} // namespace hgraph