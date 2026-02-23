#include <cstdlib>
#include <fmt/format.h>
#include <hgraph/api/python/wrapper_factory.h>
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
#include <hgraph/types/value/value.h>
#include <hgraph/util/string_utils.h>
#include <hgraph/util/scope.h>

namespace hgraph {
    // MeshNestedEngineEvaluationClock implementation
    MeshNestedEngineEvaluationClock::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock, value::Value key,
        mesh_node_ptr nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock),
                                      static_cast<NestedNode*>(nested_node)),
          _key(std::move(key)) {
    }

    nb::object MeshNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<MeshNode*>(node());
        const auto* key_schema = node_->key_type_meta();
        return key_schema->ops().to_python(_key.data(), key_schema);
    }

    void MeshNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto* node_ = static_cast<MeshNode*>(_nested_node);
        if (!node_) {
            return; // Safety check - should not happen
        }

        // Check if we should skip scheduling
        auto let = node_->last_evaluation_time();
        if ((let != MIN_DT && let > next_time) || node_->is_stopping()) { return; }

        auto rank_it = node_->active_graphs_rank_.find(_key.view());
        if (rank_it == node_->active_graphs_rank_.end()) { return; }
        int rank = rank_it->second;

        // If already scheduled for current time at current rank, skip
        if (next_time == let &&
            (rank == node_->current_eval_rank_ ||
             (node_->current_eval_graph_.has_value() &&
              ValueEqual{}(node_->current_eval_graph_.value(), _key)))) {
            return;
        }

        // Check if we need to reschedule
        auto rank_keys_it = node_->scheduled_keys_by_rank_.find(rank);
        engine_time_t tm = MIN_DT;
        if (rank_keys_it != node_->scheduled_keys_by_rank_.end()) {
            auto it = rank_keys_it->second.find(_key.view());
            if (it != rank_keys_it->second.end()) {
                tm = it->second;
            }
        }

        if (tm == MIN_DT || tm > next_time || tm < node_->graph()->evaluation_time()) {
            node_->schedule_graph(_key.view(), next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    MeshNode::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars,
                       graph_builder_s_ptr nested_graph_builder,
                       const std::unordered_map<std::string, int64_t> &input_node_ids,
                       int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                       const std::string &key_arg, const std::string &context_path)
        : TsdMapNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg) {
        // Build full context key using centralized key builder to match Python format
        full_context_path_ = keys::context_output_key(this->owning_graph_id(), context_path);
    }

    bool MeshNode::_add_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        value::Value key_val(key_type_meta_);
        key_val.emplace();
        key_type_meta_->ops().from_python(key_val.data(), key, key_type_meta_);
        value::Value depends_on_val(key_type_meta_);
        depends_on_val.emplace();
        key_type_meta_->ops().from_python(depends_on_val.data(), depends_on, key_type_meta_);
        return add_graph_dependency(key_val.view(), depends_on_val.view());
    }

    void MeshNode::_remove_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        value::Value key_val(key_type_meta_);
        key_val.emplace();
        key_type_meta_->ops().from_python(key_val.data(), key, key_type_meta_);
        value::Value depends_on_val(key_type_meta_);
        depends_on_val.emplace();
        key_type_meta_->ops().from_python(depends_on_val.data(), depends_on, key_type_meta_);
        remove_graph_dependency(key_val.view(), depends_on_val.view());
    }

    void MeshNode::do_start() {
        TsdMapNode::do_start();

        // Set up the reference output and register in GlobalState
        if (GlobalState::has_instance()) {
            auto *tsb_output = dynamic_cast<TimeSeriesBundleOutput *>(this->output().get());
            // Get the "out" and "ref" outputs from the output bundle
            auto tsd_output_ptr = (*tsb_output)["out"];
            auto &ref_output = dynamic_cast<TimeSeriesReferenceOutput &>(*(*tsb_output)["ref"]);

            // Create a TimeSeriesReference from the "out" output and set it on the "ref" output
            // Pass the shared_ptr directly to keep the output alive
            auto reference = TimeSeriesReference::make(tsd_output_ptr);
            ref_output.set_value(reference);

            // Store the ref output in GlobalState using shared_ptr-based wrapping
            GlobalState::set(full_context_path_, wrap_output(ref_output.shared_from_this()));
        } else {
            throw std::runtime_error("GlobalState instance required for MeshNode");
        }
    }

    void MeshNode::do_stop() {
        // Remove from GlobalState
        if (GlobalState::has_instance()) { GlobalState::remove(full_context_path_); }

        TsdMapNode::do_stop();
    }

    void MeshNode::eval() {
        this->mark_evaluated();

        // 1. Process keys input (additions/removals)
        auto &input_bundle = dynamic_cast<TimeSeriesBundleInput &>(*this->input());
        auto &keys = dynamic_cast<TimeSeriesSetInput &>(*input_bundle[TsdMapNode::KEYS_ARG]);
        if (keys.modified()) {
            // Iterate added keys using Value API
            for (auto key_view : keys.set_output().added_view()) {
                if (this->active_graphs_.find(key_view) == this->active_graphs_.end()) {
                    create_new_graph(key_view);

                    // If this key was pending (requested as a dependency), re-rank its dependents
                    if (auto pending_it = this->pending_keys_.find(key_view); pending_it != this->pending_keys_.end()) {
                        this->pending_keys_.erase(pending_it);
                        auto deps_it = active_graphs_dependencies_.find(key_view);
                        if (deps_it != active_graphs_dependencies_.end()) {
                            for (const auto &d : deps_it->second) {
                                re_rank(d.view(), key_view);
                            }
                        }
                    }
                }
            }
            // Iterate removed keys using Value API
            for (auto key_view : keys.set_output().removed_view()) {
                // Only remove if no dependencies
                auto deps_it = active_graphs_dependencies_.find(key_view);
                if (deps_it == active_graphs_dependencies_.end() || deps_it->second.empty()) {
                    auto rank_it = active_graphs_rank_.find(key_view);
                    if (rank_it != active_graphs_rank_.end()) {
                        auto& rank_map = scheduled_keys_by_rank_[rank_it->second];
                        if (auto sched_it = rank_map.find(key_view); sched_it != rank_map.end()) {
                            rank_map.erase(sched_it);
                        }
                    }
                    TsdMapNode::remove_graph(key_view);
                }
            }
        }

        // 2. Process pending keys (keys added due to dependencies)
        if (!this->pending_keys_.empty()) {
            std::vector<value::Value> pending_copy;
            pending_copy.reserve(pending_keys_.size());
            for (const auto& k : pending_keys_) {
                pending_copy.push_back(k.view().clone());
            }
            pending_keys_.clear();

            for (const auto &k : pending_copy) {
                create_new_graph(k.view(), 0);
                auto deps_it = active_graphs_dependencies_.find(k.view());
                if (deps_it != active_graphs_dependencies_.end()) {
                    for (const auto &d : deps_it->second) {
                        re_rank(d.view(), k.view());
                    }
                }
            }
        }

        // 3. Process graphs to remove
        if (!graphs_to_remove_.empty()) {
            std::vector<value::Value> to_remove;
            to_remove.reserve(graphs_to_remove_.size());
            for (const auto& k : graphs_to_remove_) {
                to_remove.push_back(k.view().clone());
            }
            graphs_to_remove_.clear();

            for (const auto &k : to_remove) {
                auto deps_it = active_graphs_dependencies_.find(k.view());
                if ((deps_it == active_graphs_dependencies_.end() || deps_it->second.empty()) &&
                    !keys.contains(k.view())) {
                    remove_graph(k.view());
                }
            }
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

                    for (const auto &[k, dtg] : graphs) {
                        if (dtg == dt) {
                            current_eval_graph_ = k.view().clone();
                            engine_time_t next_dtg = TsdMapNode::evaluate_graph(k.view());
                            current_eval_graph_ = std::nullopt;

                            if (next_dtg != MAX_DT && next_dtg > this->last_evaluation_time()) {
                                schedule_graph(k.view(), next_dtg);
                                next_time = std::min(next_time, next_dtg);
                            }
                        } else if (dtg != MAX_DT && dtg > this->last_evaluation_time()) {
                            schedule_graph(k.view(), dtg);
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
            auto requests = std::move(re_rank_requests_);
            re_rank_requests_.clear();
            for (const auto &[k, d] : requests) {
                re_rank(k.view(), d.view());
            }
        }

        // 6. Schedule next evaluation if needed
        if (next_time < MAX_DT) { this->graph()->schedule_node(this->node_ndx(), next_time); }
    }

    TimeSeriesDictOutputImpl &MeshNode::tsd_output() {
        // Access output bundle's "out" member - output() returns smart pointer to TimeSeriesBundleOutput
        auto *output_bundle = dynamic_cast<TimeSeriesBundleOutput *>(this->output().get());
        return dynamic_cast<TimeSeriesDictOutputImpl &>(*(*output_bundle)["out"]);
    }

    void MeshNode::create_new_graph(const value::View &key, int rank) {
        // Convert key to string for graph label
        nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
        std::string key_str = nb::repr(py_key).c_str();

        // Create new graph instance - concatenate node_id with negative count
        std::vector<int64_t> graph_id = this->node_id();
        graph_id.push_back(-static_cast<int64_t>(this->count_++));
        auto graph = this->nested_graph_builder_->make_instance(graph_id, this, key_str);

        this->active_graphs_.emplace(key.clone(), graph);
        active_graphs_rank_.emplace(key.clone(), (rank == -1) ? max_rank_ : rank);

        // Set up evaluation engine with MeshNestedEngineEvaluationClock
        graph->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            this->graph()->evaluation_engine(),
            std::make_shared<MeshNestedEngineEvaluationClock>(
                this->graph()->evaluation_engine()->engine_evaluation_clock().get(),
                key.clone(), this)));

        initialise_component(*graph);
        TsdMapNode::wire_graph(key, graph);
        current_eval_graph_ = key.clone();
        start_component(*graph);
        current_eval_graph_ = std::nullopt;
        schedule_graph(key, this->last_evaluation_time());
    }

    void MeshNode::schedule_graph(const value::View &key, engine_time_t tm) {
        auto rank_it = active_graphs_rank_.find(key);
        if (rank_it == active_graphs_rank_.end()) { return; }
        int rank = rank_it->second;

        scheduled_keys_by_rank_[rank].insert_or_assign(key.clone(), tm);

        // Update scheduled rank time
        auto sched_rank_it = scheduled_ranks_.find(rank);
        engine_time_t current_rank_time = (sched_rank_it != scheduled_ranks_.end()) ? sched_rank_it->second : MAX_DT;
        engine_time_t eval_time = this->graph()->evaluation_time();
        scheduled_ranks_[rank] = std::min(std::max(current_rank_time, eval_time), tm);

        this->graph()->schedule_node(this->node_ndx(), tm);

        // Check for circular dependencies
        if (tm == this->last_evaluation_time() && current_eval_rank_.has_value() && rank <= current_eval_rank_.value()) {
            std::string node_label = this->signature().label.has_value()
                                         ? this->signature().label.value()
                                         : this->signature().name;
            nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
            throw std::runtime_error(fmt::format("mesh {}.{} has a dependency cycle {} -> {}",
                                                 this->signature().wiring_path_name,
                                                 node_label, nb::repr(py_key).c_str(), nb::repr(py_key).c_str()));
        }
    }

    void MeshNode::remove_graph(const value::View &key) {
        // Remove error output if using exception capture
        if (this->signature().capture_exception) {
            auto &error_output_ = dynamic_cast<TimeSeriesDictOutputImpl &>(*this->error_output());
            error_output_.erase(key);
        }

        auto graph_it = this->active_graphs_.find(key);
        if (graph_it != this->active_graphs_.end()) {
            auto graph = graph_it->second;
            this->active_graphs_.erase(graph_it);

            TsdMapNode::un_wire_graph(key, graph);

            // Ensure cleanup happens even if stop_component throws (matches Python try-finally pattern)
            value::Value key_copy = key.clone();  // Clone for lambda capture
            auto cleanup = make_scope_exit([this, key_copy = std::move(key_copy)]() mutable {
                auto rank_it = active_graphs_rank_.find(key_copy.view());
                if (rank_it != active_graphs_rank_.end()) {
                    auto& rank_map = scheduled_keys_by_rank_[rank_it->second];
                    if (auto sched_it = rank_map.find(key_copy.view()); sched_it != rank_map.end()) {
                        rank_map.erase(sched_it);
                    }
                    active_graphs_rank_.erase(rank_it);
                }

                // Remove any re-rank requests involving this key
                re_rank_requests_.erase(std::remove_if(re_rank_requests_.begin(), re_rank_requests_.end(),
                                                       [&key_copy](const auto &pair) {
                                                           return ValueEqual{}(pair.first, key_copy);
                                                       }),
                                        re_rank_requests_.end());
            });

            // Use component lifecycle functions like TsdMapNode does
            stop_component(*graph);
            nested_graph_builder_->release_instance(graph);
        }
    }

    bool MeshNode::add_graph_dependency(const value::View &key, const value::View &depends_on) {
        active_graphs_dependencies_[depends_on.clone()].insert(key.clone());

        if (this->active_graphs_.find(depends_on) == this->active_graphs_.end()) {
            // Dependency doesn't exist yet, add to pending
            this->pending_keys_.insert(depends_on.clone());
            auto schedule_time = this->last_evaluation_time() + MIN_TD;
            this->graph()->schedule_node(this->node_ndx(), schedule_time);
            return false;
        } else {
            return request_re_rank(key, depends_on);
        }
    }

    void MeshNode::remove_graph_dependency(const value::View &key, const value::View &depends_on) {
        auto deps_it = active_graphs_dependencies_.find(depends_on);
        if (deps_it != active_graphs_dependencies_.end()) {
            if (auto key_it = deps_it->second.find(key); key_it != deps_it->second.end()) {
                deps_it->second.erase(key_it);
            }

            // Check if we should remove the dependency graph
            if (deps_it->second.empty()) {
                auto &input_bundle = dynamic_cast<TimeSeriesBundleInput &>(*this->input());
                auto &keys = dynamic_cast<TimeSeriesSetInput &>(*input_bundle[TsdMapNode::KEYS_ARG]);
                if (!keys.contains(depends_on)) {
                    graphs_to_remove_.insert(depends_on.clone());
                }
            }
        }
    }

    bool MeshNode::request_re_rank(const value::View &key, const value::View &depends_on) {
        auto key_rank_it = active_graphs_rank_.find(key);
        auto dep_rank_it = active_graphs_rank_.find(depends_on);
        if (key_rank_it == active_graphs_rank_.end() || dep_rank_it == active_graphs_rank_.end()) {
            return false;
        }

        if (key_rank_it->second <= dep_rank_it->second) {
            re_rank_requests_.push_back({key.clone(), depends_on.clone()});
            return false;
        }
        return true;
    }

    void MeshNode::re_rank(const value::View &key, const value::View &depends_on,
                           std::vector<value::Value> re_rank_stack) {
        auto key_rank_it = active_graphs_rank_.find(key);
        auto dep_rank_it = active_graphs_rank_.find(depends_on);
        if (key_rank_it == active_graphs_rank_.end() || dep_rank_it == active_graphs_rank_.end()) {
            return;
        }

        int prev_rank = key_rank_it->second;
        int below_rank = dep_rank_it->second;

        if (prev_rank <= below_rank) {
            // Remove from current rank schedule
            auto& rank_schedule = scheduled_keys_by_rank_[prev_rank];
            auto schedule_it = rank_schedule.find(key);
            engine_time_t schedule = (schedule_it != rank_schedule.end()) ? schedule_it->second : MIN_DT;
            if (schedule_it != rank_schedule.end()) {
                rank_schedule.erase(schedule_it);
            }

            // Assign new rank
            int new_rank = below_rank + 1;
            max_rank_ = std::max(max_rank_, new_rank);
            active_graphs_rank_.insert_or_assign(key.clone(), new_rank);

            // Reschedule if needed
            if (schedule != MIN_DT) { schedule_graph(key, schedule); }

            // Re-rank dependents
            auto deps_it = active_graphs_dependencies_.find(key);
            if (deps_it != active_graphs_dependencies_.end()) {
                for (const auto &k : deps_it->second) {
                    // Check for cycles
                    bool found_cycle = false;
                    for (const auto& stack_item : re_rank_stack) {
                        if (ValueEqual{}(stack_item, k)) {
                            found_cycle = true;
                            break;
                        }
                    }

                    if (found_cycle) {
                        std::vector<value::Value> cycle;
                        for (const auto& item : re_rank_stack) { cycle.push_back(item.view().clone()); }
                        cycle.push_back(key.clone());
                        cycle.push_back(k.view().clone());
                        std::string cycle_str;
                        for (size_t i = 0; i < cycle.size(); ++i) {
                            if (i > 0) cycle_str += " -> ";
                            nb::object py_v = key_type_meta_->ops().to_python(cycle[i].data(), key_type_meta_);
                            cycle_str += nb::repr(py_v).c_str();
                        }
                        std::string node_label =
                                this->signature().label.has_value()
                                    ? this->signature().label.value()
                                    : this->signature().name;
                        throw std::runtime_error(fmt::format("mesh {}.{} has a dependency cycle: {}",
                                                             this->signature().wiring_path_name, node_label, cycle_str));
                    }

                    std::vector<value::Value> new_stack;
                    for (const auto& item : re_rank_stack) { new_stack.push_back(item.view().clone()); }
                    new_stack.push_back(key.clone());
                    re_rank(k.view(), key, std::move(new_stack));
                }
            }
        }
    }

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        // Register single non-templated MeshNestedEngineEvaluationClock
        nb::class_<MeshNestedEngineEvaluationClock, NestedEngineEvaluationClock>(
            m, "MeshNestedEngineEvaluationClock")
            .def_prop_ro("key", &MeshNestedEngineEvaluationClock::py_key);

        // Register single non-templated MeshNode
        nb::class_<MeshNode, TsdMapNode>(m, "MeshNode")
            .def("_add_graph_dependency", &MeshNode::_add_graph_dependency, "key"_a, "depends_on"_a)
            .def("_remove_graph_dependency", &MeshNode::_remove_graph_dependency, "key"_a, "depends_on"_a);
    }
} // namespace hgraph
