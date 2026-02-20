#include <fmt/format.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node &node) {
            if (const auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

        TSInputView node_input_field(Node &node, std::string_view name) {
            auto root = node.input(node_time(node));
            if (!root) {
                return {};
            }
            auto bundle_opt = root.try_as_bundle();
            if (!bundle_opt.has_value()) {
                return {};
            }
            return bundle_opt->field(name);
        }

        std::string key_repr(const value::View &key, const value::TypeMeta *key_type_meta) {
            if (!key.valid() || key_type_meta == nullptr) {
                return "<invalid key>";
            }
            nb::object py_key = key_type_meta->ops().to_python(key.data(), key_type_meta);
            return nb::cast<std::string>(nb::repr(py_key));
        }

        bool collect_current_map_keys(const TSInputView &keys_view, TsdMapNode::key_set_type &out) {
            out.clear();
            value::View keys_value = keys_view.value();
            if (!keys_value.valid()) {
                return true;
            }

            if (keys_value.is_set()) {
                for (value::View key : keys_value.as_set()) {
                    out.insert(key.clone());
                }
                return true;
            }

            if (keys_value.is_map()) {
                for (value::View key : keys_value.as_map().keys()) {
                    out.insert(key.clone());
                }
                return true;
            }

            return false;
        }

        std::optional<value::Value> key_from_python_object(const nb::object& key_obj, const value::TypeMeta* key_type_meta) {
            if (key_type_meta == nullptr) {
                return std::nullopt;
            }
            value::Value key_value(key_type_meta);
            key_value.emplace();
            key_type_meta->ops().from_python(key_value.data(), key_obj, key_type_meta);
            return key_value;
        }

        bool extract_key_delta(const TSInputView& keys_view,
                               const value::TypeMeta* key_type_meta,
                               std::vector<value::Value>& added_out,
                               std::vector<value::Value>& removed_out) {
            added_out.clear();
            removed_out.clear();
            if (key_type_meta == nullptr) {
                return false;
            }

            nb::object delta = keys_view.delta_to_python();
            if (delta.is_none()) {
                return false;
            }

            nb::object added_obj = nb::getattr(delta, "added", nb::none());
            nb::object removed_obj = nb::getattr(delta, "removed", nb::none());

            bool has_delta = false;
            if (!added_obj.is_none()) {
                for (const auto& item : nb::iter(added_obj)) {
                    auto key_value = key_from_python_object(nb::cast<nb::object>(item), key_type_meta);
                    if (!key_value.has_value()) {
                        continue;
                    }
                    added_out.push_back(std::move(*key_value));
                    has_delta = true;
                }
            }
            if (!removed_obj.is_none()) {
                for (const auto& item : nb::iter(removed_obj)) {
                    auto key_value = key_from_python_object(nb::cast<nb::object>(item), key_type_meta);
                    if (!key_value.has_value()) {
                        continue;
                    }
                    removed_out.push_back(std::move(*key_value));
                    has_delta = true;
                }
            }

            return has_delta;
        }

        void replace_key_set(TsdMapNode::key_set_type& dst, const TsdMapNode::key_set_type& src) {
            dst.clear();
            for (const auto& key : src) {
                dst.insert(key.view().clone());
            }
        }
    }  // namespace

    MeshNestedEngineEvaluationClock::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock,
        value::Value key,
        mesh_node_ptr nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock), static_cast<NestedNode*>(nested_node)),
          _key(std::move(key)) {}

    nb::object MeshNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<MeshNode*>(node());
        if (const auto* key_schema = node_->key_type_meta(); key_schema != nullptr) {
            return key_schema->ops().to_python(_key.data(), key_schema);
        }
        return nb::none();
    }

    void MeshNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto* node_ = static_cast<MeshNode*>(_nested_node);
        if (!node_) {
            return;
        }

        auto let = node_->last_evaluation_time();
        if ((let != MIN_DT && let > next_time) || node_->is_stopping()) {
            return;
        }

        auto rank_it = node_->active_graphs_rank_.find(_key.view());
        if (rank_it == node_->active_graphs_rank_.end()) {
            return;
        }
        const int rank = rank_it->second;

        if (next_time == let) {
            if ((node_->current_eval_rank_.has_value() && rank == node_->current_eval_rank_.value()) ||
                (node_->current_eval_graph_.has_value() &&
                 ValueEqual{}(node_->current_eval_graph_->view(), _key.view()))) {
                return;
            }
        }

        engine_time_t current_schedule = MIN_DT;
        if (auto rank_map_it = node_->scheduled_keys_by_rank_.find(rank);
            rank_map_it != node_->scheduled_keys_by_rank_.end()) {
            if (auto key_it = rank_map_it->second.find(_key.view()); key_it != rank_map_it->second.end()) {
                current_schedule = key_it->second;
            }
        }

        if (current_schedule == MIN_DT || current_schedule > next_time || current_schedule < node_->graph()->evaluation_time()) {
            node_->schedule_graph(_key.view(), next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    MeshNode::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                       const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                       graph_builder_s_ptr nested_graph_builder,
                       const std::unordered_map<std::string, int64_t> &input_node_ids,
                       int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                       const std::string &key_arg, const std::string &context_path)
        : TsdMapNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta,
                     std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg) {
        full_context_path_ = keys::context_output_key(this->owning_graph_id(), context_path);
    }

    bool MeshNode::_add_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        if (key_type_meta_ == nullptr) {
            return false;
        }
        value::Value key_val(key_type_meta_);
        key_val.emplace();
        key_type_meta_->ops().from_python(key_val.data(), key, key_type_meta_);

        value::Value depends_on_val(key_type_meta_);
        depends_on_val.emplace();
        key_type_meta_->ops().from_python(depends_on_val.data(), depends_on, key_type_meta_);
        return add_graph_dependency(key_val.view(), depends_on_val.view());
    }

    void MeshNode::_remove_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        if (key_type_meta_ == nullptr) {
            return;
        }
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

        auto root = output(node_time(*this));
        auto bundle_opt = root ? root.try_as_bundle() : std::nullopt;
        if (!bundle_opt.has_value()) {
            return;
        }

        auto out = bundle_opt->field("out");
        auto ref = bundle_opt->field("ref");
        if (!out || !ref) {
            return;
        }

        ref.from_python(nb::cast(TimeSeriesReference::make(out.as_ts_view().view_data())));

        if (GlobalState::has_instance()) {
            GlobalState::set(full_context_path_, wrap_output_view(ref));
        }
    }

    void MeshNode::do_stop() {
        if (GlobalState::has_instance()) {
            GlobalState::remove(full_context_path_);
        }

        TsdMapNode::do_stop();

        scheduled_ranks_.clear();
        scheduled_keys_by_rank_.clear();
        active_graphs_rank_.clear();
        active_graphs_sequence_.clear();
        active_graphs_dependencies_.clear();
        external_keys_.clear();
        re_rank_requests_.clear();
        graphs_to_remove_.clear();
        current_eval_rank_.reset();
        current_eval_graph_.reset();
        max_rank_ = 0;
        next_graph_sequence_ = 0;
    }

    void MeshNode::eval() {
        mark_evaluated();
        const bool debug_mesh = std::getenv("HGRAPH_DEBUG_MESH_KEYS") != nullptr;
        const bool debug_dep = std::getenv("HGRAPH_DEBUG_MESH_DEP") != nullptr;

        auto keys_view = node_input_field(*this, KEYS_ARG);
        key_set_type current_keys;
        bool have_current_keys = keys_view && collect_current_map_keys(keys_view, current_keys);
        if (debug_mesh) {
            std::fprintf(stderr,
                         "[mesh_eval] node=%lld time=%lld keys_view=%d modified=%d have_current=%d current_size=%zu active=%zu pending=%zu to_remove=%zu\n",
                         static_cast<long long>(node_ndx()),
                         static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                         keys_view ? 1 : 0,
                         (keys_view && keys_view.modified()) ? 1 : 0,
                         have_current_keys ? 1 : 0,
                         current_keys.size(),
                         active_graphs_.size(),
                         pending_keys_.size(),
                         graphs_to_remove_.size());
        }

        if (keys_view && keys_view.modified()) {
            std::vector<value::Value> added_keys;
            std::vector<value::Value> removed_keys;
            const bool has_key_delta = extract_key_delta(keys_view, key_type_meta_, added_keys, removed_keys);

            if (!has_key_delta && have_current_keys) {
                for (const auto& key : current_keys) {
                    if (external_keys_.find(key.view()) == external_keys_.end()) {
                        added_keys.push_back(key.view().clone());
                    }
                }
                for (const auto& key : external_keys_) {
                    if (current_keys.find(key.view()) == current_keys.end()) {
                        removed_keys.push_back(key.view().clone());
                    }
                }
            }

            if (debug_mesh) {
                std::fprintf(stderr,
                             "[mesh_eval] node=%lld has_delta=%d added=%zu removed=%zu external_before=%zu\n",
                             static_cast<long long>(node_ndx()),
                             has_key_delta ? 1 : 0,
                             added_keys.size(),
                             removed_keys.size(),
                             external_keys_.size());
            }

            for (const auto& key : added_keys) {
                if (debug_mesh) {
                    std::fprintf(stderr,
                                 "[mesh_eval] add key=%s exists=%d\n",
                                 key_repr(key.view(), key_type_meta_).c_str(),
                                 active_graphs_.find(key.view()) != active_graphs_.end() ? 1 : 0);
                }
                if (active_graphs_.find(key.view()) == active_graphs_.end()) {
                    create_new_graph(key.view());
                } else {
                    // External key-set can add a key that is already active as an
                    // internal dependency. Mark it so the next keyed evaluation
                    // emits its current value.
                    mark_key_for_forced_emit(key.view());
                    schedule_graph(key.view(), last_evaluation_time());
                }
            }

            for (const auto& key : removed_keys) {
                auto deps_it = active_graphs_dependencies_.find(key.view());
                const bool has_dependencies = deps_it != active_graphs_dependencies_.end() && !deps_it->second.empty();
                if (debug_mesh) {
                    std::fprintf(stderr,
                                 "[mesh_eval] remove key=%s has_deps=%d active=%d\n",
                                 key_repr(key.view(), key_type_meta_).c_str(),
                                 has_dependencies ? 1 : 0,
                                 active_graphs_.find(key.view()) != active_graphs_.end() ? 1 : 0);
                }
                if (has_dependencies) {
                    continue;
                }
                if (auto rank_it = active_graphs_rank_.find(key.view()); rank_it != active_graphs_rank_.end()) {
                    if (auto sched_it = scheduled_keys_by_rank_.find(rank_it->second);
                        sched_it != scheduled_keys_by_rank_.end()) {
                        if (auto key_it = sched_it->second.find(key.view()); key_it != sched_it->second.end()) {
                            sched_it->second.erase(key_it);
                        }
                    }
                }
                remove_graph(key.view());
            }

            if (have_current_keys) {
                replace_key_set(external_keys_, current_keys);
            } else {
                external_keys_.clear();
            }
        } else if (keys_view && active_graphs_.empty() && have_current_keys) {
            for (const auto& key : current_keys) {
                if (active_graphs_.find(key.view()) == active_graphs_.end()) {
                    create_new_graph(key.view());
                }
            }
            replace_key_set(external_keys_, current_keys);
        }

        if (!pending_keys_.empty()) {
            std::vector<value::Value> pending_copy;
            pending_copy.reserve(pending_keys_.size());
            for (const auto& k : pending_keys_) {
                pending_copy.push_back(k.view().clone());
            }
            pending_keys_.clear();

            for (const auto& k : pending_copy) {
                if (active_graphs_.find(k.view()) == active_graphs_.end()) {
                    create_new_graph(k.view(), 0);
                }
                if (auto deps_it = active_graphs_dependencies_.find(k.view());
                    deps_it != active_graphs_dependencies_.end()) {
                    for (const auto& d : deps_it->second) {
                        re_rank(d.view(), k.view());
                    }
                }
            }
        }

        if (!graphs_to_remove_.empty()) {
            if (!have_current_keys && keys_view) {
                have_current_keys = collect_current_map_keys(keys_view, current_keys);
            }

            std::vector<value::Value> to_remove;
            to_remove.reserve(graphs_to_remove_.size());
            for (const auto& k : graphs_to_remove_) {
                to_remove.push_back(k.view().clone());
            }
            graphs_to_remove_.clear();

            for (const auto& k : to_remove) {
                auto deps_it = active_graphs_dependencies_.find(k.view());
                const bool has_dependencies = deps_it != active_graphs_dependencies_.end() && !deps_it->second.empty();
                const bool in_external_keys = have_current_keys && current_keys.find(k.view()) != current_keys.end();
                if (!has_dependencies && !in_external_keys) {
                    remove_graph(k.view());
                }
            }
        }

        engine_time_t next_time = MAX_DT;
        int rank = 0;
        while (rank <= max_rank_) {
            current_eval_rank_ = rank;
            auto rank_it = scheduled_ranks_.find(rank);
            const engine_time_t dt = rank_it != scheduled_ranks_.end() ? rank_it->second : MIN_DT;
            if (debug_dep) {
                std::fprintf(stderr,
                             "[mesh_dep] eval_rank=%d dt=%lld now=%lld has_keys=%d\n",
                             rank,
                             static_cast<long long>(dt.time_since_epoch().count()),
                             static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                             scheduled_keys_by_rank_.find(rank) != scheduled_keys_by_rank_.end() ? 1 : 0);
            }

            if (dt == last_evaluation_time()) {
                if (rank_it != scheduled_ranks_.end()) {
                    scheduled_ranks_.erase(rank_it);
                }
                auto graphs_it = scheduled_keys_by_rank_.find(rank);
                if (graphs_it != scheduled_keys_by_rank_.end()) {
                    auto graphs = std::move(graphs_it->second);
                    scheduled_keys_by_rank_.erase(graphs_it);

                    std::vector<std::pair<value::Value, engine_time_t>> ordered_graphs;
                    ordered_graphs.reserve(graphs.size());
                    for (const auto& [k, dtg] : graphs) {
                        ordered_graphs.emplace_back(k.view().clone(), dtg);
                    }

                    auto key_sequence = [&](const value::View& key) {
                        auto it = active_graphs_sequence_.find(key);
                        return it != active_graphs_sequence_.end()
                                   ? it->second
                                   : std::numeric_limits<uint64_t>::max();
                    };
                    std::stable_sort(
                        ordered_graphs.begin(),
                        ordered_graphs.end(),
                        [&](const auto& lhs, const auto& rhs) {
                            return key_sequence(lhs.first.view()) < key_sequence(rhs.first.view());
                        });

                    for (const auto& [k, dtg] : ordered_graphs) {
                        engine_time_t next_dtg = dtg;
                        if (dtg == dt) {
                            if (auto refresh_it = refresh_before_eval_keys_.find(k.view());
                                refresh_it != refresh_before_eval_keys_.end()) {
                                if (debug_dep) {
                                    std::fprintf(stderr,
                                                 "[mesh_dep] refresh_before_eval rank=%d key=%s now=%lld\n",
                                                 rank,
                                                 key_repr(k.view(), key_type_meta_).c_str(),
                                                 static_cast<long long>(last_evaluation_time().time_since_epoch().count()));
                                }
                                if (auto graph_it = active_graphs_.find(k.view());
                                    graph_it != active_graphs_.end() && graph_it->second) {
                                    notify_graph_input_nodes(graph_it->second, last_evaluation_time());
                                    for (const auto& nested_node : graph_it->second->nodes()) {
                                        if (nested_node) {
                                            nested_node->notify(last_evaluation_time());
                                        }
                                    }
                                }
                                refresh_before_eval_keys_.erase(refresh_it);
                            }
                            current_eval_graph_ = k.view().clone();
                            if (debug_dep) {
                                std::fprintf(stderr,
                                             "[mesh_dep] run rank=%d key=%s due=%lld\n",
                                             rank,
                                             key_repr(k.view(), key_type_meta_).c_str(),
                                             static_cast<long long>(dtg.time_since_epoch().count()));
                            }
                            next_dtg = evaluate_graph(k.view());
                            current_eval_graph_.reset();
                        }

                        if (next_dtg != MAX_DT && next_dtg > last_evaluation_time()) {
                            schedule_graph(k.view(), next_dtg);
                            next_time = std::min(next_time, next_dtg);
                        }
                    }
                }
            } else if (dt != MIN_DT && dt > last_evaluation_time()) {
                next_time = std::min(next_time, dt);
            }
            rank += 1;
        }
        current_eval_rank_.reset();
        current_eval_graph_.reset();

        if (!re_rank_requests_.empty()) {
            auto requests = std::move(re_rank_requests_);
            re_rank_requests_.clear();
            for (const auto& [k, d] : requests) {
                re_rank(k.view(), d.view());
            }
        }

        if (next_time < MAX_DT) {
            graph()->schedule_node(node_ndx(), next_time);
        }
    }

    TSDOutputView MeshNode::tsd_output(engine_time_t current_time) {
        auto out = output(current_time);
        if (!out) {
            return {};
        }

        auto bundle_opt = out.try_as_bundle();
        if (!bundle_opt.has_value()) {
            return {};
        }

        auto out_field = bundle_opt->field("out");
        if (!out_field) {
            return {};
        }

        auto dict_opt = out_field.try_as_dict();
        if (!dict_opt.has_value()) {
            return {};
        }
        return *dict_opt;
    }

    void MeshNode::create_new_graph(const value::View &key, int rank) {
        if (key_type_meta_ == nullptr) {
            throw std::runtime_error("MeshNode key type meta is not initialised");
        }

        nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
        std::string key_str = nb::cast<std::string>(nb::repr(py_key));

        auto child_owning_graph_id = node_id();
        child_owning_graph_id.push_back(-static_cast<int64_t>(count_++));

        auto graph_ = nested_graph_builder_->make_instance(child_owning_graph_id, this, key_str);
        active_graphs_.insert_or_assign(key.clone(), graph_);

        const int assigned_rank = (rank < 0) ? max_rank_ : rank;
        active_graphs_rank_.insert_or_assign(key.clone(), assigned_rank);
        active_graphs_sequence_.insert_or_assign(key.clone(), next_graph_sequence_++);
        max_rank_ = std::max(max_rank_, assigned_rank);

        graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(),
            std::make_shared<MeshNestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), key.clone(), this)));

        initialise_component(*graph_);
        wire_graph(key, graph_);
        current_eval_graph_ = key.clone();
        start_component(*graph_);
        current_eval_graph_.reset();
        schedule_graph(key, last_evaluation_time());
    }

    void MeshNode::remove_graph(const value::View &key) {
        const auto rank_it = active_graphs_rank_.find(key);
        const int rank = rank_it == active_graphs_rank_.end() ? -1 : rank_it->second;

        TsdMapNode::remove_graph(key);

        if (rank >= 0) {
            if (auto it = scheduled_keys_by_rank_.find(rank); it != scheduled_keys_by_rank_.end()) {
                if (auto key_it = it->second.find(key); key_it != it->second.end()) {
                    it->second.erase(key_it);
                }
                if (it->second.empty()) {
                    scheduled_keys_by_rank_.erase(it);
                    scheduled_ranks_.erase(rank);
                }
            }
            if (auto key_it = active_graphs_rank_.find(key); key_it != active_graphs_rank_.end()) {
                active_graphs_rank_.erase(key_it);
            }
            if (auto seq_it = active_graphs_sequence_.find(key); seq_it != active_graphs_sequence_.end()) {
                active_graphs_sequence_.erase(seq_it);
            }
        }

        if (auto key_it = external_keys_.find(key); key_it != external_keys_.end()) {
            external_keys_.erase(key_it);
        }
        if (auto refresh_it = refresh_before_eval_keys_.find(key); refresh_it != refresh_before_eval_keys_.end()) {
            refresh_before_eval_keys_.erase(refresh_it);
        }

        re_rank_requests_.erase(
            std::remove_if(
                re_rank_requests_.begin(),
                re_rank_requests_.end(),
                [&key](const auto &pair) { return ValueEqual{}(pair.first.view(), key); }),
            re_rank_requests_.end());

        while (max_rank_ > 0) {
            bool has_rank = false;
            for (const auto& [_, r] : active_graphs_rank_) {
                if (r == max_rank_) {
                    has_rank = true;
                    break;
                }
            }
            if (has_rank) {
                break;
            }
            scheduled_keys_by_rank_.erase(max_rank_);
            scheduled_ranks_.erase(max_rank_);
            max_rank_ -= 1;
        }
    }

    void MeshNode::schedule_graph(const value::View &key, engine_time_t tm) {
        auto rank_it = active_graphs_rank_.find(key);
        if (rank_it == active_graphs_rank_.end()) {
            return;
        }

        const int rank = rank_it->second;
        scheduled_keys_by_rank_[rank].insert_or_assign(key.clone(), tm);

        const engine_time_t eval_time = graph()->evaluation_time();
        const engine_time_t current = scheduled_ranks_.contains(rank) ? scheduled_ranks_[rank] : MAX_DT;
        scheduled_ranks_[rank] = std::min(std::max(current, eval_time), tm);
        graph()->schedule_node(node_ndx(), tm);

        if (tm == last_evaluation_time() && current_eval_rank_.has_value() && rank <= current_eval_rank_.value()) {
            const std::string node_label =
                signature().label.has_value() ? signature().label.value() : signature().name;
            throw std::runtime_error(
                fmt::format("mesh {}.{} has a dependency cycle {} -> {}",
                            signature().wiring_path_name,
                            node_label,
                            key_repr(key, key_type_meta_),
                            key_repr(key, key_type_meta_)));
        }
    }

    bool MeshNode::add_graph_dependency(const value::View &key, const value::View &depends_on) {
        const bool debug_dep = std::getenv("HGRAPH_DEBUG_MESH_DEP") != nullptr;
        active_graphs_dependencies_[depends_on.clone()].insert(key.clone());
        if (debug_dep) {
            std::fprintf(stderr,
                         "[mesh_dep] add key=%s depends_on=%s has_dep_graph=%d key_active=%d\n",
                         key_repr(key, key_type_meta_).c_str(),
                         key_repr(depends_on, key_type_meta_).c_str(),
                         active_graphs_.find(depends_on) != active_graphs_.end() ? 1 : 0,
                         active_graphs_.find(key) != active_graphs_.end() ? 1 : 0);
        }

        if (active_graphs_.find(depends_on) == active_graphs_.end()) {
            pending_keys_.insert(depends_on.clone());
            if (debug_dep) {
                std::fprintf(stderr,
                             "[mesh_dep] pending depends_on=%s now=%lld\n",
                             key_repr(depends_on, key_type_meta_).c_str(),
                             static_cast<long long>(last_evaluation_time().time_since_epoch().count()));
            }
            graph()->schedule_node(node_ndx(), last_evaluation_time() + MIN_TD);
            return false;
        }
        return request_re_rank(key, depends_on);
    }

    void MeshNode::remove_graph_dependency(const value::View &key, const value::View &depends_on) {
        const bool debug_dep = std::getenv("HGRAPH_DEBUG_MESH_DEP") != nullptr;
        auto deps_it = active_graphs_dependencies_.find(depends_on);
        if (deps_it == active_graphs_dependencies_.end()) {
            return;
        }

        if (auto key_it = deps_it->second.find(key); key_it != deps_it->second.end()) {
            deps_it->second.erase(key_it);
        }
        if (!deps_it->second.empty()) {
            return;
        }

        auto keys_view = node_input_field(*this, KEYS_ARG);
        key_set_type current_keys;
        const bool present_in_external_keys =
            keys_view && collect_current_map_keys(keys_view, current_keys) && current_keys.find(depends_on) != current_keys.end();
        if (!present_in_external_keys) {
            graphs_to_remove_.insert(depends_on.clone());
        }
        if (debug_dep) {
            std::fprintf(stderr,
                         "[mesh_dep] remove key=%s depends_on=%s external_keep=%d marked_remove=%d\n",
                         key_repr(key, key_type_meta_).c_str(),
                         key_repr(depends_on, key_type_meta_).c_str(),
                         present_in_external_keys ? 1 : 0,
                         graphs_to_remove_.find(depends_on) != graphs_to_remove_.end() ? 1 : 0);
        }
    }

    bool MeshNode::request_re_rank(const value::View &key, const value::View &depends_on) {
        const bool debug_dep = std::getenv("HGRAPH_DEBUG_MESH_DEP") != nullptr;
        auto key_rank_it = active_graphs_rank_.find(key);
        auto dep_rank_it = active_graphs_rank_.find(depends_on);
        if (key_rank_it == active_graphs_rank_.end() || dep_rank_it == active_graphs_rank_.end()) {
            if (debug_dep) {
                std::fprintf(stderr,
                             "[mesh_dep] request_rerank missing key=%s dep=%s key_found=%d dep_found=%d\n",
                             key_repr(key, key_type_meta_).c_str(),
                             key_repr(depends_on, key_type_meta_).c_str(),
                             key_rank_it != active_graphs_rank_.end() ? 1 : 0,
                             dep_rank_it != active_graphs_rank_.end() ? 1 : 0);
            }
            return false;
        }

        if (key_rank_it->second <= dep_rank_it->second) {
            re_rank_requests_.emplace_back(key.clone(), depends_on.clone());
            if (debug_dep) {
                std::fprintf(stderr,
                             "[mesh_dep] request_rerank enqueue key=%s rank=%d dep=%s dep_rank=%d\n",
                             key_repr(key, key_type_meta_).c_str(),
                             key_rank_it->second,
                             key_repr(depends_on, key_type_meta_).c_str(),
                             dep_rank_it->second);
            }
            return false;
        }
        if (debug_dep) {
            std::fprintf(stderr,
                         "[mesh_dep] request_rerank noop key=%s rank=%d dep=%s dep_rank=%d\n",
                         key_repr(key, key_type_meta_).c_str(),
                         key_rank_it->second,
                         key_repr(depends_on, key_type_meta_).c_str(),
                         dep_rank_it->second);
        }
        return true;
    }

    void MeshNode::re_rank(const value::View &key, const value::View &depends_on, std::vector<value::Value> re_rank_stack) {
        const bool debug_dep = std::getenv("HGRAPH_DEBUG_MESH_DEP") != nullptr;
        auto key_rank_it = active_graphs_rank_.find(key);
        auto dep_rank_it = active_graphs_rank_.find(depends_on);
        if (key_rank_it == active_graphs_rank_.end() || dep_rank_it == active_graphs_rank_.end()) {
            return;
        }

        const int prev_rank = key_rank_it->second;
        const int below = dep_rank_it->second;
        if (prev_rank > below) {
            return;
        }

        engine_time_t schedule = MIN_DT;
        if (auto rank_sched_it = scheduled_keys_by_rank_.find(prev_rank);
            rank_sched_it != scheduled_keys_by_rank_.end()) {
            if (auto schedule_it = rank_sched_it->second.find(key); schedule_it != rank_sched_it->second.end()) {
                schedule = schedule_it->second;
                rank_sched_it->second.erase(schedule_it);
            }
        }

        const int new_rank = below + 1;
        max_rank_ = std::max(max_rank_, new_rank);
        active_graphs_rank_.insert_or_assign(key.clone(), new_rank);
        refresh_before_eval_keys_.insert(key.clone());
        if (debug_dep) {
            std::fprintf(stderr,
                         "[mesh_dep] rerank key=%s old_rank=%d new_rank=%d dep=%s dep_rank=%d\n",
                         key_repr(key, key_type_meta_).c_str(),
                         prev_rank,
                         new_rank,
                         key_repr(depends_on, key_type_meta_).c_str(),
                         below);
        }

        if (schedule != MIN_DT) {
            schedule_graph(key, schedule);
        } else if (last_evaluation_time() != MIN_DT) {
            // If a key is reranked after its prior due entry has already been
            // consumed this cycle, ensure it is not stranded unscheduled.
            schedule_graph(key, last_evaluation_time() + MIN_TD);
        }

        if (auto deps_it = active_graphs_dependencies_.find(key); deps_it != active_graphs_dependencies_.end()) {
            for (const auto &k : deps_it->second) {
                bool found_cycle = false;
                for (const auto &stack_item : re_rank_stack) {
                    if (ValueEqual{}(stack_item.view(), k.view())) {
                        found_cycle = true;
                        break;
                    }
                }

                if (found_cycle) {
                    std::vector<value::Value> cycle;
                    cycle.reserve(re_rank_stack.size() + 2);
                    for (const auto& item : re_rank_stack) {
                        cycle.push_back(item.view().clone());
                    }
                    cycle.push_back(key.clone());
                    cycle.push_back(k.view().clone());

                    std::string cycle_str;
                    for (size_t i = 0; i < cycle.size(); ++i) {
                        if (i > 0) {
                            cycle_str += " -> ";
                        }
                        cycle_str += key_repr(cycle[i].view(), key_type_meta_);
                    }

                    const std::string node_label =
                        signature().label.has_value() ? signature().label.value() : signature().name;
                    throw std::runtime_error(
                        fmt::format("mesh {}.{} has a dependency cycle: {}",
                                    signature().wiring_path_name,
                                    node_label,
                                    cycle_str));
                }

                std::vector<value::Value> new_stack;
                new_stack.reserve(re_rank_stack.size() + 1);
                for (const auto& item : re_rank_stack) {
                    new_stack.push_back(item.view().clone());
                }
                new_stack.push_back(key.clone());
                re_rank(k.view(), key, std::move(new_stack));
            }
        }
    }

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        nb::class_<MeshNestedEngineEvaluationClock, NestedEngineEvaluationClock>(
            m, "MeshNestedEngineEvaluationClock")
            .def_prop_ro("key", &MeshNestedEngineEvaluationClock::py_key);

        nb::class_<MeshNode, TsdMapNode>(m, "MeshNode")
            .def("_add_graph_dependency", &MeshNode::_add_graph_dependency, "key"_a, "depends_on"_a)
            .def("_remove_graph_dependency", &MeshNode::_remove_graph_dependency, "key"_a, "depends_on"_a);
    }
}  // namespace hgraph
