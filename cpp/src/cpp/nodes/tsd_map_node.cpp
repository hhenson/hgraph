#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/scope.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph
{
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

        TSInputView node_inner_ts_input(Node &node) {
            auto root = node.input(node_time(node));
            if (!root) {
                return {};
            }

            auto bundle_opt = root.try_as_bundle();
            if (!bundle_opt.has_value()) {
                return {};
            }

            return bundle_opt->field("ts");
        }

        std::string key_repr(const value::View &key, const value::TypeMeta *key_type_meta) {
            if (!key.valid() || key_type_meta == nullptr) {
                return "<invalid key>";
            }
            nb::object py_key = key_type_meta->ops().to_python(key.data(), key_type_meta);
            return nb::cast<std::string>(nb::repr(py_key));
        }

        TSView resolve_outer_key_view(TSView outer_ts, const value::View &key) {
            if (!outer_ts || !key.valid()) {
                return {};
            }

            TSView child = outer_ts.child_by_key(key);
            if (child) {
                return child;
            }

            const TSMeta *outer_meta = outer_ts.ts_meta();
            if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
                ViewData bound_target{};
                if (resolve_bound_target_view_data(outer_ts.view_data(), bound_target)) {
                    return TSView(bound_target, outer_ts.current_time()).child_by_key(key);
                }
            }
            return {};
        }

        TSView resolve_effective_view(TSView view) {
            if (!view) {
                return {};
            }

            TSView current = view;
            for (size_t depth = 0; depth < 8; ++depth) {
                bool advanced = false;

                ViewData bound_target{};
                if (resolve_bound_target_view_data(current.view_data(), bound_target)) {
                    if (!(bound_target.path.indices == current.view_data().path.indices &&
                          bound_target.value_data == current.view_data().value_data &&
                          bound_target.time_data == current.view_data().time_data &&
                          bound_target.observer_data == current.view_data().observer_data &&
                          bound_target.delta_data == current.view_data().delta_data &&
                          bound_target.link_data == current.view_data().link_data &&
                          bound_target.projection == current.view_data().projection &&
                          bound_target.meta == current.view_data().meta)) {
                        current = TSView(bound_target, current.current_time());
                        advanced = true;
                    }
                }
                if (advanced) {
                    continue;
                }

                const TSMeta *meta = current.ts_meta();
                if (meta != nullptr && meta->kind == TSKind::REF) {
                    value::View ref_view = current.value();
                    if (ref_view.valid()) {
                        try {
                            TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
                            if (const ViewData *ref_target = ref.bound_view(); ref_target != nullptr) {
                                if (!(ref_target->path.indices == current.view_data().path.indices &&
                                      ref_target->value_data == current.view_data().value_data &&
                                      ref_target->time_data == current.view_data().time_data &&
                                      ref_target->observer_data == current.view_data().observer_data &&
                                      ref_target->delta_data == current.view_data().delta_data &&
                                      ref_target->link_data == current.view_data().link_data &&
                                      ref_target->projection == current.view_data().projection &&
                                      ref_target->meta == current.view_data().meta)) {
                                    current = TSView(*ref_target, current.current_time());
                                    advanced = true;
                                }
                            }
                        } catch (const std::exception &) {
                            // Not a TSView-backed reference.
                        }
                    }
                }

                if (!advanced) {
                    break;
                }
            }

            return current;
        }

        void bind_inner_from_outer(const TSView &outer_any, TSInputView inner_any) {
            if (!inner_any) {
                return;
            }

            if (!outer_any) {
                inner_any.unbind();
                return;
            }

            const TSMeta *outer_meta = outer_any.ts_meta();
            if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
                ViewData bound_target{};
                if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                    inner_any.as_ts_view().bind(TSView(bound_target, inner_any.current_time()));
                    return;
                }

                TimeSeriesReference ref = TimeSeriesReference::make();
                value::View ref_view = outer_any.value();
                if (ref_view.valid()) {
                    ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
                }
                ref.bind_input(inner_any);
                return;
            }

            ViewData bound_target{};
            if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                inner_any.as_ts_view().bind(TSView(bound_target, inner_any.current_time()));
            } else {
                inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_any.current_time()));
            }
        }

        bool collect_current_map_keys(const TSInputView& keys_view, TsdMapNode::key_set_type& out) {
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
    }  // namespace

    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::Value key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto &node_{*static_cast<TsdMapNode *>(node())};
        auto let = node_.last_evaluation_time();
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) {
            return;
        }

        auto it{node_.scheduled_keys_.find(_key.view())};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) {
            node_.scheduled_keys_.insert_or_assign(_key.view().clone(), next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        auto *node_ = static_cast<TsdMapNode *>(node());
        if (const auto *key_schema = node_->key_type_meta(); key_schema != nullptr) {
            return key_schema->ops().to_python(_key.data(), key_schema);
        }
        return nb::none();
    }

    TsdMapNode::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, const TSMeta *input_meta, const TSMeta *output_meta,
                           const TSMeta *error_output_meta, const TSMeta *recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                           const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {}

    nb::dict TsdMapNode::py_nested_graphs() const {
        nb::dict result;
        for (const auto &[key, graph] : active_graphs_) {
            if (key_type_meta_ == nullptr) {
                continue;
            }
            nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
            result[py_key] = nb::cast(graph);
        }
        return result;
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr &)> &callback) const {
        for (const auto &[_, graph] : active_graphs_) {
            if (graph) {
                callback(graph);
            }
        }
    }

    void TsdMapNode::initialise() {
        auto keys_view = node_input_field(*this, KEYS_ARG);
        const TSMeta *keys_meta = keys_view ? keys_view.ts_meta() : nullptr;
        if (keys_meta == nullptr) {
            key_type_meta_ = nullptr;
            return;
        }

        if (keys_meta->kind != TSKind::TSS) {
            throw std::runtime_error("TsdMapNode expected __keys__ input to be TSS");
        }
        key_type_meta_ = keys_meta->value_type != nullptr ? keys_meta->value_type->element_type : nullptr;
    }

    void TsdMapNode::do_start() {
        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(), recordable_id.has_value() ? recordable_id.value() : "map_");
        }

        // Multiplexed and shared args must be active on the outer map node so
        // value-only updates (with stable key sets) can reschedule keyed graphs.
        auto outer_root = input(node_time(*this));
        auto outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;
        if (!outer_bundle.has_value()) {
            return;
        }

        for (const auto& [arg, _] : input_node_ids_) {
            if (arg == key_arg_) {
                continue;
            }
            auto outer_arg = outer_bundle->field(arg);
            if (outer_arg) {
                outer_arg.make_active();
            }
        }
    }

    void TsdMapNode::do_stop() {
        std::vector<value::Value> keys;
        keys.reserve(active_graphs_.size());
        for (const auto &[k, _] : active_graphs_) {
            keys.push_back(k.view().clone());
        }
        for (const auto &k : keys) {
            remove_graph(k.view());
        }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    void TsdMapNode::dispose() {}

    void TsdMapNode::eval() {
        mark_evaluated();

        auto keys_view = node_input_field(*this, KEYS_ARG);
        if (keys_view && keys_view.modified()) {
            std::vector<value::Value> added_keys;
            std::vector<value::Value> removed_keys;
            const bool has_key_delta = extract_key_delta(keys_view, key_type_meta_, added_keys, removed_keys);
            if (has_key_delta) {
                for (const auto& key : added_keys) {
                    if (active_graphs_.find(key) == active_graphs_.end()) {
                        create_new_graph(key.view());
                    } else {
                        throw std::runtime_error(
                            fmt::format("[{}] Key {} already exists in active graphs", signature().wiring_path_name,
                                        key_repr(key.view(), key_type_meta_)));
                    }
                }
                for (const auto& key : removed_keys) {
                    if (active_graphs_.find(key) != active_graphs_.end()) {
                        remove_graph(key.view());
                        scheduled_keys_.erase(key);
                    } else {
                        throw std::runtime_error(
                            fmt::format("[{}] Key {} does not exist in active graphs", signature().wiring_path_name,
                                        key_repr(key.view(), key_type_meta_)));
                    }
                }
            } else {
                key_set_type current_keys;
                if (!collect_current_map_keys(keys_view, current_keys)) {
                    throw std::runtime_error("TsdMapNode expected set/map value for __keys__");
                }

                for (const auto& key : current_keys) {
                    if (active_graphs_.find(key.view()) == active_graphs_.end()) {
                        create_new_graph(key.view());
                    }
                }

                std::vector<value::Value> keys_to_remove;
                keys_to_remove.reserve(active_graphs_.size());
                for (const auto& [key, _] : active_graphs_) {
                    if (current_keys.find(key.view()) == current_keys.end()) {
                        keys_to_remove.push_back(key.view().clone());
                    }
                }
                for (const auto& key : keys_to_remove) {
                    remove_graph(key.view());
                    scheduled_keys_.erase(key);
                }
            }
        } else if (keys_view && active_graphs_.empty()) {
            key_set_type current_keys;
            if (collect_current_map_keys(keys_view, current_keys)) {
                for (const auto &key : current_keys) {
                    if (active_graphs_.find(key.view()) == active_graphs_.end()) {
                        create_new_graph(key.view());
                    }
                }
            }
        }

        auto schedule_key_now = [&](const value::View& key_view, engine_time_t now) {
            auto active_it = active_graphs_.find(key_view);
            if (active_it == active_graphs_.end()) {
                return;
            }
            auto scheduled_it = scheduled_keys_.find(active_it->first.view());
            if (scheduled_it == scheduled_keys_.end() || scheduled_it->second > now) {
                scheduled_keys_.insert_or_assign(active_it->first.view().clone(), now);
            }
        };

        auto schedule_all_active_now = [&](engine_time_t now) {
            for (const auto& [key, _] : active_graphs_) {
                auto scheduled_it = scheduled_keys_.find(key.view());
                if (scheduled_it == scheduled_keys_.end() || scheduled_it->second > now) {
                    scheduled_keys_.insert_or_assign(key.view().clone(), now);
                }
            }
        };

        auto outer_root = input(node_time(*this));
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;
        if (outer_bundle.has_value()) {
            const engine_time_t now = last_evaluation_time();
            for (const auto& [arg, _] : input_node_ids_) {
                if (arg == key_arg_) {
                    continue;
                }

                auto outer_arg = outer_bundle->field(arg);
                if (!outer_arg || !outer_arg.modified()) {
                    continue;
                }

                if (multiplexed_args_.find(arg) == multiplexed_args_.end()) {
                    schedule_all_active_now(now);
                    continue;
                }

                bool scheduled_from_delta = false;
                TSView effective_arg = resolve_effective_view(outer_arg.as_ts_view());
                if (effective_arg) {
                    const TSMeta* effective_meta = effective_arg.ts_meta();
                    if (effective_meta != nullptr && effective_meta->kind == TSKind::TSD) {
                        nb::object delta = effective_arg.delta_to_python();
                        if (nb::isinstance<nb::dict>(delta)) {
                            nb::dict delta_dict = nb::cast<nb::dict>(delta);
                            for (const auto& kv : delta_dict) {
                                auto key_value = key_from_python_object(nb::cast<nb::object>(kv.first), key_type_meta_);
                                if (!key_value.has_value()) {
                                    continue;
                                }
                                schedule_key_now(key_value->view(), now);
                                scheduled_from_delta = true;
                            }
                        }
                    }
                }

                if (!scheduled_from_delta) {
                    schedule_all_active_now(now);
                }
            }
        }

        key_time_map_type scheduled_keys;
        std::swap(scheduled_keys, scheduled_keys_);

        for (const auto &[k, dt] : scheduled_keys) {
            if (dt < last_evaluation_time()) {
                throw std::runtime_error(
                    fmt::format("Scheduled time is in the past; last evaluation time: {}, scheduled time: {}, evaluation time: {}",
                                last_evaluation_time(), dt, graph()->evaluation_time()));
            }

            const engine_time_t next_dt = (dt == last_evaluation_time()) ? evaluate_graph(k.view()) : dt;
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_.insert_or_assign(k.view().clone(), next_dt);
                graph()->schedule_node(node_ndx(), next_dt);
            }
        }
    }

    TSDOutputView TsdMapNode::tsd_output(engine_time_t current_time) {
        auto out = output(current_time);
        if (!out) {
            return {};
        }

        auto dict_opt = out.try_as_dict();
        if (!dict_opt.has_value()) {
            return {};
        }
        return *dict_opt;
    }

    void TsdMapNode::create_new_graph(const value::View &key) {
        if (key_type_meta_ == nullptr) {
            throw std::runtime_error("TsdMapNode key type meta is not initialised");
        }

        nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
        std::string key_str = nb::cast<std::string>(nb::repr(py_key));

        auto child_owning_graph_id = node_id();
        child_owning_graph_id.push_back(-static_cast<int64_t>(count_++));

        auto graph_ = nested_graph_builder_->make_instance(child_owning_graph_id, this, key_str);
        active_graphs_.emplace(key.clone(), graph_);

        graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(),
            std::make_shared<MapNestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), key.clone(), this)));

        initialise_component(*graph_);

        if (!recordable_id_.empty()) {
            set_parent_recordable_id(*graph_, fmt::format("{}[{}]", recordable_id_, key_str));
        }

        wire_graph(key, graph_);
        start_component(*graph_);
        scheduled_keys_.insert_or_assign(key.clone(), last_evaluation_time());
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        if (signature().capture_exception && has_error_output()) {
            auto err_out = error_output(node_time(*this));
            if (err_out) {
                if (auto err_dict = err_out.try_as_dict(); err_dict.has_value()) {
                    err_dict->remove(key);
                }
            }
        }

        if (auto it = active_graphs_.find(key); it != active_graphs_.end()) {
            auto nested_graph = it->second;
            active_graphs_.erase(it);

            un_wire_graph(key, nested_graph);

            auto builder = nested_graph_builder_;
            auto engine = graph()->evaluation_engine();
            auto cleanup = make_scope_exit([builder, engine, nested_graph]() mutable {
                if (builder == nullptr) {
                    return;
                }
                if (engine != nullptr) {
                    engine->add_before_evaluation_notification([builder, nested_graph]() mutable {
                        builder->release_instance(nested_graph);
                    });
                } else {
                    builder->release_instance(nested_graph);
                }
            });
            stop_component(*nested_graph);
        }
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::View &key) {
        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) {
            return MAX_DT;
        }

        auto &nested = it->second;
        refresh_multiplexed_bindings(key, nested);
        if (auto *nec = dynamic_cast<NestedEngineEvaluationClock *>(nested->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        if (signature().capture_exception && has_error_output()) {
            auto capture_nested_error = [&](const NodeError &node_error) {
                auto err_out = error_output(node_time(*this));
                if (!err_out) {
                    return;
                }
                auto err_dict = err_out.try_as_dict();
                if (!err_dict.has_value()) {
                    return;
                }

                auto error_ts = err_dict->create(key);
                auto error_ptr = nb::ref<NodeError>(new NodeError(node_error));
                error_ts.from_python(nb::cast(error_ptr));
            };

            try {
                nested->evaluate_graph();
            } catch (const std::exception &e) {
                auto msg = std::string("key: ") + key_repr(key, key_type_meta_);
                capture_nested_error(NodeError::capture_error(e, *this, msg));
            } catch (...) {
                auto msg = std::string("key: ") + key_repr(key, key_type_meta_);
                capture_nested_error(NodeError::capture_error(std::current_exception(), *this, msg));
            }
        } else {
            nested->evaluate_graph();
        }

        // Mirror Python map-node behavior: each nested graph writes to its own
        // key slot in the outer TSD output. We apply the nested output delta
        // to the keyed outer output view after each nested evaluation.
        if (output_node_id_ >= 0) {
            auto outer = tsd_output(node_time(*this));
            auto node = nested->nodes()[output_node_id_];
            auto inner = node->output(node_time(*node));
            if (outer && inner) {
                auto outer_key = outer.create(key);
                if (outer_key) {
                    TSView inner_raw = inner.as_ts_view();
                    TSView inner_effective = resolve_effective_view(inner_raw);
                    const bool outer_was_valid = outer_key.valid();

                    const TSMeta *outer_meta = outer_key.ts_meta();
                    if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
                        if (inner_effective) {
                            outer_key.from_python(nb::cast(TimeSeriesReference::make(inner_effective.view_data())));
                        } else if (inner.modified()) {
                            outer_key.invalidate();
                        }
                    } else if (inner_effective.valid()) {
                        if (inner_effective.modified()) {
                            outer_key.from_python(inner_effective.delta_to_python());
                        } else if (!outer_was_valid) {
                            outer_key.from_python(inner_effective.to_python());
                        }
                    } else if (inner_effective.modified()) {
                        outer_key.invalidate();
                    }
                }
            }
        }

        auto next = nested->evaluation_engine_clock()->next_scheduled_evaluation_time();
        if (auto *nec = dynamic_cast<NestedEngineEvaluationClock *>(nested->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        return next;
    }

    void TsdMapNode::un_wire_graph(const value::View &key, graph_s_ptr &graph) {
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_) {
                continue;
            }
            if (multiplexed_args_.find(arg) == multiplexed_args_.end()) {
                continue;
            }

            auto node = graph->nodes()[node_ndx];
            auto inner_ts = node_inner_ts_input(*node);
            if (!inner_ts) {
                continue;
            }

            if (inner_ts.active()) {
                inner_ts.make_passive();
            }
            inner_ts.unbind();
        }

        if (output_node_id_ >= 0) {
            auto out = tsd_output(node_time(*this));
            if (out) {
                out.remove(key);
            }
        }
    }

    void TsdMapNode::wire_graph(const value::View &key, graph_s_ptr &graph) {
        auto outer_root = input(node_time(*this));
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;

        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            node->notify();

            if (arg == key_arg_) {
                auto &key_node = dynamic_cast<PythonNode &>(*node);
                nb::object py_key = key_type_meta_ != nullptr
                                        ? key_type_meta_->ops().to_python(key.data(), key_type_meta_)
                                        : nb::none();
                nb::setattr(key_node.eval_fn(), "key", py_key);
                continue;
            }

            auto inner_ts = node_inner_ts_input(*node);
            if (!inner_ts) {
                continue;
            }

            if (!outer_bundle.has_value()) {
                inner_ts.unbind();
                continue;
            }

            auto outer_arg = outer_bundle->field(arg);
            if (!outer_arg) {
                inner_ts.unbind();
                continue;
            }

            if (multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                TSView outer_key_value = resolve_outer_key_view(outer_arg.as_ts_view(), key);
                bind_inner_from_outer(outer_key_value, inner_ts);
            } else {
                bind_inner_from_outer(outer_arg.as_ts_view(), inner_ts);
            }
        }

        if (output_node_id_ >= 0) {
            auto node = graph->nodes()[output_node_id_];
            auto inner_out = node->output(node_time(*node));
            auto out = tsd_output(node_time(*this));
            if (!inner_out || !out) {
                return;
            }
            (void)out.create(key);
        }
    }

    bool TsdMapNode::refresh_multiplexed_bindings(const value::View &key, graph_s_ptr &graph) {
        auto outer_root = input(node_time(*this));
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;

        bool refreshed = false;
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_) {
                continue;
            }
            if (multiplexed_args_.find(arg) == multiplexed_args_.end()) {
                continue;
            }

            auto node = graph->nodes()[node_ndx];
            auto inner_ts = node_inner_ts_input(*node);
            if (!inner_ts) {
                continue;
            }

            if (!outer_bundle.has_value()) {
                inner_ts.unbind();
                refreshed = true;
                continue;
            }

            auto outer_arg = outer_bundle->field(arg);
            if (!outer_arg) {
                inner_ts.unbind();
                refreshed = true;
                continue;
            }

            TSView outer_key_value = resolve_outer_key_view(outer_arg.as_ts_view(), key);
            bind_inner_from_outer(outer_key_value, inner_ts);
            node->notify();
            refreshed = true;
        }

        return refreshed;
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict,
                          const TSMeta *, const TSMeta *, const TSMeta *, const TSMeta *,
                          graph_builder_s_ptr, const std::unordered_map<std::string, int64_t> &, int64_t,
                          const std::unordered_set<std::string> &, const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a,
                 "input_meta"_a, "output_meta"_a, "error_output_meta"_a, "recordable_state_meta"_a,
                 "nested_graph_builder"_a, "input_node_ids"_a, "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
