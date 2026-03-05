#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/node_binding_utils.h>
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
#include <cstdlib>
#include <cstdio>

namespace hgraph
{
    namespace {
        bool debug_tsd_map_bind_enabled() {
            static const bool enabled = std::getenv("HGRAPH_DEBUG_TSD_MAP_BIND") != nullptr;
            return enabled;
        }

        bool debug_tsd_map_clock_enabled() {
            static const bool enabled = std::getenv("HGRAPH_DEBUG_TSD_MAP_CLOCK") != nullptr;
            return enabled;
        }

        bool debug_tsd_map_enabled() {
            static const bool enabled = std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr;
            return enabled;
        }

        bool debug_tsd_map_copy_enabled() {
            static const bool enabled = std::getenv("HGRAPH_DEBUG_TSD_MAP_COPY") != nullptr;
            return enabled;
        }

        TSView resolve_outer_key_view(TSView outer_ts, const value::View &key) {
            if (!outer_ts || !key.valid()) {
                return {};
            }
            const bool debug_bind = debug_tsd_map_bind_enabled();

            TSView child = outer_ts.child_by_key(key);
            TSView effective_child = hgraph::resolve_effective_view(child);
            if (debug_bind) {
                std::string child_value{"<none>"};
                std::string effective_child_value{"<none>"};
                try {
                    child_value = nb::cast<std::string>(nb::repr(child.to_python()));
                } catch (...) {}
                try {
                    effective_child_value = nb::cast<std::string>(nb::repr(effective_child.to_python()));
                } catch (...) {}
                std::fprintf(stderr,
                             "[tsd_map_bind] direct key=%s child_exists=%d child_valid=%d child_mod=%d child_value=%s effective_valid=%d effective_mod=%d effective_value=%s kind=%d\n",
                             key.valid() ? key.to_string().c_str() : "<invalid>",
                             child ? 1 : 0,
                             child.valid() ? 1 : 0,
                             child.modified() ? 1 : 0,
                             child_value.c_str(),
                             effective_child.valid() ? 1 : 0,
                             effective_child.modified() ? 1 : 0,
                             effective_child_value.c_str(),
                             static_cast<int>(outer_ts.kind()));
            }
            if (effective_child && effective_child.valid()) {
                return effective_child;
            }
            if (child && child.valid()) {
                return child;
            }

            // For link-target backed container inputs, the local child can exist as
            // an empty placeholder while the bound source has the concrete keyed
            // value. Fall back to the bound source child in that case.
            ViewData bound_target{};
            if (resolve_bound_target_view_data(outer_ts.view_data(), bound_target)) {
                TSView bound_child = TSView(bound_target, outer_ts.view_data().engine_time_ptr).child_by_key(key);
                TSView effective_bound_child = hgraph::resolve_effective_view(bound_child);
                if (debug_bind) {
                    std::string bound_child_value{"<none>"};
                    std::string effective_bound_child_value{"<none>"};
                    try {
                        bound_child_value = nb::cast<std::string>(nb::repr(bound_child.to_python()));
                    } catch (...) {}
                    try {
                        effective_bound_child_value = nb::cast<std::string>(nb::repr(effective_bound_child.to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map_bind] bound key=%s bound_child_exists=%d bound_child_valid=%d bound_child_mod=%d bound_child_value=%s effective_valid=%d effective_mod=%d effective_value=%s\n",
                                 key.valid() ? key.to_string().c_str() : "<invalid>",
                                 bound_child ? 1 : 0,
                                 bound_child.valid() ? 1 : 0,
                                 bound_child.modified() ? 1 : 0,
                                 bound_child_value.c_str(),
                                 effective_bound_child.valid() ? 1 : 0,
                                 effective_bound_child.modified() ? 1 : 0,
                                 effective_bound_child_value.c_str());
                }
                if (effective_bound_child && effective_bound_child.valid()) {
                    return effective_bound_child;
                }
                if (bound_child) {
                    return bound_child;
                }
            } else if (debug_bind) {
                std::fprintf(stderr,
                             "[tsd_map_bind] bound key=%s resolve_bound_target=0\n",
                             key.valid() ? key.to_string().c_str() : "<invalid>");
            }

            if (child) {
                return child;
            }
            return {};
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
        const bool debug_clock = debug_tsd_map_clock_enabled();
        if (debug_clock) {
            std::fprintf(stderr,
                         "[tsd_map_clock] node=%s ptr=%p ndx=%lld key=%s let=%lld next=%lld stop=%d\n",
                         node_.signature().name.c_str(),
                         static_cast<void*>(&node_),
                         static_cast<long long>(node_.node_ndx()),
                         key_repr(_key.view(), node_.key_type_meta()).c_str(),
                         static_cast<long long>(let.time_since_epoch().count()),
                         static_cast<long long>(next_time.time_since_epoch().count()),
                         node_.is_stopping() ? 1 : 0);
        }
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
        auto keys_view = hgraph::node_input_field(*this, KEYS_ARG);
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
        const bool debug_tsd_map = debug_tsd_map_enabled();
        if (debug_tsd_map) {
            std::string multiplexed;
            bool first = true;
            for (const auto& arg : multiplexed_args_) {
                if (!first) {
                    multiplexed += ",";
                }
                multiplexed += arg;
                first = false;
            }
            std::string inputs;
            first = true;
            for (const auto& [arg, ndx] : input_node_ids_) {
                if (!first) {
                    inputs += ",";
                }
                inputs += arg + ":" + std::to_string(ndx);
                first = false;
            }
            std::fprintf(stderr,
                         "[tsd_map] start node=%s ptr=%p ndx=%lld key_arg=%s multiplexed={%s} inputs={%s}\n",
                         signature().name.c_str(),
                         static_cast<void*>(this),
                         static_cast<long long>(node_ndx()),
                         key_arg_.c_str(),
                         multiplexed.c_str(),
                         inputs.c_str());
        }

        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(), recordable_id.has_value() ? recordable_id.value() : "map_");
        }

        // Multiplexed and shared args must be active on the outer map node so
        // value-only updates (with stable key sets) can reschedule keyed graphs.
        auto outer_root = input();
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
        local_input_values_.clear();
        force_emit_keys_.clear();
    }

    void TsdMapNode::dispose() {}

    void TsdMapNode::eval() {
        mark_evaluated();
        const bool debug_tsd_map = debug_tsd_map_enabled();
        if (debug_tsd_map) {
            std::fprintf(stderr,
                         "[tsd_map] eval node=%s ptr=%p ndx=%lld now=%lld active=%zu scheduled=%zu\n",
                         signature().name.c_str(),
                         static_cast<void*>(this),
                         static_cast<long long>(node_ndx()),
                         static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                         active_graphs_.size(),
                         scheduled_keys_.size());
        }

        auto keys_view = hgraph::node_input_field(*this, KEYS_ARG);
        if (keys_view && keys_view.modified()) {
            const auto add_key = [&](const value::View& key_view) {
                if (debug_tsd_map) {
                    std::fprintf(stderr, "[tsd_map]  key_add=%s\n", key_repr(key_view, key_type_meta_).c_str());
                }
                if (active_graphs_.find(key_view) == active_graphs_.end()) {
                    create_new_graph(key_view);
                }
            };

            const auto remove_key = [&](const value::View& key_view) {
                if (debug_tsd_map) {
                    std::fprintf(stderr, "[tsd_map]  key_remove=%s\n", key_repr(key_view, key_type_meta_).c_str());
                }
                if (active_graphs_.find(key_view) != active_graphs_.end()) {
                    remove_graph(key_view);
                    if (auto scheduled_it = scheduled_keys_.find(key_view); scheduled_it != scheduled_keys_.end()) {
                        scheduled_keys_.erase(scheduled_it);
                    }
                }
            };

            bool use_full_diff = active_graphs_.empty();
            bool has_key_delta = false;
            if (!use_full_diff) {
                has_key_delta = hgraph::for_each_tsd_key_delta(
                    keys_view,
                    key_type_meta_,
                    [&](value::View key_view) {
                        if (!use_full_diff && active_graphs_.find(key_view) != active_graphs_.end()) {
                            use_full_diff = true;
                        }
                    },
                    [&](value::View key_view) {
                        if (!use_full_diff && active_graphs_.find(key_view) == active_graphs_.end()) {
                            use_full_diff = true;
                        }
                    });
            }
            if (!has_key_delta) {
                use_full_diff = true;
            }

            // When starting from an empty active set, key deltas alone are
            // insufficient (delta may only include incremental additions).
            // Bootstrap from the full current key snapshot in that case.
            if (use_full_diff) {
                key_set_type current_keys;
                if (!hgraph::collect_tsd_key_set(keys_view, current_keys)) {
                    throw std::runtime_error("TsdMapNode expected set/map value for __keys__");
                }

                for (const auto& key : current_keys) {
                    if (active_graphs_.find(key.view()) == active_graphs_.end()) {
                        add_key(key.view());
                    }
                }

                for (auto it = active_graphs_.begin(); it != active_graphs_.end();) {
                    if (current_keys.find(it->first.view()) != current_keys.end()) {
                        ++it;
                        continue;
                    }
                    value::Value removed_key = it->first.view().clone();
                    ++it;
                    remove_key(removed_key.view());
                }
            } else {
                (void)hgraph::for_each_tsd_key_delta(
                    keys_view,
                    key_type_meta_,
                    [&](value::View key_view) { add_key(key_view); },
                    [&](value::View key_view) { remove_key(key_view); });
            }
        } else if (keys_view && active_graphs_.empty()) {
            key_set_type current_keys;
            if (hgraph::collect_tsd_key_set(keys_view, current_keys)) {
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

        auto outer_root = input();
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
                if (debug_tsd_map) {
                    std::string outer_value{"<none>"};
                    std::string outer_delta{"<none>"};
                    try {
                        outer_value = nb::cast<std::string>(nb::repr(outer_arg.to_python()));
                    } catch (...) {}
                    try {
                        outer_delta = nb::cast<std::string>(nb::repr(outer_arg.delta_to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map]  outer arg=%s modified=1 value=%s delta=%s\n",
                                 arg.c_str(),
                                 outer_value.c_str(),
                                 outer_delta.c_str());
                }

                bool scheduled_from_delta = false;
                auto schedule_changed_keys = [&](const auto& dict_view) {
                    const auto schedule_from = [&](auto&& keys_iterable) {
                        bool any = false;
                        for (value::View key_view : keys_iterable) {
                            if (!key_view.valid()) {
                                continue;
                            }
                            schedule_key_now(key_view, now);
                            any = true;
                        }
                        return any;
                    };

                    if (schedule_from(dict_view.modified_keys()) ||
                        schedule_from(dict_view.added_keys()) ||
                        schedule_from(dict_view.removed_keys())) {
                        scheduled_from_delta = true;
                    }
                };

                auto schedule_changed_keys_from_raw_delta = [&](const TSView& ts_view) {
                    if (!ts_view) {
                        return;
                    }
                    value::View delta = ts_view.delta_value().value();
                    if (!delta.valid() || !delta.is_tuple()) {
                        return;
                    }
                    auto tuple = delta.as_tuple();
                    if (tuple.size() == 0) {
                        return;
                    }
                    value::View changed = tuple.at(0);
                    if (!changed.valid() || !changed.is_map()) {
                        return;
                    }

                    bool any = false;
                    for (value::View key_view : changed.as_map().keys()) {
                        if (!key_view.valid()) {
                            continue;
                        }
                        schedule_key_now(key_view, now);
                        any = true;
                    }
                    if (any) {
                        scheduled_from_delta = true;
                    }
                };

                if (auto outer_dict = outer_arg.try_as_dict(); outer_dict.has_value()) {
                    schedule_changed_keys(*outer_dict);
                } else {
                    TSView effective_arg = hgraph::resolve_effective_view(outer_arg.as_ts_view());
                    if (auto effective_dict = effective_arg.try_as_dict(); effective_dict.has_value()) {
                        schedule_changed_keys(*effective_dict);
                    }
                    if (!scheduled_from_delta) {
                        schedule_changed_keys_from_raw_delta(effective_arg);
                    }
                }

                if (!scheduled_from_delta) {
                    schedule_changed_keys_from_raw_delta(outer_arg.as_ts_view());
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

            if (debug_tsd_map) {
                std::fprintf(stderr,
                             "[tsd_map]  run key=%s due=%lld now=%lld\n",
                             key_repr(k.view(), key_type_meta_).c_str(),
                             static_cast<long long>(dt.time_since_epoch().count()),
                             static_cast<long long>(last_evaluation_time().time_since_epoch().count()));
            }
            const engine_time_t next_dt = (dt == last_evaluation_time()) ? evaluate_graph(k.view()) : dt;
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_.insert_or_assign(k.view().clone(), next_dt);
                graph()->schedule_node(node_ndx(), next_dt);
            }
        }
    }

    TSDOutputView TsdMapNode::tsd_output() {
        auto out = output();
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
        pending_keys_.insert(key.clone());
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        if (signature().capture_exception && has_error_output()) {
            auto err_out = error_output();
            if (err_out) {
                if (auto err_dict = err_out.try_as_dict(); err_dict.has_value()) {
                    err_dict->remove(key);
                }
            }
        }

        if (auto it = active_graphs_.find(key); it != active_graphs_.end()) {
            const value::Value key_value = it->first.view().clone();
            auto nested_graph = it->second;
            active_graphs_.erase(it);
            if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                pending_keys_.erase(pending_it);
            }
            if (auto emit_it = force_emit_keys_.find(key_value); emit_it != force_emit_keys_.end()) {
                force_emit_keys_.erase(emit_it);
            }
            for (auto& [_, values] : local_input_values_) {
                values.erase(key.clone());
            }

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
        const value::Value key_value = it->first.view().clone();
        const bool force_emit = force_emit_keys_.find(key_value) != force_emit_keys_.end();
        const bool debug_tsd_map_copy = debug_tsd_map_copy_enabled();

        auto &nested = it->second;

        auto outer_root = input();
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_ || multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                continue;
            }

            auto node = nested->nodes()[node_ndx];
            auto inner_ts = hgraph::node_inner_ts_input(*node, false);
            if (!inner_ts) {
                continue;
            }

            if (!outer_bundle.has_value()) {
                if (inner_ts.is_bound()) {
                    inner_ts.unbind();
                    node->notify();
                }
                continue;
            }

            auto outer_arg = outer_bundle->field(arg);
            if (!outer_arg) {
                if (inner_ts.is_bound()) {
                    inner_ts.unbind();
                    node->notify();
                }
                continue;
            }

            hgraph::BindingTargetComparison binding_targets = hgraph::compare_binding_targets(inner_ts, outer_arg.as_ts_view());

            if (!inner_ts.is_bound() || binding_targets.binding_changed) {
                hgraph::bind_inner_from_outer(outer_arg.as_ts_view(), inner_ts, RefBindOrder::BoundTargetThenRefValue);
            }
            if (outer_arg.modified() || binding_targets.binding_changed) {
                node->notify();
            }
        }

        bool all_mux_inputs_valid = true;
        refresh_multiplexed_bindings(key, nested, &all_mux_inputs_valid);
        if (auto *nec = dynamic_cast<NestedEngineEvaluationClock *>(nested->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        if (signature().capture_exception && has_error_output()) {
            auto capture_nested_error = [&](const NodeError &node_error) {
                auto err_out = error_output();
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
            auto outer = tsd_output();
            auto node = nested->nodes()[output_node_id_];
            auto inner = node->output();
            if (outer && inner) {
                TSView inner_raw = inner.as_ts_view();
                TSView inner_effective = hgraph::resolve_effective_view(inner_raw);
                TSView outer_existing = outer.as_ts_view().as_dict().at_key(key);
                const bool has_outer_entry = static_cast<bool>(outer_existing);
                const bool output_init_pending = pending_keys_.find(key_value) != pending_keys_.end();

                const TSMeta* outer_meta = outer.as_ts_view().ts_meta();
                const bool ref_output =
                    outer_meta != nullptr &&
                    outer_meta->kind == TSKind::TSD &&
                    outer_meta->element_ts() != nullptr &&
                    outer_meta->element_ts()->kind == TSKind::REF;

                if (!ref_output && !all_mux_inputs_valid) {
                    outer.remove(key);
                    auto next = nested->evaluation_engine_clock()->next_scheduled_evaluation_time();
                    if (auto *nec = dynamic_cast<NestedEngineEvaluationClock *>(nested->evaluation_engine_clock().get())) {
                        nec->reset_next_scheduled_evaluation_time();
                    }
                    return next;
                }

                if (debug_tsd_map_copy) {
                    std::string delta_s{"<none>"};
                    std::string value_s{"<none>"};
                    bool raw_valid = inner_raw.valid();
                    bool raw_modified = inner_raw.modified();
                    engine_time_t inner_lmt = MIN_DT;
                    engine_time_t raw_lmt = MIN_DT;
                    if (inner_effective) {
                        inner_lmt = inner_effective.last_modified_time();
                    }
                    if (inner_raw) {
                        raw_lmt = inner_raw.last_modified_time();
                    }
                    std::string inner_path = inner_effective ? inner_effective.short_path().to_string() : "<none>";
                    std::string raw_path = inner_raw ? inner_raw.short_path().to_string() : "<none>";
                    std::string raw_value_s{"<none>"};
                    std::string raw_delta_s{"<none>"};
                    try {
                        delta_s = nb::cast<std::string>(nb::repr(inner_effective.delta_to_python()));
                    } catch (...) {}
                    try {
                        value_s = nb::cast<std::string>(nb::repr(inner_effective.to_python()));
                    } catch (...) {}
                    try {
                        raw_value_s = nb::cast<std::string>(nb::repr(inner_raw.to_python()));
                    } catch (...) {}
                    try {
                        raw_delta_s = nb::cast<std::string>(nb::repr(inner_raw.delta_to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map_copy] key=%s now=%lld inner_path=%s inner_lmt=%lld inner_valid=%d inner_modified=%d raw_path=%s raw_lmt=%lld raw_valid=%d raw_modified=%d outer_has=%d inner_delta=%s inner_value=%s raw_delta=%s raw_value=%s\n",
                                 key_repr(key, key_type_meta_).c_str(),
                                 static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                                 inner_path.c_str(),
                                 static_cast<long long>(inner_lmt.time_since_epoch().count()),
                                 inner_effective.valid() ? 1 : 0,
                                 inner_effective.modified() ? 1 : 0,
                                 raw_path.c_str(),
                                 static_cast<long long>(raw_lmt.time_since_epoch().count()),
                                 raw_valid ? 1 : 0,
                                 raw_modified ? 1 : 0,
                                 has_outer_entry ? 1 : 0,
                                 delta_s.c_str(),
                                 value_s.c_str(),
                                 raw_delta_s.c_str(),
                                 raw_value_s.c_str());
                }

                if (ref_output) {
                    // Python map-node semantics for REF outputs: keyed entries carry
                    // the inner REF payload itself (including empty/sentinel refs),
                    // not a reference to the inner node's REF output wrapper.
                    if (inner_raw.valid()) {
                        auto outer_key = outer.create(key);
                        const bool outer_was_valid = outer_key.valid();
                        const bool outer_needs_init = !outer_was_valid || output_init_pending || force_emit;
                        if (inner_raw.modified() || outer_needs_init) {
                            ViewData dst_vd = outer_key.as_ts_view().view_data();
                            apply_ref_payload(dst_vd, resolve_ref_payload_from_view(inner_raw.view_data()), node_time(*this));
                            if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                                pending_keys_.erase(pending_it);
                            }
                            if (auto emit_it = force_emit_keys_.find(key_value); emit_it != force_emit_keys_.end()) {
                                force_emit_keys_.erase(emit_it);
                            }
                        }
                    }
                } else if (inner_effective.valid()) {
                    auto outer_key = outer.create(key);
                    const bool outer_was_valid = outer_key.valid();
                    const bool outer_needs_init = !outer_was_valid || output_init_pending || force_emit;
                    if (inner_effective.modified()) {
                        // Python parity: keyed map outputs copy full nested output
                        // state for evaluated keys, which preserves removals when
                        // branch outputs switch shape (for example switch_ resets).
                        ViewData dst_vd = outer_key.as_ts_view().view_data();
                        ViewData src_vd = inner_effective.view_data();
                        copy_view_data_value(dst_vd, src_vd, node_time(*this));
                        if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                            pending_keys_.erase(pending_it);
                        }
                        if (auto emit_it = force_emit_keys_.find(key_value); emit_it != force_emit_keys_.end()) {
                            force_emit_keys_.erase(emit_it);
                        }
                    } else if (outer_needs_init) {
                        ViewData dst_vd = outer_key.as_ts_view().view_data();
                        ViewData src_vd = inner_effective.view_data();
                        copy_view_data_value(dst_vd, src_vd, node_time(*this));
                        if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                            pending_keys_.erase(pending_it);
                        }
                        if (auto emit_it = force_emit_keys_.find(key_value); emit_it != force_emit_keys_.end()) {
                            force_emit_keys_.erase(emit_it);
                        }
                    }
                } else if (has_outer_entry && inner_effective.modified()) {
                    // Python parity: keyed map outputs keep key membership stable for
                    // active graphs and only invalidate the keyed child when the nested
                    // output becomes invalid.
                    auto outer_key = outer.at_key(key);
                    if (outer_key) {
                        outer_key.invalidate();
                    }
                }

                if (debug_tsd_map_copy) {
                    std::string out_delta{"<none>"};
                    std::string out_value{"<none>"};
                    try {
                        out_delta = nb::cast<std::string>(nb::repr(outer.delta_to_python()));
                    } catch (...) {}
                    try {
                        out_value = nb::cast<std::string>(nb::repr(outer.to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map_copy] key=%s post now=%lld outer_delta=%s outer_value=%s\n",
                                 key_repr(key, key_type_meta_).c_str(),
                                 static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                                 out_delta.c_str(),
                                 out_value.c_str());
                }
            }
        }

        auto next = nested->evaluation_engine_clock()->next_scheduled_evaluation_time();
        if (auto *nec = dynamic_cast<NestedEngineEvaluationClock *>(nested->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        return next;
    }

    void TsdMapNode::notify_graph_input_nodes(graph_s_ptr &graph, engine_time_t modified_time) {
        if (!graph) {
            return;
        }
        for (const auto &[_, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            if (node) {
                node->notify(modified_time);
            }
        }
    }

    void TsdMapNode::un_wire_graph(const value::View &key, graph_s_ptr &graph) {
        const bool debug_tsd_map = debug_tsd_map_enabled();
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_) {
                continue;
            }

            auto node = graph->nodes()[node_ndx];
            auto inner_ts = hgraph::node_inner_ts_input(*node, false);
            if (!inner_ts) {
                continue;
            }

            if (inner_ts.active()) {
                inner_ts.make_passive();
            }
            inner_ts.unbind();
        }

        if (output_node_id_ >= 0) {
            auto out = tsd_output();
            if (out) {
                const bool removed = out.remove(key);
                if (debug_tsd_map) {
                    std::fprintf(stderr,
                                 "[tsd_map]  un_wire remove key=%s removed=%d out_size=%zu out_valid=%d\n",
                                 key_repr(key, key_type_meta_).c_str(),
                                 removed ? 1 : 0,
                                 out.count(),
                                 out.valid() ? 1 : 0);
                }
            }
        }
    }

    TSView TsdMapNode::resolve_multiplexed_outer_value(const std::string& arg,
                                                       const value::View& key,
                                                       const TSInputView& outer_arg,
                                                       const TSInputView& inner_ts,
                                                       bool* used_local_fallback,
                                                       bool* has_outer_key,
                                                       bool* outer_key_valid,
                                                       int* stage_id) {
        return hgraph::resolve_keyed_view_with_delta_fallback(
            key,
            outer_arg,
            inner_ts,
            key_type_meta_,
            [](const TSInputView& outer_input, const value::View& outer_key) {
                return resolve_outer_key_view(outer_input.as_ts_view(), outer_key);
            },
            [&]() -> key_value_map_type& { return local_input_values_[arg]; },
            has_outer_key,
            outer_key_valid,
            used_local_fallback,
            nullptr,
            stage_id);
    }

    void TsdMapNode::wire_graph(const value::View &key, graph_s_ptr &graph) {
        const bool debug_tsd_map = debug_tsd_map_enabled();
        auto outer_root = input();
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;

        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            node->notify();

            if (debug_tsd_map) {
                auto node_root_dbg = node->input();
                std::string root_path_dbg = node_root_dbg ? node_root_dbg.short_path().to_string() : "<none>";
                std::fprintf(stderr,
                             "[tsd_map]  wire arg=%s node_ndx=%lld node_name=%s root=%s\n",
                             arg.c_str(),
                             static_cast<long long>(node->node_ndx()),
                             node->signature().name.c_str(),
                             root_path_dbg.c_str());
            }

            if (arg == key_arg_) {
                auto &key_node = dynamic_cast<PythonNode &>(*node);
                nb::object py_key = key_type_meta_ != nullptr
                                        ? key_type_meta_->ops().to_python(key.data(), key_type_meta_)
                                        : nb::none();
                nb::setattr(key_node.eval_fn(), "key", py_key);
                continue;
            }

            auto inner_ts = hgraph::node_inner_ts_input(*node, false);
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
                bool used_local_fallback = false;
                TSView outer_key_value = resolve_multiplexed_outer_value(arg, key, outer_arg, inner_ts, &used_local_fallback);
                if (debug_tsd_map) {
                    std::string outer_key_value_py{"<none>"};
                    try {
                        outer_key_value_py = nb::cast<std::string>(nb::repr(outer_key_value.to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map]  bind mux arg=%s key=%s outer_valid=%d outer_mod=%d inner_kind=%d key_view_valid=%d key_view_mod=%d fallback=%d key_view_value=%s\n",
                                 arg.c_str(),
                                 key_repr(key, key_type_meta_).c_str(),
                                 outer_arg.valid() ? 1 : 0,
                                 outer_arg.modified() ? 1 : 0,
                                 static_cast<int>(inner_ts.as_ts_view().kind()),
                                 outer_key_value.valid() ? 1 : 0,
                                 outer_key_value.modified() ? 1 : 0,
                                 used_local_fallback ? 1 : 0,
                                 outer_key_value_py.c_str());
                }
                hgraph::bind_inner_from_outer(outer_key_value, inner_ts, RefBindOrder::BoundTargetThenRefValue);
            } else {
                hgraph::bind_inner_from_outer(outer_arg.as_ts_view(), inner_ts, RefBindOrder::BoundTargetThenRefValue);
            }
        }

        if (output_node_id_ >= 0) {
            // Python map/mesh semantics materialize keyed output slots at wire time.
            // This ensures subsequent key removals tick the outer TSD even when the
            // nested graph never published a valid value for that key.
            auto out = tsd_output();
            if (out) {
                (void)out.create(key);
            }
        }
    }

    bool TsdMapNode::refresh_multiplexed_bindings(const value::View &key, graph_s_ptr &graph, bool* all_mux_inputs_valid) {
        auto outer_root = input();
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;

        bool mux_all_valid = true;
        bool refreshed = false;
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_) {
                continue;
            }
            if (multiplexed_args_.find(arg) == multiplexed_args_.end()) {
                continue;
            }

            auto node = graph->nodes()[node_ndx];
            auto inner_ts = hgraph::node_inner_ts_input(*node, false);
            if (!inner_ts) {
                continue;
            }

            if (!outer_bundle.has_value()) {
                inner_ts.unbind();
                mux_all_valid = false;
                refreshed = true;
                continue;
            }

            auto outer_arg = outer_bundle->field(arg);
            if (!outer_arg) {
                inner_ts.unbind();
                mux_all_valid = false;
                refreshed = true;
                continue;
            }

            int stage_id = 1;
            try {
                bool has_outer_key = false;
                bool outer_key_valid = false;
                TSView outer_key_value =
                    resolve_multiplexed_outer_value(arg, key, outer_arg, inner_ts, nullptr, &has_outer_key,
                                                    &outer_key_valid, &stage_id);
                const bool key_removed_this_tick = !has_outer_key && inner_ts.is_bound();
                if (key_removed_this_tick) {
                    inner_ts.unbind();
                    node->notify();
                    mux_all_valid = false;
                    refreshed = true;
                    continue;
                }

                mux_all_valid = mux_all_valid && outer_key_value.valid();
                hgraph::BindingTargetComparison binding_targets = hgraph::compare_binding_targets(inner_ts, outer_key_value);
                bool key_specific_modified = false;
                if (has_outer_key) {
                    TSView outer_ts_view = outer_arg.as_ts_view();
                    value::View outer_delta = outer_ts_view.delta_value().value();
                    if (outer_delta.valid() && outer_delta.is_tuple()) {
                        auto tuple = outer_delta.as_tuple();
                        if (tuple.size() > 0) {
                            value::View changed = tuple.at(0);
                            if (changed.valid() && changed.is_map()) {
                                key_specific_modified = changed.as_map().contains(key);
                            }
                        }
                        if (!key_specific_modified && tuple.size() > 1) {
                            value::View added = tuple.at(1);
                            if (added.valid() && added.is_set()) {
                                key_specific_modified = added.as_set().contains(key);
                            }
                        }
                        if (!key_specific_modified && tuple.size() > 2) {
                            value::View removed = tuple.at(2);
                            if (removed.valid() && removed.is_set()) {
                                key_specific_modified = removed.as_set().contains(key);
                            }
                        }
                    }
                    if (!key_specific_modified) {
                        if (auto outer_dict = outer_arg.try_as_dict(); outer_dict.has_value()) {
                            key_specific_modified = outer_dict->was_modified(key);
                        } else {
                            key_specific_modified = outer_key_value.modified();
                        }
                    }
                }
                const bool key_value_modified = has_outer_key && (!outer_key_valid || key_specific_modified);
                if (debug_tsd_map_bind_enabled()) {
                    std::fprintf(stderr,
                                 "[tsd_map_bind] refresh arg=%s key=%s inner_path=%s inner_bound=%d inner_valid=%d inner_mod=%d inner_lmt=%lld outer_valid=%d outer_mod=%d has_key=%d key_valid=%d current_target=%d desired_target=%d binding_changed=%d key_value_modified=%d removed=%d\n",
                                 arg.c_str(),
                                 key_repr(key, key_type_meta_).c_str(),
                                 inner_ts.as_ts_view().short_path().to_string().c_str(),
                                 inner_ts.is_bound() ? 1 : 0,
                                 inner_ts.as_ts_view().valid() ? 1 : 0,
                                 inner_ts.as_ts_view().modified() ? 1 : 0,
                                 static_cast<long long>(inner_ts.as_ts_view().last_modified_time().time_since_epoch().count()),
                                 outer_key_value.valid() ? 1 : 0,
                                 outer_key_value.modified() ? 1 : 0,
                                 has_outer_key ? 1 : 0,
                                 outer_key_valid ? 1 : 0,
                                 binding_targets.current_inner_target.has_value() ? 1 : 0,
                                 binding_targets.desired_outer_target.has_value() ? 1 : 0,
                                 binding_targets.binding_changed ? 1 : 0,
                                 key_value_modified ? 1 : 0,
                                 key_removed_this_tick ? 1 : 0);
                }
                stage_id = 5;
                if (!inner_ts.is_bound() || key_value_modified || binding_targets.binding_changed) {
                    hgraph::bind_inner_from_outer(outer_key_value, inner_ts, RefBindOrder::BoundTargetThenRefValue);
                }
                if (debug_tsd_map_bind_enabled()) {
                    std::fprintf(stderr,
                                 "[tsd_map_bind] refresh_post arg=%s key=%s inner_path=%s inner_bound=%d inner_valid=%d inner_mod=%d inner_lmt=%lld\n",
                                 arg.c_str(),
                                 key_repr(key, key_type_meta_).c_str(),
                                 inner_ts.as_ts_view().short_path().to_string().c_str(),
                                 inner_ts.is_bound() ? 1 : 0,
                                 inner_ts.as_ts_view().valid() ? 1 : 0,
                                 inner_ts.as_ts_view().modified() ? 1 : 0,
                                 static_cast<long long>(inner_ts.as_ts_view().last_modified_time().time_since_epoch().count()));
                }
                stage_id = 6;
                if (key_value_modified || binding_targets.binding_changed) {
                    node->notify();
                }
            } catch (const std::exception& e) {
                if (debug_tsd_map_bind_enabled()) {
                    std::fprintf(stderr,
                                 "[tsd_map_bind] refresh exception arg=%s key=%s stage_id=%d what=%s\n",
                                 arg.c_str(),
                                 key_repr(key, key_type_meta_).c_str(),
                                 stage_id,
                                 e.what());
                }
                throw;
            }
            refreshed = true;
        }
        if (all_mux_inputs_valid != nullptr) {
            *all_mux_inputs_valid = mux_all_valid;
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
