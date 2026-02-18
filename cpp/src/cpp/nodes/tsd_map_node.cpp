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
#include <cstdlib>
#include <cstdio>

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

        bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
            return lhs.value_data == rhs.value_data &&
                   lhs.time_data == rhs.time_data &&
                   lhs.observer_data == rhs.observer_data &&
                   lhs.delta_data == rhs.delta_data &&
                   lhs.link_data == rhs.link_data &&
                   lhs.link_observer_registry == rhs.link_observer_registry &&
                   lhs.projection == rhs.projection &&
                   lhs.path.indices == rhs.path.indices &&
                   lhs.meta == rhs.meta;
        }

        bool ref_entry_targets_view(const TSView& ref_entry, const TSView& target) {
            if (!ref_entry || !target) {
                return false;
            }

            value::View ref_payload = ref_entry.value();
            if (!ref_payload.valid()) {
                return false;
            }

            TimeSeriesReference ref = TimeSeriesReference::make();
            try {
                ref = nb::cast<TimeSeriesReference>(ref_payload.to_python());
            } catch (...) {
                return false;
            }

            const ViewData* existing_target = ref.bound_view();
            if (existing_target == nullptr) {
                return false;
            }
            return same_view_identity(*existing_target, target.view_data());
        }

        TSView resolve_effective_view(TSView view);

        TSView resolve_outer_key_view(TSView outer_ts, const value::View &key) {
            if (!outer_ts || !key.valid()) {
                return {};
            }
            const bool debug_bind = std::getenv("HGRAPH_DEBUG_TSD_MAP_BIND") != nullptr;

            TSView child = outer_ts.child_by_key(key);
            TSView effective_child = resolve_effective_view(child);
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
                TSView bound_child = TSView(bound_target, outer_ts.current_time()).child_by_key(key);
                TSView effective_bound_child = resolve_effective_view(bound_child);
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

        nb::object remove_marker() {
            static nb::object marker = nb::none();
            if (!marker.is_valid()) {
                marker = nb::module_::import_("hgraph").attr("REMOVE");
            }
            return marker;
        }

        nb::object remove_if_exists_marker() {
            static nb::object marker = nb::none();
            if (!marker.is_valid()) {
                marker = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
            }
            return marker;
        }

        bool is_remove_marker(const nb::object& obj) {
            if (obj.is(remove_marker()) || obj.is(remove_if_exists_marker())) {
                return true;
            }

            // Python deltas can carry wrapped sentinels like Sentinel(REMOVE).
            // Unwrap a small number of `.name` hops and match by logical marker name.
            nb::object current = nb::getattr(obj, "name", nb::none());
            for (size_t depth = 0; depth < 4 && !current.is_none(); ++depth) {
                if (current.is(remove_marker()) || current.is(remove_if_exists_marker())) {
                    return true;
                }
                if (nb::isinstance<nb::str>(current)) {
                    std::string name = nb::cast<std::string>(current);
                    return name == "REMOVE" || name == "REMOVE_IF_EXISTS";
                }
                current = nb::getattr(current, "name", nb::none());
            }

            return false;
        }

        std::optional<nb::object> delta_value_for_key(const TSInputView& outer_arg,
                                                      const value::View& key,
                                                      const value::TypeMeta* key_type_meta) {
            if (!outer_arg || !key.valid() || key_type_meta == nullptr) {
                return std::nullopt;
            }

            nb::object delta_obj = outer_arg.delta_to_python();
            if (!nb::isinstance<nb::dict>(delta_obj)) {
                return std::nullopt;
            }

            nb::dict delta_dict = nb::cast<nb::dict>(delta_obj);
            for (const auto& kv : delta_dict) {
                auto key_value = key_from_python_object(nb::cast<nb::object>(kv.first), key_type_meta);
                if (!key_value.has_value()) {
                    continue;
                }
                if (!key_type_meta->ops().equals(key.data(), key_value->data(), key_type_meta)) {
                    continue;
                }

                nb::object value_obj = nb::cast<nb::object>(kv.second);
                if (value_obj.is_none() || is_remove_marker(value_obj)) {
                    return std::nullopt;
                }
                return value_obj;
            }

            return std::nullopt;
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
        const bool debug_clock = std::getenv("HGRAPH_DEBUG_TSD_MAP_CLOCK") != nullptr;
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
        const bool debug_tsd_map = std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr;
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
        local_input_values_.clear();
    }

    void TsdMapNode::dispose() {}

    void TsdMapNode::eval() {
        mark_evaluated();
        const bool debug_tsd_map = std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr;
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

        auto keys_view = node_input_field(*this, KEYS_ARG);
        if (keys_view && keys_view.modified()) {
            std::vector<value::Value> added_keys;
            std::vector<value::Value> removed_keys;
            const bool has_key_delta = extract_key_delta(keys_view, key_type_meta_, added_keys, removed_keys);
            if (has_key_delta) {
                for (const auto& key : added_keys) {
                    if (debug_tsd_map) {
                        std::fprintf(stderr, "[tsd_map]  key_delta add=%s\n", key_repr(key.view(), key_type_meta_).c_str());
                    }
                    if (active_graphs_.find(key) == active_graphs_.end()) {
                        create_new_graph(key.view());
                    } else {
                        throw std::runtime_error(
                            fmt::format("[{}] Key {} already exists in active graphs", signature().wiring_path_name,
                                        key_repr(key.view(), key_type_meta_)));
                    }
                }
                for (const auto& key : removed_keys) {
                    if (debug_tsd_map) {
                        std::fprintf(stderr, "[tsd_map]  key_delta remove=%s\n", key_repr(key.view(), key_type_meta_).c_str());
                    }
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

        // BasePythonNode::start() initializes/activates inputs from signature
        // defaults. Keep shared/non-multiplexed map arg stubs passive so keyed
        // execution is driven by outer delta routing rather than broad inner
        // root notifications.
        for (const auto &[arg, node_ndx] : input_node_ids_) {
            if (arg == key_arg_ || multiplexed_args_.find(arg) != multiplexed_args_.end()) {
                continue;
            }
            auto node = graph_->nodes()[node_ndx];
            auto root_input = node->input(node_time(*node));
            if (root_input && root_input.active()) {
                if (std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr) {
                    std::fprintf(stderr,
                                 "[tsd_map]  post-start passivate arg=%s node_ndx=%lld root=%s\n",
                                 arg.c_str(),
                                 static_cast<long long>(node->node_ndx()),
                                 root_input.short_path().to_string().c_str());
                }
                root_input.make_passive();
            } else if (std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr) {
                std::fprintf(stderr,
                             "[tsd_map]  post-start arg=%s node_ndx=%lld already passive\n",
                             arg.c_str(),
                             static_cast<long long>(node->node_ndx()));
            }
        }
        scheduled_keys_.insert_or_assign(key.clone(), last_evaluation_time());
        pending_keys_.insert(key.clone());
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
            const value::Value key_value = it->first.view().clone();
            auto nested_graph = it->second;
            active_graphs_.erase(it);
            if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                pending_keys_.erase(pending_it);
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
        const bool debug_tsd_map_copy = std::getenv("HGRAPH_DEBUG_TSD_MAP_COPY") != nullptr;

        auto &nested = it->second;
        bool all_mux_inputs_valid = true;
        refresh_multiplexed_bindings(key, nested, &all_mux_inputs_valid);
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
                TSView inner_raw = inner.as_ts_view();
                TSView inner_effective = resolve_effective_view(inner_raw);
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
                    try {
                        delta_s = nb::cast<std::string>(nb::repr(inner_effective.delta_to_python()));
                    } catch (...) {}
                    try {
                        value_s = nb::cast<std::string>(nb::repr(inner_effective.to_python()));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[tsd_map_copy] key=%s now=%lld inner_valid=%d inner_modified=%d outer_has=%d inner_delta=%s inner_value=%s\n",
                                 key_repr(key, key_type_meta_).c_str(),
                                 static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                                 inner_effective.valid() ? 1 : 0,
                                 inner_effective.modified() ? 1 : 0,
                                 has_outer_entry ? 1 : 0,
                                 delta_s.c_str(),
                                 value_s.c_str());
                }

                if (ref_output) {
                    if (inner_effective.valid()) {
                        const bool ref_target_matches =
                            has_outer_entry && outer_existing.valid() &&
                            ref_entry_targets_view(outer_existing, inner_effective);
                        const bool should_rebind_ref =
                            !has_outer_entry ||
                            !outer_existing.valid() ||
                            inner_effective.modified() ||
                            output_init_pending ||
                            !ref_target_matches;
                        if (should_rebind_ref) {
                            auto outer_key = outer.create(key);
                            outer_key.from_python(nb::cast(TimeSeriesReference::make(inner_effective.view_data())));
                            if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                                pending_keys_.erase(pending_it);
                            }
                        }
                    }
                } else if (inner_effective.valid()) {
                    auto outer_key = outer.create(key);
                    const bool outer_was_valid = outer_key.valid();
                    const bool outer_needs_init = !outer_was_valid || output_init_pending;
                    if (inner_effective.modified()) {
                        outer_key.from_python(inner_effective.delta_to_python());
                        if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                            pending_keys_.erase(pending_it);
                        }
                    } else if (outer_needs_init) {
                        outer_key.from_python(inner_effective.to_python());
                        if (auto pending_it = pending_keys_.find(key_value); pending_it != pending_keys_.end()) {
                            pending_keys_.erase(pending_it);
                        }
                    }
                } else if (has_outer_entry) {
                    outer.remove(key);
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
        const bool debug_tsd_map = std::getenv("HGRAPH_DEBUG_TSD_MAP") != nullptr;
        auto outer_root = input(node_time(*this));
        std::optional<TSBInputView> outer_bundle = outer_root ? outer_root.try_as_bundle() : std::nullopt;

        for (const auto &[arg, node_ndx] : input_node_ids_) {
            auto node = graph->nodes()[node_ndx];
            node->notify();

            if (debug_tsd_map) {
                auto node_root_dbg = node->input(node_time(*node));
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
                bool used_local_fallback = false;
                if (!outer_key_value.valid()) {
                    auto delta_value = delta_value_for_key(outer_arg, key, key_type_meta_);
                    const TSMeta* inner_meta = inner_ts.ts_meta();
                    const TSMeta* fallback_meta =
                        (inner_meta != nullptr && inner_meta->kind == TSKind::REF) ? inner_meta->element_ts() : inner_meta;
                    if (delta_value.has_value() && fallback_meta != nullptr) {
                        auto& per_arg_values = local_input_values_[arg];
                        auto it = per_arg_values.find(key);
                        if (it == per_arg_values.end()) {
                            auto [inserted_it, _] =
                                per_arg_values.emplace(key.clone(), std::make_unique<TSValue>(fallback_meta));
                            it = inserted_it;
                        }
                        TSView fallback_view = it->second->ts_view(inner_ts.current_time());
                        fallback_view.from_python(*delta_value);
                        outer_key_value = fallback_view;
                        used_local_fallback = true;
                    }
                }
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
                bind_inner_from_outer(outer_key_value, inner_ts);
            } else {
                bind_inner_from_outer(outer_arg.as_ts_view(), inner_ts);
                // Shared/non-multiplexed inputs are scheduled by outer map-node
                // delta routing. Keep inner bindings passive to avoid duplicate
                // eager notifications across all keyed graphs on container writes.
                auto node_root = node->input(node_time(*node));
                if (node_root && node_root.active()) {
                    if (debug_tsd_map) {
                        std::fprintf(stderr,
                                     "[tsd_map]  passivate non-mux arg=%s node_ndx=%lld root_active=1\n",
                                     arg.c_str(),
                                     static_cast<long long>(node->node_ndx()));
                    }
                    node_root.make_passive();
                } else if (inner_ts.active()) {
                    if (debug_tsd_map) {
                        std::fprintf(stderr,
                                     "[tsd_map]  passivate non-mux arg=%s node_ndx=%lld inner_active=1\n",
                                     arg.c_str(),
                                     static_cast<long long>(node->node_ndx()));
                    }
                    inner_ts.make_passive();
                } else if (debug_tsd_map) {
                    std::fprintf(stderr,
                                 "[tsd_map]  non-mux arg=%s node_ndx=%lld already passive\n",
                                 arg.c_str(),
                                 static_cast<long long>(node->node_ndx()));
                }
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

    bool TsdMapNode::refresh_multiplexed_bindings(const value::View &key, graph_s_ptr &graph, bool* all_mux_inputs_valid) {
        auto outer_root = input(node_time(*this));
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
            auto inner_ts = node_inner_ts_input(*node);
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
                TSView outer_key_value = resolve_outer_key_view(outer_arg.as_ts_view(), key);
                if (!outer_key_value.valid()) {
                    stage_id = 2;
                    auto delta_value = delta_value_for_key(outer_arg, key, key_type_meta_);
                    const TSMeta* inner_meta = inner_ts.ts_meta();
                    const TSMeta* fallback_meta =
                        (inner_meta != nullptr && inner_meta->kind == TSKind::REF) ? inner_meta->element_ts() : inner_meta;
                    if (delta_value.has_value() && fallback_meta != nullptr) {
                        stage_id = 3;
                        auto& per_arg_values = local_input_values_[arg];
                        auto it = per_arg_values.find(key);
                        if (it == per_arg_values.end()) {
                            auto [inserted_it, _] =
                                per_arg_values.emplace(key.clone(), std::make_unique<TSValue>(fallback_meta));
                            it = inserted_it;
                        }
                        stage_id = 4;
                        TSView fallback_view = it->second->ts_view(inner_ts.current_time());
                        fallback_view.from_python(*delta_value);
                        outer_key_value = fallback_view;
                    }
                }
                mux_all_valid = mux_all_valid && outer_key_value.valid();
                stage_id = 5;
                bind_inner_from_outer(outer_key_value, inner_ts);
                stage_id = 6;
                node->notify();
            } catch (const std::exception& e) {
                if (std::getenv("HGRAPH_DEBUG_TSD_MAP_BIND") != nullptr) {
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
