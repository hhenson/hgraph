#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

namespace hgraph {
    namespace {
        bool same_view_identity(const ViewData& lhs, const ViewData& rhs) {
            return lhs.value_data == rhs.value_data &&
                   lhs.time_data == rhs.time_data &&
                   lhs.observer_data == rhs.observer_data &&
                   lhs.delta_data == rhs.delta_data &&
                   lhs.link_data == rhs.link_data &&
                   lhs.projection == rhs.projection &&
                   lhs.path.indices == rhs.path.indices;
        }

        std::optional<ViewData> resolve_non_ref_target_view_data(const TSView& start_view) {
            ViewData cursor = start_view.view_data();
            const engine_time_t current_time = start_view.current_time();

            for (size_t depth = 0; depth < 64; ++depth) {
                TSView cursor_view(cursor, current_time);
                const TSMeta* meta = cursor_view.ts_meta();
                if (meta == nullptr) {
                    return std::nullopt;
                }
                if (meta->kind != TSKind::REF) {
                    return cursor;
                }

                value::View payload = cursor_view.value();
                if (payload.valid()) {
                    try {
                        TimeSeriesReference ref = nb::cast<TimeSeriesReference>(payload.to_python());
                        if (const ViewData* target = ref.bound_view();
                            target != nullptr && !same_view_identity(*target, cursor)) {
                            cursor = *target;
                            continue;
                        }
                    } catch (const std::exception&) {
                        // Not a TimeSeriesReference payload.
                    }
                }

                ViewData bound_target{};
                if (resolve_bound_target_view_data(cursor, bound_target) &&
                    !same_view_identity(bound_target, cursor)) {
                    cursor = std::move(bound_target);
                    continue;
                }

                return std::nullopt;
            }

            return std::nullopt;
        }

        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

        TSInputView node_input_field(Node &node, std::string_view name, std::optional<engine_time_t> current_time = std::nullopt) {
            auto root = node.input(current_time.value_or(node_time(node)));
            if (!root) {
                return {};
            }
            auto bundle_opt = root.try_as_bundle();
            if (!bundle_opt.has_value()) {
                return {};
            }
            return bundle_opt->field(name);
        }

        TSInputView node_inner_ts_input(Node &node, std::optional<engine_time_t> current_time = std::nullopt) {
            auto root = node.input(current_time.value_or(node_time(node)));
            if (!root) {
                return {};
            }

            auto bundle_opt = root.try_as_bundle();
            if (!bundle_opt.has_value()) {
                return {};
            }

            auto ts = bundle_opt->field("ts");
            if (!ts && bundle_opt->count() > 0) {
                ts = bundle_opt->at(0);
            }
            return ts;
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
                value::View ref_view = outer_any.value();
                if (ref_view.valid()) {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
                    ref.bind_input(inner_any);
                    return;
                }

                ViewData bound_target{};
                if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                    inner_any.as_ts_view().bind(TSView(bound_target, inner_any.current_time()));
                    return;
                }

                inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_any.current_time()));
                return;
            }

            ViewData bound_target{};
            if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                inner_any.as_ts_view().bind(TSView(bound_target, inner_any.current_time()));
            } else {
                inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_any.current_time()));
            }
        }

        bool collect_tsd_keys(const TSInputView &tsd_input, std::vector<value::Value> &out) {
            out.clear();
            if (!tsd_input) {
                return false;
            }

            auto tsd_opt = tsd_input.try_as_dict();
            if (!tsd_opt.has_value()) {
                return false;
            }

            value::View current = tsd_opt->as_ts_view().value();
            if (!current.valid()) {
                return true;
            }

            if (!current.is_map()) {
                return false;
            }

            auto map = current.as_map();
            out.reserve(map.size());
            for (value::View key: map.keys()) {
                out.push_back(key.clone());
            }
            return true;
        }

        TSView resolve_tsd_key_view(const TSInputView& tsd_input, const value::View& key) {
            if (!tsd_input || !key.valid()) {
                return {};
            }

            auto normalize_child = [current_time = tsd_input.current_time()](TSView child) -> TSView {
                if (!child) {
                    return {};
                }
                if (child.valid()) {
                    return child;
                }
                ViewData resolved_target{};
                if (resolve_bound_target_view_data(child.view_data(), resolved_target)) {
                    return TSView(resolved_target, current_time);
                }
                return child;
            };

            auto tsd_opt = tsd_input.try_as_dict();
            if (!tsd_opt.has_value()) {
                return {};
            }

            TSView direct_child = normalize_child(tsd_opt->as_ts_view().as_dict().at_key(key));
            if (direct_child && direct_child.valid()) {
                return direct_child;
            }

            ViewData bound_target{};
            if (resolve_bound_target_view_data(tsd_opt->as_ts_view().view_data(), bound_target)) {
                TSView bound_child = normalize_child(TSView(bound_target, tsd_input.current_time()).child_by_key(key));
                if (bound_child && bound_child.valid()) {
                    return bound_child;
                }
                if (bound_child) {
                    return bound_child;
                }
            }

            return direct_child;
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

        nb::object get_delta_member(const nb::object& delta_obj, const char* name) {
            nb::object member = nb::getattr(delta_obj, name, nb::none());
            if (member.is_none()) {
                return member;
            }
            if (PyCallable_Check(member.ptr()) != 0) {
                return member();
            }
            return member;
        }

        bool append_keys_from_python_iterable(const nb::object& iterable_obj,
                                              const value::TypeMeta* key_type_meta,
                                              std::vector<value::Value>& out) {
            if (iterable_obj.is_none() || key_type_meta == nullptr) {
                return false;
            }

            bool appended = false;
            for (auto item_h : nb::cast<nb::iterable>(iterable_obj)) {
                auto key_value = key_from_python_object(nb::cast<nb::object>(item_h), key_type_meta);
                if (!key_value.has_value()) {
                    continue;
                }
                out.push_back(std::move(*key_value));
                appended = true;
            }
            return appended;
        }

        bool collect_tsd_key_delta(const TSInputView& tsd_input,
                                   std::vector<value::Value>& added,
                                   std::vector<value::Value>& removed) {
            added.clear();
            removed.clear();
            if (!tsd_input) {
                return false;
            }

            const TSMeta* tsd_meta = tsd_input.ts_meta();
            const value::TypeMeta* key_type_meta =
                (tsd_meta != nullptr && tsd_meta->kind == TSKind::TSD) ? tsd_meta->key_type() : nullptr;
            if (key_type_meta == nullptr) {
                return false;
            }

            TSView key_set_view = tsd_input.as_ts_view();
            key_set_view.view_data().projection = ViewProjection::TSD_KEY_SET;
            nb::object delta_obj = key_set_view.delta_to_python();
            if (delta_obj.is_none()) {
                return true;
            }

            append_keys_from_python_iterable(get_delta_member(delta_obj, "added"), key_type_meta, added);
            append_keys_from_python_iterable(get_delta_member(delta_obj, "removed"), key_type_meta, removed);
            return true;
        }

        bool collect_tsd_changed_keys(const TSInputView& tsd_input, std::vector<value::Value>& out) {
            out.clear();
            if (!tsd_input) {
                return false;
            }

            const TSMeta* tsd_meta = tsd_input.ts_meta();
            const value::TypeMeta* key_type_meta =
                (tsd_meta != nullptr && tsd_meta->kind == TSKind::TSD) ? tsd_meta->key_type() : nullptr;
            if (key_type_meta == nullptr) {
                return false;
            }

            nb::object delta_obj = tsd_input.delta_to_python();
            if (!nb::isinstance<nb::dict>(delta_obj)) {
                return true;
            }

            nb::dict delta_dict = nb::cast<nb::dict>(delta_obj);
            out.reserve(delta_dict.size());
            for (auto item : delta_dict) {
                auto key_value = key_from_python_object(nb::cast<nb::object>(item.first), key_type_meta);
                if (!key_value.has_value()) {
                    continue;
                }
                out.push_back(std::move(*key_value));
            }
            return true;
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

        std::optional<nb::object> delta_value_for_key(const TSInputView& tsd_input,
                                                      const value::View& key,
                                                      const value::TypeMeta* key_type_meta) {
            if (!tsd_input || !key.valid() || key_type_meta == nullptr) {
                return std::nullopt;
            }

            nb::object delta_obj = tsd_input.delta_to_python();
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
    }  // namespace

    ReduceNode::ReduceNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)), input_node_ids_(input_node_ids),
          output_node_id_(output_node_id) {}

    std::unordered_map<int, graph_s_ptr> ReduceNode::nested_graphs() const {
        std::unordered_map<int, graph_s_ptr> graphs;
        if (nested_graph_) {
            graphs.emplace(0, nested_graph_);
        }
        return graphs;
    }

    void ReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    TSInputView ReduceNode::ts() {
        return node_input_field(*this, "ts");
    }

    TSInputView ReduceNode::zero() {
        return node_input_field(*this, "zero");
    }

    void ReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void ReduceNode::do_start() {
        if (nested_graph_ == nullptr) {
            return;
        }

        std::vector<value::Value> keys;
        const bool got_keys = collect_tsd_keys(ts(), keys);
        if (got_keys && !keys.empty()) {
            add_nodes_from_views(keys);
        } else {
            grow_tree();
        }

        start_component(*nested_graph_);
    }

    void ReduceNode::do_stop() {
        if (nested_graph_) {
            stop_component(*nested_graph_);
        }
    }

    void ReduceNode::dispose() {
        if (nested_graph_ == nullptr) {
            return;
        }
        dispose_component(*nested_graph_);
        nested_graph_ = nullptr;
    }

    void ReduceNode::eval() {
        mark_evaluated();
        if (nested_graph_ == nullptr) {
            return;
        }
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;

        TSInputView tsd = ts();
        std::vector<value::Value> current_keys;
        std::vector<value::Value> added_keys;
        std::vector<value::Value> removed_keys;
        const bool got_keys = collect_tsd_keys(tsd, current_keys);
        if (got_keys) {
            std::unordered_set<value::Value, ValueHash, ValueEqual> current_key_set;
            current_key_set.reserve(current_keys.size());
            for (const auto& key : current_keys) {
                current_key_set.insert(key.view().clone());
            }

            removed_keys.reserve(bound_node_indexes_.size());
            for (const auto& [bound_key, _] : bound_node_indexes_) {
                if (current_key_set.find(bound_key) == current_key_set.end()) {
                    removed_keys.push_back(bound_key.view().clone());
                }
            }

            added_keys.reserve(current_keys.size());
            for (const auto& key : current_keys) {
                if (bound_node_indexes_.find(key.view()) == bound_node_indexes_.end()) {
                    added_keys.push_back(key.view().clone());
                }
            }
        } else {
            collect_tsd_key_delta(tsd, added_keys, removed_keys);
        }
        remove_nodes_from_views(removed_keys);

        if (debug_reduce) {
            auto keys_to_str = [](const std::vector<value::Value>& keys) {
                std::string out{"["};
                bool first = true;
                for (const auto& key : keys) {
                    if (!first) {
                        out += ", ";
                    }
                    out += key.view().to_string();
                    first = false;
                }
                out += "]";
                return out;
            };
            std::string bound_out{"["};
            bool first = true;
            for (const auto& [key, ndx] : bound_node_indexes_) {
                if (!first) {
                    bound_out += ", ";
                }
                bound_out += key.view().to_string();
                bound_out += "->(" + std::to_string(std::get<0>(ndx)) + "," + std::to_string(std::get<1>(ndx)) + ")";
                first = false;
            }
            bound_out += "]";
            std::fprintf(stderr,
                         "[reduce] now=%lld current=%s removed=%s added=%s bound=%s free=%zu node_count=%lld\n",
                         static_cast<long long>(last_evaluation_time().time_since_epoch().count()),
                         keys_to_str(current_keys).c_str(),
                         keys_to_str(removed_keys).c_str(),
                         keys_to_str(added_keys).c_str(),
                         bound_out.c_str(),
                         free_node_indexes_.size(),
                         static_cast<long long>(node_count()));
        }
        add_nodes_from_views(added_keys);

        re_balance_nodes();

        std::vector<value::Value> changed_keys;
        collect_tsd_changed_keys(tsd, changed_keys);

        if (tsd) {
            auto tsd_opt = tsd.try_as_dict();
            if (tsd_opt.has_value()) {
                const TSMeta* tsd_meta = tsd.ts_meta();
                const value::TypeMeta* key_type_meta =
                    (tsd_meta != nullptr && tsd_meta->kind == TSKind::TSD) ? tsd_meta->key_type() : nullptr;

                for (const auto& changed_key : changed_keys) {
                    auto bound_it = bound_node_indexes_.find(changed_key.view());
                    if (bound_it == bound_node_indexes_.end()) {
                        continue;
                    }

                    const auto& key = bound_it->first;
                    const auto& ndx = bound_it->second;
                    auto [node_id, side] = ndx;
                    auto nodes = get_node(node_id);
                    if (side < 0 || side >= static_cast<int64_t>(nodes.size())) {
                        continue;
                    }

                    auto node = nodes[side];
                    auto inner_ts = node_inner_ts_input(*node, node_time(*this));
                    if (!inner_ts) {
                        continue;
                    }

                    TSView tsd_key_view = resolve_tsd_key_view(tsd, key.view());
                    const bool has_tsd_key = static_cast<bool>(tsd_key_view);
                    const bool tsd_key_valid = has_tsd_key && tsd_key_view.valid();
                    bool rebound = false;

                    if (tsd_key_valid) {
                        bool preserve_existing_ref_binding = false;
                        if (const TSMeta* key_meta = tsd_key_view.ts_meta();
                            key_meta != nullptr && key_meta->kind == TSKind::REF) {
                            value::View ref_payload = tsd_key_view.value();
                            if (!ref_payload.valid() && inner_ts.is_bound() && !tsd_key_view.modified()) {
                                bool compatible_target = false;
                                ViewData target{};
                                if (resolve_bound_target_view_data(tsd_key_view.view_data(), target)) {
                                    TSView target_view(target, tsd_key_view.current_time());
                                    const TSMeta* target_meta = target_view.ts_meta();
                                    const TSMeta* expected_meta = inner_ts.ts_meta();
                                    if (expected_meta != nullptr && expected_meta->kind == TSKind::REF) {
                                        expected_meta = expected_meta->element_ts();
                                    }
                                    if (target_meta != nullptr && expected_meta != nullptr) {
                                        compatible_target =
                                            target_meta == expected_meta ||
                                            (target_meta->kind == TSKind::REF && target_meta->element_ts() == expected_meta);
                                    }
                                }
                                preserve_existing_ref_binding = !compatible_target;
                            }
                        }

                        if (auto local_it = local_key_values_.find(key.view()); local_it != local_key_values_.end()) {
                            local_key_values_.erase(local_it);
                        }
                        if (!preserve_existing_ref_binding) {
                            bind_inner_from_outer(tsd_key_view, inner_ts);
                            rebound = true;
                        }

                        if (debug_reduce) {
                            if (preserve_existing_ref_binding) {
                                std::fprintf(stderr,
                                             "[reduce] refresh key=%s at_key=1 valid=1 fallback=0 -> preserve_empty_ref\n",
                                             key.view().to_string().c_str());
                            } else {
                                std::fprintf(stderr,
                                             "[reduce] refresh key=%s at_key=1 valid=1 fallback=0\n",
                                             key.view().to_string().c_str());
                            }
                        }
                    } else {
                        auto delta_value = delta_value_for_key(tsd, key.view(), key_type_meta);
                        const TSMeta* inner_meta = inner_ts.ts_meta();
                        const TSMeta* fallback_meta =
                            (inner_meta != nullptr && inner_meta->kind == TSKind::REF) ? inner_meta->element_ts() : inner_meta;

                        if (delta_value.has_value() && fallback_meta != nullptr) {
                            auto it = local_key_values_.find(key.view());
                            if (it == local_key_values_.end()) {
                                auto [inserted_it, _] =
                                    local_key_values_.emplace(key.view().clone(), std::make_unique<TSValue>(fallback_meta));
                                it = inserted_it;
                            }
                            TSView fallback_view = it->second->ts_view(inner_ts.current_time());
                            fallback_view.from_python(*delta_value);
                            bind_inner_from_outer(fallback_view, inner_ts);
                            rebound = true;
                            if (debug_reduce) {
                                std::string dv = "<repr-failed>";
                                try {
                                    dv = nb::cast<std::string>(nb::repr(*delta_value));
                                } catch (...) {}
                                std::fprintf(stderr,
                                             "[reduce] refresh key=%s at_key=%d valid=%d fallback=1 delta=%s\n",
                                             key.view().to_string().c_str(),
                                             has_tsd_key ? 1 : 0,
                                             tsd_key_valid ? 1 : 0,
                                             dv.c_str());
                            }
                        } else if (!has_tsd_key) {
                            inner_ts.unbind();
                            rebound = true;
                            if (debug_reduce) {
                                std::fprintf(stderr,
                                             "[reduce] refresh key=%s at_key=0 valid=0 fallback=0 -> unbind\n",
                                             key.view().to_string().c_str());
                            }
                        } else if (debug_reduce) {
                            std::fprintf(stderr,
                                         "[reduce] refresh key=%s at_key=1 valid=0 fallback=0 -> preserve\n",
                                         key.view().to_string().c_str());
                        }
                    }

                    if (rebound) {
                        if (!inner_ts.active()) {
                            inner_ts.make_active();
                        }
                        node->notify(node_time(*this));
                    }
                }
            }
        }

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        nested_graph_->evaluate_graph();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(nested_graph_->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        auto out = output(node_time(*this));
        auto l_out = last_output();
        if (!out || !l_out) {
            return;
        }

        const bool out_valid = out.valid();
        const bool l_out_valid = l_out.valid();
        const bool l_out_modified = l_out.modified();
        bool copied_from_normalized_ref = false;

        if (l_out_valid) {
            const TSMeta* l_out_meta = l_out.ts_meta();
            if (l_out_meta != nullptr && l_out_meta->kind == TSKind::REF) {
                if (auto normalized_target = resolve_non_ref_target_view_data(l_out.as_ts_view());
                    normalized_target.has_value()) {
                    TimeSeriesReference normalized_ref = TimeSeriesReference::make(*normalized_target);
                    bool same_ref = false;
                    if (out_valid) {
                        value::View out_value = out.value();
                        if (out_value.valid()) {
                            try {
                                TimeSeriesReference out_ref = nb::cast<TimeSeriesReference>(out_value.to_python());
                                same_ref = out_ref == normalized_ref;
                            } catch (...) {
                                same_ref = false;
                            }
                        }
                    }

                    if (!out_valid || !same_ref) {
                        out.from_python(nb::cast(normalized_ref));
                    }
                    copied_from_normalized_ref = true;
                }
            }
        }

        bool l_out_target_modified = false;
        if (auto normalized_target = resolve_non_ref_target_view_data(l_out.as_ts_view());
            normalized_target.has_value()) {
            l_out_target_modified = TSView(*normalized_target, node_time(*this)).modified();
        } else if (ViewData l_out_target{};
                   resolve_bound_target_view_data(l_out.as_ts_view().view_data(), l_out_target)) {
            l_out_target_modified = TSView(l_out_target, node_time(*this)).modified();
        }

        bool values_equal = false;
        if (out_valid && l_out_valid) {
            value::View out_value = out.value();
            value::View l_out_value = l_out.value();
            values_equal = out_value.valid() && l_out_value.valid() && out_value.equals(l_out_value);
        }

        if (!copied_from_normalized_ref &&
            (l_out_target_modified || (l_out_valid && !out_valid) || (l_out_valid && !values_equal))) {
            out.copy_from_output(l_out);
        }

        if (debug_reduce) {
            std::string out_py{"<none>"};
            std::string l_out_py{"<none>"};
            std::string out_target_py{"<none>"};
            std::string l_out_target_py{"<none>"};
            try {
                out_py = nb::cast<std::string>(nb::repr(out.to_python()));
            } catch (...) {}
            try {
                l_out_py = nb::cast<std::string>(nb::repr(l_out.to_python()));
            } catch (...) {}
            try {
                ViewData out_target{};
                if (resolve_bound_target_view_data(out.as_ts_view().view_data(), out_target)) {
                    out_target_py = nb::cast<std::string>(nb::repr(TSView(out_target, out.current_time()).to_python()));
                }
            } catch (...) {}
            try {
                ViewData l_out_target{};
                if (resolve_bound_target_view_data(l_out.as_ts_view().view_data(), l_out_target)) {
                    l_out_target_py = nb::cast<std::string>(nb::repr(TSView(l_out_target, l_out.current_time()).to_python()));
                }
            } catch (...) {}
            std::fprintf(stderr,
                         "[reduce] post out_valid=%d out=%s out_target=%s last_valid=%d last=%s last_target=%s equal=%d last_modified=%d target_modified=%d\n",
                         out_valid ? 1 : 0,
                         out_py.c_str(),
                         out_target_py.c_str(),
                         l_out_valid ? 1 : 0,
                         l_out_py.c_str(),
                         l_out_target_py.c_str(),
                         values_equal ? 1 : 0,
                         l_out_modified ? 1 : 0,
                         l_out_target_modified ? 1 : 0);
        }
    }

    TSOutputView ReduceNode::last_output() {
        const int64_t root_ndx = node_count() - 1;
        if (root_ndx < 0) {
            return {};
        }
        auto sub_graph = get_node(root_ndx);
        if (output_node_id_ < 0 || output_node_id_ >= static_cast<int64_t>(sub_graph.size())) {
            return {};
        }
        auto out_node = sub_graph[output_node_id_];
        if (!out_node) {
            return {};
        }
        return out_node->output(node_time(*out_node));
    }

    void ReduceNode::add_nodes_from_views(const std::vector<value::Value> &keys) {
        if (keys.empty()) {
            return;
        }
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;

        while (free_node_indexes_.size() < keys.size()) {
            grow_tree();
        }

        for (const auto &key : keys) {
            if (bound_node_indexes_.find(key.view()) != bound_node_indexes_.end()) {
                continue;
            }
            if (free_node_indexes_.empty()) {
                grow_tree();
            }
            if (free_node_indexes_.empty()) {
                break;
            }

            auto ndx = free_node_indexes_.back();
            free_node_indexes_.pop_back();
            if (debug_reduce) {
                std::fprintf(stderr,
                             "[reduce] bind key=%s -> (%lld,%lld) free_after_pop=%zu\n",
                             key.view().to_string().c_str(),
                             static_cast<long long>(std::get<0>(ndx)),
                             static_cast<long long>(std::get<1>(ndx)),
                             free_node_indexes_.size());
            }
            bind_key_to_node(key.view(), ndx);
        }

        if (debug_reduce) {
            std::string bound_out{"["};
            bool first = true;
            for (const auto& [key, ndx] : bound_node_indexes_) {
                if (!first) {
                    bound_out += ", ";
                }
                bound_out += key.view().to_string();
                bound_out += "->(" + std::to_string(std::get<0>(ndx)) + "," + std::to_string(std::get<1>(ndx)) + ")";
                first = false;
            }
            bound_out += "]";
            std::fprintf(stderr,
                         "[reduce] add_done bound=%s free=%zu node_count=%lld\n",
                         bound_out.c_str(),
                         free_node_indexes_.size(),
                         static_cast<long long>(node_count()));
        }
    }

    void ReduceNode::remove_nodes_from_views(const std::vector<value::Value> &keys) {
        if (keys.empty()) {
            return;
        }
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;

        for (const auto &key : keys) {
            auto it = bound_node_indexes_.find(key.view());
            if (it == bound_node_indexes_.end()) {
                continue;
            }

            auto ndx = it->second;
            if (debug_reduce) {
                std::fprintf(stderr,
                             "[reduce] remove key=%s ndx=(%lld,%lld)\n",
                             key.view().to_string().c_str(),
                             static_cast<long long>(std::get<0>(ndx)),
                             static_cast<long long>(std::get<1>(ndx)));
            }
            bound_node_indexes_.erase(it);

            if (!bound_node_indexes_.empty()) {
                auto max_it = std::max_element(
                    bound_node_indexes_.begin(), bound_node_indexes_.end(),
                    [](const auto &a, const auto &b) { return a.second < b.second; });

                value::Value max_key = max_it->first.view().clone();
                auto max_ndx = max_it->second;
                if (debug_reduce) {
                    std::fprintf(stderr,
                                 "[reduce]  max key=%s ndx=(%lld,%lld)\n",
                                 max_key.view().to_string().c_str(),
                                 static_cast<long long>(std::get<0>(max_ndx)),
                                 static_cast<long long>(std::get<1>(max_ndx)));
                }

                if (max_ndx > ndx) {
                    swap_node(ndx, max_ndx);
                    bound_node_indexes_[std::move(max_key)] = ndx;
                    ndx = max_ndx;
                }
            }

            free_node_indexes_.push_back(ndx);
            zero_node(ndx);
            if (auto local_it = local_key_values_.find(key.view()); local_it != local_key_values_.end()) {
                local_key_values_.erase(local_it);
            }
        }
    }

    void ReduceNode::re_balance_nodes() {
        if (node_count() > 8 && (free_node_indexes_.size() * 0.75) > bound_node_indexes_.size()) {
            shrink_tree();
        }
    }

    void ReduceNode::grow_tree() {
        if (nested_graph_ == nullptr || nested_graph_builder_ == nullptr) {
            return;
        }
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;

        int64_t count = node_count();
        int64_t end = 2 * count + 1;  // Not inclusive.
        int64_t top_layer_length = (end + 1) / 4;
        int64_t top_layer_end = std::max(count + top_layer_length, static_cast<int64_t>(1));
        int64_t last_node = end - 1;

        std::deque<int64_t> un_bound_outputs;
        std::vector<int64_t> wiring_info;

        for (int64_t i = count; i < end; ++i) {
            un_bound_outputs.push_back(i);
            nested_graph_->extend_graph(*nested_graph_builder_, true);

            if (i < top_layer_end) {
                auto ndx_lhs = std::make_tuple(i, std::get<0>(input_node_ids_));
                free_node_indexes_.push_back(ndx_lhs);
                zero_node(ndx_lhs);

                auto ndx_rhs = std::make_tuple(i, std::get<1>(input_node_ids_));
                free_node_indexes_.push_back(ndx_rhs);
                zero_node(ndx_rhs);
            } else {
                wiring_info.push_back(i);
            }
        }

        for (int64_t i : wiring_info) {
            TSOutputView left_parent;
            TSOutputView right_parent;

            if (i < last_node) {
                auto left_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                auto left_node = get_node(left_idx)[output_node_id_];
                left_parent = left_node ? left_node->output(node_time(*left_node)) : TSOutputView{};

                auto right_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                auto right_node = get_node(right_idx)[output_node_id_];
                right_parent = right_node ? right_node->output(node_time(*right_node)) : TSOutputView{};
            } else if (count > 0) {
                auto old_root_node = get_node(count - 1)[output_node_id_];
                left_parent = old_root_node ? old_root_node->output(node_time(*old_root_node)) : TSOutputView{};

                auto new_root_idx = un_bound_outputs.front();
                un_bound_outputs.pop_front();
                auto new_root_node = get_node(new_root_idx)[output_node_id_];
                right_parent = new_root_node ? new_root_node->output(node_time(*new_root_node)) : TSOutputView{};
            }

            auto sub_graph = get_node(i);
            auto lhs_node = sub_graph[std::get<0>(input_node_ids_)];
            auto rhs_node = sub_graph[std::get<1>(input_node_ids_)];

            auto lhs_input = node_inner_ts_input(*lhs_node, node_time(*this));
            auto rhs_input = node_inner_ts_input(*rhs_node, node_time(*this));

            if (lhs_input) {
                bind_inner_from_outer(left_parent ? left_parent.as_ts_view() : TSView{}, lhs_input);
                lhs_node->notify(node_time(*this));
            }
            if (rhs_input) {
                bind_inner_from_outer(right_parent ? right_parent.as_ts_view() : TSView{}, rhs_input);
                rhs_node->notify(node_time(*this));
            }
        }

        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            int64_t start_idx = count * node_size();
            int64_t end_idx = static_cast<int64_t>(nested_graph_->nodes().size());
            nested_graph_->start_subgraph(start_idx, end_idx);
        }

        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a > b; });
        if (debug_reduce) {
            std::fprintf(stderr,
                         "[reduce] grow count=%lld end=%lld top_end=%lld free=%zu node_count=%lld\n",
                         static_cast<long long>(count),
                         static_cast<long long>(end),
                         static_cast<long long>(top_layer_end),
                         free_node_indexes_.size(),
                         static_cast<long long>(node_count()));
        }
    }

    void ReduceNode::shrink_tree() {
        if (nested_graph_ == nullptr) {
            return;
        }

        int64_t capacity = static_cast<int64_t>(bound_node_indexes_.size() + free_node_indexes_.size());
        if (capacity <= 8) {
            return;
        }

        int64_t halved_capacity = capacity / 2;
        int64_t active_count = static_cast<int64_t>(bound_node_indexes_.size());
        if (halved_capacity < active_count) {
            return;
        }

        int64_t last_node = (node_count() - 1) / 2;
        int64_t start = last_node;
        nested_graph_->reduce_graph(start * node_size());

        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a < b; });

        const int64_t to_keep = halved_capacity - active_count;
        if (to_keep >= 0 && static_cast<size_t>(to_keep) < free_node_indexes_.size()) {
            free_node_indexes_.resize(static_cast<size_t>(to_keep));
        }

        std::sort(free_node_indexes_.begin(), free_node_indexes_.end(),
                  [](const auto &a, const auto &b) { return a > b; });
    }

    void ReduceNode::bind_key_to_node(const value::View &key, const std::tuple<int64_t, int64_t> &ndx) {
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;
        bound_node_indexes_[key.clone()] = ndx;

        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        if (side < 0 || side >= static_cast<int64_t>(nodes.size())) {
            return;
        }

        auto node = nodes[side];
        auto inner_ts = node_inner_ts_input(*node, node_time(*this));
        if (!inner_ts) {
            return;
        }

        auto tsd = ts();
        auto tsd_opt = tsd.try_as_dict();
        if (!tsd_opt.has_value()) {
            inner_ts.unbind();
            node->notify(node_time(*this));
            return;
        }

        TSView tsd_key_view = resolve_tsd_key_view(tsd, key);
        const bool has_tsd_key = static_cast<bool>(tsd_key_view);
        const bool tsd_key_valid = has_tsd_key && tsd_key_view.valid();

        if (has_tsd_key && tsd_key_valid) {
            if (auto local_it = local_key_values_.find(key); local_it != local_key_values_.end()) {
                local_key_values_.erase(local_it);
            }
            if (debug_reduce) {
                std::fprintf(stderr,
                             "[reduce] bind_key key=%s ndx=(%lld,%lld) at_key=1 valid=1 fallback=0\n",
                             key.to_string().c_str(),
                             static_cast<long long>(std::get<0>(ndx)),
                             static_cast<long long>(std::get<1>(ndx)));
            }
            bind_inner_from_outer(tsd_key_view, inner_ts);
        } else {
            const TSMeta* tsd_meta = tsd.ts_meta();
            const value::TypeMeta* key_type_meta =
                (tsd_meta != nullptr && tsd_meta->kind == TSKind::TSD) ? tsd_meta->key_type() : nullptr;
            auto delta_value = delta_value_for_key(tsd, key, key_type_meta);

            const TSMeta* inner_meta = inner_ts.ts_meta();
            const TSMeta* fallback_meta =
                (inner_meta != nullptr && inner_meta->kind == TSKind::REF) ? inner_meta->element_ts() : inner_meta;

            if (delta_value.has_value() && fallback_meta != nullptr) {
                auto it = local_key_values_.find(key);
                if (it == local_key_values_.end()) {
                    auto [inserted_it, _] = local_key_values_.emplace(key.clone(), std::make_unique<TSValue>(fallback_meta));
                    it = inserted_it;
                }
                TSView fallback_view = it->second->ts_view(inner_ts.current_time());
                fallback_view.from_python(*delta_value);
                if (debug_reduce) {
                    std::string dv = "<repr-failed>";
                    try {
                        dv = nb::cast<std::string>(nb::repr(*delta_value));
                    } catch (...) {}
                    std::fprintf(stderr,
                                 "[reduce] bind_key key=%s ndx=(%lld,%lld) at_key=%d valid=%d fallback=1 delta=%s\n",
                                 key.to_string().c_str(),
                                 static_cast<long long>(std::get<0>(ndx)),
                                 static_cast<long long>(std::get<1>(ndx)),
                                 has_tsd_key ? 1 : 0,
                                 tsd_key_valid ? 1 : 0,
                                 dv.c_str());
                }
                bind_inner_from_outer(fallback_view, inner_ts);
            } else if (has_tsd_key) {
                if (debug_reduce) {
                    std::fprintf(stderr,
                                 "[reduce] bind_key key=%s ndx=(%lld,%lld) at_key=1 valid=0 fallback=0\n",
                                 key.to_string().c_str(),
                                 static_cast<long long>(std::get<0>(ndx)),
                                 static_cast<long long>(std::get<1>(ndx)));
                }
                bind_inner_from_outer(tsd_key_view, inner_ts);
            } else {
                if (debug_reduce) {
                    std::fprintf(stderr,
                                 "[reduce] bind_key key=%s ndx=(%lld,%lld) at_key=0 valid=0 fallback=0 -> unbind\n",
                                 key.to_string().c_str(),
                                 static_cast<long long>(std::get<0>(ndx)),
                                 static_cast<long long>(std::get<1>(ndx)));
                }
                inner_ts.unbind();
            }
        }

        if (!inner_ts.active()) {
            inner_ts.make_active();
        }

        node->notify(node_time(*this));
    }

    void ReduceNode::zero_node(const std::tuple<int64_t, int64_t> &ndx) {
        auto [node_id, side] = ndx;
        auto nodes = get_node(node_id);
        if (side < 0 || side >= static_cast<int64_t>(nodes.size())) {
            return;
        }

        auto node = nodes[side];
        auto inner_ts = node_inner_ts_input(*node, node_time(*this));
        if (!inner_ts) {
            return;
        }

        auto zero_ref = zero();
        if (!zero_ref) {
            inner_ts.unbind();
        } else {
            bind_inner_from_outer(zero_ref.as_ts_view(), inner_ts);
            if (!inner_ts.active()) {
                inner_ts.make_active();
            }
        }

        node->notify(node_time(*this));
    }

    void ReduceNode::swap_node(const std::tuple<int64_t, int64_t> &src_ndx, const std::tuple<int64_t, int64_t> &dst_ndx) {
        const bool debug_reduce = std::getenv("HGRAPH_DEBUG_REDUCE") != nullptr;
        auto [src_node_id, src_side] = src_ndx;
        auto [dst_node_id, dst_side] = dst_ndx;

        auto src_nodes = get_node(src_node_id);
        auto dst_nodes = get_node(dst_node_id);
        if (src_side < 0 || src_side >= static_cast<int64_t>(src_nodes.size()) ||
            dst_side < 0 || dst_side >= static_cast<int64_t>(dst_nodes.size())) {
            return;
        }

        auto src_node = src_nodes[src_side];
        auto dst_node = dst_nodes[dst_side];
        if (!src_node || !dst_node) {
            return;
        }

        auto src_input = node_inner_ts_input(*src_node, node_time(*this));
        auto dst_input = node_inner_ts_input(*dst_node, node_time(*this));
        if (!src_input || !dst_input) {
            return;
        }

        if (debug_reduce) {
            std::string src_py{"<none>"};
            std::string dst_py{"<none>"};
            try { src_py = nb::cast<std::string>(nb::repr(src_input.to_python())); } catch (...) {}
            try { dst_py = nb::cast<std::string>(nb::repr(dst_input.to_python())); } catch (...) {}
            std::fprintf(stderr,
                         "[reduce] swap src=(%lld,%lld) dst=(%lld,%lld) src_val=%s dst_val=%s\n",
                         static_cast<long long>(src_node_id),
                         static_cast<long long>(src_side),
                         static_cast<long long>(dst_node_id),
                         static_cast<long long>(dst_side),
                         src_py.c_str(),
                         dst_py.c_str());
        }

        ViewData dst_target{};
        if (resolve_bound_target_view_data(dst_input.as_ts_view().view_data(), dst_target)) {
            src_input.as_ts_view().bind(TSView(dst_target, src_input.current_time()));
        } else {
            bind_inner_from_outer(dst_input.as_ts_view(), src_input);
        }
        src_node->notify(node_time(*this));
        dst_node->notify(node_time(*this));
    }

    int64_t ReduceNode::node_size() const {
        return nested_graph_builder_ ? static_cast<int64_t>(nested_graph_builder_->node_builders.size()) : 0;
    }

    int64_t ReduceNode::node_count() const {
        auto ns = node_size();
        if (nested_graph_ == nullptr || ns <= 0) {
            return 0;
        }
        return static_cast<int64_t>(nested_graph_->nodes().size()) / ns;
    }

    std::vector<node_s_ptr> ReduceNode::get_node(int64_t ndx) {
        if (nested_graph_ == nullptr) {
            return {};
        }

        auto ns = node_size();
        if (ns <= 0 || ndx < 0) {
            return {};
        }

        auto &all_nodes = nested_graph_->nodes();
        const int64_t start = ndx * ns;
        const int64_t end = start + ns;
        if (start < 0 || end > static_cast<int64_t>(all_nodes.size())) {
            return {};
        }

        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    const graph_s_ptr &ReduceNode::nested_graph() const { return nested_graph_; }

    const std::tuple<int64_t, int64_t> &ReduceNode::input_node_ids() const { return input_node_ids_; }

    int64_t ReduceNode::output_node_id() const { return output_node_id_; }

    nb::dict ReduceNode::py_bound_node_indexes() const {
        nb::dict result;
        const TSMeta *ts_meta = const_cast<ReduceNode *>(this)->ts().ts_meta();
        const value::TypeMeta *key_schema =
            (ts_meta != nullptr && ts_meta->kind == TSKind::TSD) ? ts_meta->key_type() : nullptr;

        for (const auto &[key, ndx] : bound_node_indexes_) {
            nb::object py_key = key_schema != nullptr
                                    ? key_schema->ops().to_python(key.data(), key_schema)
                                    : key.to_python();
            result[py_key] = nb::make_tuple(std::get<0>(ndx), std::get<1>(ndx));
        }

        return result;
    }

    const std::vector<std::tuple<int64_t, int64_t>> &ReduceNode::free_node_indexes() const {
        return free_node_indexes_;
    }

    void register_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<ReduceNode, NestedNode>(m, "ReduceNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict,
                          const TSMeta*, const TSMeta*, const TSMeta*, const TSMeta*,
                          graph_builder_s_ptr, const std::tuple<int64_t, int64_t> &, int64_t>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a,
                 "input_meta"_a, "output_meta"_a, "error_output_meta"_a, "recordable_state_meta"_a,
                 "nested_graph_builder"_a, "input_node_ids"_a, "output_node_id"_a)
            .def_prop_ro("nested_graph", &ReduceNode::nested_graph)
            .def_prop_ro("nested_graphs", &ReduceNode::nested_graphs)
            .def_prop_ro("ts", &ReduceNode::ts)
            .def_prop_ro("zero", &ReduceNode::zero)
            .def_prop_ro("input_node_ids", &ReduceNode::input_node_ids)
            .def_prop_ro("output_node_id", &ReduceNode::output_node_id)
            .def_prop_ro("bound_node_indexes", &ReduceNode::py_bound_node_indexes)
            .def_prop_ro("free_node_indexes", &ReduceNode::free_node_indexes);
    }
}  // namespace hgraph
