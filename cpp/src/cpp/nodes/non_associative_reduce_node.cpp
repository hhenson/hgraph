#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>

#include <optional>
#include <utility>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node& node) {
            if (const auto* et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

        TSInputView node_input_field(Node& node, std::string_view name, std::optional<engine_time_t> current_time = std::nullopt) {
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

        TSInputView node_inner_ts_input(Node& node, std::optional<engine_time_t> current_time = std::nullopt) {
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

        void bind_inner_from_outer(const TSView& outer_any, TSInputView inner_any) {
            if (!inner_any) {
                return;
            }

            if (!outer_any) {
                inner_any.unbind();
                return;
            }

            const engine_time_t* inner_time_ptr = inner_any.as_ts_view().view_data().engine_time_ptr;
            const TSMeta* outer_meta = outer_any.ts_meta();
            if (outer_meta != nullptr && outer_meta->kind == TSKind::REF) {
                value::View ref_view = outer_any.value();
                if (ref_view.valid()) {
                    TimeSeriesReference ref = nb::cast<TimeSeriesReference>(ref_view.to_python());
                    ref.bind_input(inner_any);
                    return;
                }

                ViewData bound_target{};
                if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                    inner_any.as_ts_view().bind(TSView(bound_target, inner_time_ptr));
                    return;
                }

                inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_time_ptr));
                return;
            }

            ViewData bound_target{};
            if (resolve_bound_target_view_data(outer_any.view_data(), bound_target)) {
                inner_any.as_ts_view().bind(TSView(bound_target, inner_time_ptr));
            } else {
                inner_any.as_ts_view().bind(TSView(outer_any.view_data(), inner_time_ptr));
            }
        }

        int64_t tsd_size(const TSInputView& tsd_input) {
            if (!tsd_input || !tsd_input.valid()) {
                return 0;
            }
            auto dict_opt = tsd_input.try_as_dict();
            if (!dict_opt.has_value()) {
                return 0;
            }
            return static_cast<int64_t>(dict_opt->count());
        }

        std::optional<value::Value> key_from_index(const TSInputView& tsd_input, int64_t index) {
            if (!tsd_input) {
                return std::nullopt;
            }

            const TSMeta* tsd_meta = tsd_input.ts_meta();
            const value::TypeMeta* key_type_meta =
                (tsd_meta != nullptr && tsd_meta->kind == TSKind::TSD) ? tsd_meta->key_type() : nullptr;
            if (key_type_meta == nullptr) {
                return std::nullopt;
            }

            value::Value key(key_type_meta);
            key.emplace();
            key_type_meta->ops().from_python(key.data(), nb::int_(index), key_type_meta);
            return key;
        }

        TSView resolve_tsd_index_view(const TSInputView& tsd_input, int64_t index) {
            auto key_opt = key_from_index(tsd_input, index);
            if (!key_opt.has_value()) {
                return {};
            }
            const value::View key = key_opt->view();

            const engine_time_t* input_time_ptr = tsd_input.as_ts_view().view_data().engine_time_ptr;
            auto normalize_child = [input_time_ptr](TSView child) -> TSView {
                if (!child) {
                    return {};
                }
                if (child.valid()) {
                    return child;
                }
                ViewData resolved_target{};
                if (resolve_bound_target_view_data(child.view_data(), resolved_target)) {
                    return TSView(resolved_target, input_time_ptr);
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
                TSView bound_child = normalize_child(TSView(bound_target, input_time_ptr).child_by_key(key));
                if (bound_child && bound_child.valid()) {
                    return bound_child;
                }
                if (bound_child) {
                    return bound_child;
                }
            }

            return direct_child;
        }

        bool values_equal(const value::View& lhs, const value::View& rhs) {
            return lhs.valid() && rhs.valid() && lhs.equals(rhs);
        }
    }  // namespace

    TsdNonAssociativeReduceNode::TsdNonAssociativeReduceNode(
        int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
        nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
        const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
        graph_builder_s_ptr nested_graph_builder,
        const std::tuple<int64_t, int64_t> &input_node_ids, int64_t output_node_id)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(std::move(nested_graph_builder)),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id) {}

    void TsdNonAssociativeReduceNode::initialise() {
        nested_graph_ = arena_make_shared<Graph>(std::vector<int64_t>{node_ndx()}, std::vector<node_s_ptr>{}, this, "", &graph()->traits());
        nested_graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(), std::make_shared<NestedEngineEvaluationClock>(graph()->evaluation_engine_clock().get(), this)));
        initialise_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_start() {
        if (nested_graph_ == nullptr) {
            return;
        }

        auto tsd = node_input_field(*this, "ts", node_time(*this));
        if (tsd && tsd.valid()) {
            update_changes();
            notify(node_time(*this));
        }

        start_component(*nested_graph_);
    }

    void TsdNonAssociativeReduceNode::do_stop() {
        if (nested_graph_) {
            stop_component(*nested_graph_);
        }
    }

    void TsdNonAssociativeReduceNode::dispose() {
        if (nested_graph_) {
            dispose_component(*nested_graph_);
            nested_graph_ = nullptr;
        }
    }

    void TsdNonAssociativeReduceNode::eval() {
        mark_evaluated();
        if (nested_graph_ == nullptr) {
            return;
        }

        auto tsd = node_input_field(*this, "ts", node_time(*this));
        if (tsd && tsd.modified()) {
            update_changes();
        }

        if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(nested_graph_->evaluation_engine_clock().get());
            nec != nullptr) {
            nec->reset_next_scheduled_evaluation_time();
        }
        nested_graph_->evaluate_graph();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock*>(nested_graph_->evaluation_engine_clock().get());
            nec != nullptr) {
            nec->reset_next_scheduled_evaluation_time();
        }

        bind_output();
    }

    void TsdNonAssociativeReduceNode::update_changes() {
        const int64_t sz = node_count();
        const int64_t new_size = tsd_size(node_input_field(*this, "ts", node_time(*this)));
        if (sz == new_size) {
            return;
        }
        if (sz > new_size) {
            erase_nodes_from(new_size);
        } else {
            extend_nodes_to(new_size);
        }
    }

    void TsdNonAssociativeReduceNode::extend_nodes_to(int64_t sz) {
        if (nested_graph_ == nullptr || nested_graph_builder_ == nullptr) {
            return;
        }

        const int64_t curr_size = node_count();
        auto tsd = node_input_field(*this, "ts", node_time(*this));
        auto zero = node_input_field(*this, "zero", node_time(*this));

        for (int64_t ndx = curr_size; ndx < sz; ++ndx) {
            nested_graph_->extend_graph(*nested_graph_builder_, true);

            auto new_graph = get_node(ndx);
            if (new_graph.empty()) {
                continue;
            }

            auto lhs_node = new_graph[std::get<0>(input_node_ids_)];
            auto rhs_node = new_graph[std::get<1>(input_node_ids_)];
            if (!lhs_node || !rhs_node) {
                continue;
            }

            auto lhs_input = node_inner_ts_input(*lhs_node, node_time(*this));
            auto rhs_input = node_inner_ts_input(*rhs_node, node_time(*this));

            if (lhs_input) {
                if (ndx == 0) {
                    bind_inner_from_outer(zero ? zero.as_ts_view() : TSView{}, lhs_input);
                } else {
                    auto prev_graph = get_node(ndx - 1);
                    TSOutputView lhs_parent;
                    if (!prev_graph.empty() &&
                        output_node_id_ >= 0 &&
                        output_node_id_ < static_cast<int64_t>(prev_graph.size()) &&
                        prev_graph[output_node_id_] != nullptr) {
                        lhs_parent = prev_graph[output_node_id_]->output(node_time(*prev_graph[output_node_id_]));
                    }
                    bind_inner_from_outer(lhs_parent ? lhs_parent.as_ts_view() : TSView{}, lhs_input);
                }
                if (!lhs_input.active()) {
                    lhs_input.make_active();
                }
                lhs_node->notify(node_time(*this));
            }

            if (rhs_input) {
                TSView rhs_outer = resolve_tsd_index_view(tsd, ndx);
                bind_inner_from_outer(rhs_outer, rhs_input);
                if (!rhs_input.active()) {
                    rhs_input.make_active();
                }
                rhs_node->notify(node_time(*this));
            }
        }

        if (nested_graph_->is_started() || nested_graph_->is_starting()) {
            nested_graph_->start_subgraph(curr_size * node_size(), static_cast<int64_t>(nested_graph_->nodes().size()));
        }
    }

    void TsdNonAssociativeReduceNode::erase_nodes_from(int64_t ndx) {
        if (nested_graph_ == nullptr) {
            return;
        }
        nested_graph_->reduce_graph(ndx * node_size());
    }

    void TsdNonAssociativeReduceNode::bind_output() {
        auto out = output(node_time(*this));
        if (!out) {
            return;
        }

        const TSMeta* out_meta = out.ts_meta();
        const bool output_is_ref = out_meta != nullptr && out_meta->kind == TSKind::REF;
        auto refs_equal = [](const nb::object& lhs, const nb::object& rhs) {
            try {
                TimeSeriesReference lhs_ref = nb::cast<TimeSeriesReference>(lhs);
                TimeSeriesReference rhs_ref = nb::cast<TimeSeriesReference>(rhs);
                return lhs_ref == rhs_ref;
            } catch (...) {
                return false;
            }
        };

        const int64_t nc = node_count();
        if (nc == 0) {
            auto zero = node_input_field(*this, "zero", node_time(*this));
            if (!zero) {
                return;
            }
            if (output_is_ref) {
                nb::object desired = zero.to_python();
                bool same_ref = false;
                if (out.valid()) {
                    same_ref = refs_equal(out.to_python(), desired);
                }
                if (!out.valid() || !same_ref) {
                    out.from_python(desired);
                }
            } else if (!out.valid() || !zero.valid() || !values_equal(out.value(), zero.value())) {
                out.copy_from_input(zero);
            }
            return;
        }

        auto sub_graph = get_node(nc - 1);
        if (sub_graph.empty() || output_node_id_ < 0 || output_node_id_ >= static_cast<int64_t>(sub_graph.size())) {
            return;
        }
        auto out_node = sub_graph[output_node_id_];
        if (!out_node) {
            return;
        }

        auto last_out = out_node->output(node_time(*out_node));
        if (!last_out) {
            return;
        }

        if (output_is_ref) {
            nb::object desired = last_out.to_python();
            bool same_ref = false;
            if (out.valid()) {
                same_ref = refs_equal(out.to_python(), desired);
            }
            if (!out.valid() || !same_ref) {
                out.from_python(desired);
            }
        } else if (!out.valid() || !last_out.valid() || !values_equal(out.value(), last_out.value())) {
            out.copy_from_output(last_out);
        }
    }

    nb::object TsdNonAssociativeReduceNode::last_output_value() {
        const int64_t nc = node_count();
        if (nc == 0) {
            auto zero = node_input_field(*this, "zero", node_time(*this));
            return zero ? zero.to_python() : nb::none();
        }

        auto sub_graph = get_node(nc - 1);
        if (sub_graph.empty() || output_node_id_ < 0 || output_node_id_ >= static_cast<int64_t>(sub_graph.size())) {
            return nb::none();
        }
        auto out_node = sub_graph[output_node_id_];
        if (!out_node) {
            return nb::none();
        }
        auto out = out_node->output(node_time(*out_node));
        return out ? out.to_python() : nb::none();
    }

    int64_t TsdNonAssociativeReduceNode::node_size() const {
        return nested_graph_builder_ ? static_cast<int64_t>(nested_graph_builder_->node_builders.size()) : 0;
    }

    int64_t TsdNonAssociativeReduceNode::node_count() const {
        auto ns = node_size();
        if (nested_graph_ == nullptr || ns <= 0) {
            return 0;
        }
        return static_cast<int64_t>(nested_graph_->nodes().size()) / ns;
    }

    std::vector<node_s_ptr> TsdNonAssociativeReduceNode::get_node(int64_t ndx) {
        if (nested_graph_ == nullptr) {
            return {};
        }

        const auto ns = node_size();
        if (ns <= 0 || ndx < 0) {
            return {};
        }

        auto& all_nodes = nested_graph_->nodes();
        const int64_t start = ndx * ns;
        const int64_t end = start + ns;
        if (start < 0 || end > static_cast<int64_t>(all_nodes.size())) {
            return {};
        }

        return {all_nodes.begin() + start, all_nodes.begin() + end};
    }

    std::unordered_map<int, graph_s_ptr> TsdNonAssociativeReduceNode::nested_graphs() {
        if (nested_graph_) {
            return {{0, nested_graph_}};
        }
        return {};
    }

    void TsdNonAssociativeReduceNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        if (nested_graph_) {
            callback(nested_graph_);
        }
    }

    void register_non_associative_reduce_node_with_nanobind(nb::module_ &m) {
        nb::class_<TsdNonAssociativeReduceNode, NestedNode>(m, "TsdNonAssociativeReduceNode")
            .def_prop_ro("nested_graphs", &TsdNonAssociativeReduceNode::nested_graphs);
    }
}  // namespace hgraph
