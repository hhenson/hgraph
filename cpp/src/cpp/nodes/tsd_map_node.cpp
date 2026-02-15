#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/scope.h>

namespace hgraph
{
    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::Value key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
        auto &node_{*static_cast<TsdMapNode *>(node())};
        auto let = node_.last_evaluation_time();
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) {
            return;
        }
        auto it{node_.scheduled_keys_.find(_key.view())};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) {
            node_.scheduled_keys_.insert_or_assign(_key.view().clone(), next_time);
        }
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<TsdMapNode*>(node());
        if (const auto *key_schema = node_->key_type_meta(); key_schema != nullptr) {
            return key_schema->ops().to_python(_key.data(), key_schema);
        }
        return nb::none();
    }

    TsdMapNode::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
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
            if (!key_type_meta_) {
                continue;
            }
            nb::object py_key = key_type_meta_->ops().to_python(key.data(), key_type_meta_);
            result[py_key] = nb::cast(graph);
        }
        return result;
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        for (const auto &[_, graph] : active_graphs_) {
            if (graph) {
                callback(graph);
            }
        }
    }

    void TsdMapNode::initialise() {
        auto root = input(graph()->evaluation_time());
        if (!root) {
            key_type_meta_ = nullptr;
            return;
        }
        auto bundle = root.try_as_bundle();
        if (!bundle.has_value()) {
            key_type_meta_ = nullptr;
            return;
        }
        auto keys_view = bundle->field(KEYS_ARG);
        if (!keys_view || keys_view.ts_meta() == nullptr || keys_view.ts_meta()->kind != TSKind::TSS) {
            key_type_meta_ = nullptr;
            return;
        }
        key_type_meta_ = keys_view.ts_meta()->value_type;
    }

    void TsdMapNode::do_start() {
        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(), recordable_id.has_value() ? recordable_id.value() : "map_");
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
        throw std::runtime_error("TsdMapNode TS migration pending: legacy TimeSeriesInput/Output path removed");
    }

    TimeSeriesDictOutputImpl &TsdMapNode::tsd_output() {
        throw std::runtime_error("TsdMapNode::tsd_output unavailable in TS runtime cutover");
    }

    void TsdMapNode::create_new_graph(const value::View &) {
        throw std::runtime_error("TsdMapNode::create_new_graph pending TS migration");
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        if (auto it = active_graphs_.find(key); it != active_graphs_.end()) {
            auto graph = it->second;
            active_graphs_.erase(it);
            auto cleanup = make_scope_exit([this, graph = std::move(graph)]() {
                if (nested_graph_builder_) {
                    nested_graph_builder_->release_instance(graph);
                }
            });
            stop_component(*graph);
        }
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::View &) {
        return MAX_DT;
    }

    void TsdMapNode::un_wire_graph(const value::View &, graph_s_ptr &) {}

    void TsdMapNode::wire_graph(const value::View &, graph_s_ptr &) {
        throw std::runtime_error("TsdMapNode::wire_graph pending TS migration");
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict,
                          const TSMeta*, const TSMeta*, const TSMeta*, const TSMeta*,
                          graph_builder_s_ptr, const std::unordered_map<std::string, int64_t> &, int64_t,
                          const std::unordered_set<std::string> &, const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a,
                 "input_meta"_a, "output_meta"_a, "error_output_meta"_a, "recordable_state_meta"_a,
                 "nested_graph_builder"_a, "input_node_ids"_a, "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
