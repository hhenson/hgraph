#include <hgraph/types/tss.h>
#include <hgraph/types/value/value.h>

#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
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

namespace hgraph
{
    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::PlainValue key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto &node_{*static_cast<TsdMapNode *>(node())};
        auto  let{node_.last_evaluation_time()};
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) { return; }

        auto it{node_.scheduled_keys_.find(_key.const_view())};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) {
            node_.scheduled_keys_.insert_or_assign(_key.const_view().clone(), next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<TsdMapNode*>(node());
        const auto* key_schema = node_->key_type_meta();
        return key_schema->ops->to_python(_key.data(), key_schema);
    }

    TsdMapNode::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                           nb::dict scalars,
                           const TSMeta* input_meta, const TSMeta* output_meta,
                           const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                           graph_builder_s_ptr nested_graph_builder,
                           const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                           const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars,
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    nb::dict TsdMapNode::py_nested_graphs() const {
        nb::dict result;
        for (const auto &[key, graph] : active_graphs_) {
            nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
            result[py_key] = nb::cast(graph);
        }
        return result;
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        for (const auto &[key, graph] : active_graphs_) {
            if (graph) { callback(graph); }
        }
    }

    void TsdMapNode::initialise() {
        // TODO: Convert to TSInput-based approach
        // This method needs to get the "__keys__" field from TSInputView
        // to extract key type metadata from TimeSeriesSetInput
        // For now, stub with TODO
        throw std::runtime_error("TsdMapNode::initialise needs TSInput conversion for keys access - TODO");
    }

    void TsdMapNode::do_start() {
        // Note: In Python, super().do_start() is called here, but in C++ the base Node class
        // do_start() is pure virtual and has no implementation to call.
        auto trait{graph()->traits().get_trait_or(RECORDABLE_ID_TRAIT, nb::none())};
        if (!trait.is_none()) {
            auto recordable_id{signature().record_replay_id};
            recordable_id_ = get_fq_recordable_id(graph()->traits(), recordable_id.has_value() ? recordable_id.value() : "map_");
        }
    }

    void TsdMapNode::do_stop() {
        // Collect all keys first (can't erase while iterating)
        std::vector<value::PlainValue> keys;
        keys.reserve(active_graphs_.size());
        for (const auto &[k, _] : active_graphs_) {
            keys.push_back(k.const_view().clone());
        }
        for (const auto &k : keys) {
            remove_graph(k.const_view());
        }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    void TsdMapNode::dispose() {}

    void TsdMapNode::eval() {
        mark_evaluated();

        // TODO: Convert to TSInput-based approach
        // This method needs to:
        // 1. Access the "__keys__" field from TSInputView to get the TSS
        // 2. Check modified, iterate added/removed keys
        // 3. Create/remove graphs for keys
        // For now, just process scheduled keys

        key_time_map_type scheduled_keys;
        std::swap(scheduled_keys, scheduled_keys_);

        for (const auto &[k, dt] : scheduled_keys) {
            if (dt < last_evaluation_time()) {
                nb::object py_key = key_type_meta_->ops->to_python(k.const_view().data(), key_type_meta_);
                throw std::runtime_error(
                    fmt::format("Scheduled time is in the past; last evaluation time: {}, scheduled time: {}, evaluation time: {}",
                                last_evaluation_time(), dt, graph()->evaluation_time()));
            }
            engine_time_t next_dt;
            if (dt == last_evaluation_time()) {
                next_dt = evaluate_graph(k.const_view());
            } else {
                next_dt = dt;
            }
            if (next_dt != MAX_DT && next_dt > last_evaluation_time()) {
                scheduled_keys_.insert_or_assign(k.const_view().clone(), next_dt);
                graph()->schedule_node(node_ndx(), next_dt);
            }
        }
    }

    TimeSeriesDictOutputImpl &TsdMapNode::tsd_output() {
        // TODO: Convert to TSOutput-based approach
        // This method needs to get the TSD output from TSOutputView
        throw std::runtime_error("TsdMapNode::tsd_output needs TSOutput conversion - TODO");
    }

    void TsdMapNode::create_new_graph(const value::View &key) {
        // Convert key to string for graph label
        nb::object py_key = key_type_meta_->ops->to_python(key.data(), key_type_meta_);
        std::string key_str = nb::repr(py_key).c_str();

        // Extend parent's node_id with the new instance counter
        auto child_owning_graph_id = node_id();
        child_owning_graph_id.push_back(-static_cast<int64_t>(count_++));
        auto graph_{
            nested_graph_builder_->make_instance(child_owning_graph_id, this, key_str)
        };

        active_graphs_.emplace(key.clone(), graph_);

        graph_->set_evaluation_engine(std::make_shared<NestedEvaluationEngine>(
            graph()->evaluation_engine(),
            std::make_shared<MapNestedEngineEvaluationClock>(graph()->evaluation_engine()->engine_evaluation_clock().get(),
                                                            key.clone(), this)));

        initialise_component(*graph_);

        if (!recordable_id_.empty()) {
            auto nested_recordable_id = fmt::format("{}[{}]", recordable_id_, key_str);
            set_parent_recordable_id(*graph_, nested_recordable_id);
        }

        wire_graph(key, graph_);
        start_component(*graph_);
        scheduled_keys_.emplace(key.clone(), last_evaluation_time());
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        // TODO: Convert to TSOutput-based approach for error_output access
        // Need to erase error output for the key if capture_exception is true

        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) { return; }
        auto graph = it->second;
        active_graphs_.erase(it);

        un_wire_graph(key, graph);

        auto cleanup = make_scope_exit([this, graph = graph]() {
            // Release the graph back to the builder pool (which will call dispose)
            nested_graph_builder_->release_instance(graph);
        });
        stop_component(*graph);
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::View &key) {
        auto it = active_graphs_.find(key);
        if (it == active_graphs_.end()) {
            return MAX_DT;
        }
        auto &graph = it->second;
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }

        // TODO: Convert error handling to TSOutput-based approach for error_output access
        // For now, just evaluate without capture_exception handling
        graph->evaluate_graph();

        auto next = graph->evaluation_engine_clock()->next_scheduled_evaluation_time();
        if (auto nec = dynamic_cast<NestedEngineEvaluationClock *>(graph->evaluation_engine_clock().get())) {
            nec->reset_next_scheduled_evaluation_time();
        }
        return next;
    }

    void TsdMapNode::un_wire_graph(const value::View &key, graph_s_ptr &graph) {
        // TODO: Convert to TSInput/TSOutput-based approach for cross-graph wiring
        // This method needs to:
        // 1. Access outer node inputs via TSInputView
        // 2. Unwire inner node inputs from outer inputs
        // 3. Erase output TSD key
        // Requires inner node TSInput access and re-binding semantics
        throw std::runtime_error("TsdMapNode::un_wire_graph needs TSInput/TSOutput conversion - TODO");
    }

    void TsdMapNode::wire_graph(const value::View &key, graph_s_ptr &graph) {
        // TODO: Convert to TSInput/TSOutput-based approach for cross-graph wiring
        // This method needs to:
        // 1. Access outer node inputs via TSInputView
        // 2. Wire inner node inputs from outer inputs (TSD per-key or REF binding)
        // 3. Wire inner node output to outer node's TSD output element
        // Requires inner node TSInput/TSOutput access
        throw std::runtime_error("TsdMapNode::wire_graph needs TSInput/TSOutput conversion - TODO");
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        // Register single non-templated MapNestedEngineEvaluationClock
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        // Register single non-templated TsdMapNode
        // Constructor not exposed to Python - nodes are created via builders
        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
