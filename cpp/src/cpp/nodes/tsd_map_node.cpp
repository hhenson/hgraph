#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/traits.h>
#include <hgraph/util/lifecycle.h>
#include <hgraph/util/string_utils.h>

#include <hgraph/util/scope.h>

namespace hgraph
{
    template <typename K>
    MapNestedEngineEvaluationClock<K>::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, K key,
                                                                      tsd_map_node_ptr<K> nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)), _key(key) {}

    template <typename K> void MapNestedEngineEvaluationClock<K>::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        auto &node_{*static_cast<TsdMapNode<K> *>(node())};
        auto  let{node_.last_evaluation_time()};
        if ((let != MIN_DT && let >= next_time) || node_.is_stopping()) { return; }

        auto it{node_.scheduled_keys_.find(_key)};
        if (it == node_.scheduled_keys_.end() || it->second > next_time) { node_.scheduled_keys_[_key] = next_time; }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    template <typename K>
    TsdMapNode<K>::TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                              nb::dict scalars, graph_builder_s_ptr nested_graph_builder,
                              const std::unordered_map<std::string, int64_t> &input_node_ids, int64_t output_node_id,
                              const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg,
                              const TSMeta* input_meta, const TSMeta* output_meta,
                              const TSMeta* error_output_meta, const TSMeta* recordable_state_meta)
        : NestedNode(node_ndx, owning_graph_id, signature, scalars,
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builder_(nested_graph_builder),
          input_node_ids_(input_node_ids), output_node_id_(output_node_id), multiplexed_args_(multiplexed_args), key_arg_(key_arg) {
    }

    template <typename K> std::unordered_map<K, graph_s_ptr> &TsdMapNode<K>::nested_graphs() { return active_graphs_; }

    template <typename K> void TsdMapNode<K>::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        for (const auto &[key, graph] : active_graphs_) {
            if (graph) { callback(graph); }
        }
    }

    template <typename K> void TsdMapNode<K>::initialise() {}

    // TODO: Implement TsdMapNode

    template <typename K> void TsdMapNode<K>::do_start() {
        throw std::runtime_error("TsdMapNode::do_start not yet implemented");
    }

    template <typename K> void TsdMapNode<K>::do_stop() {
        for (const auto &[k, graph] : active_graphs_) {
            if (graph) { stop_component(*graph); }
        }
        active_graphs_.clear();
        scheduled_keys_.clear();
        pending_keys_.clear();
    }

    template <typename K> void TsdMapNode<K>::dispose() {}

    template <typename K> void TsdMapNode<K>::eval() {
        throw std::runtime_error("TsdMapNode::eval not yet implemented");
    }

    template <typename K> ts::TSOutput &TsdMapNode<K>::tsd_output() {
        throw std::runtime_error("TsdMapNode::tsd_output not yet implemented");
    }

    template <typename K> void TsdMapNode<K>::create_new_graph(const K &) {
        throw std::runtime_error("TsdMapNode::create_new_graph not yet implemented");
    }

    template <typename K> void TsdMapNode<K>::remove_graph(const K &key) {
        auto it = active_graphs_.find(key);
        if (it != active_graphs_.end()) {
            if (it->second) { stop_component(*it->second); }
            active_graphs_.erase(it);
        }
    }

    template <typename K> engine_time_t TsdMapNode<K>::evaluate_graph(const K &) {
        throw std::runtime_error("TsdMapNode::evaluate_graph not yet implemented");
    }

    template <typename K> void TsdMapNode<K>::un_wire_graph(const K &, graph_s_ptr &) {
        throw std::runtime_error("TsdMapNode::un_wire_graph not yet implemented");
    }

    template <typename K> void TsdMapNode<K>::wire_graph(const K &, graph_s_ptr &) {
        throw std::runtime_error("TsdMapNode::wire_graph not yet implemented");
    }

    using TsdMapNode_bool = TsdMapNode<bool>;

    // Explicit template instantiations to ensure symbols are emitted in this TU
    template struct TsdMapNode<bool>;
    template struct TsdMapNode<int64_t>;
    template struct TsdMapNode<double>;
    template struct TsdMapNode<engine_date_t>;
    template struct TsdMapNode<engine_time_t>;
    template struct TsdMapNode<engine_time_delta_t>;
    template struct TsdMapNode<nb::object>;

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        // Register MapNestedEngineEvaluationClock specializations with 'key' property so Python can discover map_ keys
        nb::class_<MapNestedEngineEvaluationClock<bool>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_bool")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<bool>::key);
        nb::class_<MapNestedEngineEvaluationClock<int64_t>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_int")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<int64_t>::key);
        nb::class_<MapNestedEngineEvaluationClock<double>, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock_float")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<double>::key);
        nb::class_<MapNestedEngineEvaluationClock<engine_date_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_date")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<engine_date_t>::key);
        nb::class_<MapNestedEngineEvaluationClock<engine_time_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_datetime")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<engine_time_t>::key);
        nb::class_<MapNestedEngineEvaluationClock<engine_time_delta_t>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_timedelta")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<engine_time_delta_t>::key);
        nb::class_<MapNestedEngineEvaluationClock<nb::object>, NestedEngineEvaluationClock>(
            m, "MapNestedEngineEvaluationClock_object")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock<nb::object>::key);

        nb::class_<TsdMapNode<bool>, NestedNode>(m, "TsdMapNode_bool")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<bool>::nested_graphs);
        nb::class_<TsdMapNode<int64_t>, NestedNode>(m, "TsdMapNode_int")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<int64_t>::nested_graphs);
        nb::class_<TsdMapNode<double>, NestedNode>(m, "TsdMapNode_float")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<double>::nested_graphs);
        nb::class_<TsdMapNode<engine_date_t>, NestedNode>(m, "TsdMapNode_date")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<engine_date_t>::nested_graphs);
        nb::class_<TsdMapNode<engine_time_t>, NestedNode>(m, "TsdMapNode_datetime")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<engine_time_t>::nested_graphs);
        nb::class_<TsdMapNode<engine_time_delta_t>, NestedNode>(m, "TsdMapNode_timedelta")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<engine_time_delta_t>::nested_graphs);
        nb::class_<TsdMapNode<nb::object>, NestedNode>(m, "TsdMapNode_object")
            .def(nb::init<int64_t, std::vector<int64_t>, NodeSignature::s_ptr, nb::dict, graph_builder_s_ptr,
                          const std::unordered_map<std::string, int64_t> &, int64_t, const std::unordered_set<std::string> &,
                          const std::string &>(),
                 "node_ndx"_a, "owning_graph_id"_a, "signature"_a, "scalars"_a, "nested_graph_builder"_a, "input_node_ids"_a,
                 "output_node_id"_a, "multiplexed_args"_a, "key_arg"_a)
            .def_prop_ro("nested_graphs", &TsdMapNode<nb::object>::nested_graphs);
    }
}  // namespace hgraph