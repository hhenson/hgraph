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
#include <hgraph/util/string_utils.h>
#include <hgraph/util/scope.h>

namespace hgraph {
    // MeshNestedEngineEvaluationClock implementation
    template<typename K>
    MeshNestedEngineEvaluationClock<K>::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock, K key,
        mesh_node_ptr<K> nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock),
                                      static_cast<NestedNode*>(nested_node)),
          _key(key) {
    }

    template<typename K>
    void MeshNestedEngineEvaluationClock<K>::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        // Cast nested_node_ptr to MeshNode<K> using dynamic_cast
        auto node = dynamic_cast<MeshNode<K> *>(_nested_node);
        if (!node) {
            return; // Safety check - should not happen
        }

        // Check if we should skip scheduling
        auto let = node->last_evaluation_time();
        if ((let != MIN_DT && let > next_time) || node->is_stopping()) { return; }

        auto rank = node->active_graphs_rank_[_key];

        // If already scheduled for current time at current rank, skip
        if (next_time == let &&
            (rank == node->current_eval_rank_ ||
             (node->current_eval_graph_.has_value() && std::equal_to<K>()(node->current_eval_graph_.value(), _key)))) {
            return;
        }

        // Check if we need to reschedule
        auto it = node->scheduled_keys_by_rank_[rank].find(_key);
        engine_time_t tm = (it != node->scheduled_keys_by_rank_[rank].end()) ? it->second : MIN_DT;

        if (tm == MIN_DT || tm > next_time || tm < node->graph()->evaluation_time()) {
            node->schedule_graph(_key, next_time);
        }

        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
    }

    template<typename K>
    MeshNode<K>::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                          nb::dict scalars,
                          graph_builder_s_ptr nested_graph_builder,
                          const std::unordered_map<std::string, int64_t> &input_node_ids,
                          int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                          const std::string &key_arg, const std::string &context_path,
                          const TimeSeriesTypeMeta* input_meta, const TimeSeriesTypeMeta* output_meta,
                          const TimeSeriesTypeMeta* error_output_meta, const TimeSeriesTypeMeta* recordable_state_meta)
        : TsdMapNode<K>(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                        std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg,
                        input_meta, output_meta, error_output_meta, recordable_state_meta) {
        // Build full context key using centralized key builder to match Python format
        full_context_path_ = keys::context_output_key(this->owning_graph_id(), context_path);
    }

    // V2 stubs - MeshNode uses V1 APIs extensively
    // TODO: Implement MeshNode for V2

    template<typename K>
    void MeshNode<K>::do_start() {
        throw std::runtime_error("MeshNode::do_start not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::do_stop() {
        if (GlobalState::has_instance()) { GlobalState::remove(full_context_path_); }
        TsdMapNode<K>::do_stop();
    }

    template<typename K>
    void MeshNode<K>::eval() {
        throw std::runtime_error("MeshNode::eval not yet implemented for V2");
    }

    template<typename K>
    TimeSeriesDictOutput_T<K> &MeshNode<K>::tsd_output() {
        throw std::runtime_error("MeshNode::tsd_output not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::create_new_graph(const K &, int) {
        throw std::runtime_error("MeshNode::create_new_graph not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::schedule_graph(const K &, engine_time_t) {
        throw std::runtime_error("MeshNode::schedule_graph not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::remove_graph(const K &key) {
        auto graph_it = this->active_graphs_.find(key);
        if (graph_it != this->active_graphs_.end()) {
            auto graph = graph_it->second;
            this->active_graphs_.erase(graph_it);
            if (graph) { stop_component(*graph); }
            scheduled_keys_by_rank_[active_graphs_rank_[key]].erase(key);
            active_graphs_rank_.erase(key);
        }
    }

    template<typename K>
    bool MeshNode<K>::add_graph_dependency(const K &, const K &) {
        throw std::runtime_error("MeshNode::add_graph_dependency not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::remove_graph_dependency(const K &, const K &) {
        throw std::runtime_error("MeshNode::remove_graph_dependency not yet implemented for V2");
    }

    template<typename K>
    bool MeshNode<K>::request_re_rank(const K &, const K &) {
        throw std::runtime_error("MeshNode::request_re_rank not yet implemented for V2");
    }

    template<typename K>
    void MeshNode<K>::re_rank(const K &, const K &, std::vector<K>) {
        throw std::runtime_error("MeshNode::re_rank not yet implemented for V2");
    }

    // Template instantiations
    template struct MeshNode<bool>;
    template struct MeshNode<int64_t>;
    template struct MeshNode<double>;
    template struct MeshNode<engine_date_t>;
    template struct MeshNode<engine_time_t>;
    template struct MeshNode<engine_time_delta_t>;
    template struct MeshNode<nb::object>;

    template struct MeshNestedEngineEvaluationClock<bool>;
    template struct MeshNestedEngineEvaluationClock<int64_t>;
    template struct MeshNestedEngineEvaluationClock<double>;
    template struct MeshNestedEngineEvaluationClock<engine_date_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_delta_t>;
    template struct MeshNestedEngineEvaluationClock<nb::object>;

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        // Register MeshNode specializations
        nb::class_<MeshNode<bool>, TsdMapNode<bool> >(m, "MeshNode_bool")
                .def("_add_graph_dependency", &MeshNode<bool>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<bool>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<int64_t>, TsdMapNode<int64_t> >(m, "MeshNode_int")
                .def("_add_graph_dependency", &MeshNode<int64_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<int64_t>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<double>, TsdMapNode<double> >(m, "MeshNode_float")
                .def("_add_graph_dependency", &MeshNode<double>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<double>::_remove_graph_dependency, "key"_a, "depends_on"_a);
        nb::class_<MeshNode<engine_date_t>, TsdMapNode<engine_date_t> >(m, "MeshNode_date")
                .def("_add_graph_dependency", &MeshNode<engine_date_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_date_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<engine_time_t>, TsdMapNode<engine_time_t> >(m, "MeshNode_date_time")
                .def("_add_graph_dependency", &MeshNode<engine_time_t>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_time_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<engine_time_delta_t>, TsdMapNode<engine_time_delta_t> >(m, "MeshNode_time_delta")
                .def("_add_graph_dependency", &MeshNode<engine_time_delta_t>::_add_graph_dependency, "key"_a,
                     "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<engine_time_delta_t>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);
        nb::class_<MeshNode<nb::object>, TsdMapNode<nb::object> >(m, "MeshNode_object")
                .def("_add_graph_dependency", &MeshNode<nb::object>::_add_graph_dependency, "key"_a, "depends_on"_a)
                .def("_remove_graph_dependency", &MeshNode<nb::object>::_remove_graph_dependency, "key"_a,
                     "depends_on"_a);

        // Register MeshNestedEngineEvaluationClock specializations with 'key' property so Python can discover mesh keys
        nb::class_<MeshNestedEngineEvaluationClock<bool>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_bool")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<bool>::key);
        nb::class_<MeshNestedEngineEvaluationClock<int64_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_int")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<int64_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<double>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_float")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<double>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_date_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_date")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_date_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_time_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_date_time")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_time_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<engine_time_delta_t>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_time_delta")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<engine_time_delta_t>::key);
        nb::class_<MeshNestedEngineEvaluationClock<nb::object>, NestedEngineEvaluationClock>(
                    m, "MeshNestedEngineEvaluationClock_object")
                .def_prop_ro("key", &MeshNestedEngineEvaluationClock<nb::object>::key);
    }
} // namespace hgraph