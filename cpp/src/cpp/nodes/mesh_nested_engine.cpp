#include <hgraph/nodes/mesh_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/util/errors.h>

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
        // Cast nested_node_ptr to MeshNode<K>
        auto node = *_nested_node->visit(cast_to_expected<MeshNode<K>*>);

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

    template struct MeshNestedEngineEvaluationClock<bool>;
    template struct MeshNestedEngineEvaluationClock<int64_t>;
    template struct MeshNestedEngineEvaluationClock<double>;
    template struct MeshNestedEngineEvaluationClock<engine_date_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_t>;
    template struct MeshNestedEngineEvaluationClock<engine_time_delta_t>;
    template struct MeshNestedEngineEvaluationClock<nb::object>;

    void register_mesh_nested_engine_with_nanobind(nb::module_ &m) {
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
