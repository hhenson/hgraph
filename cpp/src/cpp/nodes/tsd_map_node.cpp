#include <hgraph/nodes/tsd_map_node.h>
#include <stdexcept>

namespace hgraph
{
    MapNestedEngineEvaluationClock::MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                                                   value::PlainValue key,
                                                                   tsd_map_node_ptr nested_node)
        : NestedEngineEvaluationClock(engine_evaluation_clock, static_cast<NestedNode *>(nested_node)),
          _key(std::move(key)) {}

    void MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        throw std::runtime_error("not implemented: MapNestedEngineEvaluationClock::update_next_scheduled_evaluation_time");
    }

    nb::object MapNestedEngineEvaluationClock::py_key() const {
        throw std::runtime_error("not implemented: MapNestedEngineEvaluationClock::py_key");
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
        throw std::runtime_error("not implemented: TsdMapNode::py_nested_graphs");
    }

    void TsdMapNode::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const {
        throw std::runtime_error("not implemented: TsdMapNode::enumerate_nested_graphs");
    }

    void TsdMapNode::initialise() {
        throw std::runtime_error("not implemented: TsdMapNode::initialise");
    }

    void TsdMapNode::do_start() {
        throw std::runtime_error("not implemented: TsdMapNode::do_start");
    }

    void TsdMapNode::do_stop() {
        throw std::runtime_error("not implemented: TsdMapNode::do_stop");
    }

    void TsdMapNode::dispose() {
        throw std::runtime_error("not implemented: TsdMapNode::dispose");
    }

    void TsdMapNode::eval() {
        throw std::runtime_error("not implemented: TsdMapNode::eval");
    }

    void* TsdMapNode::tsd_output() {
        throw std::runtime_error("not implemented: TsdMapNode::tsd_output");
    }

    void TsdMapNode::create_new_graph(const value::View &key) {
        throw std::runtime_error("not implemented: TsdMapNode::create_new_graph");
    }

    void TsdMapNode::remove_graph(const value::View &key) {
        throw std::runtime_error("not implemented: TsdMapNode::remove_graph");
    }

    engine_time_t TsdMapNode::evaluate_graph(const value::View &key) {
        throw std::runtime_error("not implemented: TsdMapNode::evaluate_graph");
    }

    void TsdMapNode::un_wire_graph(const value::View &key, graph_s_ptr &graph) {
        throw std::runtime_error("not implemented: TsdMapNode::un_wire_graph");
    }

    void TsdMapNode::wire_graph(const value::View &key, graph_s_ptr &graph) {
        throw std::runtime_error("not implemented: TsdMapNode::wire_graph");
    }

    void register_tsd_map_with_nanobind(nb::module_ &m) {
        nb::class_<MapNestedEngineEvaluationClock, NestedEngineEvaluationClock>(m, "MapNestedEngineEvaluationClock")
            .def_prop_ro("key", &MapNestedEngineEvaluationClock::py_key);

        nb::class_<TsdMapNode, NestedNode>(m, "TsdMapNode")
            .def_prop_ro("nested_graphs", &TsdMapNode::py_nested_graphs);
    }
}  // namespace hgraph
