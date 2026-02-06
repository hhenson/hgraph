#include <hgraph/nodes/mesh_node.h>
#include <stdexcept>

namespace hgraph {
    // MeshNestedEngineEvaluationClock implementation
    MeshNestedEngineEvaluationClock::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock, value::PlainValue key,
        mesh_node_ptr nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock),
                                      static_cast<NestedNode*>(nested_node)),
          _key(std::move(key)) {
    }

    nb::object MeshNestedEngineEvaluationClock::py_key() const {
        throw std::runtime_error("not implemented: MeshNestedEngineEvaluationClock::py_key");
    }

    void MeshNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        throw std::runtime_error("not implemented: MeshNestedEngineEvaluationClock::update_next_scheduled_evaluation_time");
    }

    MeshNode::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars,
                       const TSMeta* input_meta, const TSMeta* output_meta,
                       const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                       graph_builder_s_ptr nested_graph_builder,
                       const std::unordered_map<std::string, int64_t> &input_node_ids,
                       int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                       const std::string &key_arg, const std::string &context_path)
        : TsdMapNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta,
                     std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg) {
    }

    bool MeshNode::_add_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        throw std::runtime_error("not implemented: MeshNode::_add_graph_dependency");
    }

    void MeshNode::_remove_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        throw std::runtime_error("not implemented: MeshNode::_remove_graph_dependency");
    }

    void MeshNode::do_start() {
        throw std::runtime_error("not implemented: MeshNode::do_start");
    }

    void MeshNode::do_stop() {
        throw std::runtime_error("not implemented: MeshNode::do_stop");
    }

    void MeshNode::eval() {
        throw std::runtime_error("not implemented: MeshNode::eval");
    }

    void* MeshNode::tsd_output() {
        throw std::runtime_error("not implemented: MeshNode::tsd_output");
    }

    void MeshNode::create_new_graph(const value::View &key, int rank) {
        throw std::runtime_error("not implemented: MeshNode::create_new_graph");
    }

    void MeshNode::remove_graph(const value::View &key) {
        throw std::runtime_error("not implemented: MeshNode::remove_graph");
    }

    void MeshNode::schedule_graph(const value::View &key, engine_time_t tm) {
        throw std::runtime_error("not implemented: MeshNode::schedule_graph");
    }

    bool MeshNode::add_graph_dependency(const value::View &key, const value::View &depends_on) {
        throw std::runtime_error("not implemented: MeshNode::add_graph_dependency");
    }

    void MeshNode::remove_graph_dependency(const value::View &key, const value::View &depends_on) {
        throw std::runtime_error("not implemented: MeshNode::remove_graph_dependency");
    }

    bool MeshNode::request_re_rank(const value::View &key, const value::View &depends_on) {
        throw std::runtime_error("not implemented: MeshNode::request_re_rank");
    }

    void MeshNode::re_rank(const value::View &key, const value::View &depends_on,
                           std::vector<value::PlainValue> re_rank_stack) {
        throw std::runtime_error("not implemented: MeshNode::re_rank");
    }

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        nb::class_<MeshNestedEngineEvaluationClock, NestedEngineEvaluationClock>(
            m, "MeshNestedEngineEvaluationClock")
            .def_prop_ro("key", &MeshNestedEngineEvaluationClock::py_key);

        nb::class_<MeshNode, TsdMapNode>(m, "MeshNode")
            .def("_add_graph_dependency", &MeshNode::_add_graph_dependency, "key"_a, "depends_on"_a)
            .def("_remove_graph_dependency", &MeshNode::_remove_graph_dependency, "key"_a, "depends_on"_a);
    }
} // namespace hgraph
