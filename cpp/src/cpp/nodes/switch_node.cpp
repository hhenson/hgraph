#include "hgraph/util/string_utils.h"

#include <fmt/format.h>
#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/python_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/runtime/record_replay.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {


    template<typename K>
    SwitchNode<K>::SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                              nb::dict scalars, const std::unordered_map<K, graph_builder_s_ptr> &nested_graph_builders,
                              const std::unordered_map<K, std::unordered_map<std::string, int> > &input_node_ids,
                              const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked,
                              graph_builder_s_ptr default_graph_builder,
                              const std::unordered_map<std::string, int> &default_input_node_ids,
                              int default_output_node_id,
                              const TimeSeriesTypeMeta* input_meta, const TimeSeriesTypeMeta* output_meta,
                              const TimeSeriesTypeMeta* error_output_meta, const TimeSeriesTypeMeta* recordable_state_meta)
        : NestedNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta),
          nested_graph_builders_(nested_graph_builders), input_node_ids_(input_node_ids),
          output_node_ids_(output_node_ids),
          reload_on_ticked_(reload_on_ticked), default_graph_builder_(std::move(default_graph_builder)),
          default_input_node_ids_(default_input_node_ids), default_output_node_id_(default_output_node_id) {
        // For typed keys (bool, int, etc.), the default_graph_builder is now passed as a parameter
    }

    template<typename K>
    std::unordered_map<int, graph_s_ptr> SwitchNode<K>::nested_graphs() const {
        if (active_graph_ != nullptr) { return {{static_cast<int>(count_), active_graph_}}; }
        return {};
    }

    template<typename K>
    void SwitchNode<K>::enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const {
        if (active_graph_) {
            callback(active_graph_);
        }
    }

    // V2 stubs - SwitchNode uses V1 APIs extensively
    // TODO: Implement SwitchNode for V2

    template<typename K>
    void SwitchNode<K>::initialise() {
        // Switch node doesn't create graphs upfront
    }

    template<typename K>
    void SwitchNode<K>::do_start() {
        throw std::runtime_error("SwitchNode::do_start not yet implemented for V2");
    }

    template<typename K>
    void SwitchNode<K>::do_stop() {
        if (active_graph_) { stop_component(*active_graph_); }
    }

    template<typename K>
    void SwitchNode<K>::dispose() {
        if (active_graph_) {
            active_graph_builder_->release_instance(active_graph_);
            active_graph_builder_ = nullptr;
            active_graph_ = nullptr;
        }
    }

    template<typename K>
    void SwitchNode<K>::eval() {
        throw std::runtime_error("SwitchNode::eval not yet implemented for V2");
    }

    template<typename K>
    void SwitchNode<K>::wire_graph(graph_s_ptr &) {
        throw std::runtime_error("SwitchNode::wire_graph not yet implemented for V2");
    }

    template<typename K>
    void SwitchNode<K>::unwire_graph(graph_s_ptr &) {
        throw std::runtime_error("SwitchNode::unwire_graph not yet implemented for V2");
    }

    // Template instantiations
    template struct SwitchNode<bool>;
    template struct SwitchNode<int64_t>;
    template struct SwitchNode<double>;
    template struct SwitchNode<engine_date_t>;
    template struct SwitchNode<engine_time_t>;
    template struct SwitchNode<engine_time_delta_t>;
    template struct SwitchNode<nb::object>;

    void register_switch_node_with_nanobind(nb::module_ &m) {
        nb::class_<SwitchNode<bool>, NestedNode>(m, "SwitchNode_bool")
                .def_prop_ro("nested_graphs", &SwitchNode<bool>::nested_graphs);

        nb::class_<SwitchNode<int64_t>, NestedNode>(m, "SwitchNode_int")
                .def_prop_ro("nested_graphs", &SwitchNode<int64_t>::nested_graphs);

        nb::class_<SwitchNode<double>, NestedNode>(m, "SwitchNode_float")
                .def_prop_ro("nested_graphs", &SwitchNode<double>::nested_graphs);

        nb::class_<SwitchNode<engine_date_t>, NestedNode>(m, "SwitchNode_date")
                .def_prop_ro("nested_graphs", &SwitchNode<engine_date_t>::nested_graphs);

        nb::class_<SwitchNode<engine_time_t>, NestedNode>(m, "SwitchNode_date_time")
                .def_prop_ro("nested_graphs", &SwitchNode<engine_time_t>::nested_graphs);

        nb::class_<SwitchNode<engine_time_delta_t>, NestedNode>(m, "SwitchNode_time_delta")
                .def_prop_ro("nested_graphs", &SwitchNode<engine_time_delta_t>::nested_graphs);

        nb::class_<SwitchNode<nb::object>, NestedNode>(m, "SwitchNode_object")
                .def_prop_ro("nested_graphs", &SwitchNode<nb::object>::nested_graphs);
    }
} // namespace hgraph