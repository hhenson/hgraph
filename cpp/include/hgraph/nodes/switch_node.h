#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include "hgraph/types/ts.h"

#include <hgraph/nodes/nested_node.h>
#include <optional>
#include <unordered_map>

namespace hgraph {
    void register_switch_node_with_nanobind(nb::module_ & m);

    template<typename K>
    struct SwitchNode;
    template<typename K>
    using switch_node_ptr = nb::ref<SwitchNode<K> >;

    template<typename K>
    struct SwitchNode : NestedNode {
        SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                   nb::dict scalars,
                   const std::unordered_map<K, graph_builder_ptr> &nested_graph_builders,
                   const std::unordered_map<K, std::unordered_map<std::string, int> > &input_node_ids,
                   const std::unordered_map<K, int> &output_node_ids, bool reload_on_ticked,
                   graph_builder_ptr default_graph_builder = nullptr,
                   const std::unordered_map<std::string, int> &default_input_node_ids = {},
                   int default_output_node_id = -1);

        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        std::unordered_map<int, graph_ptr> nested_graphs() const;

        void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const override;

    protected:
        void do_eval() override {
        }

        void wire_graph(graph_ptr &graph);

        void unwire_graph(graph_ptr &graph);

        std::unordered_map<K, graph_builder_ptr> nested_graph_builders_;
        std::unordered_map<K, std::unordered_map<std::string, int> > input_node_ids_;
        std::unordered_map<K, int> output_node_ids_;
        TimeSeriesValueInput<K> *key_ts;

        bool reload_on_ticked_;
        graph_ptr active_graph_{};
        graph_builder_ptr active_graph_builder_{};
        std::optional<K> active_key_{};
        int64_t count_{0};
        time_series_output_ptr old_output_{nullptr};
        graph_builder_ptr default_graph_builder_{nullptr};
        std::unordered_map<std::string, int> default_input_node_ids_;
        int default_output_node_id_{-1};
        std::string recordable_id_;
        bool graph_reset_{false};
    };
} // namespace hgraph

#endif  // SWITCH_NODE_H