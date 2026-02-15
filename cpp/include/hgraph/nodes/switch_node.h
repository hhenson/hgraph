#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/feature_extension.h>
#include <optional>
#include <unordered_map>

namespace hgraph {
    void register_switch_node_with_nanobind(nb::module_ & m);

    /**
     * @brief Non-templated SwitchNode using Value-based key storage.
     *
     * This class stores switch keys using the Value system with TypeMeta
     * for type information. Keys are stored as Value with heterogeneous
     * lookup support via ValueHash and ValueEqual.
     *
     * Maps are shared between the builder and all node instances via shared_ptr
     * since Value doesn't support copy construction.
     */
    struct SwitchNode final : NestedNode {
        using s_ptr = std::shared_ptr<SwitchNode>;

        // Map types using Value keys with heterogeneous lookup
        using graph_builders_map = std::unordered_map<value::Value, graph_builder_s_ptr,
                                                       ValueHash, ValueEqual>;
        using input_node_ids_map = std::unordered_map<value::Value, std::unordered_map<std::string, int>,
                                                       ValueHash, ValueEqual>;
        using output_node_ids_map = std::unordered_map<value::Value, int,
                                                        ValueHash, ValueEqual>;

        // Shared pointer types for maps (shared between builder and node instances)
        using graph_builders_map_ptr = std::shared_ptr<graph_builders_map>;
        using input_node_ids_map_ptr = std::shared_ptr<input_node_ids_map>;
        using output_node_ids_map_ptr = std::shared_ptr<output_node_ids_map>;

        SwitchNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                   nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                   const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                   const value::TypeMeta* key_type,
                   graph_builders_map_ptr nested_graph_builders,
                   input_node_ids_map_ptr input_node_ids,
                   output_node_ids_map_ptr output_node_ids,
                   bool reload_on_ticked,
                   graph_builder_s_ptr default_graph_builder = nullptr,
                   std::unordered_map<std::string, int> default_input_node_ids = {},
                   int default_output_node_id = -1);

        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        [[nodiscard]] std::unordered_map<int, graph_s_ptr> nested_graphs() const;

        void enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)>& callback) const override;

        [[nodiscard]] const value::TypeMeta* key_type() const { return _key_type; }

        VISITOR_SUPPORT()

    protected:
        void do_eval() override {}

        void wire_graph(graph_s_ptr &graph);

        void unwire_graph(graph_s_ptr &graph);

        // Key comparison using TypeMeta ops
        [[nodiscard]] bool keys_equal(const value::View& a, const value::View& b) const;

        const value::TypeMeta* _key_type;
        graph_builders_map_ptr _nested_graph_builders;
        input_node_ids_map_ptr _input_node_ids;
        output_node_ids_map_ptr _output_node_ids;
        bool _reload_on_ticked;
        graph_s_ptr _active_graph{};
        graph_builder_s_ptr _active_graph_builder{};
        std::optional<value::Value> _active_key;
        int64_t _count{0};
        graph_builder_s_ptr _default_graph_builder{nullptr};
        std::unordered_map<std::string, int> _default_input_node_ids;
        int _default_output_node_id{-1};
        std::string _recordable_id;
        bool _graph_reset{false};
    };

} // namespace hgraph

#endif  // SWITCH_NODE_H
