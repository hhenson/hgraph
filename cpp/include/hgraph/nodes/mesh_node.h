#ifndef MESH_NODE_H
#define MESH_NODE_H

#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <map>
#include <set>

namespace hgraph
{
    void register_mesh_node_with_nanobind(nb::module_ &m);

    struct MeshNode;
    using mesh_node_ptr = MeshNode*;
    using mesh_node_s_ptr = std::shared_ptr<MeshNode>;

    /**
     * Non-templated evaluation clock for MeshNode.
     * Stores key as Value and uses TypeMeta for Python conversion.
     */
    struct MeshNestedEngineEvaluationClock : NestedEngineEvaluationClock
    {
        MeshNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                        value::Value key, mesh_node_ptr nested_node);

        const value::Value& key() const { return _key; }

        [[nodiscard]] nb::object py_key() const override;

        void update_next_scheduled_evaluation_time(engine_time_t next_time) override;

      private:
        value::Value _key;
    };

    /**
     * C++ implementation of PythonMeshNodeImpl.
     * Extends TsdMapNode to implement mesh/dependency graph with rank-based scheduling.
     * Non-templated: uses Value for type-erased key storage.
     */
    struct MeshNode final : TsdMapNode
    {
        // Type aliases for Value-based storage
        using key_int_map_type = std::unordered_map<value::Value, int,
                                                    ValueHash, ValueEqual>;
        using key_set_map_type = std::unordered_map<value::Value, std::unordered_set<value::Value, ValueHash, ValueEqual>,
                                                    ValueHash, ValueEqual>;

        MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature, nb::dict scalars,
                 const TSMeta* input_meta, const TSMeta* output_meta,
                 const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                 graph_builder_s_ptr nested_graph_builder, const std::unordered_map<std::string, int64_t> &input_node_ids,
                 int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg,
                 const std::string &context_path);

        // Non-copyable due to move-only Value members
        MeshNode(const MeshNode&) = delete;
        MeshNode& operator=(const MeshNode&) = delete;

        // Public wrappers for Python to manage dependencies (mirror Python API)
        bool _add_graph_dependency(const nb::object &key, const nb::object &depends_on);
        void _remove_graph_dependency(const nb::object &key, const nb::object &depends_on);

        VISITOR_SUPPORT()

      protected:
        void do_start() override;

        void do_stop() override;

        void eval() override;

        TSDOutputView tsd_output(engine_time_t current_time) override;

        void create_new_graph(const value::View &key, int rank = -1);

        void remove_graph(const value::View &key);

        void schedule_graph(const value::View &key, engine_time_t tm);

        bool add_graph_dependency(const value::View &key, const value::View &depends_on);

        void remove_graph_dependency(const value::View &key, const value::View &depends_on);

        bool request_re_rank(const value::View &key, const value::View &depends_on);

        void re_rank(const value::View &key, const value::View &depends_on,
                     std::vector<value::Value> re_rank_stack = {});

      private:
        std::string                              full_context_path_;
        std::map<int, engine_time_t>             scheduled_ranks_;
        std::map<int, key_time_map_type>         scheduled_keys_by_rank_;
        key_int_map_type                         active_graphs_rank_;
        key_set_map_type                         active_graphs_dependencies_;
        std::vector<std::pair<value::Value, value::Value>> re_rank_requests_;
        key_set_type                             graphs_to_remove_;
        std::optional<int>                       current_eval_rank_;
        std::optional<value::Value>         current_eval_graph_;
        int                                      max_rank_{0};

        friend MeshNestedEngineEvaluationClock;
    };
}  // namespace hgraph

#endif  // MESH_NODE_H
