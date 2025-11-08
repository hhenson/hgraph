#ifndef MESH_NODE_H
#define MESH_NODE_H

#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <map>
#include <set>

namespace hgraph {
    void register_mesh_node_with_nanobind(nb::module_ & m);

    template<typename K>
    struct MeshNode;
    template<typename K>
    using mesh_node_ptr = nb::ref<MeshNode<K> >;

    template<typename K>
    struct MeshNestedEngineEvaluationClock : NestedEngineEvaluationClock {
        MeshNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, K key,
                                        mesh_node_ptr<K> nested_node);

        K key() const { return _key; }

        void update_next_scheduled_evaluation_time(engine_time_t next_time) override;

    private:
        K _key;
    };

    /**
     * C++ implementation of PythonMeshNodeImpl.
     * Extends TsdMapNode to implement mesh/dependency graph with rank-based scheduling.
     */
    template<typename K>
    struct MeshNode : TsdMapNode<K> {
        MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature, nb::dict scalars,
                 graph_builder_ptr nested_graph_builder, const std::unordered_map<std::string, int64_t> &input_node_ids,
                 int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                 const std::string &key_arg, const std::string &context_path);

        // Public wrappers for Python to manage dependencies (mirror Python API)
        bool _add_graph_dependency(const K &key, const K &depends_on) { return add_graph_dependency(key, depends_on); }
        void _remove_graph_dependency(const K &key, const K &depends_on) { remove_graph_dependency(key, depends_on); }

    protected:
        void do_start() override;

        void do_stop() override;

        void eval() override;

        TimeSeriesDictOutput_T<K> &tsd_output() override;

        void create_new_graph(const K &key, int rank = -1);

        void remove_graph(const K &key);

        void schedule_graph(const K &key, engine_time_t tm);

        bool add_graph_dependency(const K &key, const K &depends_on);

        void remove_graph_dependency(const K &key, const K &depends_on);

        bool request_re_rank(const K &key, const K &depends_on);

        void re_rank(const K &key, const K &depends_on, std::vector<K> re_rank_stack = {});

    private:
        std::string full_context_path_;
        std::map<int, engine_time_t> scheduled_ranks_;
        std::map<int, std::unordered_map<K, engine_time_t> > scheduled_keys_by_rank_;
        std::unordered_map<K, int> active_graphs_rank_;
        std::unordered_map<K, std::unordered_set<K> > active_graphs_dependencies_;
        std::vector<std::pair<K, K> > re_rank_requests_;
        std::unordered_set<K> graphs_to_remove_;
        std::optional<int> current_eval_rank_;
        std::optional<K> current_eval_graph_;
        int max_rank_{0};

        friend MeshNestedEngineEvaluationClock<K>;
    };
} // namespace hgraph

#endif  // MESH_NODE_H