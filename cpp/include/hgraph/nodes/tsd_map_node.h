#ifndef MAP_NODE_H
#define MAP_NODE_H

#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/types/tsd.h>

namespace hgraph {
    void register_tsd_map_with_nanobind(nb::module_ & m);

    template<typename K>
    struct TsdMapNode;
    template<typename K>
    using tsd_map_node_ptr = nb::ref<TsdMapNode<K> >;

    template<typename K>
    struct MapNestedEngineEvaluationClock : NestedEngineEvaluationClock {
        MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock, K key,
                                       tsd_map_node_ptr<K> nested_node);

        void update_next_scheduled_evaluation_time(engine_time_t next_time) override;

    private:
        K _key;
    };

    template<typename K>
    struct TsdMapNode : NestedNode {
        static inline std::string KEYS_ARG = "__keys__";
        static inline std::string _KEY_ARG = "__key_arg__";

        TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                   nb::dict scalars,
                   graph_builder_ptr nested_graph_builder,
                   const std::unordered_map<std::string, int64_t> &input_node_ids,
                   int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                   const std::string &key_arg);

        std::unordered_map<K, graph_ptr> &nested_graphs();

        void enumerate_nested_graphs(const std::function<void(graph_ptr)>& callback) const override;

    protected:
        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        void do_eval() override {
        };

        virtual TimeSeriesDictOutput_T<K> &tsd_output();

        void create_new_graph(const K &key);

        void remove_graph(const K &key);

        engine_time_t evaluate_graph(const K &key);

        void un_wire_graph(const K &key, graph_ptr &graph);

        void wire_graph(const K &key, graph_ptr &graph);

        // Protected members accessible by derived classes (e.g., MeshNode)
        graph_builder_ptr nested_graph_builder_;
        std::unordered_map<K, graph_ptr> active_graphs_;
        std::unordered_set<K> pending_keys_;
        int64_t count_{1};

    private:
        std::unordered_map<std::string, int64_t> input_node_ids_;
        int64_t output_node_id_;
        std::unordered_set<std::string> multiplexed_args_;
        std::string key_arg_;
        std::unordered_map<K, engine_time_t> scheduled_keys_;
        std::string recordable_id_;

        friend MapNestedEngineEvaluationClock<K>;
    };
} // namespace hgraph

#endif  // MAP_NODE_H