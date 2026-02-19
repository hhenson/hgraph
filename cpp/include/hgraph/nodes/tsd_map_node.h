#ifndef MAP_NODE_H
#define MAP_NODE_H

#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/value/value.h>

#include <memory>

namespace hgraph
{
    void register_tsd_map_with_nanobind(nb::module_ &m);

    struct TsdMapNode;
    using tsd_map_node_ptr = TsdMapNode*;
    using tsd_map_node_s_ptr = std::shared_ptr<TsdMapNode>;

    /**
     * Non-templated evaluation clock for TsdMapNode.
     * Stores key as Value and uses TypeMeta for Python conversion.
     */
    struct MapNestedEngineEvaluationClock : NestedEngineEvaluationClock
    {
        MapNestedEngineEvaluationClock(EngineEvaluationClock::ptr engine_evaluation_clock,
                                       value::Value key, tsd_map_node_ptr nested_node);

        void update_next_scheduled_evaluation_time(engine_time_t next_time) override;

        const value::Value& key() const { return _key; }

        [[nodiscard]] nb::object py_key() const override;

      private:
        value::Value _key;
    };

    /**
     * Non-templated TsdMapNode using Value for type-erased key storage.
     * Uses TypeMeta for Python conversions.
     */
    struct TsdMapNode : NestedNode
    {
        // Use Value for type-erased key storage
        using key_graph_map_type = std::unordered_map<value::Value, graph_s_ptr,
                                                      ValueHash, ValueEqual>;
        using key_time_map_type = std::unordered_map<value::Value, engine_time_t,
                                                     ValueHash, ValueEqual>;
        using key_set_type = std::unordered_set<value::Value, ValueHash, ValueEqual>;
        using key_value_map_type = std::unordered_map<value::Value, std::unique_ptr<TSValue>, ValueHash, ValueEqual>;
        using key_ref_snapshot_map_type = std::unordered_map<value::Value, value::Value, ValueHash, ValueEqual>;
        using arg_key_value_map_type = std::unordered_map<std::string, key_value_map_type>;

        static inline std::string KEYS_ARG = "__keys__";
        static inline std::string _KEY_ARG = "__key_arg__";

        TsdMapNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature, nb::dict scalars,
                   const TSMeta* input_meta, const TSMeta* output_meta,
                   const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                   graph_builder_s_ptr nested_graph_builder, const std::unordered_map<std::string, int64_t> &input_node_ids,
                   int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args, const std::string &key_arg);

        // Non-copyable due to move-only Value members
        TsdMapNode(const TsdMapNode&) = delete;
        TsdMapNode& operator=(const TsdMapNode&) = delete;

        // Returns Python dict for inspection
        nb::dict py_nested_graphs() const;

        void enumerate_nested_graphs(const std::function<void(const graph_s_ptr&)> &callback) const override;

        // Getter for key type metadata
        const value::TypeMeta* key_type_meta() const { return key_type_meta_; }

        VISITOR_SUPPORT()

      protected:
        void initialise() override;

        void do_start() override;

        void do_stop() override;

        void dispose() override;

        void eval() override;

        void do_eval() override {};

        virtual TSDOutputView tsd_output(engine_time_t current_time);

        void create_new_graph(const value::View &key);

        void remove_graph(const value::View &key);

        engine_time_t evaluate_graph(const value::View &key);

        void un_wire_graph(const value::View &key, graph_s_ptr &graph);

        void wire_graph(const value::View &key, graph_s_ptr &graph);

        bool refresh_multiplexed_bindings(const value::View &key, graph_s_ptr &graph, bool* all_mux_inputs_valid = nullptr);

        void mark_key_for_forced_emit(const value::View &key) {
            force_emit_keys_.insert(key.clone());
        }

        // Protected members accessible by derived classes (e.g., MeshNode)
        graph_builder_s_ptr nested_graph_builder_;
        key_graph_map_type  active_graphs_;
        key_set_type        pending_keys_;
        key_set_type        force_emit_keys_;
        int64_t             count_{1};
        const value::TypeMeta* key_type_meta_{nullptr};

      private:
        std::unordered_map<std::string, int64_t> input_node_ids_;
        int64_t                                  output_node_id_;
        std::unordered_set<std::string>          multiplexed_args_;
        std::string                              key_arg_;
        key_time_map_type                        scheduled_keys_;
        arg_key_value_map_type                   local_input_values_;
        key_value_map_type                       local_output_values_;
        key_ref_snapshot_map_type                last_ref_source_values_;
        std::string                              recordable_id_;

        friend MapNestedEngineEvaluationClock;
    };
}  // namespace hgraph

#endif  // MAP_NODE_H
