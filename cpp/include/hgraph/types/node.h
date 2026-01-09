#ifndef NODE_H
#define NODE_H

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_input_root.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <hgraph/util/lifecycle.h>
#include <memory>

#include <ddv/visitable.h>

namespace hgraph
{
    template <typename Enum> typename std::enable_if<std::is_enum<Enum>::value, Enum>::type operator|(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(static_cast<underlying>(lhs) | static_cast<underlying>(rhs));
    }

    template <typename Enum> typename std::enable_if<std::is_enum<Enum>::value, Enum>::type operator&(Enum lhs, Enum rhs) {
        using underlying = typename std::underlying_type<Enum>::type;
        return static_cast<Enum>(static_cast<underlying>(lhs) & static_cast<underlying>(rhs));
    }

    enum class NodeTypeEnum : char8_t {
        NONE             = 0,
        SOURCE_NODE      = 1,
        PUSH_SOURCE_NODE = SOURCE_NODE | (1 << 1),
        PULL_SOURCE_NODE = SOURCE_NODE | (1 << 2),
        COMPUTE_NODE     = 1 << 3,
        SINK_NODE        = 1 << 4
    };

    void node_type_enum_py_register(nb::module_ &m);

    enum InjectableTypesEnum : int16_t {
        NONE             = 0,
        STATE            = 1,
        RECORDABLE_STATE = 1 << 1,
        SCHEDULER        = 1 << 2,
        OUTPUT           = 1 << 3,
        CLOCK            = 1 << 4,
        ENGINE_API       = 1 << 5,
        LOGGER           = 1 << 6,
        NODE             = 1 << 7,
        TRAIT            = 1 << 8
    };

    void injectable_type_enum(nb::module_ &m);

    // NodeSignature - external object created from Python, keeps nb::intrusive_base
    struct HGRAPH_EXPORT NodeSignature : nanobind::intrusive_base
    {
        using ptr   = NodeSignature *;
        using s_ptr = nanobind::ref<NodeSignature>;

        NodeSignature(std::string name, NodeTypeEnum node_type, std::vector<std::string> args,
                      std::optional<std::unordered_map<std::string, nb::object>> time_series_inputs,
                      std::optional<nb::object> time_series_output, std::optional<nb::dict> scalars, nb::object src_location,
                      std::optional<std::unordered_set<std::string>>                      active_inputs,
                      std::optional<std::unordered_set<std::string>>                      valid_inputs,
                      std::optional<std::unordered_set<std::string>>                      all_valid_inputs,
                      std::optional<std::unordered_set<std::string>>                      context_inputs,
                      std::optional<std::unordered_map<std::string, InjectableTypesEnum>> injectable_inputs, size_t injectables,
                      bool capture_exception, int64_t trace_back_depth, std::string wiring_path_name,
                      std::optional<std::string> label, bool capture_values, std::optional<std::string> record_replay_id,
                      bool has_nested_graphs);

        std::string                                                         name;
        NodeTypeEnum                                                        node_type;
        std::vector<std::string>                                            args;
        std::optional<std::unordered_map<std::string, nb::object>>          time_series_inputs;
        std::optional<nb::object>                                           time_series_output;
        std::optional<nb::dict>                                             scalars;
        nb::object                                                          src_location;
        std::optional<std::unordered_set<std::string>>                      active_inputs;
        std::optional<std::unordered_set<std::string>>                      valid_inputs;
        std::optional<std::unordered_set<std::string>>                      all_valid_inputs;
        std::optional<std::unordered_set<std::string>>                      context_inputs;
        std::optional<std::unordered_map<std::string, InjectableTypesEnum>> injectable_inputs;
        size_t                                                              injectables;
        bool                                                                capture_exception;
        int64_t                                                             trace_back_depth;
        std::string                                                         wiring_path_name;
        std::optional<std::string>                                          label;
        bool                                                                capture_values;
        std::optional<std::string>                                          record_replay_id;
        bool                                                                has_nested_graphs;

        [[nodiscard]] nb::object get_arg_type(const std::string &arg) const;

        [[nodiscard]] std::string signature() const;

        [[nodiscard]] bool uses_scheduler() const;

        [[nodiscard]] bool uses_clock() const;

        [[nodiscard]] bool uses_engine() const;

        [[nodiscard]] bool uses_state() const;

        [[nodiscard]] bool uses_recordable_state() const;

        [[nodiscard]] std::optional<std::string> recordable_state_arg() const;

        [[nodiscard]] std::optional<nb::object> recordable_state() const;

        [[nodiscard]] bool uses_output_feedback() const;

        [[nodiscard]] bool is_source_node() const;

        [[nodiscard]] bool is_push_source_node() const;

        [[nodiscard]] bool is_pull_source_node() const;

        [[nodiscard]] bool is_compute_node() const;

        [[nodiscard]] bool is_sink_node() const;

        [[nodiscard]] bool is_recordable() const;

        [[nodiscard]] nb::dict to_dict() const;

        [[nodiscard]] s_ptr copy_with(const nb::kwargs &kwargs) const;

        static void register_with_nanobind(nb::module_ &m);
    };

    // NodeScheduler - owned by Node, uses shared_ptr
    struct NodeScheduler : std::enable_shared_from_this<NodeScheduler>
    {
        using ptr   = NodeScheduler *;
        using s_ptr = std::shared_ptr<NodeScheduler>;

        explicit NodeScheduler(node_ptr node);

        [[nodiscard]] engine_time_t next_scheduled_time() const;

        [[nodiscard]] bool requires_scheduling() const;

        [[nodiscard]] bool is_scheduled() const;

        [[nodiscard]] bool is_scheduled_now() const;

        [[nodiscard]] bool has_tag(const std::string &tag) const;

        engine_time_t pop_tag(const std::string &tag);

        engine_time_t pop_tag(const std::string &tag, engine_time_t default_time);

        void schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock = false);

        void schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock = false);

        void un_schedule(const std::string &tag);

        void un_schedule();

        void reset();

        void advance();

      protected:
        void _on_alarm(engine_time_t when, std::string tag);

      private:
        // Back-pointer to owning node (raw pointer, no ownership)
        node_ptr                                        _node;
        std::set<std::pair<engine_time_t, std::string>> _scheduled_events;
        std::unordered_map<std::string, engine_time_t>  _tags;
        std::unordered_map<std::string, engine_time_t>  _alarm_tags;
        engine_time_t                                   _last_scheduled_time{MIN_DT};
    };

    using node_types =
        tp::tpack<PushQueueNode, ContextStubSourceNode, LastValuePullNode, BasePythonNode, NestedNode,
            // BasePythonNode descendands
            PythonGeneratorNode, PythonNode,
            // NestedNode descendands (all non-templated now, including SwitchNode)
            ComponentNode, TsdNonAssociativeReduceNode, NestedGraphNode, TryExceptNode,
            ReduceNode, TsdMapNode, MeshNode, SwitchNode>;
    inline constexpr auto node_types_v = node_types{};

    using NodeVisitor = decltype(tp::make_v<ddv::mux>(node_types_v))::type;

    // Node - runtime object, uses shared_ptr
    struct HGRAPH_EXPORT Node : ComponentLifeCycle,
                                Notifiable,
                                arena_enable_shared_from_this<Node>,
                                ddv::visitable<Node, NodeVisitor>
    {
        using ptr   = Node *;
        using s_ptr = std::shared_ptr<Node>;

        // Legacy constructor (for existing code during transition)
        Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, node_signature_s_ptr signature, nb::dict scalars);

        // New constructor with TSMeta - Node constructs TSValue internally
        Node(int64_t node_ndx, std::vector<int64_t> owning_graph_id, node_signature_s_ptr signature, nb::dict scalars,
             const TSMeta* input_meta, const TSMeta* output_meta,
             const TSMeta* error_output_meta = nullptr, const TSMeta* recordable_state_meta = nullptr);

        virtual void eval();

        void notify(engine_time_t modified_time) override;

        void notify();

        void notify_next_cycle();

        int64_t node_ndx() const;

        const std::vector<int64_t> &owning_graph_id() const;

        std::vector<int64_t> node_id() const;

        const NodeSignature &signature() const;

        const nb::dict &scalars() const;

        graph_ptr graph();

        graph_ptr graph() const;

        void set_graph(graph_ptr value);

        auto start_inputs() const { return _start_inputs; }

        // ========== Legacy Method Stubs ==========
        // These methods throw runtime errors - callers need to migrate to TSValue-based access
        // TODO: Remove these once all callers are migrated

        time_series_bundle_input_s_ptr& input();
        const time_series_bundle_input_s_ptr& input() const;
        void set_input(const time_series_bundle_input_s_ptr& value);
        virtual void reset_input(const time_series_bundle_input_s_ptr& value);

        time_series_output_s_ptr& output();
        void set_output(const time_series_output_s_ptr& value);

        time_series_output_s_ptr& error_output();
        void set_error_output(const time_series_output_s_ptr& value);

        time_series_bundle_output_s_ptr& recordable_state();
        void set_recordable_state(const time_series_bundle_output_s_ptr& value);

        bool has_recordable_state() const;

        NodeScheduler::s_ptr &scheduler();

        bool has_scheduler() const;

        void unset_scheduler();

        // ========== TSInputRoot/TSValue Access Methods ==========
        // These provide type-safe access to the new TSInputRoot/TSValue storage

        /**
         * @brief Check if the node has TSInputRoot-based input storage.
         */
        [[nodiscard]] bool has_ts_input() const { return _ts_input.has_value(); }

        /**
         * @brief Check if the node has TSValue-based output storage.
         */
        [[nodiscard]] bool has_ts_output() const { return _ts_output.has_value(); }

        /**
         * @brief Get the TSInputRoot for input binding and navigation.
         * @return Reference to the input root
         */
        [[nodiscard]] TSInputRoot& ts_input() { return *_ts_input; }

        /**
         * @brief Get the TSInputRoot (const).
         */
        [[nodiscard]] const TSInputRoot& ts_input() const { return *_ts_input; }

        /**
         * @brief Get the TSValue for output binding.
         * @return Pointer to the output TSValue, or nullptr if no output
         */
        [[nodiscard]] TSValue* ts_output() { return _ts_output.has_value() ? &*_ts_output : nullptr; }

        /**
         * @brief Get the TSValue for output binding (const).
         */
        [[nodiscard]] const TSValue* ts_output() const { return _ts_output.has_value() ? &*_ts_output : nullptr; }

        /**
         * @brief Get the TSValue for error output binding.
         * @return Pointer to the error output TSValue, or nullptr if none
         */
        [[nodiscard]] TSValue* ts_error_output() { return _ts_error_output.has_value() ? &*_ts_error_output : nullptr; }

        /**
         * @brief Get the TSValue for error output binding (const).
         */
        [[nodiscard]] const TSValue* ts_error_output() const { return _ts_error_output.has_value() ? &*_ts_error_output : nullptr; }

        /**
         * @brief Get the TSValue for recordable state binding.
         * @return Pointer to the recordable state TSValue, or nullptr if none
         */
        [[nodiscard]] TSValue* ts_recordable_state() { return _ts_recordable_state.has_value() ? &*_ts_recordable_state : nullptr; }

        /**
         * @brief Get the TSValue for recordable state binding (const).
         */
        [[nodiscard]] const TSValue* ts_recordable_state() const { return _ts_recordable_state.has_value() ? &*_ts_recordable_state : nullptr; }

        /**
         * @brief Get a bundle view of the input (always TSB).
         * @return TSBView for accessing input fields
         */
        [[nodiscard]] TSBView input_view();

        /**
         * @brief Get a bundle view of the input (const).
         * @return TSBView for reading input fields
         */
        [[nodiscard]] TSBView input_view() const;

        /**
         * @brief Get a mutable view of the output.
         * @return TSMutableView for reading and writing output
         */
        [[nodiscard]] TSMutableView output_view();

        /**
         * @brief Get a read-only view of the output.
         * @return TSView for reading output
         */
        [[nodiscard]] TSView output_view() const;

        /**
         * @brief Get a mutable view of the error output.
         * @return TSMutableView for reading and writing error output
         */
        [[nodiscard]] TSMutableView error_output_view();

        /**
         * @brief Get a mutable view of the recordable state.
         * @return TSMutableView for reading and writing state
         */
        [[nodiscard]] TSMutableView state_view();

        // Performance optimization: provide access to cached evaluation time pointer
        [[nodiscard]] const engine_time_t *cached_evaluation_time_ptr() const { return _cached_evaluation_time_ptr; }

        friend struct Graph;
        friend struct NodeScheduler;

        void add_start_input(const time_series_reference_input_s_ptr &input);

        bool has_input() const;

        bool has_output() const;

        std::string repr() const;

        std::string str() const;

      protected:
        void start() override;

        void stop() override;

        virtual void do_start() = 0;

        virtual void do_stop() = 0;

        virtual void do_eval() = 0;

        void _initialise_inputs();

      private:
        int64_t                         _node_ndx;
        std::vector<int64_t>            _owning_graph_id;
        node_signature_s_ptr            _signature;
        nb::dict                        _scalars;
        graph_ptr                       _graph;             // back-pointer, not owned
        NodeScheduler::s_ptr            _scheduler;         // owned
        // I am not a fan of this approach to managing the start inputs, but for now keep consistent with current code base in
        // Python.
        std::vector<time_series_reference_input_s_ptr> _start_inputs;  // owned

        // Performance optimization: Cache evaluation time pointer from graph
        // Set once when graph is assigned to node, never changes
        const engine_time_t *_cached_evaluation_time_ptr{nullptr};

        // ========== New TSValue/TSInputRoot Storage ==========
        // These replace the shared_ptr-based time-series storage
        std::optional<TSInputRoot> _ts_input;        // Input with link support (always TSB)
        std::optional<TSValue> _ts_output;           // Output storage (any TS type)
        std::optional<TSValue> _ts_error_output;     // Error output path
        std::optional<TSValue> _ts_recordable_state; // Recordable state path
    };
}  // namespace hgraph

#endif  // NODE_H