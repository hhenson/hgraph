// runtime/node.h — the type-erased runtime node: NodeTypeMetaData (schema),
// NodeOps (fn-ptr behaviour table), NodeBuilder (build-time only), and the
// owning/borrowing NodeValue/NodeView pair. Nodes are constructed into the
// graph's pre-allocated storage; behaviour dispatches through interned ops
// tables, never virtuals. Design records:
// docs/source/developer_guide/architecture.rst (+ wiring.rst for authoring).
#ifndef HGRAPH_RUNTIME_NODE_H
#define HGRAPH_RUNTIME_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/evaluation_clock.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/runtime/node_fwd.h>
#include <hgraph/runtime/node_type_ref.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class GraphValue;
    class GraphView;
    class NodeBuilder;
    class NodeValue;
    struct NodeSchedulerState;
    struct ResolutionMap;  // type_resolution.h — generic-node wiring resolution

    /** Runtime node category used by graph construction and evaluation. */
    enum class NodeKind : std::uint8_t
    {
        Compute,
        PushSource,
        PullSource,
        Sink,
        Nested,
    };

    /** Owned storage counters returned through the type-erased node API. */
    struct HGRAPH_EXPORT NodeStorageMetrics
    {
        /** Bytes reserved in the graph's fixed node allocation. */
        std::size_t static_bytes{0};
        /** Constructed nested graph slots, including a retained prior generation. */
        std::size_t nested_graph_count{0};
        /** Addressable nested graph slots across all storage banks. */
        std::size_t nested_graph_capacity{0};
        /** Stable allocation blocks backing the nested graph slots. */
        std::size_t nested_graph_blocks{0};
        /** Slot payload/index bytes attributable to constructed entries. */
        std::size_t dynamic_live_bytes{0};
        /** Slot payload/index/bitmap bytes retained at current capacity. */
        std::size_t dynamic_reserved_bytes{0};
    };

    /**
     * Interned node schema descriptor.
     *
     * The schema records the node contract only: input/output time-series
     * shapes, optional local state schema, kind, and readiness selectors. The
     * executable behaviour lives in ``NodeOps`` and its context.
     */
    struct HGRAPH_EXPORT NodeTypeMetaData
    {
        SchemaHeader header{};
        const char *display_name{nullptr};

        const TSValueTypeMetaData *input_schema{nullptr};
        const TSValueTypeMetaData *output_schema{nullptr};
        TSEndpointSchema           output_endpoint_schema{};
        const TSValueTypeMetaData *error_output_schema{nullptr};
        const TSValueTypeMetaData *recordable_state_schema{nullptr};
        const ValueTypeMetaData   *state_schema{nullptr};
        const ValueTypeMetaData   *scalar_schema{nullptr};

        NodeKind node_kind{NodeKind::Compute};
        bool     uses_scheduler{false};
        bool     uses_global_state{false};
        bool     uses_evaluation_clock{false};
        // True when this node consumes and/or produces time-series values
        // through the Python object boundary. Wiring uses this to request
        // output-local Python-aware storage from upstream producers.
        bool     uses_python_values{false};
        // When set, the framework schedules this node for the current cycle during
        // ``start`` (the declarative form of a source doing schedule_now() itself).
        bool     schedule_on_start{false};
        bool     captures_errors{false};
        ErrorCaptureOptions error_capture{};

        // nullopt means "use the runtime default"; an engaged empty vector is
        // an explicit empty selector set.
        std::optional<std::vector<std::size_t>> active_inputs{};
        // Inputs subscribed to collection membership rather than child-value ticks.
        std::vector<std::size_t>                structural_inputs{};
        std::optional<std::vector<std::size_t>> valid_inputs{};
        std::vector<std::size_t>                all_valid_inputs{};

        [[nodiscard]] std::string_view name() const noexcept;
        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool has_state() const noexcept;
        [[nodiscard]] bool has_scalars() const noexcept;
        [[nodiscard]] bool has_error_output() const noexcept;
        [[nodiscard]] bool has_recordable_state() const noexcept;
        [[nodiscard]] constexpr const SchemaHeader &schema_header() const noexcept { return header; }
    };

    /**
     * Type-erased node operation table.
     *
     * The table is intentionally passive: it is a context pointer plus
     * function pointers. Runtime policy such as graph scheduling and
     * lifecycle sequencing is driven by ``NodeView`` / ``GraphView`` through
     * these operations.
     */
    struct HGRAPH_EXPORT NodeOps
    {
        const void *context{nullptr};

        void (*attach_graph_impl)(const void *context, void *memory, GraphValue *graph,
                                  std::size_t node_index) = nullptr;
        GraphValue *(*graph_impl)(const void *context, const void *memory) noexcept = nullptr;
        std::size_t (*node_index_impl)(const void *context, const void *memory) noexcept = nullptr;
        std::string_view (*label_impl)(const void *context, const void *memory) noexcept = nullptr;

        bool (*started_impl)(const void *context, const void *memory) noexcept = nullptr;
        void (*start_impl)(const void *context, const NodeView &view, DateTime evaluation_time) = nullptr;
        void (*stop_impl)(const void *context, const NodeView &view, DateTime evaluation_time) = nullptr;
        // Returns true when the node completed its evaluation, false when the node
        // requested a pause (it must be re-evaluated to resume — see the pause/resume
        // protocol in the execution-layer docs). Ordinary nodes always return true.
        bool (*evaluate_impl)(const void *context, const NodeView &view, DateTime evaluation_time) = nullptr;

        bool (*has_input_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_output_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_state_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_scalars_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_scheduler_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_error_output_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*has_recordable_state_impl)(const void *context, const void *memory) noexcept = nullptr;

        TSInputView (*input_view_impl)(const void *context, void *memory,
                                                     DateTime evaluation_time) = nullptr;
        TSOutputView (*output_view_impl)(const void *context, void *memory,
                                                       DateTime evaluation_time) = nullptr;
        ValueView (*state_view_impl)(const void *context, void *memory) = nullptr;
        void (*replace_state_impl)(const void *context, void *memory, Value value) = nullptr;
        ValueView (*scalars_view_impl)(const void *context, void *memory) = nullptr;
        NodeSchedulerState *(*scheduler_state_impl)(const void *context, void *memory) = nullptr;
        GlobalStateView (*global_state_view_impl)(const void *context, void *memory) = nullptr;
        ClockPtr (*evaluation_clock_ptr_impl)(const void *context, void *memory) = nullptr;
        TSOutputView (*error_output_view_impl)(const void *context, void *memory,
                                                             DateTime evaluation_time) = nullptr;
        TSOutputView (*recordable_state_view_impl)(const void *context, void *memory,
                                                                 DateTime evaluation_time) = nullptr;

        /**
         * Optional cold-path storage inspection. The context is
         * ``extended_view_context`` because ``context`` is reserved for the
         * common node runtime operations.
         */
        NodeStorageMetrics (*storage_metrics_impl)(const void *context,
                                                   const void *memory) noexcept = nullptr;

        const void *extended_view_type_id{nullptr};
        const void *extended_view_context{nullptr};
    };

    /** User callbacks used by the first-pass native node builder. */
    struct HGRAPH_EXPORT NodeCallbacks
    {
        std::function<void(const NodeView &, DateTime)> start{};
        std::function<void(const NodeView &, DateTime)> evaluate{};
        std::function<void(const NodeView &, DateTime)> stop{};
    };

    struct HGRAPH_EXPORT NodeStorageField
    {
        std::string_view                name{};
        const MemoryUtils::StoragePlan *plan{nullptr};
    };

    /** Planned field name of the static-node prepared-slot array (RFC 0008
        stage 5). The layout caches its offset like every standard component,
        so derived-type rebuilds (error capture, passive inputs) resolve
        their own layout instead of inheriting a stale offset. */
    inline constexpr std::string_view node_prepared_inputs_field{"prepared_inputs"};

    /**
     * Build (and intern) the node storage plan. Components destroy in
     * **reverse** order, so field placement encodes link direction:
     * ``extra_fields`` sit before ``output`` — right for fields holding data
     * the node's forwarding output links INTO (``nested_`` / ``switch_``
     * child graphs: the output link unbinds first, while the child is alive).
     * ``extra_fields_after_output`` destroy BEFORE the output — required for
     * fields holding links INTO the node's own output (``map_`` children,
     * whose terminal forwarding outputs write the owned TSD's elements).
     */
    [[nodiscard]] HGRAPH_EXPORT const MemoryUtils::StoragePlan &node_storage_plan_for(
        const NodeTypeMetaData &schema,
        std::span<const NodeStorageField> extra_fields = {},
        std::span<const NodeStorageField> extra_fields_after_output = {});

    struct HGRAPH_EXPORT NodeTypeDescriptor
    {
        struct DynamicDebug
        {
            const TypeRecord    *key_type{nullptr};
            const TypeRecord    *element_type{nullptr};
            DebugDynamicLayout   layout{};
        };

        NodeTypeMetaData                  schema{};
        const MemoryUtils::StoragePlan   *storage_plan{nullptr};
        NodeCallbacks                     callbacks{};
        NodeOps                           ops{};
        std::string                       implementation_label{};
        std::vector<DebugField>           debug_fields{};
        std::optional<DynamicDebug>        dynamic_debug{};
    };

    /**
     * Borrowed type-erased view over a node allocation.
     *
     * ``NodeView`` carries the interned type record and the node memory. Time-
     * sensitive lifecycle operations and input/output projections receive the
     * evaluation time explicitly. The active input notification target is
     * recovered from the node runtime memory.
     */
    class HGRAPH_EXPORT NodeView
    {
      public:
        NodeView() noexcept;
        explicit NodeView(NodePtr pointer) noexcept;
        NodeView(NodeTypeRef type, void *memory) noexcept;
        NodeView(const NodeView &) = delete;
        NodeView &operator=(const NodeView &) = delete;
        NodeView(NodeView &&) noexcept = default;
        NodeView &operator=(NodeView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] NodeTypeRef type() const noexcept;
        [[nodiscard]] NodePtr pointer() const noexcept;
        [[nodiscard]] const NodeTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;
        /** The node's planned prepared-slot array (RFC 0008 stage 5), or
            null when this node type carries none. Type-erased so the node
            layer stays independent of the static-node header; static nodes
            cast to their ``PreparedInputSlotRoute`` array. */
        [[nodiscard]] void *prepared_input_routes() const noexcept;

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] NodeKind node_kind() const noexcept;
        [[nodiscard]] bool started() const noexcept;
        [[nodiscard]] std::size_t node_index() const noexcept;
        [[nodiscard]] GraphValue *graph_value() const noexcept;
        [[nodiscard]] GraphView graph() const;

        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool has_state() const noexcept;
        [[nodiscard]] bool has_scalars() const noexcept;
        [[nodiscard]] bool has_scheduler() const noexcept;
        [[nodiscard]] bool has_error_output() const noexcept;
        [[nodiscard]] bool has_recordable_state() const noexcept;

        [[nodiscard]] TSInputView input(DateTime evaluation_time) const;
        [[nodiscard]] TSOutputView output(DateTime evaluation_time) const;
        [[nodiscard]] ValueView state() const;
        void replace_state(Value value) const;
        [[nodiscard]] ValueView scalars() const;
        /** Borrow this node's persistent scheduler state (only valid when ``has_scheduler``). */
        [[nodiscard]] NodeSchedulerState &scheduler_state() const;
        [[nodiscard]] GlobalStateView global_state() const;
        [[nodiscard]] ClockPtr evaluation_clock_ptr() const;
        [[nodiscard]] EvaluationClockView evaluation_clock() const;
        [[nodiscard]] TSOutputView error_output(DateTime evaluation_time) const;
        [[nodiscard]] TSOutputView recordable_state(DateTime evaluation_time) const;
        /** Owned storage counters; the normal execution path never calls this. */
        [[nodiscard]] NodeStorageMetrics storage_metrics() const noexcept;

        template <typename T>
        [[nodiscard]] T as() const
        {
            if (!valid()) { throw std::logic_error("NodeView::as<T> requires a live node"); }
            const NodeOps &node_ops = ops();
            if (node_ops.extended_view_type_id != T::node_view_type_id())
            {
                throw std::invalid_argument("NodeView::as<T> requested an unsupported node extension view");
            }
            return T::from_node(NodeView{pointer()}, node_ops.extended_view_context);
        }

        /** Non-throwing test of whether this node exposes the extended view ``T``. */
        template <typename T>
        [[nodiscard]] bool is() const noexcept
        {
            return valid() && ops().extended_view_type_id == T::node_view_type_id();
        }

        void start(DateTime evaluation_time) const;
        void stop(DateTime evaluation_time) const;
        /// Returns true if the node completed, false if it requested a pause.
        bool evaluate(DateTime evaluation_time) const;

      private:
        [[nodiscard]] const NodeOps &ops() const;

        NodePtr pointer_{};
    };

    /**
     * Owning node value.
     *
     * ``NodeValue`` owns the node allocation through the same
     * ``ErasedOwner``/type-record pattern used by values and time-series data.
     * The stable active-input notification identity lives in the node runtime
     * storage header and is reached through the node memory pointer.
     */
    class HGRAPH_EXPORT NodeValue final
    {
      public:
        using storage_type = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        NodeValue() noexcept;
        explicit NodeValue(const NodeBuilder &builder, std::size_t node_index = 0);
        ~NodeValue();

        NodeValue(const NodeValue &) = delete;
        NodeValue &operator=(const NodeValue &) = delete;
        NodeValue(NodeValue &&other) noexcept;
        NodeValue &operator=(NodeValue &&other) noexcept;

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] NodeTypeRef type() const noexcept;
        [[nodiscard]] const NodeTypeMetaData *schema() const noexcept;

        [[nodiscard]] NodeView view();
        [[nodiscard]] NodeView view() const;

        void attach_graph(GraphValue *graph, std::size_t node_index);

      private:
        storage_type storage_{};
    };

    /**
     * Reusable builder for node values.
     *
     * Builders are construction recipes. They cache a node type plus input
     * endpoint annotation and can be reused to create many runtime node
     * instances.
     */
    class HGRAPH_EXPORT NodeBuilder
    {
      public:
        NodeBuilder();

        [[nodiscard]] static NodeBuilder native(NodeTypeMetaData schema,
                                                NodeCallbacks callbacks = {},
                                                TSEndpointSchema input_endpoint = {},
                                                std::string_view implementation_label = {});

        [[nodiscard]] static NodeBuilder from_descriptor(NodeTypeDescriptor descriptor,
                                                         TSEndpointSchema input_endpoint = {});

        /**
         * Build from a descriptor whose callbacks/ops are identified by a
         * process-stable token and may therefore share a canonical runtime
         * type with equivalent descriptors.  The token must be non-null and
         * outlive every graph using the descriptor.  Captured per-builder
         * state must use ``from_descriptor`` instead.
         */
        [[nodiscard]] static NodeBuilder
        from_canonical_descriptor(NodeTypeDescriptor descriptor,
                                  const void *runtime_type_id,
                                  TSEndpointSchema input_endpoint = {});

        /**
         * Typed front-end over ``native``: build this node from a static node
         * implementation ``TImplementation`` (a stateless struct with a static
         * ``eval`` taking ``In<>`` / ``Out<>`` / ``State<>`` parameters, plus
         * optional ``start`` / ``stop``). Defined in
         * ``<hgraph/types/static_node.h>``, which must be included to use it.
         */
        template <typename TImplementation>
        NodeBuilder &implementation();

        /**
         * Generic-node front-end: build ``TImplementation`` resolving its type
         * variables (``TsVar`` / ``ScalarVar``) through ``resolution`` (see
         * ``type_resolution.h``). Used by the wiring layer for generic nodes; the
         * concrete overload above is unaffected. Defined in ``static_node.h``.
         */
        template <typename TImplementation>
        NodeBuilder &implementation(const ResolutionMap &resolution);

        NodeBuilder &label(std::string label);
        [[nodiscard]] std::string_view label() const noexcept;

        /** Override the input endpoint annotation for this node instance. */
        NodeBuilder &input_endpoint(TSEndpointSchema endpoint);

        /**
         * Override the output endpoint annotation for this node instance —
         * e.g. a nested operator re-homing the node's output as a
         * **forwarding** endpoint that writes through to externally-owned
         * storage (``map_`` binds a child's terminal output into the parent's
         * TSD element).
         */
        NodeBuilder &output_endpoint(TSEndpointSchema endpoint);
        [[nodiscard]] const TSEndpointSchema &output_endpoint() const noexcept;

        /**
         * Request the physical value representation for this instance's owned
         * output. Forwarding endpoints and unsupported schemas ignore the
         * request conservatively.
         */
        NodeBuilder &output_value_storage(ValueStorageVariant storage) noexcept;
        [[nodiscard]] ValueStorageVariant output_value_storage() const noexcept;

        /** Per-instance immutable scalar configuration (its shape is the schema's ``scalar_schema``). */
        NodeBuilder &scalars(Value scalars);
        [[nodiscard]] const Value &scalars() const noexcept;

        /**
         * Clone this builder's binding with error capture enabled: the new
         * binding has ``error_output_schema = error_schema`` and
         * ``captures_errors = true`` (so the node storage allocates an error
         * output and ``evaluate`` runs under a try/catch). Only supported for
         * native nodes (the standard runtime ops); a custom-ops node throws.
         * Used by ``exception_time_series`` to activate a node's error output.
         */
        [[nodiscard]] NodeBuilder with_error_capture(
            const TSValueTypeMetaData *error_schema,
            ErrorCaptureOptions options = {}) const;

        /**
         * A rebound builder whose ``active_inputs`` exclude ``slots`` (the
         * wiring-time ``passive(port)`` marker; Python parity). Throws when
         * every input would become passive — such a node could never fire.
         */
        [[nodiscard]] NodeBuilder with_passive_inputs(std::span<const std::size_t> slots) const;

        [[nodiscard]] NodeTypeRef type() const;
        [[nodiscard]] const TSEndpointSchema &input_endpoint() const noexcept;
        /**
         * Construct this node directly into caller-owned storage matching
         * ``type().checked_plan()``. The caller owns destruction through the
         * same type/plan; this is used by graph storage to colocate
         * variable-sized node payloads in the graph allocation.
         */
        void construct_node_storage(void *memory, std::size_t node_index = 0) const;
        [[nodiscard]] NodeValue make_node(std::size_t node_index = 0) const;

      private:
        friend class NodeValue;

        [[nodiscard]] static NodeBuilder
        from_descriptor_impl(NodeTypeDescriptor descriptor,
                             const void *runtime_type_id,
                             TSEndpointSchema input_endpoint);
        NodeBuilder(NodeTypeRef type, TSEndpointSchema input_endpoint);

        NodeTypeRef            type_{};
        TSEndpointSchema       input_endpoint_{};
        TSEndpointSchema       output_endpoint_{};
        ValueStorageVariant    output_value_storage_{
            ValueStorageVariant::Native};
        std::string            label_{};
        Value                  scalars_{};
    };

    /** Clear node schema/ops contexts after their common TypeRecords are reset. */
    HGRAPH_EXPORT void clear_node_runtime_types() noexcept;

    static_assert(offsetof(NodeTypeMetaData, header) == 0);

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_NODE_H
