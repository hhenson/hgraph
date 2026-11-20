#ifndef HGRAPH_TYPES_GRAPH_WIRING_CORE_H
#define HGRAPH_TYPES_GRAPH_WIRING_CORE_H

#include <hgraph/types/static_node_core.h>
#include <hgraph/types/signature_defaults.h>

#include <array>
#include <functional>
#include <optional>
#include <span>
#include <tuple>
#include <typeindex>
#include <variant>
#include <vector>

namespace hgraph
{
    class Wiring;
    /**
     * C++ graph wiring (slice 1: top-level node wiring, no scalars yet).
     *
     * A graph is authored as a struct with a static ``compose(Wiring &)`` body that
     * calls ``wire<NodeType>(w, ports...)`` to add nodes; each call returns a typed
     * ``Port`` to the node's output, and passing ports as inputs records edges.
     * Nodes are **interned** (identical node + inputs → one node) and the graph is
     * **topologically sorted and ranked** when built. The runtime ``GraphBuilder``
     * it produces is consumed exactly as a hand-built one.
     *
     * See the developer guide *Graph Wiring* for the full design (including the
     * planned scalar inputs, sub-graph composition and the Python-shared core).
     */

    struct WiringInstance;
    struct WiringDelayedBindingState;
    struct WiringDelayedBindingControl;

    /**
     * Erased wiring-time handle to a time-series source.
     *
     * A peered source references a producing node, a source root (ordinary
     * output, error output, or recordable-state output), and optional ``path``
     * within that root. A structural source has no producing node of its own; its
     * children describe the peered or structural sources for each fixed child
     * slot. Target information belongs on ``WiringInputRef`` only.
     */
    struct WiringPortRef
    {
        struct PeeredSource
        {
            const WiringInstance     *node{nullptr};
            std::vector<std::size_t>  path{};
            GraphEdgeSourceKind       output_kind{GraphEdgeSourceKind::Output};
        };

        struct StructuralSource
        {
            std::vector<WiringPortRef> children{};
        };

        /**
         * A sub-graph boundary placeholder: the source is either the
         * ``arg_index``-th declared input or a local captured-input index
         * (``path`` walks within it). Boundary sources exist only while compiling a sub-graph
         * (``compile_subgraph<G>``); ``Wiring::finish_subgraph`` converts them
         * into nested-graph input bindings — no stub node is ever created.
         */
        struct BoundarySource
        {
            std::size_t              arg_index{0};
            std::vector<std::size_t> path{};
            bool                     captured{false};
        };

        /** A typed wiring source whose producer is supplied later in compose(). */
        struct DelayedSource
        {
            std::shared_ptr<WiringDelayedBindingState> state{};
            std::vector<std::size_t>                   path{};
        };

        enum class SourceKind
        {
            Unbound,
            Null,
            Peered,
            Structural,
            Boundary,
            Delayed,
        };

        const TSValueTypeMetaData *schema{nullptr};
        /** Cold source observation beneath a metadata-only facade. The
            logical schema continues to enforce SIGNAL capabilities. */
        const TSValueTypeMetaData *observation_schema{nullptr};

        [[nodiscard]] const TSValueTypeMetaData *declared_source_view() const noexcept
        {
            return observation_schema != nullptr ? observation_schema : schema;
        }

        [[nodiscard]] static WiringPortRef peered_source(const WiringInstance *node,
                                                         std::vector<std::size_t> path,
                                                         const TSValueTypeMetaData *schema,
                                                         GraphEdgeSourceKind output_kind = GraphEdgeSourceKind::Output)
        {
            if (node == nullptr) { throw std::logic_error("WiringPortRef::peered_source requires a node"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = PeeredSource{node, std::move(path), output_kind};
            return ref;
        }

        /**
         * Wiring-time **argument adornment** (Python's ``pass_through()`` /
         * ``no_key()`` wrappers): consumed by operator implementations during
         * classification; never part of graph structure (edges, bindings and
         * source interning ignore it — operator configs that depend on it
         * must fold it into their own interning identity).
         */
        enum class ArgTag : std::uint8_t
        {
            None        = 0,
            PassThrough = 1,   ///< do not demultiplex — pass the input whole
            NoKey       = 2,   ///< demultiplex, but exclude from key inference
            /** The consuming node's input becomes PASSIVE (removed from its
                active list; Python's ``passive(ts)`` marker). Applies to the
                tagged USAGE only — tag a copy, the original port is untouched. */
            Passive     = 3,
        };

        ArgTag arg_tag{ArgTag::None};

        [[nodiscard]] WiringPortRef with_arg_tag(ArgTag tag) const
        {
            WiringPortRef tagged{*this};
            tagged.arg_tag = tag;
            return tagged;
        }

        [[nodiscard]] static WiringPortRef structural_source(const TSValueTypeMetaData *schema,
                                                             std::vector<WiringPortRef> children)
        {
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = StructuralSource{std::move(children)};
            return ref;
        }

        [[nodiscard]] static WiringPortRef null_source(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::null_source requires a schema"); }
            WiringPortRef ref;
            ref.schema = schema;
            return ref;
        }

        [[nodiscard]] static WiringPortRef boundary_source(std::size_t arg_index,
                                                           std::vector<std::size_t> path,
                                                           const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::boundary_source requires a schema"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = BoundarySource{arg_index, std::move(path), false};
            return ref;
        }

        /**
         * A placeholder for an enclosing-wiring source captured by a compiled
         * child. ``finish_subgraph`` appends the real source after the child's
         * declared arguments and resolves this local capture index to that
         * final boundary ordinal.
         */
        [[nodiscard]] static WiringPortRef captured_boundary_source(
            std::size_t capture_index,
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::logic_error("WiringPortRef::captured_boundary_source requires a schema");
            }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = BoundarySource{capture_index, std::move(path), true};
            return ref;
        }

        [[nodiscard]] static WiringPortRef delayed_source(
            std::shared_ptr<WiringDelayedBindingState> state,
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *schema)
        {
            if (state == nullptr) { throw std::logic_error("WiringPortRef::delayed_source requires state"); }
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::delayed_source requires a schema"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = DelayedSource{std::move(state), std::move(path)};
            return ref;
        }

        [[nodiscard]] SourceKind source_kind() const noexcept
        {
            if (std::holds_alternative<PeeredSource>(source_)) { return SourceKind::Peered; }
            if (std::holds_alternative<StructuralSource>(source_)) { return SourceKind::Structural; }
            if (std::holds_alternative<BoundarySource>(source_)) { return SourceKind::Boundary; }
            if (std::holds_alternative<DelayedSource>(source_)) { return SourceKind::Delayed; }
            return schema != nullptr ? SourceKind::Null : SourceKind::Unbound;
        }

        [[nodiscard]] bool is_peered_source() const noexcept { return source_kind() == SourceKind::Peered; }
        [[nodiscard]] bool is_structural_source() const noexcept { return source_kind() == SourceKind::Structural; }
        [[nodiscard]] bool is_null_source() const noexcept { return source_kind() == SourceKind::Null; }
        [[nodiscard]] bool is_unbound_source() const noexcept { return source_kind() == SourceKind::Unbound; }
        [[nodiscard]] bool is_boundary_source() const noexcept { return source_kind() == SourceKind::Boundary; }
        [[nodiscard]] bool is_delayed_source() const noexcept { return source_kind() == SourceKind::Delayed; }
        [[nodiscard]] bool is_captured_boundary_source() const noexcept
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            return source != nullptr && source->captured;
        }

        [[nodiscard]] std::size_t boundary_arg_index() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            if (source->captured)
            {
                throw std::logic_error("WiringPortRef source is a captured sub-graph boundary");
            }
            return source->arg_index;
        }

        [[nodiscard]] std::size_t boundary_capture_index() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr || !source->captured)
            {
                throw std::logic_error("WiringPortRef source is not a captured sub-graph boundary");
            }
            return source->arg_index;
        }

        [[nodiscard]] const std::vector<std::size_t> &boundary_path() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            return source->path;
        }

        [[nodiscard]] WiringPortRef projected_boundary_source(
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *projected_schema) const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr)
            {
                throw std::logic_error("WiringPortRef source is not a sub-graph boundary");
            }
            return source->captured
                       ? captured_boundary_source(source->arg_index, std::move(path), projected_schema)
                       : boundary_source(source->arg_index, std::move(path), projected_schema);
        }

        [[nodiscard]] const std::shared_ptr<WiringDelayedBindingState> &delayed_state() const
        {
            const auto *source = std::get_if<DelayedSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not delayed"); }
            return source->state;
        }

        [[nodiscard]] const std::vector<std::size_t> &delayed_path() const
        {
            const auto *source = std::get_if<DelayedSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not delayed"); }
            return source->path;
        }

        [[nodiscard]] WiringPortRef projected_delayed_source(
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *projected_schema) const
        {
            return delayed_source(delayed_state(), std::move(path), projected_schema);
        }

        [[nodiscard]] const WiringInstance *peered_node() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->node;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->output_kind;
        }

        [[nodiscard]] const std::vector<WiringPortRef> &structural_children() const
        {
            const auto *source = std::get_if<StructuralSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not structural"); }
            return source->children;
        }

        /** Whether a fixed structural source carries a direct REF child.
         *
         * Such a source must retain reference identity when it crosses a
         * compiled-graph boundary. A structure containing only ordinary
         * children is instead materialized through an owned output so its
         * observers have the usual node-owned lifetime.
         */
        [[nodiscard]] bool structural_has_reference_children() const noexcept
        {
            const auto *source = std::get_if<StructuralSource>(&source_);
            if (source == nullptr) { return false; }
            return std::ranges::any_of(source->children, [](const WiringPortRef &child) {
                return child.schema != nullptr && child.schema->kind == TSTypeKind::REF;
            });
        }

        [[nodiscard]] const WiringInstance *peered_node_or_null() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->node : nullptr;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path_or_empty() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source != nullptr) { return source->path; }
            static const std::vector<std::size_t> empty_path;
            return empty_path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind_or_default() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->output_kind : GraphEdgeSourceKind::Output;
        }

        /** Source identity used for capture deduplication; argument tags are usage metadata. */
        [[nodiscard]] bool same_source_as(const WiringPortRef &other) const noexcept
        {
            if (schema != other.schema || source_kind() != other.source_kind()) { return false; }
            switch (source_kind())
            {
                case SourceKind::Unbound:
                case SourceKind::Null:
                    return true;
                case SourceKind::Peered:
                    return peered_node() == other.peered_node() &&
                           peered_path() == other.peered_path() &&
                           peered_output_kind() == other.peered_output_kind();
                case SourceKind::Boundary:
                    return is_captured_boundary_source() == other.is_captured_boundary_source() &&
                           (is_captured_boundary_source()
                                ? boundary_capture_index() == other.boundary_capture_index()
                                : boundary_arg_index() == other.boundary_arg_index()) &&
                           boundary_path() == other.boundary_path();
                case SourceKind::Delayed:
                    return delayed_state() == other.delayed_state() &&
                           delayed_path() == other.delayed_path();
                case SourceKind::Structural:
                {
                    const auto &left  = structural_children();
                    const auto &right = other.structural_children();
                    if (left.size() != right.size()) { return false; }
                    for (std::size_t index = 0; index < left.size(); ++index)
                    {
                        if (!left[index].same_source_as(right[index])) { return false; }
                    }
                    return true;
                }
            }
            return false;
        }

      private:
        std::variant<std::monostate, PeeredSource, StructuralSource, BoundarySource, DelayedSource> source_{};
    };

    /** Shared state behind one leaf of a delayed binding. */
    struct WiringDelayedBindingState
    {
        Wiring                        *wiring{nullptr};
        const TSValueTypeMetaData     *schema{nullptr};
        std::optional<WiringPortRef>   source{};
    };

    /** Shared handle state for a delayed binding and all copies of its facade. */
    struct WiringDelayedBindingControl
    {
        Wiring                    *wiring{nullptr};
        const TSValueTypeMetaData *schema{nullptr};
        WiringPortRef              port{};
        std::vector<std::shared_ptr<WiringDelayedBindingState>>
            leaves{};
        bool bound{false};
    };

    /** Consumer-side wiring input: source port plus optional target path on the consuming node. */
    struct WiringInputRef
    {
        WiringPortRef             source{};
        std::vector<std::size_t>  target_path{};
        bool                      rank_dependency{true};
    };

    struct WiringServiceImplementationEndpoint;
    struct WiringServiceClientRecord;
    struct WiringDefaultServiceSelector;

    /**
     * The interned wiring identity. It pairs a node's ``NodeBuilder`` (which
     * carries any per-instance scalar configuration) with its time-series input
     * edges; identity is the node definition plus the input edges **and** the
     * scalar values. Runtime edges are derived from ``inputs`` at build time.
     */
    struct WiringInstance
    {
        std::type_index                     definition{typeid(void)};
        NodeBuilder                         builder;
        std::vector<WiringInputRef>          inputs;
        std::vector<const WiringInstance *> rank_dependencies;
    };

    struct CompiledSubGraph;   // defined in subgraph_wiring.h

    /**
     * The node's resolved schema identity — the (registry-interned, hence
     * stable) schema pointers that enter the interning key. Normally derived
     * from a builder's ``NodeTypeMetaData``; supplied explicitly for the
     * deferred-builder ``add_node`` overload.
     */
    struct WiringNodeSchema
    {
        const TSValueTypeMetaData *input{nullptr};
        const TSValueTypeMetaData *output{nullptr};
        const TSValueTypeMetaData *error_output{nullptr};
        const TSValueTypeMetaData *recordable_state{nullptr};
        const ValueTypeMetaData   *scalar{nullptr};
        const ValueTypeMetaData   *state{nullptr};

        bool operator==(const WiringNodeSchema &) const noexcept = default;
    };

    enum class WiringKind : std::uint8_t
    {
        TopLevel,
        SubGraph,
    };

    /** Immutable configuration supplied before graph composition begins. */
    struct WiringOptions
    {
        bool is_realtime{false};
        /** Disallow external event sources throughout this wiring tree.
         * Externally driven workers advance solely on their caller's clock.
         */
        bool allow_push_sources{true};
        /** Bind the active authoring GlobalContext; isolated workers opt out. */
        bool inherit_global_context{true};
    };

    struct WiringObserverRegistry;

    class WiringObservationScope;
    class WiringObserver;
    struct WiringScopeEvent;
    struct WiringResolutionEvent;
    struct TypeRealizationOptions;
    /**
     * Holds a graph's label on the wiring path while it wires without
     * observers, so a failure can say where it happened (runtime spec WIR-4;
     * ``graph_wiring.rst``, "Failure reports"). An observed graph holds its
     * label through its ``WiringObservationScope`` instead.
     */
    class HGRAPH_CLASS_EXPORT WiringPathScope
    {
      public:
        WiringPathScope(Wiring &wiring, std::string label);
        WiringPathScope(const WiringPathScope &)            = delete;
        WiringPathScope &operator=(const WiringPathScope &) = delete;
        ~WiringPathScope() noexcept;

      private:
        Wiring *wiring_;
    };

    /**
     * Shared runtime wiring core: accumulates interned ``WiringInstance``s and, on
     * ``finish``, topologically sorts + ranks them into a ``GraphBuilder``. (The
     * Python wiring bridge will drive this same core.)
     */
    class HGRAPH_CLASS_EXPORT Wiring
    {
      public:
        explicit Wiring(WiringKind kind = WiringKind::TopLevel,
                        WiringOptions options = {});
        /** A top-level wiring bound to an explicitly selected live seed (the
            bridge's state): reads and writes go to ``seed`` for the duration
            of the wiring, the build fixes a copy. Never combined with an
            active ``GlobalContext``. The seed must outlive the wiring's
            build; ``release_seed`` drops the binding early. */
        explicit Wiring(GlobalState &seed, WiringOptions options = {});
        ~Wiring();
        Wiring(const Wiring &)            = delete;
        Wiring &operator=(const Wiring &) = delete;
        Wiring(Wiring &&) noexcept;
        Wiring &operator=(Wiring &&) noexcept;

        /** Process-unique identity for associating wiring-lifetime adapter
            state across owned and borrowed language wrappers. */
        [[nodiscard]] std::uint64_t identity() const noexcept;

        /**
         * Retain extension-owned state until this Wiring is destroyed.
         *
         * Language bridges use this for wiring-time state recorded through a
         * borrowed wrapper: the wrapper may be ephemeral, while the state is
         * still needed by a later build_services()/finish() call on the
         * underlying Wiring. The runtime does not inspect the retained value.
         */
        void retain_extension_state(std::shared_ptr<void> state);

        /**
         * Retain opaque state through the produced ``GraphBuilder`` and every
         * graph instance built from it. Operator-provider leases use this to
         * keep module code logically active for as long as a selected plan can
         * call it. The runtime never inspects the retained value.
         */
        void retain_graph_state(std::shared_ptr<void> state);

        /**
         * Return wiring-lifetime state for ``key``, creating it once on first
         * use. Native wiring extensions use this to share planning state
         * without process globals; the state is owned by this Wiring.
         */
        [[nodiscard]] std::shared_ptr<void> acquire_extension_state(
            std::type_index key,
            std::function<std::shared_ptr<void>()> create);

        /** Register an idempotent extension finalizer run after lazy service
         *  materialization and before rank dependencies are applied. */
        void register_pre_rank_finalizer(std::function<void(Wiring &)> finalizer);

        /** Whether this wiring is a root graph or an isolated child graph. */
        [[nodiscard]] WiringKind kind() const noexcept;
        /** Whether this graph is being composed for a real-time executor. */
        [[nodiscard]] bool is_realtime() const noexcept;
        /** Immutable composition policies, including isolated-worker restrictions. */
        [[nodiscard]] WiringOptions options() const noexcept;

        /** User-facing label copied to the produced graph. */
        Wiring &label(std::string label);
        [[nodiscard]] std::string_view label() const noexcept;

        /**
         * Intern a node with its input edges + scalar configuration and return its
         * output port. ``def`` is the node *definition's* stable identity
         * (``typeid(T)`` for a C++ static node) — two calls with the same ``def``,
         * equal inputs **and** equal ``scalars`` dedup to one instance. ``builder``
         * is the build artifact stored for ``finish`` (the ``scalars`` are recorded
         * on it). Pass an empty ``Value`` for a node with no scalar inputs.
         */
        /**
         * Diagnostic label hint (issue #247): the next ``add_node`` whose
         * builder schema or default label names ``expected_operator`` labels
         * its node, then the hint clears. Lets erased wire
         * paths attach user-facing identity (a python function name) without
         * threading a label through every candidate wire signature; the
         * operator-name guard keeps auxiliary nodes (const lifts) unlabeled.
         */
        void set_pending_node_label(std::string expected_operator, std::string label);
        /** Consume a pending label using an explicit operator alias.
         *
         * Erased language bridges call operators by their public name while
         * the selected native node schema names the concrete implementation.
         * This hook labels only that bridge-directed call; ordinary native
         * operator wiring keeps the implementation label used by observers.
         */
        void apply_pending_node_label(std::string_view operator_alias,
                                      NodeBuilder &builder);
        void clear_pending_node_label() noexcept;

        /** The supplied scalars identify the call and configure a builder that
         * has no scalar value of its own. A preconfigured builder keeps its value. */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringInputRef> inputs,
                               Value scalars);

        /**
         * Convenience overload for ordinary positional node inputs. Each source
         * port is bound to the input path matching its argument index.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                               Value scalars);

        /**
         * Add a value-producing node without interning. Use this for nodes whose
         * identity is their allocation site rather than their structural inputs,
         * such as feedback sources.
         */
        WiringPortRef add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringInputRef> inputs,
                                      Value scalars);

        /** Convenience overload for ordinary positional unique-node inputs. */
        WiringPortRef add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringPortRef> inputs,
                                      Value scalars);

        /**
         * Set a graph trait — parent-chained key-value metadata carried onto
         * the produced ``GraphBuilder`` (see ``GraphView::trait_or`` and the
         * record/replay design record, P5). Typical use:
         * ``w.set_trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, ...)``.
         */
        void set_trait(std::string_view name, const ValueView &value);
        void set_trait(std::string_view name, Value &&value);


        /**
         * Add a layout-only dependency. ``node`` is ranked after ``depends_on``,
         * but no runtime edge is emitted.
         */
        void add_rank_dependency(const WiringInstance *node, const WiringInstance *depends_on);

        /**
         * Declare a same-cycle boundary pair (shared-output relays): the
         * ``source`` is rank-constrained after the ``capture``, and ``finish``
         * VALIDATES the final order, so the runtime schedules the source for
         * the current evaluation time with no hot-path checks. Deferred
         * request stubs do not use this helper. Ranked reply-less
         * request/reply clients use the service-rank contract instead because
         * their source may be external to a nested graph. Same-time work is
         * ordered as the first sending client, then the source, then later
         * sending clients.
         */
        void add_same_cycle_pair(const WiringInstance *capture, const WiringInstance *source);

        /**
         * Register an implementation-owned service/adaptor identity. This mirrors
         * Python's wiring-context duplicate checks: clients may refer to a path
         * many times, but only one implementation may own a concrete interface
         * identity in a wiring graph.
         */
        void register_built_service_path(std::string path, std::string_view kind);

        void register_service_client_path(std::string path, std::string_view kind,
                                          std::string_view interface_name = {},
                                          std::string_view specialization = {});
        void register_service_rank_anchor(std::string path, const WiringInstance *node);
        void register_service_client_rank(std::string path, std::string_view kind,
                                          const WiringInstance *node, bool receive);

        /** Record a service/adaptor implementation candidate without composing
            it. The materializer is invoked once, at build_services(), when at
            least one of its concrete interface paths has been requested. */
        void register_service_implementation_candidate(
            std::vector<std::string> paths,
            std::string description,
            std::function<void(Wiring &)> materialize);

        /** Record the interface-default implementation used as a fallback for
            any concrete user path of the same service/adaptor identity. */
        void register_default_service_implementation_candidate(
            std::string path_prefix,
            std::string path_suffix,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        /** As above, restricted to clients carrying one concrete generic
            interface specialization. */
        void register_default_service_implementation_candidate(
            std::string path_prefix,
            std::string path_suffix,
            std::string specialization,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        /** Record an atomic multi-interface default implementation. Demand
            through any selector materializes the whole group at that user
            path; an overlapping exact implementation is ambiguous. */
        void register_default_service_implementation_candidate(
            std::vector<std::pair<std::string, std::string>> path_selectors,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        void register_default_service_implementation_candidate(
            std::vector<WiringDefaultServiceSelector> path_selectors,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        /** Record an implementation resolver which inspects current client
            demand itself. Catch-all resolvers compose once during the
            fixed-point build phase, never at registration time. */
        void register_catch_all_service_implementation_candidate(
            std::string description,
            std::function<void(Wiring &)> materialize);

        /** Materialize requested implementation candidates to a fixed point.
            Safe to call explicitly; finish() calls it again for later demand. */
        void build_services();
        [[nodiscard]] std::vector<std::pair<std::string, std::string>>
        service_client_paths() const;
        [[nodiscard]] std::vector<WiringServiceClientRecord>
        service_client_records() const;
        [[nodiscard]] std::vector<std::pair<std::string, std::string>>
        built_service_paths() const;
        [[nodiscard]] std::string_view service_materialization_path() const noexcept;

        /**
         * Stable state for the active implementation scope. Service/adaptor
         * client registration flips the value to true. Consumers may retain
         * the handle until pre-rank finalization, after the scope has closed.
         */
        [[nodiscard]] std::shared_ptr<const bool>
        service_implementation_boundary_dependency() const;

        /**
         * RAII wrapper for implementation-owned service/adaptor stub scopes.
         *
         * Construction pushes the expected stub set; ``complete`` validates and
         * pops it. Destruction cancels an unfinished scope, so exceptions while
         * wiring an implementation cannot leak a stale active scope into the
         * enclosing graph wiring.
         */
        class HGRAPH_CLASS_EXPORT ServiceImplementationScope
        {
          public:
            ServiceImplementationScope() noexcept = default;
            ServiceImplementationScope(Wiring &wiring,
                                       std::string description,
                                       std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                       bool require_all = true);
            ServiceImplementationScope(Wiring &wiring,
                                       std::string description,
                                       std::vector<std::string> required_endpoints);
            ServiceImplementationScope(const ServiceImplementationScope &) = delete;
            ServiceImplementationScope &operator=(const ServiceImplementationScope &) = delete;
            ServiceImplementationScope(ServiceImplementationScope &&other) noexcept;
            ServiceImplementationScope &operator=(ServiceImplementationScope &&other) noexcept;
            ~ServiceImplementationScope() noexcept;

            void complete();

          private:
            void cancel_if_active() noexcept;

            Wiring *wiring_{nullptr};
            bool    active_{false};
        };

        [[nodiscard]] ServiceImplementationScope service_implementation_scope(
            std::string description,
            std::vector<WiringServiceImplementationEndpoint> required_endpoints,
            bool require_all = true);
        [[nodiscard]] ServiceImplementationScope service_implementation_scope(
            std::string description,
            std::vector<std::string> required_endpoints);

        void begin_service_implementation(std::string description, std::vector<std::string> required_endpoints);
        void begin_service_implementation(std::string description,
                                          std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                          bool require_all = true);
        [[nodiscard]] std::vector<std::string> service_implementation_used_endpoints() const;
        void register_service_implementation_stub(std::string endpoint, std::string_view kind);
        [[nodiscard]] ResolutionMap service_implementation_stub_resolution(const std::string &endpoint) const;
        void end_service_implementation();
        void cancel_service_implementation() noexcept;

        /**
         * Deferred-builder overload: intern by ``(def, schema, inputs, scalars)``
         * and call ``make_builder`` only when no interned instance exists. Use
         * when constructing the builder has a side effect or real cost that must
         * not happen for a deduped instance — e.g. a nested-graph node
         * registering its program-lifetime child-graph context.
         */
        WiringPortRef add_node(std::type_index def, const WiringNodeSchema &schema,
                               std::span<const WiringInputRef> inputs, Value scalars,
                               std::function<NodeBuilder()> make_builder);

        /** Convenience form of the deferred-builder overload for ordinary inputs. */
        WiringPortRef add_node(std::type_index def, const WiringNodeSchema &schema,
                               std::span<const WiringPortRef> inputs, Value scalars,
                               std::function<NodeBuilder()> make_builder);

        /**
         * Activate error capture on an already-added node: re-bind its builder
         * with an error output (``error_output_schema`` = ``error_schema``,
         * ``captures_errors`` = true) so its evaluation runs under a try/catch
         * and the error output is allocated. Returns the error-output schema.
         * Used by ``exception_time_series`` (the node is the port's producer).
         */
        const TSValueTypeMetaData *activate_error_capture(const WiringInstance *node,
                                                          const TSValueTypeMetaData *error_schema,
                                                          ErrorCaptureOptions options = {});

        /**
         * A view over the wiring-time ``GlobalState``. A ``compose`` body can seed
         * the store here; ``finish`` carries the populated state onto the produced
         * ``GraphBuilder`` (and thence onto each graph it builds).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;
        /** State visible to operator resolution; a child wiring reads the root's seed. */
        [[nodiscard]] GlobalStateView operator_state() noexcept;
        /** Declare a run-local exact entry storage binding without preparing
            or changing the owner seed. Repeated declarations require the same
            binding, not just the same schema. Root and child wirings share declarations. Finish
            validates the final seed and prepares the builder-owned store. */
        void prepare_global_entry(std::string_view key, ValueTypeRef binding);
        [[nodiscard]] bool has_global_entry(std::string_view key) const;
        /** The bound live seed, or null for a stateless wiring (and after a
            C++ context exited or ``release_seed``). */
        [[nodiscard]] GlobalState *seed_state() const noexcept;
        /** The shared, detachable binding itself, for a holder that must
            outlive this wiring without copying the raw pointer out (a
            prepared ``lower`` execution). Empty for a stateless wiring. */
        [[nodiscard]] GlobalSeed seed() const noexcept;
        /** Drop the seed binding; reads fall back to the internal store. An
            explicitly seeded wiring detaches the binding for every holder. */
        void release_seed() noexcept;
        /** Representation policy for values built while wiring: the enclosing
            realization snapshot's, else the bound seed's configuration. */
        [[nodiscard]] TypeRealizationOptions realization_options() const;
        /**
         * Wiring-time logger for graph composition diagnostics. The returned
         * view has no node context; runtime value logging belongs in a node or
         * logging operator.
         */
        [[nodiscard]] LoggerView logger() const;

        /** Add a borrowed wiring observer. It must outlive this wiring and its children. */
        void add_wiring_observer(WiringObserver *observer);
        [[nodiscard]] bool has_wiring_observers() const noexcept;
        /** Fresh child wiring carrying the same observer registry and current path. */
        [[nodiscard]] Wiring child_wiring() const;
        [[nodiscard]] std::vector<std::string> current_wiring_path() const;
        void notify_overload_resolution(const WiringResolutionEvent &event) const;

        [[nodiscard]] WiringObservationScope observation_scope(WiringScopeEvent event);
        template <typename Fn>
        decltype(auto) observe(WiringScopeEvent event, Fn &&fn);

        /** Topologically sort + rank the wired nodes into a rank-ordered GraphBuilder. */
        [[nodiscard]] GraphBuilder finish() &&;

        /**
         * Build a runnable graph from the wiring **as it currently stands**,
         * leaving the wiring open for further wiring (interactive/notebook
         * sessions — design record ``developer_guide/notebook.rst``). Same
         * validation as ``finish()``; the wiring ``GlobalState`` is COPIED
         * onto the builder rather than moved. Repeated snapshots are safe:
         * rank-dependency application de-duplicates.
         */
        [[nodiscard]] GraphBuilder snapshot() &;

        /**
         * Compile this wiring as a **sub-graph**: rank the nodes into a child
         * ``GraphBuilder`` and convert every boundary-sourced input into a
         * nested-graph input binding instead of an edge. ``output`` is the
         * sub-graph's returned output port, ``input_schemas`` the declared
         * boundary arg schemas in arg
         * order. Used by ``compile_subgraph<G>`` (see ``subgraph_wiring.h``).
         */
        [[nodiscard]] CompiledSubGraph finish_subgraph(
            std::optional<WiringPortRef> output,
            std::vector<const TSValueTypeMetaData *> input_schemas) &&;

        /** Register an enclosing-wiring source as an implicit child input. */
        [[nodiscard]] WiringPortRef capture_outer_source(WiringPortRef source);

        /** Claim a component's fully-qualified recordable id for this wiring;
            a second claim of the same id throws (one component instance per
            id per graph build - python parity).

            The one exception is ``InlineRepeat``: a function wired once per
            index on this wiring claims the same id each time, and those are
            instances of ONE component, not two components that collide. Such a
            claim returns ``false``. Only a repeat ACROSS indices is an
            instance: an id claimed twice within one index is two call sites
            sharing an id, and an id first claimed outside the repeat is
            another component altogether. Both are still duplicates. */
        bool claim_component_id(std::string_view fq_recordable_id);
        /**
         * A user function unrolled inline, once per index, on this wiring --
         * what ``map_`` over a fixed-size list does. A keyed ``map_`` wires its
         * function once, as a child template, and needs none of this.
         *
         * While one is open a repeated component id is an instance rather than
         * a duplicate (``claim_component_id``), and a worker graph's runtime
         * scope gives way to the user's: the function is the user's, so its
         * nodes are processed unless a component says otherwise, exactly as in
         * the child wiring a keyed ``map_`` makes (RFC 0039).
         */
        class HGRAPH_CLASS_EXPORT InlineRepeat
        {
          public:
            explicit InlineRepeat(Wiring &wiring);
            ~InlineRepeat();
            /** Begin the next index. What one index claims twice is a duplicate;
                what the next index claims again is an instance. */
            void next_index();
            InlineRepeat(const InlineRepeat &) = delete;
            InlineRepeat &operator=(const InlineRepeat &) = delete;

          private:
            Wiring     &wiring_;
            std::string scope_;
            bool        rescoped_{false};
        };
        /** Select the checkpoint ownership scope; returns the previous scope. */
        std::string checkpoint_component(std::string component_id);
        /** Current checkpoint ownership scope, including for child-plan caches. */
        [[nodiscard]] std::string_view checkpoint_component() const noexcept;
        /** Include the component's complete returned binding in its compatibility identity. */
        void checkpoint_component_output(const WiringPortRef &output);
        /** Wire this whole graph as a worker-hosted graph (RFC 0039): every
            node receives an identity, and one that cannot be checkpointed
            RECORDS why (``NodeCheckpointIdentity::refusal``) instead of
            refusing to wire. Most worker graphs are never captured and must
            still wire; the caller and the worker process must still arrive at
            the same identities without being told to. Call before adding a
            node. */
        void checkpoint_worker_graph();
        /** True inside a worker graph: a component wired here is an identity
            scope whether or not recovery is configured, and what it would
            refuse is recorded (``refuse_checkpoint_component``). */
        [[nodiscard]] bool checkpoint_records_refusals() const noexcept;
        /** Record ``reason`` on every node of the current component scope that
            has none. The worker-graph form of a component refusing to wire. */
        void refuse_checkpoint_component(std::string_view reason);

      private:
        friend class WiringObservationScope;
        friend class WiringPathScope;

        Wiring(WiringKind kind,
               WiringOptions options,
               std::shared_ptr<WiringObserverRegistry> observers,
               std::vector<std::string> path);
        [[nodiscard]] WiringScopeEvent begin_observation(WiringScopeEvent event);
        void end_observation(const WiringScopeEvent &event, std::string_view error);
        void apply_service_rank_dependencies();
        void finalize_extensions();
        void assign_checkpoint_identity(NodeBuilder &builder, std::span<const WiringInputRef> inputs);
        [[nodiscard]] NodeCheckpointIdentity checkpoint_identity_for(NodeBuilder &builder, std::span<const WiringInputRef> inputs);
        /** Shared body of finish()/snapshot(): validate + rank + build; the
            wiring GlobalState is moved when consuming, copied otherwise. */
        [[nodiscard]] GraphBuilder finish_top_level(bool consume_state);
        void validate_same_cycle_pairs(
            const std::unordered_map<const WiringInstance *, std::size_t> &index_of) const;

        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    /** Typed wiring handle: an output port carrying its static schema. */
    template <typename Schema>
    class Port
    {
      public:
        using schema = Schema;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        /** Erase to the runtime port form (the runtime schema comes from ``Schema``). */
        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        // A concrete ``Schema`` stamps its interned runtime schema onto the ref; a
        // non-concrete (type-variable) schema keeps the ref's runtime-resolved
        // schema — the form an operator graph overload's generic ``Port``
        // parameter receives (e.g. ``Port<TSL<TsVar<"V">>>``).
        void stamp_schema() noexcept
        {
            if (const auto *meta = schema_descriptor<Schema>::ts_meta(); meta != nullptr)
            {
                if (meta->kind == TSTypeKind::SIGNAL)
                {
                    if (ref_.schema != nullptr && ref_.schema->kind != TSTypeKind::SIGNAL)
                    {
                        ref_.observation_schema = ref_.schema;
                    }
                }
                else { ref_.observation_schema = nullptr; }
                ref_.schema = meta;
            }
        }

        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
    };

    /**
     * **Erased** output port: a generic node whose output type is only known at
     * wiring time (resolved from inferred type variables, not supplied explicitly)
     * returns this form, carrying the resolved runtime schema instead of a static
     * one. Downstream ``wire<>`` accepts it and matches/unifies against the runtime
     * schema. (A generic source wired with an explicit output schema returns the
     * ordinary typed ``Port<S>`` instead.)
     */
    template <>
    class Port<void>
    {
      public:
        using schema = void;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] const TSValueTypeMetaData      *runtime_schema() const noexcept { return ref_.schema; }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
    };

    template <fixed_string Name, typename S> struct NamedPort;

    /** Base of every ``Operator<>`` marker; ``wire<>`` routes a type deriving it to operator dispatch. */
    struct operator_tag
    {
    };

    namespace graph_wiring_detail
    {
        template <typename TImplementation>
        [[nodiscard]] NodeBuilder build_node_builder()
        {
            NodeBuilder nb;
            nb.implementation<TImplementation>();
            return nb;
        }

        // Recognise the typed port handle and recover an ``In<Name, S>``'s schema S.
        template <typename T> struct is_port : std::false_type {};
        template <typename S> struct is_port<Port<S>> : std::true_type {};
        template <fixed_string N, typename S> struct is_port<NamedPort<N, S>> : std::true_type {};
        // The erased port (``Port<void>``): carries only a runtime schema.
        template <typename T> struct is_erased_port : std::false_type {};
        template <> struct is_erased_port<Port<void>> : std::true_type {};
        template <typename T>
        struct dereferenced_static_schema
        {
            using type = T;
        };
        template <typename T>
        using dereferenced_static_schema_t = typename dereferenced_static_schema<T>::type;

        template <typename TSchema>
        struct dereferenced_static_schema<REF<TSchema>>
        {
            using type = dereferenced_static_schema_t<TSchema>;
        };
        template <typename TKey, typename TValueSchema>
        struct dereferenced_static_schema<TSD<TKey, TValueSchema>>
        {
            using type = TSD<TKey, dereferenced_static_schema_t<TValueSchema>>;
        };
        template <typename TElementSchema, auto FixedSize>
        struct dereferenced_static_schema<TSL<TElementSchema, FixedSize>>
        {
            using type = TSL<dereferenced_static_schema_t<TElementSchema>, FixedSize>;
        };
        template <typename TElementSchema>
        struct dereferenced_static_schema<Args<TElementSchema>>
        {
            using type = Args<dereferenced_static_schema_t<TElementSchema>>;
        };
        template <fixed_string Name, typename TSchema>
        struct dereferenced_field
        {
            using type = Field<Name, dereferenced_static_schema_t<TSchema>>;
        };
        template <typename TField>
        struct dereferenced_static_field;
        template <fixed_string Name, typename TSchema>
        struct dereferenced_static_field<Field<Name, TSchema>> : dereferenced_field<Name, TSchema>
        {
        };
        template <typename... TFields>
        struct dereferenced_static_schema<UnNamedTSB<TFields...>>
        {
            using type = UnNamedTSB<typename dereferenced_static_field<TFields>::type...>;
        };
        template <typename... TFields>
        struct dereferenced_static_schema<Kwargs<TFields...>>
        {
            using type = Kwargs<typename dereferenced_static_field<TFields>::type...>;
        };
        template <fixed_string Name, typename... TFields>
        struct dereferenced_static_schema<TSB<Name, TFields...>>
        {
            using type = TSB<Name, typename dereferenced_static_field<TFields>::type...>;
        };
        template <typename ValueBundle, typename... TFields> struct dereferenced_static_schema<NominalTSB<ValueBundle, TFields...>>
        {
            using type = NominalTSB<typename static_schema_detail::native_field_schema<ValueBundle>::type,
                                    typename dereferenced_static_field<TFields>::type...>;
        };

        template <auto Lhs, auto Rhs>
        [[nodiscard]] consteval bool static_size_equivalent()
        {
            using lhs = static_schema_detail::size_parameter_descriptor<Lhs>;
            using rhs = static_schema_detail::size_parameter_descriptor<Rhs>;
            if constexpr (lhs::is_concrete() && rhs::is_concrete())
            {
                return lhs::concrete_size() == rhs::concrete_size();
            }
            else if constexpr (!lhs::is_concrete() && !rhs::is_concrete())
            {
                return lhs::name() == rhs::name();
            }
            else { return false; }
        }

        template <typename Lhs, typename Rhs>
        struct static_schema_equivalent : std::bool_constant<std::is_same_v<Lhs, Rhs>>
        {
        };

        template <typename LhsElement, auto LhsSize, typename RhsElement, auto RhsSize>
        struct static_schema_equivalent<TSL<LhsElement, LhsSize>, TSL<RhsElement, RhsSize>>
            : std::bool_constant<static_schema_equivalent<LhsElement, RhsElement>::value &&
                                 static_size_equivalent<LhsSize, RhsSize>()>
        {
        };

        template <typename LhsKey, typename LhsValue, typename RhsKey, typename RhsValue>
        struct static_schema_equivalent<TSD<LhsKey, LhsValue>, TSD<RhsKey, RhsValue>>
            : std::bool_constant<std::is_same_v<LhsKey, RhsKey> &&
                                 static_schema_equivalent<LhsValue, RhsValue>::value>
        {
        };

        /** Read-only nullable widening. The runtime endpoint borrows the
            producer's payload and tracking; no conversion node is needed. */
        template <typename InputSchema, typename OutputSchema>
        struct statically_adapts_to_optional : std::false_type
        {
        };

        template <typename TValue>
        struct statically_adapts_to_optional<TS<Optional<TValue>>, TS<TValue>> : std::true_type
        {
        };

        template <typename InputSchema, typename OutputSchema>
        inline constexpr bool statically_accepts_output_v =
            std::is_same_v<InputSchema, SIGNAL> ||
            static_schema_equivalent<dereferenced_static_schema_t<InputSchema>,
                                     dereferenced_static_schema_t<OutputSchema>>::value ||
            statically_adapts_to_optional<InputSchema,
                                          dereferenced_static_schema_t<OutputSchema>>::value;
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef adapt_source_for_input(
            Wiring &w, const TSValueTypeMetaData *input_schema, WiringPortRef source);
        // Drop the leading ``Wiring &`` from a ``compose`` parameter tuple.
        template <typename Tuple> struct drop_first;
        template <typename A0, typename... As> struct drop_first<std::tuple<A0, As...>> { using type = std::tuple<As...>; };

        // Coerce a wiring-time scalar argument to its underlying value of type V.
        // The argument may be a plain value (used directly) or a ``Scalar<Name, T>``
        // selector (its value is unpacked) — so a scalar received as a node/graph
        // parameter can be forwarded straight on, without an explicit ``.value()``.
        // The names need not match; only the value type must be convertible.
        template <typename V, typename Arg>
        [[nodiscard]] V coerce_scalar_value(Arg &&arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (static_node_detail::is_scalar_selector<A>::value)
            {
                static_assert(std::is_convertible_v<typename A::value_type, V>,
                              "wire/build_graph: the Scalar<> argument's value type does not match the target "
                              "Scalar<> type");
                return static_cast<V>(arg.value());
            }
            else
            {
                static_assert(std::is_convertible_v<A, V>,
                              "wire/build_graph: scalar argument is not convertible to the target Scalar<> type");
                return static_cast<V>(std::forward<Arg>(arg));
            }
        }
        // Build a graph ``compose`` ``Scalar<Name, T>`` parameter from a wiring
        // argument (a plain value or a ``Scalar<>`` selector to unpack).
        template <typename ScalarParam, typename Arg>
        [[nodiscard]] ScalarParam make_scalar_param(Arg &&arg)
        {
            return ScalarParam{coerce_scalar_value<typename ScalarParam::value_type>(std::forward<Arg>(arg))};
        }
        template <typename X, typename OutSchema, typename... Args>
        struct wire_customization
        {
            static constexpr bool enabled = false;
        };

        enum class node_collection_pack_kind : std::uint8_t
        {
            none,
            tsl,
            tsb,
        };

        template <typename TSchema>
        struct node_collection_pack_kind_of
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::none>
        {
        };
        template <typename TElementSchema>
        struct node_collection_pack_kind_of<Args<TElementSchema>>
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::tsl>
        {
        };
        template <typename... TFields>
        struct node_collection_pack_kind_of<Kwargs<TFields...>>
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::tsb>
        {
        };
        template <typename TSchema>
        struct node_collection_pack_kind_of<REF<TSchema>> : node_collection_pack_kind_of<TSchema>
        {
        };

        template <typename Param, typename Arg> struct positional_argument : std::false_type {};

        template <fixed_string Name, typename S, auto... Policies, typename Arg>
        struct positional_argument<In<Name, S, Policies...>, Arg>
        {
            static constexpr bool matches()
            {
                if constexpr (!is_port<Arg>::value) { return false; }
                else if constexpr (is_erased_port<Arg>::value) { return true; }
                else { return statically_accepts_output_v<S, typename Arg::schema>; }
            }
            static constexpr bool value =
                node_collection_pack_kind_of<S>::value == node_collection_pack_kind::none &&
                static_node_detail::resolved_input_binding<Policies...>() != InputBinding::Context && matches();
        };

        template <fixed_string Name, typename T, typename Arg>
        struct positional_argument<Scalar<Name, T>, Arg> : std::bool_constant<
            static_node_detail::is_scalar_selector<Arg>::value || std::is_convertible_v<const Arg &, T>> {};

        template <typename X, typename OutSchema, typename... Args>
        consteval bool complete_positional_call()
        {
            if constexpr (!std::is_void_v<OutSchema> || !requires { &X::eval; } || requires { &X::compose; } ||
                          std::is_base_of_v<operator_tag, X> || requires { X::defaults(); } ||
                          !requires { typename signature_defaults<X>::no_defaults; } ||
                          wire_customization<X, OutSchema, Args...>::enabled) { return false; }
            else
            {
                using signature = StaticNodeSignature<X>;
                using params = typename signature::wire_param_types;
                if constexpr (signature::is_generic() || std::tuple_size_v<params> != sizeof...(Args)) { return false; }
                else
                {
                    return []<std::size_t... I>(std::index_sequence<I...>) {
                        return (positional_argument<std::tuple_element_t<I, params>, Args>::value && ...);
                    }(std::index_sequence_for<Args...>{});
                }
            }
        }

        HGRAPH_EXPORT void check_erased_input(const TSValueTypeMetaData *expected,
                                             const TSValueTypeMetaData *source);
        HGRAPH_EXPORT void check_port_cast(const TSValueTypeMetaData *expected,
                                            const TSValueTypeMetaData *source);

        template <typename S>
        void validate_port_cast_schema(const TSValueTypeMetaData *source)
        {
            static_assert(!std::is_void_v<S>, "Port::as<Schema>() requires a concrete output schema");
            check_port_cast(schema_descriptor<S>::ts_meta(), source);
        }

        [[nodiscard]] HGRAPH_EXPORT Value make_concrete_scalars(Wiring &w, const ValueTypeMetaData *schema);
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef add_concrete_node(
            Wiring &w, std::type_index definition, NodeBuilder builder,
            std::span<const WiringPortRef> inputs, Value scalars);
    }  // namespace graph_wiring_detail

    /** Wire a concrete static node with its complete positional input and scalar arguments. */
    template <typename X, typename OutSchema = void, typename... Args>
        requires (graph_wiring_detail::complete_positional_call<X, OutSchema, Args...>())
    auto wire(Wiring &w, const Args &...args)
    {
        using signature = StaticNodeSignature<X>;
        using params = typename signature::wire_param_types;
        std::array<WiringPortRef, signature::input_count()> inputs;
        Value scalars;
        if constexpr (signature::scalar_count() > 0)
        {
            scalars = graph_wiring_detail::make_concrete_scalars(w, signature::scalar_schema());
        }
        [[maybe_unused]] std::size_t input_index = 0;
        [[maybe_unused]] auto assign = [&]<typename Param>(const auto &argument) {
            if constexpr (static_node_detail::is_input_selector<Param>::value)
            {
                const auto *expected = static_node_detail::input_selector_traits<Param>::ts_meta();
                auto source = argument.erased();
                if constexpr (graph_wiring_detail::is_erased_port<std::remove_cvref_t<decltype(argument)>>::value)
                {
                    graph_wiring_detail::check_erased_input(expected, source.schema);
                }
                inputs[input_index++] = graph_wiring_detail::adapt_source_for_input(w, expected, std::move(source));
            }
            else
            {
                using T = typename Param::value_type;
                auto mutation = scalars.as_bundle().begin_mutation();
                mutation[Param::field_name.sv()].set(graph_wiring_detail::coerce_scalar_value<T>(argument));
            }
        };
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (assign.template operator()<std::tuple_element_t<I, params>>(args), ...);
        }(std::index_sequence_for<Args...>{});

        WiringPortRef out = graph_wiring_detail::add_concrete_node(
            w, typeid(X), graph_wiring_detail::build_node_builder<X>(), inputs, std::move(scalars));
        if constexpr (signature::has_output()) { return Port<typename signature::output_schema_type>{w, std::move(out)}; }
    }

    template <typename Schema>
    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<Schema>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }

    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<void>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }
    /**
     * Compile-time reflection of a graph's ``compose`` signature — the graph-level
     * mirror of ``StaticNodeSignature``. It reflects ``&G::compose`` **skipping the
     * leading ``Wiring&``**: ``Port`` parameters are the graph's time-series inputs,
     * ``Scalar`` parameters are its scalar inputs, and the return type is its
     * time-series output(s).
     */
    template <typename G>
    struct StaticGraphSignature
    {
      private:
        using compose_args = typename static_node_detail::fn_traits<decltype(&G::compose)>::args_tuple;
        using params       = typename graph_wiring_detail::drop_first<compose_args>::type;
        using indices      = std::make_index_sequence<std::tuple_size_v<params>>;

        template <std::size_t... I>
        static constexpr std::size_t count_ports(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (graph_wiring_detail::is_port<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_scalars(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_scalar_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

      public:
        /** Tuple of the ``compose`` parameter selector types (``Port`` / ``Scalar``), in order. */
        using param_types = params;
        /** The graph's time-series output type (the ``compose`` return type), or ``void``. */
        using output_type = typename static_node_detail::fn_traits<decltype(&G::compose)>::return_type;

        [[nodiscard]] static constexpr std::size_t param_count() { return std::tuple_size_v<params>; }
        [[nodiscard]] static constexpr std::size_t input_count() { return count_ports(indices{}); }
        [[nodiscard]] static constexpr std::size_t scalar_count() { return count_scalars(indices{}); }
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_GRAPH_WIRING_CORE_H
