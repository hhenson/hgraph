#ifndef HGRAPH_TYPES_METADATA_TYPE_REALIZATION_H
#define HGRAPH_TYPES_METADATA_TYPE_REALIZATION_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value_type_ref.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace hgraph
{
    class GlobalStateView;
    class TypeRegistry;

    enum class PolymorphicCompoundStoragePolicy : std::uint8_t
    {
        Inline,
        Pooled,
    };

    struct TypeRealizationOptions
    {
        PolymorphicCompoundStoragePolicy polymorphic_compound_storage{
            PolymorphicCompoundStoragePolicy::Inline};

        [[nodiscard]] friend constexpr bool
        operator==(const TypeRealizationOptions &, const TypeRealizationOptions &) noexcept = default;
    };

    /** Store graph type-realisation policy in wiring-time GlobalState. */
    HGRAPH_EXPORT void set_type_realization_options(GlobalStateView state,
                                                    TypeRealizationOptions options);
    /** Convenience graph-level switch for the pooled polymorphic strategy. */
    HGRAPH_EXPORT void set_pooled_compound_scalar_storage(GlobalStateView state,
                                                          bool enabled = true);
    /** Read graph type-realisation policy; an absent entry selects inline storage. */
    [[nodiscard]] HGRAPH_EXPORT TypeRealizationOptions
    type_realization_options(GlobalStateView state);

    enum class GraphValueRepresentation : std::uint8_t
    {
        Exact,
        InlineUnion,
        PooledUnion,
    };

    /** Data-only debugger/diagnostic view; no private strategy layout leaks. */
    struct TypeRealizationInspection
    {
        GraphValueRepresentation representation{GraphValueRepresentation::Exact};
        std::size_t alternative_count{0};
        std::size_t minimum_leaf_size{0};
        std::size_t maximum_leaf_size{0};
        std::size_t external_size{0};
        std::size_t graph_size{0};
    };

    /**
     * Immutable graph-wiring snapshot of the registered Bundle hierarchy.
     *
     * Canonical exact bindings remain process-wide. A snapshot adds a
     * graph-specific closed-union binding for a declared Bundle that had
     * registered children when the snapshot was captured, and for an abstract
     * Bundle whose construction must be resolved to a concrete descendant.
     * Later registrations cannot change its alternatives or storage size.
     */
    class HGRAPH_CLASS_EXPORT TypeRealizationSnapshot
        : public std::enable_shared_from_this<TypeRealizationSnapshot>
    {
      public:
        [[nodiscard]] static std::shared_ptr<const TypeRealizationSnapshot>
        capture(TypeRegistry &registry, TypeRealizationOptions options = {});

        [[nodiscard]] std::uint64_t generation() const noexcept;
        [[nodiscard]] TypeRealizationOptions options() const noexcept;
        [[nodiscard]] bool pooled_compound_storage_enabled() const noexcept;
        [[nodiscard]] bool is_polymorphic(const ValueTypeMetaData *schema) const noexcept;
        [[nodiscard]] std::vector<const ValueTypeMetaData *>
        alternatives(const ValueTypeMetaData *schema) const;
        /** Realize one exact schema without replacing it by its closed union. */
        [[nodiscard]] ValueTypeRef exact_type_for(const ValueTypeMetaData *schema) const;
        [[nodiscard]] ValueTypeRef type_for(const ValueTypeMetaData *schema) const;
        /** Realize storage owned by a graph, including eligible pooled unions. */
        [[nodiscard]] ValueTypeRef graph_type_for(const ValueTypeMetaData *schema) const;
        [[nodiscard]] TypeRealizationInspection
        inspect(const ValueTypeMetaData *schema) const;

      private:
        struct Impl;
        explicit TypeRealizationSnapshot(std::shared_ptr<Impl> impl) noexcept;

        std::shared_ptr<Impl> impl_;
    };

    /** Thread-local construction scope used while a graph creates its nodes. */
    class HGRAPH_CLASS_EXPORT TypeRealizationScope
    {
      public:
        explicit TypeRealizationScope(const TypeRealizationSnapshot *snapshot) noexcept;
        TypeRealizationScope(const TypeRealizationScope &) = delete;
        TypeRealizationScope &operator=(const TypeRealizationScope &) = delete;
        ~TypeRealizationScope() noexcept;

      private:
        const TypeRealizationSnapshot *previous_{nullptr};
    };

    [[nodiscard]] HGRAPH_EXPORT const TypeRealizationSnapshot *active_type_realization() noexcept;

    /** Limits graph-local representation selection to graph storage construction. */
    class HGRAPH_CLASS_EXPORT GraphValueRealizationScope
    {
      public:
        GraphValueRealizationScope() noexcept;
        GraphValueRealizationScope(const GraphValueRealizationScope &) = delete;
        GraphValueRealizationScope &operator=(const GraphValueRealizationScope &) = delete;
        ~GraphValueRealizationScope() noexcept;

      private:
        bool previous_{false};
    };

    [[nodiscard]] HGRAPH_EXPORT bool active_graph_value_realization() noexcept;

    /** Resolve through the active snapshot and the current storage scope. */
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef
    value_type_for_active_realization(const ValueTypeMetaData *schema);

    /** Resolve storage for a wiring-time value. Reuse an enclosing graph's
        closed-union snapshot, or capture the current registered hierarchy
        under ``options`` (the wiring's ``realization_options()``: its bound
        seed's configuration -- never an ambient state). */
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef
    value_type_for_wiring(const ValueTypeMetaData *schema,
                          const TypeRealizationOptions &options = {});

    /** Test-only registry teardown hook; call after TypeRecordRegistry reset. */
    HGRAPH_EXPORT void clear_type_realization_snapshots() noexcept;
}

#endif
