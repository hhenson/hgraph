#ifndef HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H
#define HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H

#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    /**
     * Per-kind ops surfaces for the value-layer container kinds.
     *
     * This header defines only the *type* surfaces — the layout-agnostic
     * vtables that views call through. Each storage flavour
     * (compact value-layer, slot-store-backed time-series, etc.) lives
     * in its own ``*_container_ops.h`` and supplies the matching
     * thunks plus a canonical-binding factory. The compact value-layer
     * implementation lives in ``compact_container_ops.h``.
     *
     * Each container kind extends the base ``ValueOps`` (hash / equals
     * / compare / to_string) with the read accessors the typed views
     * need — ``size``, ``element_at``, ``contains``, ``front``,
     * ``head``, etc. The typed view receives a pointer to the
     * kind-specific ops sub-class (checked from the binding's
     * ``const ValueOps *``) and calls through it without ever casting
     * the storage data pointer to a concrete C++ storage class.
     *
     * Order semantics:
     *
     * - ``List`` / ``CyclicBuffer`` / ``Queue`` — order-dependent
     *   (lexicographic hash and compare).
     * - ``Set`` — order-independent (xor of element hashes;
     *   subset+superset for equality).
     * - ``Map`` — order-independent (xor of (key, value) pair hashes;
     *   same key set with same key→value for equality).
     */

    // -----------------------------------------------------------------
    // Per-kind ops hierarchy — mirrors the specialised-view hierarchy.
    //
    //   ValueOps              — hash, equals, compare, to_string
    //     IndexedValueOps     — adds size, element_at, element_binding
    //                           (used by every kind whose view exposes
    //                            indexed iteration)
    //       ListValueOps      — no additions
    //       CyclicBufferValueOps — adds head
    //       QueueValueOps     — adds front
    //       SetValueOps       — adds contains
    //       MapValueOps       — adds contains plus the value side
    //                           (value_at by key, value_at_index,
    //                            value_binding). ``element_at`` /
    //                           ``element_binding`` on the indexed
    //                           base point at the *key* surface, so
    //                           the map iterator walks keys via the
    //                           base ops and pairs them with values
    //                           via the derived ops.
    //
    // Views reach the kind-specific surface through ``checked_value_ops``,
    // which validates the runtime ops ABI before narrowing the pointer.
    // No view ever casts the storage data pointer to a concrete C++
    // storage class — that keeps the views layout-agnostic.
    // -----------------------------------------------------------------

    // Forward-declared because ``MapValueOps::key_set`` returns a
    // ``SetView`` by value. The view definition lives in
    // ``specialized_views.h``.
    class SetView;

    struct IndexedValueOps : ValueOps
    {
        std::size_t (*size)(const void *context, const void *memory) noexcept = nullptr;
        const void *(*element_at)(const void *context, const void *memory, std::size_t index) = nullptr;
        ValueTypeRef (*element_binding)(const void *context, const void *memory,
                                                   std::size_t index) noexcept = nullptr;

        /**
         * Iteration is exposed as a single op that returns a
         * ``Range<ValueView>``. The range carries an opaque
         * ``context`` (ops state), a ``memory`` pointer (value
         * storage), an ordinal ``limit`` (the high water mark of the
         * index space), an optional ``predicate`` filter (null for
         * dense layouts; non-null for slot-stored layouts that need
         * to skip dead slots) and a ``projector`` that builds a
         * ``ValueView`` from ``(context, memory, index)``.
         * Range-based views call ``make_range(context, memory)`` and
         * use the range's own iterator surface, which keeps
         * iteration layout-agnostic.
         */
        Range<ValueView> (*make_range)(const void *context, const void *memory) = nullptr;
        Range<ValueView> (*make_mutable_range)(const void *context, void *memory) = nullptr;
        /** Mutable element access (LAST member: positional initializers stay
            valid). Null = fall back to ``element_at``. A Bundle installs one
            that MARKS the field live (field validity, core_concepts.rst) and
            returns real memory even when unset. */
        void *(*mutable_element_at)(const void *context, void *memory, std::size_t index) = nullptr;
        /** Change the logical extent of bounded indexed storage. Null for
            representations whose size is structural or fixed exactly. */
        void (*resize)(const void *context, void *memory, std::size_t size) = nullptr;
    };

    struct ListValueOps : IndexedValueOps
    {
    };

    /**
     * Structural-mutation surface for a *mutable* (dynamic) list. The base
     * ``ListValueOps`` read surface stays unchanged; this extends it with the
     * hooks ``MutableListView`` calls. ``push_back`` / ``set`` copy from the
     * supplied typed element; ``clear`` destroys all elements. Only bindings
     * over mutable list storage install these; the compact (immutable) list
     * leaves them null.
     */
    struct MutableListValueOps : ListValueOps
    {
        void (*push_back)(const void *context, void *memory, ValueTypeRef element_type,
                          const void *element) = nullptr;
        void (*set_element)(const void *context, void *memory, std::size_t index,
                            ValueTypeRef element_type, const void *element) = nullptr;
        /** Remove the element at ``index``, shifting later elements down. */
        void (*erase)(const void *context, void *memory, std::size_t index) = nullptr;
        void (*pop_back)(const void *context, void *memory) = nullptr;
        void (*clear)(const void *context, void *memory) = nullptr;
        /** Append an UNSET element - a hole (element validity); LAST member
            so positional initializers stay valid. */
        void (*push_back_unset)(const void *context, void *memory) = nullptr;
    };

    struct CyclicBufferValueOps : IndexedValueOps
    {
        std::size_t (*head)(const void *memory) noexcept = nullptr;
    };

    struct QueueValueOps : IndexedValueOps
    {
        const void *(*front)(const void *memory) = nullptr;
    };

    struct SetValueOps : IndexedValueOps
    {
        bool (*contains)(const void *context, const void *memory, const void *key) = nullptr;
    };

    /**
     * Mutation surface for a structurally-mutable set. ``add`` inserts a copy of
     * the supplied key (no-op on a present key); ``remove`` removes a key;
     * ``clear`` empties the set. Both return whether the set changed.
     */
    struct MutableSetValueOps : SetValueOps
    {
        bool (*add)(const void *context, void *memory, const void *key) = nullptr;
        bool (*remove)(const void *context, void *memory, const void *key) = nullptr;
        void (*clear)(const void *context, void *memory) = nullptr;
    };

    struct MapValueOps : IndexedValueOps
    {
        bool (*contains)(const void *context, const void *memory, const void *key) = nullptr;
        const void *(*value_at)(const void *context, const void *memory, const void *key) = nullptr;
        const void *(*value_at_index)(const void *context, const void *memory, std::size_t index) = nullptr;
        ValueTypeRef (*value_binding)(const void *context, const void *memory) noexcept = nullptr;
        /**
         * Three iteration surfaces. Each follows the predicate /
         * projector pattern; the bound thunks decide what the
         * projector returns.
         *
         * - ``make_keys_range`` — yields ``ValueView`` over keys
         *   only. Equivalent to the ``IndexedValueOps::make_range``
         *   inherited from the base (the map's indexed surface
         *   walks keys), but exposed on ``MapValueOps`` for
         *   discoverability. The map-specific range builders receive the
         *   same ops context as the indexed base so non-compact layouts can
         *   project through type-erased layout state.
         * - ``make_values_range`` — yields ``ValueView`` over
         *   values only.
         * - ``make_kv_range`` — yields ``std::pair<ValueView,
         *   ValueView>`` (key, value) pairs.
         */
        Range<ValueView> (*make_keys_range)(const void *context, const void *memory) = nullptr;
        Range<ValueView> (*make_values_range)(const void *context, const void *memory) = nullptr;
        KeyValueRange<ValueView, ValueView> (*make_kv_range)(const void *context,
                                                             const void *memory) = nullptr;
        /**
         * Build a read-only ``SetView`` over the map's keys. The
         * returned view is a *wrapper* over the map's memory — it
         * uses set-shaped ops that delegate into the map's read
         * accessors, so the implementation does not require unifying
         * the map and set storage layouts. Each map kind installs its
         * own ``key_set`` thunk paired with a SetValueOps adapter
         * that knows that map's layout.
         */
        SetView (*key_set)(const void *context, ValueTypeRef map_binding, const void *memory) = nullptr;
    };

    /**
     * Structural-mutation surface for a *mutable* (dynamic) map. Extends the
     * ``MapValueOps`` read surface with the hooks ``MutableMapView`` calls.
     * ``insert`` copies the supplied key and value (or replaces the value if
     * the key is present); ``erase`` removes a key; ``clear`` empties the map.
     * Only bindings over mutable map storage install these.
     */
    struct MutableMapValueOps : MapValueOps
    {
        void (*insert)(const void *context, void *memory, const void *key, const void *value) = nullptr;
        void (*erase)(const void *context, void *memory, const void *key) = nullptr;
        void (*clear)(const void *context, void *memory) = nullptr;
        /**
         * Return mutable value memory for ``key``, default-constructing an entry
         * (with a default value) when the key is absent. Lets callers assign the
         * value in place — avoiding building and copying a temporary value.
         * Requires a default-constructible value type.
         */
        void *(*value_or_emplace)(const void *context, void *memory, const void *key) = nullptr;
    };

    namespace value_ops_detail
    {
        template <typename Ops>
        inline constexpr bool supported_ops_type_v =
            std::is_same_v<Ops, ValueOps> || std::is_same_v<Ops, IndexedValueOps> ||
            std::is_same_v<Ops, ListValueOps> || std::is_same_v<Ops, MutableListValueOps> ||
            std::is_same_v<Ops, CyclicBufferValueOps> || std::is_same_v<Ops, QueueValueOps> ||
            std::is_same_v<Ops, SetValueOps> || std::is_same_v<Ops, MutableSetValueOps> ||
            std::is_same_v<Ops, MapValueOps> || std::is_same_v<Ops, MutableMapValueOps>;

        [[nodiscard]] constexpr std::string_view value_ops_kind_name(ValueOpsKind kind) noexcept
        {
            switch (kind)
            {
            case ValueOpsKind::Invalid: return "Invalid";
            case ValueOpsKind::Base: return "Base";
            case ValueOpsKind::Indexed: return "Indexed";
            case ValueOpsKind::List: return "List";
            case ValueOpsKind::MutableList: return "MutableList";
            case ValueOpsKind::CyclicBuffer: return "CyclicBuffer";
            case ValueOpsKind::Queue: return "Queue";
            case ValueOpsKind::Set: return "Set";
            case ValueOpsKind::MutableSet: return "MutableSet";
            case ValueOpsKind::Map: return "Map";
            case ValueOpsKind::MutableMap: return "MutableMap";
            }
            return "Unknown";
        }

        template <typename Ops>
        [[nodiscard]] consteval ValueOpsKind requested_ops_kind()
        {
            static_assert(supported_ops_type_v<Ops>, "unsupported ValueOps type");
            if constexpr (std::is_same_v<Ops, ValueOps>) return ValueOpsKind::Base;
            else if constexpr (std::is_same_v<Ops, IndexedValueOps>) return ValueOpsKind::Indexed;
            else if constexpr (std::is_same_v<Ops, ListValueOps>) return ValueOpsKind::List;
            else if constexpr (std::is_same_v<Ops, MutableListValueOps>) return ValueOpsKind::MutableList;
            else if constexpr (std::is_same_v<Ops, CyclicBufferValueOps>) return ValueOpsKind::CyclicBuffer;
            else if constexpr (std::is_same_v<Ops, QueueValueOps>) return ValueOpsKind::Queue;
            else if constexpr (std::is_same_v<Ops, SetValueOps>) return ValueOpsKind::Set;
            else if constexpr (std::is_same_v<Ops, MutableSetValueOps>) return ValueOpsKind::MutableSet;
            else if constexpr (std::is_same_v<Ops, MapValueOps>) return ValueOpsKind::Map;
            else return ValueOpsKind::MutableMap;
        }

        [[nodiscard]] constexpr bool value_ops_compatible(ValueOpsKind actual,
                                                          ValueOpsKind requested) noexcept
        {
            if (actual < ValueOpsKind::Base || actual > ValueOpsKind::MutableMap) return false;
            switch (requested)
            {
            case ValueOpsKind::Base: return true;
            case ValueOpsKind::Indexed: return actual != ValueOpsKind::Base;
            case ValueOpsKind::List:
                return actual == ValueOpsKind::List || actual == ValueOpsKind::MutableList;
            case ValueOpsKind::MutableList: return actual == ValueOpsKind::MutableList;
            case ValueOpsKind::CyclicBuffer: return actual == ValueOpsKind::CyclicBuffer;
            case ValueOpsKind::Queue: return actual == ValueOpsKind::Queue;
            case ValueOpsKind::Set:
                return actual == ValueOpsKind::Set || actual == ValueOpsKind::MutableSet;
            case ValueOpsKind::MutableSet: return actual == ValueOpsKind::MutableSet;
            case ValueOpsKind::Map:
                return actual == ValueOpsKind::Map || actual == ValueOpsKind::MutableMap;
            case ValueOpsKind::MutableMap: return actual == ValueOpsKind::MutableMap;
            case ValueOpsKind::Invalid: return false;
            }
            return false;
        }
    }  // namespace value_ops_detail

    template <typename Ops>
        requires(value_ops_detail::supported_ops_type_v<Ops>)
    [[nodiscard]] inline const Ops *try_value_ops(const ValueOps *ops) noexcept
    {
        if (ops == nullptr ||
            !value_ops_detail::value_ops_compatible(ops->kind, value_ops_detail::requested_ops_kind<Ops>()))
        {
            return nullptr;
        }
        return static_cast<const Ops *>(ops);
    }

    template <typename Ops>
        requires(value_ops_detail::supported_ops_type_v<Ops>)
    [[nodiscard]] inline const Ops *try_value_ops(ValueTypeRef binding) noexcept
    {
        return binding ? try_value_ops<Ops>(binding.ops()) : nullptr;
    }

    template <typename Ops>
        requires(value_ops_detail::supported_ops_type_v<Ops>)
    [[nodiscard]] inline const Ops *checked_value_ops(const ValueOps *ops, std::string_view consumer)
    {
        if (const auto *result = try_value_ops<Ops>(ops); result != nullptr) return result;
        const auto actual = ops != nullptr ? value_ops_detail::value_ops_kind_name(ops->kind) : std::string_view{"null"};
        throw std::logic_error(std::string{consumer} + ": requested " +
                               std::string{value_ops_detail::value_ops_kind_name(
                                   value_ops_detail::requested_ops_kind<Ops>())} +
                               " ValueOps, actual " + std::string{actual});
    }

    template <typename Ops>
        requires(value_ops_detail::supported_ops_type_v<Ops>)
    [[nodiscard]] inline const Ops *checked_value_ops(ValueTypeRef binding, std::string_view consumer)
    {
        return checked_value_ops<Ops>(binding ? binding.ops() : nullptr, consumer);
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_CONTAINER_OPS_H
