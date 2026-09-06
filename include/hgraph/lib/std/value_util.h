#ifndef HGRAPH_LIB_STD_VALUE_UTIL_H
#define HGRAPH_LIB_STD_VALUE_UTIL_H

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <concepts>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * Small value-layer construction helpers for ordinary scalar containers.
     *
     * These functions are the convenient public counterpart to the lower-level
     * ``ListBuilder`` / ``SetBuilder`` / ``MapBuilder`` APIs. They use the static
     * scalar descriptors to register/find canonical scalar bindings, then produce
     * immutable compact ``Value`` containers with the standard value-layer schema.
     */

    /**
     * The value bindings a node's ``start`` hook reads off its bound output
     * and carries in ``State`` for ``eval`` (lock-free per-tick ruling
     * 2026-07-02; std-operator audit 2026-08-15). Nothing here resolves a
     * realization: a TS-valued output's layout carries the realized value
     * binding and a graph-local representation published its portable
     * owning type when it was realized (``value_owning_type``); a TSS
     * output's set layout carries its key binding; a compact container's
     * plan carries its element / key / value bindings
     * (``compact_element_binding`` / ``compact_map_bindings``). Interning a
     * compact result type for a non-compact output (a fixed array, a TSS) is
     * a start-time registry lookup, which the ruling allows.
     *
     * ``primary``/``secondary`` meaning is the owning node's: a collection
     * kernel caches its element binding in ``primary``; a map kernel caches
     * key in ``primary`` and element in ``secondary``. ``result`` is the
     * output's own portable value type, so eval publishes via
     * ``build_storage()`` + ``Value{result, &storage}`` — the builders' plain
     * ``build()`` re-interns the result type per call, which is itself a
     * type-system lock.
     */
    struct ResolvedBindings
    {
        ValueTypeRef primary{nullptr};
        ValueTypeRef secondary{nullptr};
        ValueTypeRef result{nullptr};
    };

    /** True when an output's storage carries a portable value type readable
        off its layout without resolution: a TS-valued output's realized value
        (or the external owner a graph-local representation published at
        realization) and a fixed structured output's (TSB, fixed TSL) bundle
        or list, which its storage answers from its own state. A slot-backed
        or dynamic output (TSS, TSD, dynamic TSL, TSW) is not one: its value
        surface is a projection over its storage whose owning type is resolved
        per call, and its bindings are on its own layout
        (``resolve_set_bindings`` reads the TSS one). */
    [[nodiscard]] constexpr bool output_carries_value_binding(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
            case TSTypeKind::TS:
            case TSTypeKind::TSB: return true;
            case TSTypeKind::TSL: return schema.fixed_size() > 0;
            default: return false;
        }
    }

    /** The portable value type of an output that carries one
        (``output_carries_value_binding``): the storage's realized binding, or
        the external owning type a graph-local representation published at
        realization. Read from the bound view; never resolved. */
    [[nodiscard]] inline ValueTypeRef output_value_binding(const TSOutputView &out)
    {
        const auto *schema = out.schema();
        if (schema == nullptr || !output_carries_value_binding(*schema))
        {
            throw std::logic_error(
                "output_value_binding: only a TS, TSB or fixed TSL output carries a portable value binding");
        }
        const auto binding = value_owning_type(out.data_view().layout().value_binding);
        if (binding == nullptr) { throw std::logic_error("output has no realized value binding"); }
        return binding;
    }

    /** The bindings of a list / set type: element in ``primary``, the type a
        builder publishes as in ``result``. A compact container answers from
        its plan and is its own result; a non-compact indexed realization (a
        fixed array) answers its element from its ops and publishes through a
        compact source list the target's assignment materialises. */
    [[nodiscard]] inline ResolvedBindings collection_bindings_of(const ValueTypeRef &collection)
    {
        const auto *ops = collection ? collection.ops() : nullptr;
        if (ops == nullptr) { throw std::logic_error("collection bindings: unbound collection type"); }
        switch (ops->kind)
        {
            case ValueOpsKind::List:
            case ValueOpsKind::Set:
            case ValueOpsKind::CyclicBuffer:
            case ValueOpsKind::Queue:
                if (ops == &compact_set_ops() || ops == &compact_cyclic_buffer_ops() || ops == &compact_queue_ops() ||
                    ops == &compact_list_ops() || collection.lifecycle_context() != nullptr)
                {
                    // Every compact strategy; compact_element_binding
                    // validates the table and reads the plan's element.
                    return ResolvedBindings{.primary = compact_element_binding(collection), .result = collection};
                }
                [[fallthrough]];
            case ValueOpsKind::Indexed:
            {
                const auto *indexed = static_cast<const IndexedValueOps *>(ops);
                if (indexed->element_binding == nullptr)
                {
                    throw std::logic_error("collection bindings: the realization exposes no element binding");
                }
                const auto element = indexed->element_binding(indexed->context, nullptr, 0);
                if (element == nullptr) { throw std::logic_error("collection bindings: unresolved element binding"); }
                return ResolvedBindings{.primary = element, .result = compact_list_type(element, *collection.schema())};
            }
            default: throw std::logic_error("collection bindings: the output is not a list or set");
        }
    }

    /** The bindings of a compact map type: key in ``primary``, value in ``secondary``. */
    [[nodiscard]] inline ResolvedBindings map_bindings_of(const ValueTypeRef &map)
    {
        const auto [key, value] = compact_map_bindings(map);
        return ResolvedBindings{.primary = key, .secondary = value, .result = map};
    }

    /** The ResolvedBindings of a scalar-list / tuple output. start-hook only. */
    [[nodiscard]] inline ResolvedBindings resolve_list_bindings(const TSOutputView &out)
    {
        return collection_bindings_of(output_value_binding(out));
    }

    /** The ResolvedBindings of a scalar-set output, or of a TSS output (its
        element is the set layout's key binding; the compact set the node
        builds and diffs against the output is the result). */
    [[nodiscard]] inline ResolvedBindings resolve_set_bindings(const TSOutputView &out)
    {
        if (out.schema() != nullptr && out.schema()->kind == TSTypeKind::TSS)
        {
            const auto key = out.data_view().as_set().layout().key_binding;
            if (key == nullptr) { throw std::logic_error("TSS output layout has no key binding"); }
            return ResolvedBindings{.primary = key, .result = compact_set_type(key)};
        }
        return collection_bindings_of(output_value_binding(out));
    }

    /** The ResolvedBindings of a scalar-map output. */
    [[nodiscard]] inline ResolvedBindings resolve_map_bindings(const TSOutputView &out)
    {
        return map_bindings_of(output_value_binding(out));
    }

    /** Lock-free per-tick construction from start-resolved bindings — the
        builders' own ``build()`` re-interns the result type per call. */
    [[nodiscard]] inline MapBuilder map_builder_for(const ResolvedBindings &bindings)
    {
        return MapBuilder{bindings.primary, bindings.secondary};
    }

    [[nodiscard]] inline Value finish_map(MapBuilder &builder, const ResolvedBindings &bindings)
    {
        MapStorage storage = builder.build_storage();
        return Value{bindings.result, &storage};
    }

    [[nodiscard]] inline Value finish_list(ListBuilder &builder, const ResolvedBindings &bindings)
    {
        ListStorage storage = builder.build_storage();
        return Value{bindings.result, &storage};
    }

    [[nodiscard]] inline Value finish_set(SetBuilder &builder, const ResolvedBindings &bindings)
    {
        SetStorage storage = builder.build_storage();
        return Value{bindings.result, &storage};
    }

    template <typename T>
    [[nodiscard]] ValueTypeRef scalar_value_binding()
    {
        const auto *meta    = scalar_descriptor<std::remove_cvref_t<T>>::value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        if (binding == nullptr) { throw std::logic_error("scalar value type has no canonical binding"); }
        return binding;
    }

    template <typename T, typename U>
    requires std::constructible_from<std::remove_cvref_t<T>, U &&>
    [[nodiscard]] Value value(U &&input)
    {
        using ValueT = std::remove_cvref_t<T>;
        return Value{ValueT{std::forward<U>(input)}};
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_list(TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        ListBuilder builder{scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push_back(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_list(std::initializer_list<T> values)
    {
        return make_list<std::remove_cv_t<T>>(values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_set(TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        SetBuilder builder{scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            (void)builder.insert(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_set(std::initializer_list<T> values)
    {
        return make_set<std::remove_cv_t<T>>(values.begin(), values.end());
    }

    template <typename TKey, typename TValue, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    [[nodiscard]] Value make_map(TIterator first, TSentinel last)
    {
        using KeyT   = std::remove_cvref_t<TKey>;
        using ValueT = std::remove_cvref_t<TValue>;

        MapBuilder builder{scalar_value_binding<KeyT>(), scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            KeyT   key{(*first).first};
            ValueT value{(*first).second};
            builder.set_item(key, value);
        }
        return builder.build();
    }

    template <typename TKey, typename TValue>
    [[nodiscard]] Value make_map(std::initializer_list<std::pair<TKey, TValue>> values)
    {
        return make_map<TKey, TValue>(values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_cyclic_buffer(std::size_t capacity, TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        CyclicBufferBuilder builder{scalar_value_binding<ValueT>(), capacity};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push_back(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_cyclic_buffer(std::size_t capacity, std::initializer_list<T> values)
    {
        return make_cyclic_buffer<std::remove_cv_t<T>>(capacity, values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_queue(TIterator first, TSentinel last, std::size_t max_capacity = 0)
    {
        using ValueT = std::remove_cvref_t<T>;
        QueueBuilder builder{scalar_value_binding<ValueT>(), max_capacity};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_queue(std::initializer_list<T> values, std::size_t max_capacity = 0)
    {
        return make_queue<std::remove_cv_t<T>>(values.begin(), values.end(), max_capacity);
    }
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    /** Names the opaque scalar backing ``State<ResolvedBindings>``. */
    template <>
    struct scalar_name<stdlib::ResolvedBindings>
    {
        static constexpr std::string_view value{"ResolvedBindings"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph
{
    // ResolvedBindings backs node State across the runtime, the Python
    // module, and extensions; keep one exported plan/ops address (see the
    // standard-scalar-binding note in type_registry.h).
    extern template HGRAPH_EXPORT const MemoryUtils::StoragePlan &
    MemoryUtils::plan_for<stdlib::ResolvedBindings>() noexcept;
    extern template HGRAPH_EXPORT const ValueOps &ops_for<stdlib::ResolvedBindings>() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_LIB_STD_VALUE_UTIL_H
