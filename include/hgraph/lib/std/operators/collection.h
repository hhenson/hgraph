#ifndef HGRAPH_LIB_STD_OPERATORS_COLLECTION_H
#define HGRAPH_LIB_STD_OPERATORS_COLLECTION_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <cstddef>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Collection operator **definitions** (markers only): aggregations / reductions, set
     * operations and ``TSD`` (dictionary) re-shaping. Mirrors the Python ``hgraph``
     * aggregation operators (``_operators.py``) and the TSD / mapping operators
     * (``_tsd_and_mapping.py``). The aggregations and set operations are **variadic**:
     * unary = over a collection / running, n-ary = element-wise.
     */

    // ---- Aggregations / reductions ----

    /** Sum numeric values according to input shape and arity.
        Unary scalar input produces a running sum; unary collection input reduces its
        current members; multiple inputs are added element by element. An optional reset
        clears running state before admitting a same-cycle source tick.
        @param ts Value, collection, or variadic inputs to sum.
        @param reset Optional signal that resets a running sum to its identity.
        @param __strict__ When true, require every variadic input to be valid.
        @return The running, reduced, or element-wise sum selected by overload resolution.
        @par Python example
        @code{.py}
        running_total = hg.sum_(amount, reset=session_start)
        basket_total = hg.sum_(prices)
        @endcode */
    struct sum_ : Operator<"sum_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Calculate a mean according to input shape and arity.
        Unary scalar input produces a running mean; unary collection input averages its
        current members; multiple inputs are averaged element by element.
        @param ts Value, collection, or variadic inputs to average.
        @param default_value Fallback used when a collection has no values to average.
        @return The overload-selected mean, promoted to floating point where required.
        @par Python example
        @code{.py}
        running_average = hg.mean(price)
        cross_sectional_average = hg.mean(prices_by_symbol)
        @endcode */
    struct mean : Operator<"mean", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Calculate standard deviation according to input shape and arity.
        Unary scalar input is running; unary collection input reduces current members;
        multiple inputs are evaluated element by element.
        @param ts Value, collection, window, or variadic inputs.
        @param ddof Delta degrees of freedom: the variance divisor is ``N - ddof``.
        @param default_value Fallback used when the selected sample cannot produce a result.
        @return Standard deviation using the overload-selected numeric schema.
        @par Python example
        @code{.py}
        sample_volatility = hg.std(returns_window, ddof=1)
        @endcode */
    struct std_
        : Operator<"std", In<"ts", TsVar<"S">>, Scalar<"ddof", Int>,
                   Out<TsVar<"O">>>
    {
    };

    /** Calculate variance according to the selected input shape.
        A numeric ``TS`` produces the running population variance of all values observed
        so far. A collection-valued ``TS``, ``TSS``, ``TSD``, or ``TSL`` produces the
        sample variance of its current valid elements (dividing by ``N - 1``), with
        zero for fewer than two elements. Binary inputs calculate the sample variance
        between the current values; fixed-list inputs are handled element by element.
        Unlike ``std``, ``var`` has no ``ddof`` parameter.
        @param ts Numeric series or a numeric collection whose variance is required.
        @param default_value Compatibility input accepted by container overloads; numeric
                             variance still publishes zero when fewer than two elements
                             are present.
        @param lhs Left input for a binary or element-wise variance.
        @param rhs Right input for a binary or element-wise variance.
        @return Running population variance or current sample variance, according to the
                selected overload.
        @par Python example
        @code{.py}
        running_variance = hg.var(returns)
        @endcode */
    struct var_ : Operator<"var", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- Set operations (variadic over the inputs) ----

    /** Combine all members present in any input set.
        @param ts Set-valued or variadic set inputs.
        @return A set containing each distinct member from any input.
        @par Python example
        @code{.py}
        all_symbols = hg.union(primary_symbols, secondary_symbols)
        @endcode */
    struct union_ : Operator<"union", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Keep members present in every input set.
        @param ts Set-valued or variadic set inputs.
        @return The common members of all inputs.
        @par Python example
        @code{.py}
        common_symbols = hg.intersection(listed, liquid)
        @endcode */
    struct intersection_ : Operator<"intersection", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Remove every member of the later sets from the first set.
        @param ts Ordered set inputs; the first is the minuend.
        @param lhs First set for binary overloads.
        @param rhs Members removed from ``lhs``.
        @return Members present only in the first input.
        @par Python example
        @code{.py}
        available = hg.difference(all_symbols, suspended_symbols)
        @endcode */
    struct difference_ : Operator<"difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Keep members present in an odd number of input sets, excluding shared members.
        @param ts Set-valued or variadic set inputs.
        @return The symmetric difference of the inputs.
        @par Python example
        @code{.py}
        changed_membership = hg.symmetric_difference(before, after)
        @endcode */
    struct symmetric_difference_ : Operator<"symmetric_difference", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    // ---- TSD / dictionary re-shaping ----

    /** Project the current keys of a mapping or keyed time-series dictionary.
        Key additions and removals are emitted incrementally for ``TSD`` inputs.
        @param ts Mapping or keyed dictionary.
        @return Its keys as a set-valued time series.
        @par Python example
        @code{.py}
        symbols = hg.keys_(prices)
        @endcode */
    struct keys_ : Operator<"keys_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Project dictionary values in the representation selected by the overload.
        @param ts Mapping or keyed time-series dictionary.
        @return A tuple, list, or keyed value collection corresponding to ``ts``.
        @par Python example
        @code{.py}
        current_prices = hg.values_(prices)
        @endcode */
    struct values_ : Operator<"values_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Replace input dictionary keys according to a live key mapping.
        Entries without a usable target key are omitted; mapping changes move the
        corresponding current value to its new key.
        @param ts Keyed values to transform.
        @param new_keys Mapping from each existing key to its replacement.
        @return The same values indexed by their mapped keys.
        @par Python example
        @code{.py}
        by_identifier = hg.rekey(by_symbol, symbol_to_id)
        @endcode */
    struct rekey : Operator<"rekey", In<"ts", TsVar<"S">>, In<"new_keys", TsVar<"K">>, Out<TsVar<"O">>>
    {
    };

    /** Invert a keyed dictionary so each value becomes a key.
        Duplicate values require ``unique=False``, which collects their original keys in
        a time-series set instead of choosing one arbitrarily.
        @param ts Keyed scalar values to invert.
        @param unique Assert values are unique when true; collect duplicates when false.
        @return An inverted keyed dictionary.
        @par Python example
        @code{.py}
        symbols_by_sector = hg.flip(sector_by_symbol, unique=False)
        @endcode */
    struct flip : Operator<"flip", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Partition a keyed dictionary into nested dictionaries using a live key-to-group map.
        Mapping changes move an entry between partitions without changing its inner key.
        @param ts Keyed values to partition.
        @param partitions Mapping from each input key to its outer partition key.
        @return ``TSD[K1, TSD[K, V]]`` grouped by the mapped partition.
        @par Python example
        @code{.py}
        prices_by_sector = hg.partition(prices, sector_by_symbol)
        @endcode */
    struct partition : Operator<"partition", In<"ts", TsVar<"S">>, In<"partitions", TsVar<"P">>, Out<TsVar<"O">>>
    {
    };

    /** Build the ``combine[TSD]`` grouping from keys and element ports;
        ``TSD.from_ts`` wires this family. */
    struct combine_tsd : Operator<"combine_tsd", In<"keys", TsVar<"A">>, In<"values", TsVar<"B">>, Out<TsVar<"O">>>
    {
    };

    /** Assemble the ``combine[TS[CompoundScalar]]`` grouping from field ports. */
    struct combine_cs : Operator<"combine_cs", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Build a keyed dictionary from a live key and one arbitrary time-series value.
        A key change removes the old entry and publishes the current value under the new key.
        @param key Key under which the value is exposed.
        @param value Time-series value stored at that key.
        @param remove_key Optional boolean stream that removes the active key when true.
        @return A keyed dictionary containing at most the active entry.
        @par Python example
        @code{.py}
        one_price = hg.make_tsd(symbol, price)
        @endcode */
    struct make_tsd : Operator<"make_tsd", In<"key", TS<ScalarVar<"K">>>, In<"value", TsVar<"V">>,
                               Out<TsVar<"O">>>
    {
    };

    /** Three-input C++ wiring form with an explicit remove signal. */
    struct make_tsd_with_remove
        : Operator<"make_tsd", In<"key", TS<ScalarVar<"K">>>, In<"value", TsVar<"V">>,
                   In<"remove_key", TS<Bool>>, Out<TsVar<"O">>>
    {
    };

    /** Provide the LIST-valued ``min_`` grouping from a packed TSL. */
    struct extremum_ts_list_marker : Operator<"min_ts_list", In<"tsl", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Provide the LIST-valued ``max_`` grouping from a packed TSL. */
    struct extremum_ts_list_max_marker : Operator<"max_ts_list", In<"tsl", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Build the ``combine[TS[frozendict]]`` mapping grouping from key and
        value time-series. */
    struct combine_map : Operator<"combine_map", In<"keys", TsVar<"A">>, In<"values", TsVar<"B">>, Out<TsVar<"O">>>
    {
    };

    /** Flatten partitioned dictionaries by removing the outer partition key.
        Inner keys are expected to be unique across partitions; conflicting updates follow
        the selected overload's deterministic ordering.
        @param ts Nested partitioned dictionary.
        @return The merged inner keyed dictionary.
        @par Python example
        @code{.py}
        prices = hg.unpartition(prices_by_sector)
        @endcode */
    struct unpartition : Operator<"unpartition", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Swap outer and inner keys of a nested keyed dictionary while preserving values.
        @param ts ``TSD[K, TSD[K1, V]]`` input.
        @return ``TSD[K1, TSD[K, V]]`` with both key levels inverted.
        @par Python example
        @code{.py}
        by_metric_then_symbol = hg.flip_keys(by_symbol_then_metric)
        @endcode */
    struct flip_keys : Operator<"flip_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Provide the keyed-dictionary ``filter_by`` grouping: keep entries whose
        per-key boolean match is true after ``map_`` evaluates the expression. */
    struct filter_tsd_by_matches
        : Operator<"filter_tsd_by_matches", In<"ts", TsVar<"S">>, In<"matches", TsVar<"M">>, Out<TsVar<"O">>>
    {
    };

    /** Flatten both key levels of a nested dictionary into tuple keys.
        @param ts ``TSD[K, TSD[K1, V]]`` input.
        @return ``TSD[tuple[K, K1], V]``.
        @par Python example
        @code{.py}
        flat = hg.collapse_keys(nested)
        @endcode */
    struct collapse_keys : Operator<"collapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Expand tuple keys into the two levels of a nested keyed dictionary.
        @param ts ``TSD[tuple[K, K1], V]`` input.
        @param remove_empty When true, remove an outer entry after its last inner key is removed.
        @return ``TSD[K, TSD[K1, V]]``.
        @par Python example
        @code{.py}
        nested = hg.uncollapse_keys(flat, remove_empty=True)
        @endcode */
    struct uncollapse_keys : Operator<"uncollapse_keys", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    namespace collection_detail
    {
        template <typename T>
        struct is_tsl_schema : std::false_type
        {
        };

        template <typename ElementSchema, auto FixedSize>
        struct is_tsl_schema<TSL<ElementSchema, FixedSize>> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_tsl_schema_v = is_tsl_schema<T>::value;

        template <typename T>
        struct is_tsb_schema : std::false_type
        {
        };

        template <typename... Fields>
        struct is_tsb_schema<UnNamedTSB<Fields...>> : std::true_type
        {
        };

        template <fixed_string Name, typename... Fields>
        struct is_tsb_schema<TSB<Name, Fields...>> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_tsb_schema_v = is_tsb_schema<T>::value;

        template <typename T>
        struct tsl_schema_traits;

        template <typename ElementSchema, auto FixedSize>
        struct tsl_schema_traits<TSL<ElementSchema, FixedSize>>
        {
            using element_schema = ElementSchema;
            static constexpr bool fixed_size_concrete =
                static_schema_detail::size_parameter_descriptor<FixedSize>::is_concrete();
            static constexpr std::size_t fixed_size =
                fixed_size_concrete ? static_schema_detail::size_parameter_descriptor<FixedSize>::concrete_size()
                                    : std::size_t{0};
        };

        template <typename T>
        struct tsb_schema_traits;

        template <typename... Fields>
        struct tsb_schema_traits<UnNamedTSB<Fields...>>
        {
            static constexpr std::size_t field_count = sizeof...(Fields);
        };

        template <fixed_string Name, typename... Fields>
        struct tsb_schema_traits<TSB<Name, Fields...>>
        {
            static constexpr std::size_t field_count = sizeof...(Fields);
        };

        template <typename T>
        struct port_schema
        {
            using type = void;
        };

        template <typename Schema>
        struct port_schema<Port<Schema>>
        {
            using type = Schema;
        };

        template <typename T>
        using port_schema_t = typename port_schema<std::remove_cvref_t<T>>::type;

        template <typename First, typename...>
        struct first_type
        {
            using type = First;
        };

        template <typename... T>
        using first_type_t = typename first_type<T...>::type;

        template <typename First, typename... Rest>
        inline constexpr bool typed_ports_same_v =
            !std::is_void_v<port_schema_t<First>> &&
            (std::is_same_v<port_schema_t<First>, port_schema_t<Rest>> && ...);

        inline void validate_to_tsl_inputs(const TSValueTypeMetaData &element_schema,
                                           std::span<const WiringPortRef> refs)
        {
            for (const WiringPortRef &ref : refs)
            {
                if (!graph_wiring_detail::input_accepts_output_schema(&element_schema, ref.schema))
                {
                    throw std::logic_error("to_tsl input port schema does not match the TSL element schema");
                }
            }
        }

        inline void validate_inferred_to_tsl_inputs(const TSValueTypeMetaData &element_schema,
                                                    std::span<const WiringPortRef> refs)
        {
            for (const WiringPortRef &ref : refs)
            {
                if (ref.schema == nullptr || !time_series_schema_equivalent(&element_schema, ref.schema))
                {
                    throw std::logic_error(
                        "to_tsl without an explicit schema requires all input schemas to match exactly");
                }
            }
        }

        inline void validate_to_tsb_inputs(const TSValueTypeMetaData &output_schema,
                                           std::span<const WiringPortRef> refs)
        {
            if (output_schema.kind != TSTypeKind::TSB || output_schema.field_count() != refs.size())
            {
                throw std::logic_error("to_tsb input count must match the TSB field count");
            }

            for (std::size_t index = 0; index < refs.size(); ++index)
            {
                const TSValueTypeMetaData *expected = output_schema.fields()[index].type;
                if (!graph_wiring_detail::input_accepts_output_schema(expected, refs[index].schema))
                {
                    throw std::logic_error("to_tsb input port schema does not match the TSB field schema");
                }
            }
        }

        template <std::size_t Size>
        [[nodiscard]] WiringPortRef structural_collection_port(const TSValueTypeMetaData             *output_schema,
                                                               const std::array<WiringPortRef, Size> &refs)
        {
            return WiringPortRef::structural_source(output_schema,
                                                    std::vector<WiringPortRef>(refs.begin(), refs.end()));
        }
    }  // namespace collection_detail

    /**
     * Wire several time-series ports into a fixed non-peered ``TSL``.
     *
     * ``to_tsl<TS<Str>>(w, a, b)`` and ``to_tsl<TSL<TS<Str>>>(w, a, b)`` both return
     * ``Port<TSL<TS<Str>, 2>>``. If every input is already a typed ``Port<S>``, the
     * element schema can be inferred and the same fixed-size typed port is returned.
     * If the inputs are erased ports and no schema is supplied, the result is an
     * erased ``Port<void>`` with a runtime fixed TSL schema inferred from matching
     * input schemas.
     */
    template <typename OutOrElementSchema = void, typename... Ports>
    [[nodiscard]] auto to_tsl(Wiring &w, const Ports &...ports)
    {
        static_assert(sizeof...(Ports) > 0, "to_tsl requires at least one input");
        static_assert((graph_wiring_detail::is_port<std::remove_cvref_t<Ports>>::value && ...),
                      "to_tsl inputs must be time-series Ports");

        constexpr std::size_t size = sizeof...(Ports);
        std::array<WiringPortRef, size> refs{ports.erased()...};

        if constexpr (std::is_void_v<OutOrElementSchema>)
        {
            if constexpr (collection_detail::typed_ports_same_v<Ports...>)
            {
                using FirstPort = collection_detail::first_type_t<Ports...>;
                using Element   = collection_detail::port_schema_t<FirstPort>;
                using Result    = TSL<Element, size>;

                const auto *output_schema = schema_descriptor<Result>::ts_meta();
                collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
                WiringPortRef out = collection_detail::structural_collection_port(output_schema, refs);
                return Port<Result>{w, std::move(out)};
            }
            else
            {
                const auto *element_schema = refs.front().schema;
                if (element_schema == nullptr)
                {
                    throw std::logic_error("to_tsl cannot infer an output schema from an untyped input");
                }
                collection_detail::validate_inferred_to_tsl_inputs(*element_schema, refs);
                const auto *output_schema = TypeRegistry::instance().tsl(element_schema, size);
                WiringPortRef out = collection_detail::structural_collection_port(output_schema, refs);
                return Port<void>{w, std::move(out)};
            }
        }
        else if constexpr (collection_detail::is_tsl_schema_v<OutOrElementSchema>)
        {
            using Traits = collection_detail::tsl_schema_traits<OutOrElementSchema>;
            static_assert(!Traits::fixed_size_concrete || Traits::fixed_size == 0 || Traits::fixed_size == size,
                          "to_tsl explicit fixed TSL output size must match the input count");
            using Result = TSL<typename Traits::element_schema,
                               Traits::fixed_size == 0 ? size : Traits::fixed_size>;

            const auto *output_schema = schema_descriptor<Result>::ts_meta();
            collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
            WiringPortRef out = collection_detail::structural_collection_port(output_schema, refs);
            return Port<Result>{w, std::move(out)};
        }
        else
        {
            using Result = TSL<OutOrElementSchema, size>;

            const auto *output_schema = schema_descriptor<Result>::ts_meta();
            collection_detail::validate_to_tsl_inputs(*output_schema->element_ts(), refs);
            WiringPortRef out = collection_detail::structural_collection_port(output_schema, refs);
            return Port<Result>{w, std::move(out)};
        }
    }

    /**
     * Wire time-series ports into a non-peered ``TSB`` using the explicit TSB
     * schema's field order. For example,
     * ``to_tsb<TSB<"AB", Field<"a", TS<Int>>, Field<"b", TS<Str>>>>(w, a, b)``
     * binds ``a`` to field ``a`` and ``b`` to field ``b``.
     */
    template <typename OutSchema, typename... Ports>
    [[nodiscard]] auto to_tsb(Wiring &w, const Ports &...ports)
    {
        static_assert(collection_detail::is_tsb_schema_v<OutSchema>,
                      "to_tsb requires an explicit TSB or UnNamedTSB output schema");
        static_assert(sizeof...(Ports) == collection_detail::tsb_schema_traits<OutSchema>::field_count,
                      "to_tsb input count must match the TSB field count");
        static_assert((graph_wiring_detail::is_port<std::remove_cvref_t<Ports>>::value && ...),
                      "to_tsb inputs must be time-series Ports");

        constexpr std::size_t size = sizeof...(Ports);
        std::array<WiringPortRef, size> refs{ports.erased()...};

        const auto *output_schema = schema_descriptor<OutSchema>::ts_meta();
        collection_detail::validate_to_tsb_inputs(*output_schema, refs);
        WiringPortRef out = collection_detail::structural_collection_port(output_schema, refs);
        return Port<OutSchema>{w, std::move(out)};
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_COLLECTION_H
