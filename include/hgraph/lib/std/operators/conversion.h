#ifndef HGRAPH_LIB_STD_OPERATORS_CONVERSION_H
#define HGRAPH_LIB_STD_OPERATORS_CONVERSION_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/date_time.h>

namespace hgraph::stdlib
{
    /**
     * Type / shape conversion operator **definitions** (markers only). Mirrors the Python
     * ``hgraph`` conversion operators (``_time_series_conversion.py``, ``_type_operators.py``)
     * plus the ``str_`` / ``type_`` introspection helpers and the ``zero`` / ``nothing`` /
     * ``default`` graph utilities. The target type of ``convert`` / ``cast_`` / etc. is
     * supplied as an explicit output schema at the wiring site (``wire<convert, TS<Int>>(…)``).
     *
     * .. note::
     *
     *    The data-frame / table / JSON conversion operators (``to_table`` / ``from_table`` /
     *    ``to_json`` / ``from_json`` / ``to_data_frame`` / ``from_data_frame`` / …) are
     *    **deferred** — they need scalar value types (``Frame`` / ``JSON`` / ``TABLE``) the
     *    C++ value layer does not model yet.
     */

    /** Create a source that emits one configured value and then becomes passive.
        With no delay it ticks at graph start; otherwise it ticks at
        ``start_time + delay``. Subscript ``const`` when the output shape cannot be
        inferred from the Python value.
        @param value Python value adapted to the selected time-series schema.
        @param delay Optional engine-time delay before the single tick.
        @return A source of the inferred or explicitly selected output type.
        @par Python example
        @code{.py}
        answer = hg.const[TS[int]](42)
        delayed = hg.const("ready", delay=timedelta(seconds=1))
        @endcode */
    struct const_
        : Operator<"const", Scalar<"value", ScalarVar<"T">>, TypeArg<"tp", TsVar<"S">, AutoResolve>,
                   Scalar<"delay", TimeDelta>, Out<TsVar<"S">>>
    {
    };

    /** Convert a time series to an explicitly selected output shape.
        Use the subscripted form when input types alone cannot determine the target;
        conversion preserves tick timing while adapting each delta or value.
        @param ts Input time series to convert.
        @return The same logical stream represented by the selected output schema.
        @par Python example
        @code{.py}
        tuple_values = hg.convert[TS[tuple[str, ...]]](set_values)
        @endcode */
    struct convert : Operator<"convert", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Build one composite time-series value from component ports.
        The selected output type determines whether inputs become tuple/list elements,
        bundle fields, mapping entries, set members, temporal components, or JSON fields.
        Strict structural forms wait for every required component to become valid.
        @param ts Positional component ports used by collection and structural overloads.
        @param args Positional component ports accepted by the Python helper.
        @param kwargs Named fields accepted by bundle, compound-scalar, and JSON forms.
        @param __strict__ When true, suppress output until every required component is valid.
        @return A composite port of the explicitly selected or structurally inferred type.
        @par Python example
        @code{.py}
        point = hg.combine[TSB[Point]](x=x, y=y)
        values = hg.combine(a, b, c)
        @endcode */
    struct combine : Operator<"combine", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Provide the ``combine[TSS](a, b, ...)`` set grouping from a packed TSL. */
    struct combine_tss_from_tsl_marker
        : Operator<"combine_tss_from_tsl", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Accumulate successive input ticks into a collection-valued time series.
        The required output subscript chooses tuple, mapping, set, or keyed collection
        semantics. Key/value collection forms update the entry named by each key tick.
        @param ts Value stream to accumulate.
        @param key Key stream used by mapping and keyed-dictionary overloads.
        @param value Value stream paired with ``key``.
        @param reset Optional signal that clears the accumulated collection.
        @return The selected collection type, updated as input ticks arrive.
        @par Python example
        @code{.py}
        history = hg.collect[TS[tuple[int, ...]]](value)
        by_name = hg.collect[TS[dict[str, int]]](key=name, value=value)
        @endcode */
    struct collect : Operator<"collect", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Expand each collection-valued input tick into a sequence of element ticks.
        Elements are scheduled in collection iteration order over successive engine cycles.
        @param ts Collection-valued time series to expand.
        @return A stream whose individual ticks are the collection's elements.
        @par Python example
        @code{.py}
        element = hg.emit(hg.const((1, 2, 3)))
        @endcode */
    struct emit : Operator<"emit", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Cast scalar values to an explicitly selected scalar output type.
        @param ts Scalar time series to cast.
        @return A port with the requested scalar schema and unchanged tick timing.
        @par Python example
        @code{.py}
        floating = hg.cast_[TS[float]](integer)
        @endcode */
    struct cast_ : Operator<"cast_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Downcast values to an explicitly selected derived type with runtime checking.
        Python supports the release/0.5-compatible ``downcast_(Derived, ts)`` spelling
        and the output-selected ``downcast_[TS[Derived]](ts)`` spelling. Both wire this
        native operator with the same resolved output schema.
        @param tp Derived CompoundScalar class selected at Python wiring time; C++
        callers select the equivalent output schema in ``wire<downcast_, TS<Derived>>``.
        @param ts Base-typed input.
        @return The same values viewed through the selected derived schema.
        @par Python example
        @code{.py}
        derived = hg.downcast_(Derived, base)
        equivalent = hg.downcast_[TS[Derived]](base)
        @endcode */
    struct downcast_ : Operator<"downcast_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Downcast a time-series reference to an explicitly selected derived schema.
        This is an unchecked reference conversion; use it only when the referenced runtime
        value is known to satisfy the target contract.
        @param ts Reference to the base-typed time series.
        @return A reference with the selected derived schema.
        @par Python example
        @code{.py}
        derived_ref = hg.downcast_ref[REF[TS[Derived]]](base_ref)
        @endcode */
    struct downcast_ref
        : Operator<"downcast_ref", In<"ts", REF<TsVar<"S">>>, Out<REF<TsVar<"O">>>>
    {
    };

    /** Convert each valid input value to its string representation.
        @param ts Values to render.
        @return A ``TS[str]`` that ticks when ``ts`` ticks.
        @par Python example
        @code{.py}
        label = hg.str_(value)
        @endcode */
    struct str_ : Operator<"str_", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };

    /** Report the Python runtime type of each input value.
        @param ts Values to inspect.
        @return A time series of Python type objects.
        @par Python example
        @code{.py}
        value_type = hg.type_(value)
        @endcode */
    struct type_ : Operator<"type_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** Produce the identity value for an operator and explicitly selected output type.
        For example addition uses zero while multiplication uses one; collection
        overloads choose the corresponding empty or identity value.
        @param op Operator whose identity is required. This choice is fixed at wiring time.
        @return A constant source of the selected type's identity for ``op``.
        @par Python example
        @code{.py}
        additive_identity = hg.zero[TS[int]](hg.add_)
        @endcode */
    struct zero_ : Operator<"zero", Scalar<"op", WiredFn>, Out<TsVar<"S">>>
    {
    };

    /** Create an invalid source of an explicitly selected type that never ticks.
        This is useful as an empty branch or optional graph input without inventing a value.
        @return A permanently invalid port of the selected type.
        @par Python example
        @code{.py}
        absent = hg.nothing[TS[int]]()
        @endcode */
    struct nothing : Operator<"nothing", TypeArg<"tp", TsVar<"O">, AutoResolve>, Out<TsVar<"O">>>
    {
    };

    /** Forward ``ts`` when it is valid and otherwise expose ``default_value``.
        Once the primary input becomes valid it takes precedence; later invalidation makes
        the fallback visible again.
        @param ts Preferred input.
        @param default_value Fallback input used while ``ts`` is invalid.
        @return A port that is valid whenever either input supplies a value.
        @par Python example
        @code{.py}
        price_or_zero = hg.default(price, 0.0)
        @endcode */
    struct default_ : Operator<"default", In<"ts", TsVar<"S">>, In<"default_value", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_CONVERSION_H
