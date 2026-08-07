#ifndef HGRAPH_TYPES_OPERATOR_TYPE_RESOLUTION_H
#define HGRAPH_TYPES_OPERATOR_TYPE_RESOLUTION_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>

#include <cstddef>
#include <string_view>

namespace hgraph::operator_type_resolution
{
    /**
     * Small, null-safe helpers for operator ``requires_`` and
     * ``resolve_default_types`` implementations.
     *
     * Operator default resolution runs before ``requires_``. A resolver must
     * therefore be at least as defensive as the predicate it pairs with:
     * inspect the normalised call shape, return quietly when the candidate
     * is not applicable, and bind only when the output is known.
     *
     * Extraction helpers return a pointer, or ``nullptr`` when the requested
     * shape is not present. Typical patterns are:
     *
     * - ``time_series_schema_at`` extracts a TS argument schema, dereferencing
     *   REF by default.
     * - ``time_series_schema_at`` plus typed ``time_series_schema_matches<S>``
     *   answers recursive shape questions without a separate kind-specific API.
     * - ``time_series_schema_as<S>`` returns the extracted schema only when it
     *   matches the recursive static shape.
     * - ``fixed_tsl_arg`` extracts only a positive fixed-size TSL.
     * - ``ts_value_schema`` extracts ``T`` from ``TS<T>``.
     * - ``ts_map_value_schema`` extracts the scalar map schema from
     *   ``TS<Map[K, V]>``.
     *
     * Output helpers are split by overload kind. Static-node overloads whose
     * output pattern is a local variable such as ``O`` should use
     * ``local_output_bound`` and ``bind_local_output``. Graph overloads whose
     * erased output pattern is ``__out__`` should use ``output_bound`` and
     * ``bind_output``.
     *
     * Graph-overload example:
     *
     * ````cpp
     * static bool requires_(const ResolutionMap &, OperatorCallContext context)
     * {
     *     return fixed_tsl_arg(context, 1) != nullptr;
     * }
     *
     * static void resolve_default_types(ResolutionMap &resolution,
     *                                   OperatorCallContext context)
     * {
     *     if (output_bound(resolution)) { return; }
     *     const auto *tsl = fixed_tsl_arg(context, 1);
     *     if (tsl == nullptr) { return; }
     *     bind_output(resolution, tsl->element_ts(), "V");
     * }
     * ````
     *
     * Static-node example:
     *
     * ````cpp
     * static void resolve_default_types(ResolutionMap &resolution,
     *                                   OperatorCallContext)
     * {
     *     if (local_output_bound(resolution, "O")) { return; }
     *     const auto *map = ts_map_value_schema(resolution.find_ts("S"));
     *     if (map == nullptr) { return; }
     *     bind_local_output(resolution,
     *                       TypeRegistry::instance().ts(
     *                           TypeRegistry::instance().set(map->key_type)),
     *                       "O");
     * }
     * ````
     *
     * Prefer these helpers over ad hoc ``context.args[N]`` indexing so the
     * resolver and predicate share the same guard semantics.
     */

    /** Whether returned schemas should preserve REF wrappers or dereference them. */
    enum class SchemaRefMode
    {
        Direct,
        Dereference
    };

    /** Broad static patterns for readable kind checks in operator predicates. */
    using AnyTS  = TS<ScalarVar<"__value">>;
    using AnyTSS = TSS<ScalarVar<"__element">>;
    using AnyTSD = TSD<ScalarVar<"__key">, TsVar<"__value">>;
    using AnyTSL = TSL<TsVar<"__element">, SIZE<"__size">>;
    using AnyTSW = TSWAny<ScalarVar<"__element">>;
    using AnyTSB = UnNamedTSB<TsVar<"__schema">>;
    using AnyREF = REF<TsVar<"__target">>;

    /** Return argument ``index``, or null when the call is shorter. */
    [[nodiscard]] inline const WiringArg *arg_at(OperatorCallContext context, std::size_t index) noexcept
    {
        return index < context.args.size() ? &context.args[index] : nullptr;
    }

    /** Return argument ``index`` only when it is a time-series argument. */
    [[nodiscard]] inline const WiringArg *time_series_arg_at(OperatorCallContext context,
                                                             std::size_t index) noexcept
    {
        const WiringArg *arg = arg_at(context, index);
        return arg != nullptr && arg->kind == WiringArg::Kind::TimeSeries ? arg : nullptr;
    }

    /** Return argument ``index`` only when it is a scalar argument. */
    [[nodiscard]] inline const WiringArg *scalar_arg_at(OperatorCallContext context, std::size_t index) noexcept
    {
        const WiringArg *arg = arg_at(context, index);
        return arg != nullptr && arg->kind == WiringArg::Kind::Scalar ? arg : nullptr;
    }

    /**
     * Return the time-series schema for ``arg``. ``Dereference`` turns a
     * REF-shaped port into its target schema; ``Direct`` preserves the REF
     * wrapper so callers can gate on the actual surface shape.
     */
    [[nodiscard]] inline const TSValueTypeMetaData *time_series_schema(
        const WiringArg &arg,
        SchemaRefMode    mode = SchemaRefMode::Dereference) noexcept
    {
        if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { return nullptr; }
        return mode == SchemaRefMode::Dereference ? TypeRegistry::instance().dereference(arg.port.schema)
                                                  : arg.port.schema;
    }

    /** Return the time-series schema at ``index``, or null for non-TS/missing args. */
    [[nodiscard]] inline const TSValueTypeMetaData *time_series_schema_at(
        OperatorCallContext context,
        std::size_t         index,
        SchemaRefMode       mode = SchemaRefMode::Dereference) noexcept
    {
        const WiringArg *arg = time_series_arg_at(context, index);
        return arg != nullptr ? time_series_schema(*arg, mode) : nullptr;
    }

    /** Match a concrete input schema against a static recursive time-series pattern. */
    template <typename Schema>
    [[nodiscard]] inline bool time_series_schema_matches(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return false; }
        ResolutionMap scratch;
        return input_ts_pattern_match(to_pattern<Schema>(), schema, scratch);
    }

    /** Return ``schema`` when it matches a recursive static time-series pattern. */
    template <typename Schema>
    [[nodiscard]] inline const TSValueTypeMetaData *time_series_schema_as(
        const TSValueTypeMetaData *schema,
        SchemaRefMode             mode = SchemaRefMode::Dereference)
    {
        const TSValueTypeMetaData *resolved =
            mode == SchemaRefMode::Dereference ? TypeRegistry::instance().dereference(schema) : schema;
        return time_series_schema_matches<Schema>(resolved) ? resolved : nullptr;
    }

    /** Extract an argument schema and return it only when it matches ``Schema``. */
    template <typename Schema>
    [[nodiscard]] inline const TSValueTypeMetaData *time_series_schema_as(
        const WiringArg &arg,
        SchemaRefMode    mode = SchemaRefMode::Dereference)
    {
        return time_series_schema_as<Schema>(time_series_schema(arg, mode), SchemaRefMode::Direct);
    }

    /** Extract argument ``index`` and return it only when it matches ``Schema``. */
    template <typename Schema>
    [[nodiscard]] inline const TSValueTypeMetaData *time_series_schema_at_as(
        OperatorCallContext context,
        std::size_t         index,
        SchemaRefMode       mode = SchemaRefMode::Dereference)
    {
        return time_series_schema_as<Schema>(time_series_schema_at(context, index, mode), SchemaRefMode::Direct);
    }

    /** Match argument ``index`` against a static recursive time-series pattern. */
    template <typename Schema>
    [[nodiscard]] inline bool time_series_arg_matches(
        OperatorCallContext context,
        std::size_t         index,
        SchemaRefMode       mode = SchemaRefMode::Dereference)
    {
        return time_series_schema_at_as<Schema>(context, index, mode) != nullptr;
    }

    /** Return a fixed-size TSL schema at ``index``. Dynamic TSL returns null. */
    [[nodiscard]] inline const TSValueTypeMetaData *fixed_tsl_arg(
        OperatorCallContext context,
        std::size_t         index,
        SchemaRefMode       mode = SchemaRefMode::Dereference)
    {
        const TSValueTypeMetaData *schema = time_series_schema_at_as<AnyTSL>(context, index, mode);
        return schema != nullptr && schema->fixed_size() > 0 ? schema : nullptr;
    }

    /** True when both arguments are fixed-size TSL with the same size. */
    [[nodiscard]] inline bool same_fixed_tsl_size(OperatorCallContext context,
                                                  std::size_t lhs_index,
                                                  std::size_t rhs_index,
                                                  SchemaRefMode mode = SchemaRefMode::Dereference)
    {
        const TSValueTypeMetaData *lhs = fixed_tsl_arg(context, lhs_index, mode);
        const TSValueTypeMetaData *rhs = fixed_tsl_arg(context, rhs_index, mode);
        return lhs != nullptr && rhs != nullptr && lhs->fixed_size() == rhs->fixed_size();
    }

    /** Return a TS value schema when ``ts`` is a plain ``TS<T>``. */
    [[nodiscard]] inline const ValueTypeMetaData *ts_value_schema(const TSValueTypeMetaData *ts) noexcept
    {
        return ts != nullptr && ts->kind == TSTypeKind::TS ? ts->value_schema : nullptr;
    }

    /** Return ``T`` from a scalar ``List<T>``/``Set<T>`` schema, or null otherwise. */
    [[nodiscard]] inline const ValueTypeMetaData *collection_element_schema(
        const ValueTypeMetaData *value) noexcept
    {
        if (value == nullptr) { return nullptr; }
        const auto kind = value->try_value_kind();
        return kind == ValueTypeKind::List || kind == ValueTypeKind::Set ? value->element_type : nullptr;
    }

    /** Return the collection element when present, otherwise the original scalar schema. */
    [[nodiscard]] inline const ValueTypeMetaData *collection_element_or_self_schema(
        const ValueTypeMetaData *value) noexcept
    {
        if (const ValueTypeMetaData *element = collection_element_schema(value)) { return element; }
        return value;
    }

    /** Return the element type for a homogeneous fixed tuple, or null otherwise. */
    [[nodiscard]] inline const ValueTypeMetaData *homogeneous_tuple_element_schema(
        const ValueTypeMetaData *value) noexcept
    {
        if (value == nullptr || value->try_value_kind() != ValueTypeKind::Tuple || value->field_count == 0)
        {
            return nullptr;
        }
        const ValueTypeMetaData *first = value->fields[0].type;
        for (std::size_t index = 1; index < value->field_count; ++index)
        {
            if (value->fields[index].type != first) { return nullptr; }
        }
        return first;
    }

    /** Return ``T`` from ``List<T>`` or homogeneous fixed ``Tuple<T, ...>``. */
    [[nodiscard]] inline const ValueTypeMetaData *tuple_element_schema(
        const ValueTypeMetaData *value) noexcept
    {
        if (value == nullptr) { return nullptr; }
        const auto kind = value->try_value_kind();
        if (kind == ValueTypeKind::List) { return value->element_type; }
        if (kind == ValueTypeKind::Tuple) { return homogeneous_tuple_element_schema(value); }
        return nullptr;
    }

    /** Return a TS value schema at ``index`` when the argument is a plain ``TS<T>``. */
    [[nodiscard]] inline const ValueTypeMetaData *ts_value_schema_at(
        OperatorCallContext context,
        std::size_t         index,
        SchemaRefMode       mode = SchemaRefMode::Dereference) noexcept
    {
        return ts_value_schema(time_series_schema_at(context, index, mode));
    }

    /** Return the map value schema from TS[Map[K, V]], or null for non-map values. */
    [[nodiscard]] inline const ValueTypeMetaData *ts_map_value_schema(const TSValueTypeMetaData *ts) noexcept
    {
        const ValueTypeMetaData *value = ts_value_schema(ts);
        return value != nullptr && value->try_value_kind() == ValueTypeKind::Map ? value : nullptr;
    }

    /** Return the erased graph-output binding, or null when it is unresolved. */
    [[nodiscard]] inline const TSValueTypeMetaData *output_schema(
        const ResolutionMap &resolution,
        SchemaRefMode        mode = SchemaRefMode::Direct) noexcept
    {
        const TSValueTypeMetaData *schema = resolution.find_ts("__out__");
        return mode == SchemaRefMode::Dereference ? TypeRegistry::instance().dereference(schema) : schema;
    }

    /** Match the resolved graph output against a static recursive time-series pattern. */
    template <typename Schema>
    [[nodiscard]] inline bool output_matches(
        const ResolutionMap &resolution,
        SchemaRefMode        mode = SchemaRefMode::Direct)
    {
        const TSValueTypeMetaData *schema = output_schema(resolution, mode);
        if (schema == nullptr) { return false; }
        ResolutionMap scratch;
        return output_ts_pattern_match(to_pattern<Schema>(), schema, scratch);
    }

    /** Return ``T`` when the resolved graph output is ``TS<T>``. */
    [[nodiscard]] inline const ValueTypeMetaData *output_ts_value_schema(
        const ResolutionMap &resolution,
        SchemaRefMode        mode = SchemaRefMode::Direct) noexcept
    {
        return ts_value_schema(output_schema(resolution, mode));
    }

    /** True when ``resolution`` already carries the erased graph-output binding. */
    [[nodiscard]] inline bool output_bound(const ResolutionMap &resolution) noexcept
    {
        return resolution.is_resolved<ResolutionKind::TimeSeries>("__out__");
    }

    /** True when a local output type variable such as ``O`` is already bound. */
    [[nodiscard]] inline bool local_output_bound(const ResolutionMap &resolution,
                                                 std::string_view     local_var) noexcept
    {
        return !local_var.empty() &&
               resolution.is_resolved<ResolutionKind::TimeSeries>(local_var);
    }

    /** Bind ``name`` only when ``schema`` is non-null and the name is still unbound. */
    inline void bind_if_unbound(ResolutionMap &resolution,
                                std::string_view name,
                                const TSValueTypeMetaData *schema)
    {
        if (schema != nullptr &&
            !resolution.is_resolved<ResolutionKind::TimeSeries>(name))
        {
            resolution.bind_ts(name, schema);
        }
    }

    /**
     * Bind the resolved graph-overload output. ``__out__`` is the erased
     * graph-output sentinel and is only defaulted when absent. ``local_var`` is
     * for graph implementations whose compose signature also needs an internal
     * named variable such as ``V``; rebinding it preserves ResolutionMap's
     * inconsistent-resolution check.
     */
    inline void bind_output(ResolutionMap &resolution,
                            const TSValueTypeMetaData *output,
                            std::string_view local_var = {})
    {
        if (output == nullptr) { return; }
        if (!local_var.empty()) { resolution.bind_ts(local_var, output); }
        bind_if_unbound(resolution, "__out__", output);
    }

    /** Bind graph output to the schema of argument ``index``. */
    inline void bind_output_like_arg(ResolutionMap &resolution,
                                     OperatorCallContext context,
                                     std::size_t index,
                                     std::string_view local_var = {},
                                     SchemaRefMode mode = SchemaRefMode::Dereference)
    {
        bind_output(resolution, time_series_schema_at(context, index, mode), local_var);
    }

    /**
     * Bind only a static-node local output variable such as ``O``. Use this
     * when the implementation's declared output pattern is the local variable,
     * not the erased graph-output sentinel ``__out__``. Rebinding is deliberate:
     * a mismatch rejects the candidate through ResolutionMap.
     */
    inline void bind_local_output(ResolutionMap &resolution,
                                  const TSValueTypeMetaData *output,
                                  std::string_view local_var)
    {
        if (output != nullptr && !local_var.empty()) { resolution.bind_ts(local_var, output); }
    }

    /** Bind local output to the schema of argument ``index``. */
    inline void bind_local_output_like_arg(ResolutionMap &resolution,
                                           OperatorCallContext context,
                                           std::size_t index,
                                           std::string_view local_var,
                                           SchemaRefMode mode = SchemaRefMode::Dereference)
    {
        bind_local_output(resolution, time_series_schema_at(context, index, mode), local_var);
    }
}  // namespace hgraph::operator_type_resolution

#endif  // HGRAPH_TYPES_OPERATOR_TYPE_RESOLUTION_H
