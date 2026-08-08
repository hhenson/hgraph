#ifndef HGRAPH_TYPES_TYPE_PATTERN_H
#define HGRAPH_TYPES_TYPE_PATTERN_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/static_schema.h>      // TS / TSS / TSL / TSD / REF / SIGNAL, TsVar / ScalarVar, descriptors
#include <hgraph/types/type_resolution.h>    // ResolutionMap

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    struct DimensionPattern
    {
        bool variable{false};
        std::string name{};
        std::size_t value{0};

        [[nodiscard]] static DimensionPattern fixed(std::size_t size) noexcept
        {
            return DimensionPattern{.value = size};
        }

        [[nodiscard]] static DimensionPattern var(std::string variable_name)
        {
            return DimensionPattern{.variable = true, .name = std::move(variable_name)};
        }
    };

    /**
     * Runtime *type pattern* — the wiring-time form of a (possibly generic) schema
     * used for operator overload **matching** and **ranking**. It is the single
     * representation shared by C++ and (eventually) Python operator candidates: a
     * C++ node's selector schema types are *lowered* into a ``TypePattern`` via
     * ``to_pattern<S>()``; a Python overload builds the same tree directly from its
     * type metadata. One pattern representation, one matcher — see
     * ``docs/source/developer_guide/operators.rst``.
     *
     * A concrete sub-tree collapses to a single ``Concrete`` leaf (the fully-interned
     * registry pointer); only the parts that contain a type variable keep their
     * structure (so e.g. ``TS<Int>`` lowers to ``Concrete`` while
     * ``TS<ScalarVar<"T">>`` lowers to ``TS(ScalarVar("T"))``).
     */

    /** Scalar-layer pattern: a scalar variable, an interned scalar, or a recursive scalar container shape. */
    struct ScalarPattern
    {
        enum class Kind
        {
            Var,
            Concrete,
            UnknownTuple,
            HomogeneousTuple,
            FixedTuple,
            Set,
            Map,
            Series,
            Frame,
            Array,
            Bundle
        };

        Kind                     kind{Kind::Concrete};
        std::string              name{};            ///< ``Var``: the variable name.
        const ValueTypeMetaData *meta{nullptr};     ///< ``Concrete``: the interned scalar.
        std::vector<const ValueTypeMetaData *> constraints{};  ///< ``Var``: accepted concrete scalar schemas.
        std::vector<ScalarPattern> children{};      ///< recursive scalar payloads.
        std::vector<DimensionPattern> dimensions{}; ///< ``Array`` dimensions, outermost first.
        bool                       schema_var{false}; ///< ``Bundle``: true when ``name`` is a schema variable.
        std::string                bundle_origin{}; ///< ``Bundle``: qualified generic origin, when constrained.

        [[nodiscard]] static ScalarPattern var(std::string name,
                                               std::vector<const ValueTypeMetaData *> constraints = {})
        {
            ScalarPattern p;
            p.kind        = Kind::Var;
            p.name        = std::move(name);
            p.constraints = std::move(constraints);
            return p;
        }
        [[nodiscard]] static ScalarPattern concrete(const ValueTypeMetaData *meta)
        {
            ScalarPattern p;
            p.kind = Kind::Concrete;
            p.meta = meta;
            return p;
        }
        [[nodiscard]] static ScalarPattern unknown_tuple()
        {
            ScalarPattern p;
            p.kind = Kind::UnknownTuple;
            return p;
        }
        [[nodiscard]] static ScalarPattern unknown_tuple(ScalarPattern element)
        {
            ScalarPattern p;
            p.kind = Kind::UnknownTuple;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static ScalarPattern homogeneous_tuple(ScalarPattern element)
        {
            ScalarPattern p;
            p.kind = Kind::HomogeneousTuple;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static ScalarPattern fixed_tuple(std::vector<ScalarPattern> elements)
        {
            ScalarPattern p;
            p.kind     = Kind::FixedTuple;
            p.children = std::move(elements);
            return p;
        }
        [[nodiscard]] static ScalarPattern set(ScalarPattern element)
        {
            ScalarPattern p;
            p.kind = Kind::Set;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static ScalarPattern map(ScalarPattern key, ScalarPattern value)
        {
            ScalarPattern p;
            p.kind = Kind::Map;
            p.children.push_back(std::move(key));
            p.children.push_back(std::move(value));
            return p;
        }
        [[nodiscard]] static ScalarPattern series(ScalarPattern element)
        {
            ScalarPattern p;
            p.kind = Kind::Series;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static ScalarPattern frame(ScalarPattern schema,
                                                 std::optional<ScalarPattern> metadata = std::nullopt)
        {
            ScalarPattern p;
            p.kind = Kind::Frame;
            p.children.push_back(std::move(schema));
            if (metadata.has_value()) { p.children.push_back(std::move(*metadata)); }
            return p;
        }
        [[nodiscard]] static ScalarPattern array(ScalarPattern element,
                                                 std::vector<DimensionPattern> shape)
        {
            ScalarPattern p;
            p.kind = Kind::Array;
            p.children.push_back(std::move(element));
            p.dimensions = std::move(shape);
            return p;
        }
        [[nodiscard]] static ScalarPattern bundle()
        {
            ScalarPattern p;
            p.kind = Kind::Bundle;
            return p;
        }
        [[nodiscard]] static ScalarPattern bundle_var(std::string schema_variable)
        {
            ScalarPattern p;
            p.kind       = Kind::Bundle;
            p.name       = std::move(schema_variable);
            p.schema_var = true;
            return p;
        }
        [[nodiscard]] static ScalarPattern bundle_generic(std::string schema_variable,
                                                          std::string qualified_origin,
                                                          std::vector<ScalarPattern> arguments)
        {
            ScalarPattern p;
            p.kind          = Kind::Bundle;
            p.name          = std::move(schema_variable);
            p.schema_var    = true;
            p.bundle_origin = std::move(qualified_origin);
            p.children      = std::move(arguments);
            return p;
        }
    };

    /** Time-series-layer pattern node. */
    struct TypePattern
    {
        enum class Kind
        {
            Var,        ///< a ``TsVar`` (a whole-time-series variable, e.g. ``TIME_SERIES_TYPE``)
            Concrete,   ///< a fully-interned TS leaf
            TS,         ///< ``TS<scalar>``    (scalar held in ``scalar``)
            TSS,        ///< ``TSS<scalar>``   (element held in ``scalar``)
            TSL,        ///< ``TSL<elem, N>``  (element held in ``children[0]``, size in ``fixed_size``)
            TSD,        ///< ``TSD<key, val>`` (key in ``scalar``, value in ``children[0]``)
            TSW,        ///< ``TSW<scalar,P,M>`` (element held in ``scalar``, sizes in ``fixed_size`` / ``min_size``)
            TSB,        ///< ``TSB`` / ``UnNamedTSB`` (fields in ``field_names`` + ``children``)
            REF,        ///< ``REF<target>``   (target in ``children[0]``)
            Signal      ///< ``SIGNAL``
        };

        Kind                       kind{Kind::Signal};
        std::string                name{};          ///< ``Var``: the variable name.
        const TSValueTypeMetaData *meta{nullptr};    ///< ``Concrete``: the interned TS schema.
        std::vector<const TSValueTypeMetaData *> constraints{};  ///< ``Var``: accepted concrete TS schemas.
        ScalarPattern              scalar{};         ///< ``TS`` / ``TSS`` payload, ``TSD`` key.
        std::vector<TypePattern>   children{};       ///< ``TSL`` elem / ``TSD`` value / ``REF`` target.
        std::vector<std::string>   field_names{};    ///< ``TSB`` fields, parallel to ``children``.
        std::string                bundle_name{};    ///< named ``TSB`` bundle name.
        std::size_t                fixed_size{0};    ///< ``TSL`` fixed size / ``TSW`` period (0 = dynamic / unconstrained).
        std::string                size_name{};      ///< ``TSL`` size variable name.
        std::vector<std::size_t>   size_constraints{}; ///< ``TSL`` size variable accepted concrete sizes.
        std::size_t                min_size{0};      ///< ``TSW`` min period.
        bool                       any_window{false}; ///< ``TSW`` wildcard over tick/duration window shape.
        bool                       named_bundle{false}; ///< true for nominal ``TSB<Name,...>``.
        bool                       size_var{false};  ///< true when ``TSL`` size is a named variable.
        bool                       schema_var{false}; ///< true when ``TSB`` binds the whole schema to ``name``.

        [[nodiscard]] static TypePattern var(std::string name,
                                             std::vector<const TSValueTypeMetaData *> constraints = {})
        {
            TypePattern p;
            p.kind        = Kind::Var;
            p.name        = std::move(name);
            p.constraints = std::move(constraints);
            return p;
        }
        [[nodiscard]] static TypePattern concrete(const TSValueTypeMetaData *meta)
        {
            TypePattern p;
            p.kind = Kind::Concrete;
            p.meta = meta;
            return p;
        }
        [[nodiscard]] static TypePattern ts(ScalarPattern value)
        {
            TypePattern p;
            p.kind   = Kind::TS;
            p.scalar = std::move(value);
            return p;
        }
        [[nodiscard]] static TypePattern tss(ScalarPattern element)
        {
            TypePattern p;
            p.kind   = Kind::TSS;
            p.scalar = std::move(element);
            return p;
        }
        [[nodiscard]] static TypePattern tsl(TypePattern element, std::size_t fixed_size)
        {
            TypePattern p;
            p.kind       = Kind::TSL;
            p.fixed_size = fixed_size;
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static TypePattern tsl_var(TypePattern element,
                                                 std::string size_name,
                                                 std::vector<std::size_t> constraints = {})
        {
            TypePattern p;
            p.kind             = Kind::TSL;
            p.size_var         = true;
            p.size_name        = std::move(size_name);
            p.size_constraints = std::move(constraints);
            p.children.push_back(std::move(element));
            return p;
        }
        [[nodiscard]] static TypePattern tsw(ScalarPattern element, std::size_t period, std::size_t min_period)
        {
            TypePattern p;
            p.kind       = Kind::TSW;
            p.scalar     = std::move(element);
            p.fixed_size = period;
            p.min_size   = min_period;
            return p;
        }
        [[nodiscard]] static TypePattern tsw_any(ScalarPattern element)
        {
            TypePattern p;
            p.kind       = Kind::TSW;
            p.scalar     = std::move(element);
            p.any_window = true;
            return p;
        }
        [[nodiscard]] static TypePattern tsd(ScalarPattern key, TypePattern value)
        {
            TypePattern p;
            p.kind   = Kind::TSD;
            p.scalar = std::move(key);
            p.children.push_back(std::move(value));
            return p;
        }
        [[nodiscard]] static TypePattern tsb(std::vector<std::string> field_names,
                                             std::vector<TypePattern> children,
                                             std::string bundle_name = {},
                                             bool named_bundle = false)
        {
            TypePattern p;
            p.kind         = Kind::TSB;
            p.field_names  = std::move(field_names);
            p.children     = std::move(children);
            p.bundle_name  = std::move(bundle_name);
            p.named_bundle = named_bundle;
            return p;
        }
        [[nodiscard]] static TypePattern tsb_var(std::string schema_variable)
        {
            TypePattern p;
            p.kind       = Kind::TSB;
            p.name       = std::move(schema_variable);
            p.schema_var = true;
            return p;
        }
        [[nodiscard]] static TypePattern ref(TypePattern target)
        {
            TypePattern p;
            p.kind = Kind::REF;
            p.children.push_back(std::move(target));
            return p;
        }
        [[nodiscard]] static TypePattern signal()
        {
            TypePattern p;
            p.kind = Kind::Signal;
            return p;
        }
    };

    // -----------------------------------------------------------------
    // The one matcher / ranker / resolver (runtime; shared C++ & Python).
    // -----------------------------------------------------------------

    /**
     * Match ``pattern`` against the concrete interned schema ``concrete``, binding
     * any variable leaves into ``map`` (rejecting an inconsistent re-bind) and
     * verifying concrete leaves. Returns false on any mismatch; ``map`` may be
     * partially populated on failure (callers use a fresh map per candidate).
     */
    [[nodiscard]] HGRAPH_EXPORT bool ts_pattern_match(const TypePattern &pattern, const TSValueTypeMetaData *concrete,
                                                      ResolutionMap &map);

    /**
     * INPUT-direction match: mirrors graph input binding. SIGNAL accepts any
     * time-series source, REF wrappers adapt at the consumer, and concrete
     * leaves use the same compatibility rule as ordinary wiring.
     */
    [[nodiscard]] HGRAPH_EXPORT bool input_ts_pattern_match(const TypePattern &pattern,
                                                            const TSValueTypeMetaData *concrete,
                                                            ResolutionMap &map);

    /**
     * OUTPUT-direction match: REF transparency does NOT apply to a top-level
     * variable — a caller requesting ``REF[X]`` binds the variable to the
     * reference schema verbatim (the produced port must BE a reference;
     * consumers of a value port adapt at input binding, not here).
     */
    [[nodiscard]] HGRAPH_EXPORT bool output_ts_pattern_match(const TypePattern &pattern,
                                                             const TSValueTypeMetaData *concrete,
                                                             ResolutionMap &map);

    /** Scalar-layer counterpart of ``ts_pattern_match``. */
    [[nodiscard]] HGRAPH_EXPORT bool scalar_pattern_match(const ScalarPattern &pattern,
                                                          const ValueTypeMetaData *concrete, ResolutionMap &map);

    /** Match and bind the ``TSL`` size part of ``pattern`` against a concrete runtime size. */
    [[nodiscard]] HGRAPH_EXPORT bool size_pattern_match(const TypePattern &pattern,
                                                        std::size_t concrete_size,
                                                        ResolutionMap &map);

    /** Integer specificity of a pattern (lower = more specific). See *Operators > Ranking*. */
    [[nodiscard]] HGRAPH_EXPORT int ts_pattern_rank(const TypePattern &pattern);

    /** Scalar-layer counterpart of ``ts_pattern_rank``. */
    [[nodiscard]] HGRAPH_EXPORT int scalar_pattern_rank(const ScalarPattern &pattern);

    /** Substitute bound variables and produce the concrete schema, or ``nullptr`` if unbound. */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *ts_pattern_resolve(const TypePattern &pattern,
                                                                             const ResolutionMap &map);

    /** Scalar-layer counterpart of ``ts_pattern_resolve``. */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *scalar_pattern_resolve(const ScalarPattern &pattern,
                                                                               const ResolutionMap &map);

    /**
     * Replace scalar-variable leaves in a copied pattern. This is the shared
     * specialization primitive for runtime-authored generic schemas: a
     * replacement may be another variable (renaming) or a concrete/nested
     * scalar pattern. Unmentioned variables remain unresolved.
     */
    [[nodiscard]] HGRAPH_EXPORT ScalarPattern substitute_scalar_patterns(
        ScalarPattern pattern,
        const std::unordered_map<std::string, ScalarPattern> &replacements);
    [[nodiscard]] HGRAPH_EXPORT TypePattern substitute_scalar_patterns(
        TypePattern pattern,
        const std::unordered_map<std::string, ScalarPattern> &replacements);

    /** Replace named dimensions in Array and TSL patterns. */
    [[nodiscard]] HGRAPH_EXPORT ScalarPattern substitute_size_patterns(
        ScalarPattern pattern,
        const std::unordered_map<std::string, DimensionPattern> &replacements);
    [[nodiscard]] HGRAPH_EXPORT TypePattern substitute_size_patterns(
        TypePattern pattern,
        const std::unordered_map<std::string, DimensionPattern> &replacements);

    /** Human-readable rendering, for error messages. */
    [[nodiscard]] HGRAPH_EXPORT std::string ts_pattern_to_string(const TypePattern &pattern);
    [[nodiscard]] HGRAPH_EXPORT std::string scalar_pattern_to_string(const ScalarPattern &pattern);

    // -----------------------------------------------------------------
    // Compile-time lowering of a static schema type into a runtime pattern.
    // Known schema forms are lowered recursively; opaque concrete forms remain
    // Concrete leaves.
    // -----------------------------------------------------------------

    template <typename S>
    struct ts_pattern_lower
    {
        [[nodiscard]] static TypePattern lower()
        {
            static_assert(schema_descriptor<S>::is_concrete(),
                          "to_pattern<S>() requires a supported static time-series schema");
            return TypePattern::concrete(schema_descriptor<S>::ts_meta());
        }
    };

    template <typename T>
    struct scalar_pattern_lower
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            static_assert(scalar_descriptor<T>::is_concrete(),
                          "to_scalar_pattern<T>() requires a supported static scalar schema");
            return ScalarPattern::concrete(scalar_descriptor<T>::value_meta());
        }
    };

    template <typename S>
    [[nodiscard]] inline TypePattern to_pattern();

    template <typename T>
    [[nodiscard]] inline ScalarPattern to_scalar_pattern();

    namespace type_pattern_detail
    {
        template <typename... C>
        [[nodiscard]] inline std::vector<const TSValueTypeMetaData *> ts_constraints()
        {
            std::vector<const TSValueTypeMetaData *> out;
            out.reserve(sizeof...(C));
            (out.push_back(schema_descriptor<C>::ts_meta()), ...);
            return out;
        }

        template <typename... C>
        [[nodiscard]] inline std::vector<const ValueTypeMetaData *> scalar_constraints()
        {
            std::vector<const ValueTypeMetaData *> out;
            out.reserve(sizeof...(C));
            (out.push_back(scalar_descriptor<C>::value_meta()), ...);
            return out;
        }

        template <typename... Fields>
        [[nodiscard]] inline std::vector<std::string> tsb_field_names()
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(Fields));
            (names.emplace_back(ts_field_descriptor<Fields>::field_name()), ...);
            return names;
        }

        template <typename... Fields>
        [[nodiscard]] inline std::vector<TypePattern> tsb_field_patterns()
        {
            std::vector<TypePattern> patterns;
            patterns.reserve(sizeof...(Fields));
            (patterns.push_back(to_pattern<typename ts_field_descriptor<Fields>::schema>()), ...);
            return patterns;
        }
    }  // namespace type_pattern_detail

    template <typename S>
    [[nodiscard]] inline TypePattern to_pattern()
    {
        return ts_pattern_lower<S>::lower();
    }

    template <typename T>
    [[nodiscard]] inline ScalarPattern to_scalar_pattern()
    {
        return scalar_pattern_lower<T>::lower();
    }

    template <fixed_string Name, typename... C>
    struct ts_pattern_lower<TsVar<Name, C...>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            return TypePattern::var(std::string{Name.sv()}, type_pattern_detail::ts_constraints<C...>());
        }
    };

    template <typename V>
    struct ts_pattern_lower<TS<V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::ts(to_scalar_pattern<V>()); }
    };

    template <typename V>
    struct ts_pattern_lower<TSS<V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tss(to_scalar_pattern<V>()); }
    };

    template <typename C, auto N>
    struct ts_pattern_lower<TSL<C, N>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            using size = static_schema_detail::size_parameter_descriptor<N>;
            if constexpr (size::is_concrete())
            {
                return TypePattern::tsl(to_pattern<C>(), size::concrete_size());
            }
            else
            {
                return TypePattern::tsl_var(to_pattern<C>(), std::string{size::name()}, size::constraints());
            }
        }
    };

    template <typename C>
    struct ts_pattern_lower<Args<C>> : ts_pattern_lower<TSL<C, SIZE<"args_len">>>
    {
    };

    template <typename V, std::size_t Period, std::size_t MinPeriod>
    struct ts_pattern_lower<TSW<V, Period, MinPeriod>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            return TypePattern::tsw(to_scalar_pattern<V>(), Period, MinPeriod);
        }
    };

    template <typename V>
    struct ts_pattern_lower<TSWAny<V>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            return TypePattern::tsw_any(to_scalar_pattern<V>());
        }
    };

    template <typename K, typename V>
    struct ts_pattern_lower<TSD<K, V>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tsd(to_scalar_pattern<K>(), to_pattern<V>()); }
    };

    template <typename S>
    struct ts_pattern_lower<REF<S>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::ref(to_pattern<S>()); }
    };

    template <>
    struct ts_pattern_lower<SIGNAL>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::signal(); }
    };

    template <typename... Fields>
    struct ts_pattern_lower<UnNamedTSB<Fields...>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            return TypePattern::tsb(type_pattern_detail::tsb_field_names<Fields...>(),
                                    type_pattern_detail::tsb_field_patterns<Fields...>());
        }
    };

    template <fixed_string VarName, typename... C>
    struct ts_pattern_lower<UnNamedTSB<TsVar<VarName, C...>>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tsb_var(std::string{VarName.sv()}); }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_pattern_lower<TSB<Name, Fields...>>
    {
        [[nodiscard]] static TypePattern lower()
        {
            return TypePattern::tsb(type_pattern_detail::tsb_field_names<Fields...>(),
                                    type_pattern_detail::tsb_field_patterns<Fields...>(),
                                    std::string{Name.sv()}, true);
        }
    };

    template <fixed_string Name, fixed_string VarName, typename... C>
    struct ts_pattern_lower<TSB<Name, TsVar<VarName, C...>>>
    {
        [[nodiscard]] static TypePattern lower() { return TypePattern::tsb_var(std::string{VarName.sv()}); }
    };

    template <typename... Fields>
    struct ts_pattern_lower<Kwargs<Fields...>> : ts_pattern_lower<UnNamedTSB<Fields...>>
    {
    };

    template <>
    struct ts_pattern_lower<Kwargs<>> : ts_pattern_lower<UnNamedTSB<TsVar<"kwargs">>>
    {
    };

    template <fixed_string Name, typename... C>
    struct scalar_pattern_lower<ScalarVar<Name, C...>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::var(std::string{Name.sv()}, type_pattern_detail::scalar_constraints<C...>());
        }
    };

    template <>
    struct scalar_pattern_lower<UnknownTuple<>>
    {
        [[nodiscard]] static ScalarPattern lower() { return ScalarPattern::unknown_tuple(); }
    };

    template <typename TElement>
    struct scalar_pattern_lower<UnknownTuple<TElement>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::unknown_tuple(to_scalar_pattern<TElement>());
        }
    };

    template <typename TElement>
    struct scalar_pattern_lower<HomogeneousTuple<TElement>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::homogeneous_tuple(to_scalar_pattern<TElement>());
        }
    };

    template <typename TElement, auto... TDimensions>
    struct scalar_pattern_lower<ArrayOf<TElement, TDimensions...>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            std::vector<DimensionPattern> dimensions;
            dimensions.reserve(sizeof...(TDimensions) == 0 ? 1 : sizeof...(TDimensions));
            if constexpr (sizeof...(TDimensions) == 0)
            {
                dimensions.push_back(DimensionPattern::fixed(0));
            }
            else
            {
                ([&]() {
                    using dimension = static_schema_detail::size_parameter_descriptor<TDimensions>;
                    if constexpr (dimension::is_concrete())
                    {
                        dimensions.push_back(DimensionPattern::fixed(dimension::concrete_size()));
                    }
                    else
                    {
                        dimensions.push_back(DimensionPattern::var(std::string{dimension::name()}));
                    }
                }(), ...);
            }
            return ScalarPattern::array(to_scalar_pattern<TElement>(), std::move(dimensions));
        }
    };

    template <typename... TElements>
    struct scalar_pattern_lower<FixedTuple<TElements...>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            std::vector<ScalarPattern> elements;
            elements.reserve(sizeof...(TElements));
            (elements.push_back(to_scalar_pattern<TElements>()), ...);
            return ScalarPattern::fixed_tuple(std::move(elements));
        }
    };

    template <typename TElement>
    struct scalar_pattern_lower<Set<TElement>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::set(to_scalar_pattern<TElement>());
        }
    };

    template <typename TKey, typename TValue>
    struct scalar_pattern_lower<Map<TKey, TValue>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::map(to_scalar_pattern<TKey>(), to_scalar_pattern<TValue>());
        }
    };

    template <typename TElement>
    struct scalar_pattern_lower<SeriesOf<TElement>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            return ScalarPattern::series(to_scalar_pattern<TElement>());
        }
    };

    template <typename TSchema, typename TMetadata>
    struct scalar_pattern_lower<FrameOf<TSchema, TMetadata>>
    {
        [[nodiscard]] static ScalarPattern lower()
        {
            if constexpr (std::is_void_v<TMetadata>)
            {
                return ScalarPattern::frame(to_scalar_pattern<TSchema>());
            }
            else
            {
                return ScalarPattern::frame(to_scalar_pattern<TSchema>(),
                                            to_scalar_pattern<TMetadata>());
            }
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TYPE_PATTERN_H
