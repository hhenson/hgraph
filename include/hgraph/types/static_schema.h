#ifndef HGRAPH_CPP_ROOT_STATIC_SCHEMA_H
#define HGRAPH_CPP_ROOT_STATIC_SCHEMA_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/type_carrier.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Static schema is the C++-side compile-time vocabulary for describing
     * the same shapes the runtime ``TypeRegistry`` interns. Each marker
     * type below is a small struct template that carries its shape entirely
     * in template parameters; nothing instantiates at runtime.
     *
     * The bridge to the runtime is the ``schema_descriptor<T>`` /
     * ``scalar_descriptor<T>`` / ``ts_field_descriptor<F>`` trait family at
     * the bottom of this header. Each descriptor has one or two compile-
     * time queries (``is_concrete()``) and a single accessor that calls
     * into the registry every time:
     *
     * - ``scalar_descriptor<T>::value_meta()``
     *     ↪ ``TypeRegistry::register_scalar<T>(...)``
     * - ``schema_descriptor<TSchema>::ts_meta()``
     *     ↪ ``TypeRegistry::ts(...)`` / ``tss(...)`` / ``tsd(...)`` / etc.
     *
     * The descriptors deliberately do **not** cache the resolved pointer
     * in a function-local static. The registry's intern tables are
     * already the canonical cache: a second call into ``ts(...)`` /
     * ``tsd(...)`` / etc. with the same inputs returns the same pointer
     * via the InternTable hash lookup. Skipping the descriptor-side cache
     * keeps the lookup correct across registry resets (tests reset the
     * registries between cases via the test-fixture listener).
     *
     * See ``data_structures/schemas/static_schema.rst`` for the design
     * narrative.
     */

    /**
     * Compile-time string literal usable as a non-type template parameter.
     * Used to embed names (field names, bundle names) directly into a
     * schema type.
     */
    template <std::size_t N>
    struct fixed_string
    {
        char value[N]{};

        constexpr fixed_string(const char (&src)[N])
        {
            for (std::size_t i = 0; i < N; ++i) { value[i] = src[i]; }
        }

        [[nodiscard]] constexpr std::string_view sv() const noexcept
        {
            // Strip the trailing null terminator from the literal.
            return std::string_view{value, N - 1};
        }
    };

    template <std::size_t N>
    fixed_string(const char (&)[N]) -> fixed_string<N>;

    // -----------------------------------------------------------------
    // Time-series marker types
    // -----------------------------------------------------------------

    /** Scalar time-series schema; equivalent to ``TS[T]`` in the runtime. */
    template <typename TValue>
    struct TS
    {
        using value_type = TValue;
    };

    /** Set time-series schema; equivalent to ``TSS[T]``. */
    template <typename TValue>
    struct TSS
    {
        using value_type = TValue;
    };

    /** Dict time-series schema; equivalent to ``TSD[K, V]``. */
    template <typename TKey, typename TValueSchema>
    struct TSD
    {
        using key_type     = TKey;
        using value_schema = TValueSchema;
    };

    /** Named size variable used in generic fixed-size schemas, e.g. ``TSL<TS<Int>, SIZE<"N">>``. */
    template <fixed_string Name, std::size_t... TConstraints>
    struct SizeVar
    {
        static constexpr auto name_sv = Name;

        [[nodiscard]] constexpr bool operator==(const SizeVar &) const noexcept = default;
    };

    template <fixed_string Name, std::size_t... TConstraints>
    inline constexpr SizeVar<Name, TConstraints...> SIZE{};

    /** Time-series type variable used in generic schemas. Resolution happens at wiring time. */
    template <fixed_string Name, typename... TConstraints>
    struct TsVar;

    /** List time-series schema; equivalent to ``TSL[T, N?]``. Concrete ``N == 0`` is dynamic. */
    template <typename TElementSchema, auto FixedSize = 0>
    struct TSL
    {
        using element_schema             = TElementSchema;
        static constexpr auto fixed_size = FixedSize;
    };

    /** Node input marker for Python-style ``*args`` packing; resolves as ``TSL<T, SIZE<"args_len">>``. */
    template <typename TElementSchema = TsVar<"args">>
    struct Args
    {
        using element_schema             = TElementSchema;
        static constexpr auto fixed_size = SIZE<"args_len">;
    };

    /**
     * Tick-based sliding window; equivalent to runtime ``TSW`` constructed
     * via ``tsw(value_type, period, min_period)``. Duration-based windows
     * are not yet expressible as a compile-time type — use the runtime
     * registry for that case. Omitting ``MinPeriod`` makes it equal to
     * ``Period``.
     */
    template <typename TValue, std::size_t Period, std::size_t MinPeriod = Period>
    struct TSW
    {
        using value_type                      = TValue;
        static constexpr std::size_t period     = Period;
        static constexpr std::size_t min_period = MinPeriod;
    };

    /** Window time-series schema wildcard; matches any tick/duration window period. */
    template <typename TValue>
    struct TSWAny
    {
        using value_type = TValue;
    };

    /** REF marker; equivalent to runtime ``REF<TSchema>``. */
    template <typename TSchema>
    struct REF
    {
        using target_schema = TSchema;
    };

    /** Signal marker. */
    struct SIGNAL
    {};

    /** Named field used inside ``Bundle`` / ``UnNamedBundle`` / ``TSB`` / ``UnNamedTSB``. */
    template <fixed_string Name, typename TSchema>
    struct Field
    {
        using schema                  = TSchema;
        static constexpr auto name_sv = Name;
    };

    /** Un-named (structural) value-layer compound; corresponds to ``un_named_bundle``. */
    template <typename... TFields>
    struct UnNamedBundle
    {};

    /** Named value-layer compound; corresponds to ``bundle(name, fields)``. */
    template <fixed_string Name, typename... TFields>
    struct Bundle
    {
        static constexpr auto name_sv = Name;
    };

    /** One-pointer, on-demand owner for a value-layer schema. */
    template <typename TValue>
    struct Owned
    {
        using value_type = TValue;
    };

    /** Immutable one-pointer handle into the process-wide shared-value arena. */
    template <typename TValue>
    struct Shared
    {
        using value_type = TValue;
    };

    /** Type-erased scalar value. Storage is the native ``Any`` box, whose
        contained ``Value`` retains its concrete schema and ownership. */
    struct AnyValue
    {};

    /** Un-named (structural) time-series bundle; corresponds to ``un_named_tsb``. */
    template <typename... TFields>
    struct UnNamedTSB
    {};

    /** Node input marker for Python-style ``**kwargs`` packing; ``Kwargs<>`` resolves as ``UnNamedTSB<TsVar<"kwargs">>``. */
    template <typename... TFields>
    struct Kwargs
    {};

    /** Named time-series bundle; corresponds to ``tsb(name, fields)``. */
    template <fixed_string Name, typename... TFields>
    struct TSB
    {
        static constexpr auto name_sv = Name;
    };

    /** Derive a TSB schema from a value-layer Bundle by lifting each field to ``TS<Field>``. */
    template <typename TValueBundle>
    struct TimeSeriesBundleFromScalar
    {
        static_assert(!std::is_same_v<TValueBundle, TValueBundle>,
                      "TimeSeriesBundleFromScalar requires Bundle or UnNamedBundle");
    };

    template <typename... TFields>
    struct TimeSeriesBundleFromScalar<UnNamedBundle<TFields...>>
    {
        using type = UnNamedTSB<Field<TFields::name_sv, TS<typename TFields::schema>>...>;
    };

    template <fixed_string Name, typename... TFields>
    struct TimeSeriesBundleFromScalar<Bundle<Name, TFields...>>
    {
        using type = TSB<Name, Field<TFields::name_sv, TS<typename TFields::schema>>...>;
    };

    /** Public shorthand for the canonical field-expanded form of a scalar Bundle. */
    template <typename TValueBundle>
    using TSBFromScalar = typename TimeSeriesBundleFromScalar<TValueBundle>::type;

    template <fixed_string Name, typename... TConstraints>
    struct TsVar
    {
        static constexpr auto name_sv = Name;
    };

    /** Scalar (value-layer) type variable used in generic schemas. */
    template <fixed_string Name, typename... TConstraints>
    struct ScalarVar
    {
        static constexpr auto name_sv = Name;
    };

    /**
     * ``TypeArg`` default marker: the omitted argument resolves from the
     * carried pattern itself after the candidate's resolvers ran (Python's
     * ``AUTO_RESOLVE``). See ``TypeArg``.
     */
    struct AutoResolve
    {
    };

    /**
     * A wiring-time *type argument* parameter (RFC 0033): the value the caller
     * passes is a type — a time-series schema, a scalar schema or a size —
     * and matching it binds the variables of ``Pattern`` in the candidate's
     * resolution map, before the resolvers run. Declared in an
     * implementation's ``eval`` / ``compose`` parameter list at the position
     * the call expects it, exactly like ``Scalar<>``; it is not a runtime
     * field (``StaticNodeSignature`` leaves it out of the scalar layout) and
     * ``eval`` receives an empty placeholder, so a node's types keep coming
     * from its template parameters. A graph ``compose`` receives the
     * ``TypeCarrier`` the dispatcher matched or materialised.
     *
     * ``Pattern`` is the carried pattern: a time-series marker (``TsVar<"O">``,
     * ``TS<ScalarVar<"T">>``), a scalar marker (``ScalarVar<"T">``, ``Int``)
     * or a ``SizeVar<"N">``. ``Default`` is ``void`` (required argument),
     * ``AutoResolve`` (deferred: materialised from ``Pattern`` after the
     * resolvers) or another pattern type (deferred: materialised from that
     * pattern, then matched against ``Pattern``). A concrete default comes
     * from the ``defaults()`` hook as a ``TypeCarrier`` value.
     */
    template <fixed_string Name, typename Pattern, typename Default = void>
    struct TypeArg
    {
        static constexpr auto field_name = Name;
        using pattern_type              = Pattern;
        using default_type              = Default;

        TypeCarrier carrier{};

        [[nodiscard]] const TypeCarrier &value() const noexcept { return carrier; }
    };

    /** Scalar tuple shape that can match either a runtime tuple or homogeneous tuple-list. */
    template <typename... TElements>
    struct UnknownTuple
    {
        static_assert(sizeof...(TElements) <= 1, "UnknownTuple accepts zero or one element pattern");
    };

    /** Scalar homogeneous tuple, equivalent to Python ``tuple[T, ...]``. */
    template <typename TElement>
    struct HomogeneousTuple
    {
        using element_type = TElement;
    };

    /** Shaped numerical array. Zero (or an omitted dimension) denotes an unbounded 1-D array. */
    template <typename TElement, auto... TDimensions>
    struct ArrayOf
    {
        using element_type = TElement;
    };

    /** Scalar fixed-position tuple. ``Tuple`` is the user-facing alias. */
    template <typename... TElements>
    struct FixedTuple
    {
    };

    template <typename... TElements>
    using Tuple = FixedTuple<TElements...>;

    /** Scalar set. */
    template <typename TElement>
    struct Set
    {
        using element_type = TElement;
    };

    /** Scalar map. */
    template <typename TKey, typename TValue>
    struct Map
    {
        using key_type   = TKey;
        using value_type = TValue;
    };

    /** Arrow Series scalar qualified by its element schema. Runtime storage is ``Series``. */
    template <typename TElement>
    struct SeriesOf
    {
        using element_type = TElement;
    };

    /** Arrow Frame scalar qualified by row and optional Arrow-metadata Bundle schemas. */
    template <typename TSchema, typename TMetadata = void>
    struct FrameOf
    {
        using schema_type = TSchema;
        using metadata_type = TMetadata;
    };

    // -----------------------------------------------------------------
    // Descriptor traits: bridge to the runtime registry
    // -----------------------------------------------------------------

    namespace static_schema_detail
    {
        template <typename T>
        inline constexpr bool always_false_v = false;

        template <typename T>
        struct size_var_type_descriptor
        {
            static constexpr bool is_size_var = false;
            [[nodiscard]] static constexpr std::string_view name() noexcept { return {}; }
            [[nodiscard]] static std::vector<std::size_t> constraints() { return {}; }
        };

        template <fixed_string Name, std::size_t... TConstraints>
        struct size_var_type_descriptor<SizeVar<Name, TConstraints...>>
        {
            static constexpr bool is_size_var = true;
            [[nodiscard]] static constexpr std::string_view name() noexcept { return Name.sv(); }
            [[nodiscard]] static std::vector<std::size_t> constraints() { return {TConstraints...}; }
        };

        template <auto TSize>
        struct size_parameter_descriptor
        {
            using raw_type = std::remove_cvref_t<decltype(TSize)>;
            using var      = size_var_type_descriptor<raw_type>;

            static_assert(std::is_integral_v<raw_type> || var::is_size_var,
                          "TSL fixed size must be an integral constant or SIZE<\"Name\">");

            [[nodiscard]] static constexpr bool is_concrete() noexcept { return !var::is_size_var; }
            [[nodiscard]] static constexpr std::size_t concrete_size() noexcept
            {
                if constexpr (is_concrete()) { return static_cast<std::size_t>(TSize); }
                else { return 0; }
            }
            [[nodiscard]] static constexpr std::string_view name() noexcept { return var::name(); }
            [[nodiscard]] static std::vector<std::size_t> constraints() { return var::constraints(); }
        };

        /**
         * Storage for the canonical scalar registration name. ``T`` carries
         * its display name through ``TypeRegistry::register_scalar<T>``;
         * for built-in scalars the name has to be supplied somewhere. We
         * specialise this template per-type for the built-ins; user-
         * defined scalars must specialise it once to register their name.
         */
        template <typename T>
        struct scalar_name;

        template <>
        struct scalar_name<bool>        { static constexpr std::string_view value{"bool"};        };
        template <>
        struct scalar_name<std::int8_t> { static constexpr std::string_view value{"int8"};        };
        template <>
        struct scalar_name<std::int16_t>{ static constexpr std::string_view value{"int16"};       };
        template <>
        struct scalar_name<std::int32_t>{ static constexpr std::string_view value{"int32"};       };
        template <>
        struct scalar_name<std::int64_t>{ static constexpr std::string_view value{"int"};         };
        template <>
        struct scalar_name<std::uint8_t>{ static constexpr std::string_view value{"uint8"};       };
        template <>
        struct scalar_name<std::uint16_t>{ static constexpr std::string_view value{"uint16"};      };
        template <>
        struct scalar_name<std::uint32_t>{ static constexpr std::string_view value{"uint32"};      };
        template <>
        struct scalar_name<std::uint64_t>{ static constexpr std::string_view value{"uint64"};      };
        template <>
        struct scalar_name<float>       { static constexpr std::string_view value{"float32"};     };
        template <>
        struct scalar_name<double>      { static constexpr std::string_view value{"float"};       };
        template <>
        struct scalar_name<std::string> { static constexpr std::string_view value{"str"};         };
        template <>
        struct scalar_name<Date>        { static constexpr std::string_view value{"date"};        };
        template <>
        struct scalar_name<DateTime>    { static constexpr std::string_view value{"datetime"};    };
        template <>
        struct scalar_name<TimeDelta>   { static constexpr std::string_view value{"timedelta"};   };
        template <>
        struct scalar_name<Time>        { static constexpr std::string_view value{"time"};        };
        template <>
        struct scalar_name<Bytes>       { static constexpr std::string_view value{"bytes"};       };
        template <>
        struct scalar_name<TypeCarrier> { static constexpr std::string_view value{"type"};        };
    }  // namespace static_schema_detail

    /**
     * Descriptor for a value-layer scalar type. The default specialisation
     * handles concrete built-in scalars; ``ScalarVar<...>`` is handled by
     * its own specialisation and reports as non-concrete.
     */
    template <typename T>
    struct scalar_descriptor
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            return TypeRegistry::instance().register_scalar<T>(
                static_schema_detail::scalar_name<T>::value);
        }
    };

    template <fixed_string Name, typename... TConstraints>
    struct scalar_descriptor<ScalarVar<Name, TConstraints...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const ValueTypeMetaData *value_meta() noexcept { return nullptr; }
    };

    template <typename... TElements>
    struct scalar_descriptor<UnknownTuple<TElements...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const ValueTypeMetaData *value_meta() noexcept { return nullptr; }
    };

    template <typename TElement>
    struct scalar_descriptor<HomogeneousTuple<TElement>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TElement>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().list(scalar_descriptor<TElement>::value_meta(), 0, true);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TElement, auto... TDimensions>
    struct scalar_descriptor<ArrayOf<TElement, TDimensions...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TElement>::is_concrete() &&
                   (static_schema_detail::size_parameter_descriptor<TDimensions>::is_concrete() && ...);
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                if constexpr (sizeof...(TDimensions) == 0)
                {
                    return TypeRegistry::instance().array(
                        scalar_descriptor<TElement>::value_meta(), 0);
                }
                else
                {
                    const std::vector<std::size_t> dimensions{
                        static_schema_detail::size_parameter_descriptor<TDimensions>::concrete_size()...};
                    return TypeRegistry::instance().array(
                        scalar_descriptor<TElement>::value_meta(), dimensions);
                }
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename... TElements>
    struct scalar_descriptor<FixedTuple<TElements...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return (scalar_descriptor<TElements>::is_concrete() && ...);
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                std::vector<const ValueTypeMetaData *> elements;
                elements.reserve(sizeof...(TElements));
                (elements.push_back(scalar_descriptor<TElements>::value_meta()), ...);
                return TypeRegistry::instance().tuple(elements);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TElement>
    struct scalar_descriptor<Set<TElement>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TElement>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete()) { return TypeRegistry::instance().set(scalar_descriptor<TElement>::value_meta()); }
            else { return nullptr; }
        }
    };

    template <typename TKey, typename TValue>
    struct scalar_descriptor<Map<TKey, TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TKey>::is_concrete() &&
                   scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().map(scalar_descriptor<TKey>::value_meta(),
                                                    scalar_descriptor<TValue>::value_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TElement>
    struct scalar_descriptor<SeriesOf<TElement>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TElement>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().series(scalar_descriptor<TElement>::value_meta());
            }
            else { return nullptr; }
        }
    };

    template <typename TSchema, typename TMetadata>
    struct scalar_descriptor<FrameOf<TSchema, TMetadata>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TSchema>::is_concrete() &&
                   (std::is_void_v<TMetadata> || scalar_descriptor<TMetadata>::is_concrete());
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                if constexpr (std::is_void_v<TMetadata>)
                {
                    return TypeRegistry::instance().frame(scalar_descriptor<TSchema>::value_meta());
                }
                else
                {
                    return TypeRegistry::instance().frame(scalar_descriptor<TSchema>::value_meta(),
                                                          scalar_descriptor<TMetadata>::value_meta());
                }
            }
            else { return nullptr; }
        }
    };

    template <typename TValue>
    struct scalar_descriptor<Owned<TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().owned(scalar_descriptor<TValue>::value_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TValue>
    struct scalar_descriptor<Shared<TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().shared(scalar_descriptor<TValue>::value_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <>
    struct scalar_descriptor<AnyValue>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            return TypeRegistry::instance().any();
        }
    };

    /** Descriptor for a static time-series schema. Specialised per marker type below. */
    template <typename TSchema>
    struct schema_descriptor
    {
        static_assert(static_schema_detail::always_false_v<TSchema>,
                      "Unsupported static time-series schema");
    };

    template <typename TValue>
    struct schema_descriptor<TS<TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().ts(scalar_descriptor<TValue>::value_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TValue>
    struct schema_descriptor<TSS<TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().tss(scalar_descriptor<TValue>::value_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TKey, typename TValueSchema>
    struct schema_descriptor<TSD<TKey, TValueSchema>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TKey>::is_concrete() &&
                   schema_descriptor<TValueSchema>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().tsd(scalar_descriptor<TKey>::value_meta(),
                                                    schema_descriptor<TValueSchema>::ts_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TElementSchema, auto FixedSize>
    struct schema_descriptor<TSL<TElementSchema, FixedSize>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return schema_descriptor<TElementSchema>::is_concrete() &&
                   static_schema_detail::size_parameter_descriptor<FixedSize>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().tsl(
                    schema_descriptor<TElementSchema>::ts_meta(),
                    static_schema_detail::size_parameter_descriptor<FixedSize>::concrete_size());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TElementSchema>
    struct schema_descriptor<Args<TElementSchema>>
        : schema_descriptor<TSL<TElementSchema, SIZE<"args_len">>>
    {
    };

    template <typename TValue, std::size_t Period, std::size_t MinPeriod>
    struct schema_descriptor<TSW<TValue, Period, MinPeriod>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TValue>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().tsw(
                    scalar_descriptor<TValue>::value_meta(), Period, MinPeriod);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename TValue>
    struct schema_descriptor<TSWAny<TValue>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta() noexcept { return nullptr; }
    };

    template <typename TSchema>
    struct schema_descriptor<REF<TSchema>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return schema_descriptor<TSchema>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                return TypeRegistry::instance().ref(schema_descriptor<TSchema>::ts_meta());
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <>
    struct schema_descriptor<SIGNAL>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            return TypeRegistry::instance().signal();
        }
    };

    template <fixed_string Name, typename... TConstraints>
    struct schema_descriptor<TsVar<Name, TConstraints...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta() noexcept { return nullptr; }
    };

    // -----------------------------------------------------------------
    // Field descriptor (TSB / Bundle members) and bundle/TSB descriptors
    // -----------------------------------------------------------------

    /** Descriptor for a ``Field<Name, TSchema>`` used in TSB / UnNamedTSB. */
    template <typename TField>
    struct ts_field_descriptor;

    template <fixed_string Name, typename TSchema>
    struct ts_field_descriptor<Field<Name, TSchema>>
    {
        using schema = TSchema;

        [[nodiscard]] static std::string field_name() { return std::string{Name.sv()}; }

        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return schema_descriptor<TSchema>::is_concrete();
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            return schema_descriptor<TSchema>::ts_meta();
        }
    };

    /** Descriptor for a ``Field<Name, TSchema>`` used in Bundle / UnNamedBundle. */
    template <typename TField>
    struct value_field_descriptor;

    template <fixed_string Name, typename TSchema>
    struct value_field_descriptor<Field<Name, TSchema>>
    {
        using schema = TSchema;

        [[nodiscard]] static std::string field_name() { return std::string{Name.sv()}; }

        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return scalar_descriptor<TSchema>::is_concrete();
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            return scalar_descriptor<TSchema>::value_meta();
        }
    };

    template <typename... TFields>
    struct schema_descriptor<UnNamedTSB<TFields...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return (ts_field_descriptor<TFields>::is_concrete() && ...);
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(sizeof...(TFields));
                (fields.emplace_back(ts_field_descriptor<TFields>::field_name(),
                                     ts_field_descriptor<TFields>::ts_meta()),
                 ...);
                return TypeRegistry::instance().un_named_tsb(fields);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <fixed_string VarName, typename... TConstraints>
    struct schema_descriptor<UnNamedTSB<TsVar<VarName, TConstraints...>>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta() noexcept { return nullptr; }
    };

    template <fixed_string Name, typename... TFields>
    struct schema_descriptor<TSB<Name, TFields...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return (ts_field_descriptor<TFields>::is_concrete() && ...);
        }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta()
        {
            if constexpr (is_concrete())
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(sizeof...(TFields));
                (fields.emplace_back(ts_field_descriptor<TFields>::field_name(),
                                     ts_field_descriptor<TFields>::ts_meta()),
                 ...);
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <fixed_string Name, fixed_string VarName, typename... TConstraints>
    struct schema_descriptor<TSB<Name, TsVar<VarName, TConstraints...>>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return false; }

        [[nodiscard]] static const TSValueTypeMetaData *ts_meta() noexcept { return nullptr; }
    };

    template <typename... TFields>
    struct schema_descriptor<Kwargs<TFields...>> : schema_descriptor<UnNamedTSB<TFields...>>
    {
    };

    template <>
    struct schema_descriptor<Kwargs<>> : schema_descriptor<UnNamedTSB<TsVar<"kwargs">>>
    {
    };

    /** Descriptor for value-layer Bundle/UnNamedBundle. */
    template <typename TBundle>
    struct value_schema_descriptor
    {
        static_assert(static_schema_detail::always_false_v<TBundle>,
                      "value_schema_descriptor only specialises for Bundle / UnNamedBundle");
    };

    template <typename... TFields>
    struct value_schema_descriptor<UnNamedBundle<TFields...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return (value_field_descriptor<TFields>::is_concrete() && ...);
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                fields.reserve(sizeof...(TFields));
                (fields.emplace_back(value_field_descriptor<TFields>::field_name(),
                                     value_field_descriptor<TFields>::value_meta()),
                 ...);
                return TypeRegistry::instance().un_named_bundle(fields);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <fixed_string Name, typename... TFields>
    struct value_schema_descriptor<Bundle<Name, TFields...>>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept
        {
            return (value_field_descriptor<TFields>::is_concrete() && ...);
        }

        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            if constexpr (is_concrete())
            {
                std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                fields.reserve(sizeof...(TFields));
                (fields.emplace_back(value_field_descriptor<TFields>::field_name(),
                                     value_field_descriptor<TFields>::value_meta()),
                 ...);
                return TypeRegistry::instance().bundle(Name.sv(), fields);
            }
            else
            {
                return nullptr;
            }
        }
    };

    template <typename... TFields>
    struct scalar_descriptor<UnNamedBundle<TFields...>> : value_schema_descriptor<UnNamedBundle<TFields...>>
    {
    };

    template <fixed_string Name, typename... TFields>
    struct scalar_descriptor<Bundle<Name, TFields...>> : value_schema_descriptor<Bundle<Name, TFields...>>
    {
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_STATIC_SCHEMA_H
