#ifndef HGRAPH_TYPES_TYPE_RESOLUTION_H
#define HGRAPH_TYPES_TYPE_RESOLUTION_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_carrier.h>   // ResolutionKind, TypeCarrier

#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Wiring-time resolution of type variables (``TsVar`` / ``ScalarVar``) to concrete
     * registry metadata. A generic node is authored once over deferred schemas; at the
     * wiring site each variable is bound — from a connected input port (``unify``), an
     * inferred scalar value, or an explicit ``ts_type<...>()`` — and the node's concrete
     * input / output / scalar schemas are then computed by substituting the bindings
     * into the (otherwise compile-time) schema descriptors (``resolve``).
     *
     * This is the C++ counterpart of the resolution 2603 performs in its Python wiring
     * layer; keeping it in the wiring core lets the eventual Python bridge reuse it and
     * is the seed for generic ``map_`` / ``reduce`` / ``switch_``.
     */
    struct ResolutionMap
    {
        std::unordered_map<std::string, const TSValueTypeMetaData *> ts_vars;
        std::unordered_map<std::string, const ValueTypeMetaData *>   scalar_vars;
        std::unordered_map<std::string, std::size_t>                 size_vars;

        /**
         * Whether ``name`` already has a concrete wiring-time resolution.
         *
         * Keep the store-specific distinction here, rather than making each
         * resolver front end repeat three subtly different lookups. Dynamic
         * authoring bridges normally use ``Any`` because a resolver target's
         * kind is determined by the value it returns; typed C++ wiring can ask
         * for the exact store at compile time.
         */
        template <ResolutionKind Kind = ResolutionKind::Any>
        [[nodiscard]] bool is_resolved(std::string_view name) const
        {
            if constexpr (Kind == ResolutionKind::TimeSeries)
            {
                return ts_vars.contains(std::string{name});
            }
            else if constexpr (Kind == ResolutionKind::Scalar)
            {
                return scalar_vars.contains(std::string{name});
            }
            else if constexpr (Kind == ResolutionKind::Size)
            {
                return size_vars.contains(std::string{name});
            }
            else
            {
                static_assert(Kind == ResolutionKind::Any);
                return is_resolved<ResolutionKind::TimeSeries>(name) ||
                       is_resolved<ResolutionKind::Scalar>(name) ||
                       is_resolved<ResolutionKind::Size>(name);
            }
        }

        /** Bind time-series variable ``name``; re-binding to a different meta is an error. */
        void bind_ts(std::string_view name, const TSValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error(fmt::format("type variable '{}' resolved to null", name)); }
            auto [it, inserted] = ts_vars.try_emplace(std::string{name}, meta);
            if (!inserted && it->second != meta)
            {
                throw std::logic_error(fmt::format("type variable '{}' resolved inconsistently", name));
            }
        }

        /** Bind scalar variable ``name``; re-binding to a different meta is an error. */
        void bind_scalar(std::string_view name, const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error(fmt::format("scalar variable '{}' resolved to null", name)); }
            auto [it, inserted] = scalar_vars.try_emplace(std::string{name}, meta);
            if (!inserted && it->second != meta)
            {
                throw std::logic_error(fmt::format("scalar variable '{}' resolved inconsistently", name));
            }
        }

        /** Bind size variable ``name``; re-binding to a different value is an error. */
        void bind_size(std::string_view name, std::size_t size)
        {
            auto [it, inserted] = size_vars.try_emplace(std::string{name}, size);
            if (!inserted && it->second != size)
            {
                throw std::logic_error(fmt::format("size variable '{}' resolved inconsistently", name));
            }
        }

        [[nodiscard]] const TSValueTypeMetaData *ts(std::string_view name) const
        {
            auto it = ts_vars.find(std::string{name});
            if (it == ts_vars.end()) { throw std::logic_error(fmt::format("unresolved type variable '{}'", name)); }
            return it->second;
        }

        [[nodiscard]] const TSValueTypeMetaData *find_ts(std::string_view name) const
        {
            auto it = ts_vars.find(std::string{name});
            return it != ts_vars.end() ? it->second : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *scalar(std::string_view name) const
        {
            auto it = scalar_vars.find(std::string{name});
            if (it == scalar_vars.end()) { throw std::logic_error(fmt::format("unresolved scalar variable '{}'", name)); }
            return it->second;
        }

        [[nodiscard]] const ValueTypeMetaData *find_scalar(std::string_view name) const
        {
            auto it = scalar_vars.find(std::string{name});
            return it != scalar_vars.end() ? it->second : nullptr;
        }

        [[nodiscard]] std::size_t size(std::string_view name) const
        {
            auto it = size_vars.find(std::string{name});
            if (it == size_vars.end()) { throw std::logic_error(fmt::format("unresolved size variable '{}'", name)); }
            return it->second;
        }

        [[nodiscard]] std::optional<std::size_t> find_size(std::string_view name) const
        {
            auto it = size_vars.find(std::string{name});
            return it != size_vars.end() ? std::optional<std::size_t>{it->second} : std::nullopt;
        }
    };

    // -----------------------------------------------------------------
    // scalar_resolver<T> — resolve a scalar schema type to its value meta,
    // substituting a ``ScalarVar`` leaf from the map.
    // -----------------------------------------------------------------
    template <auto TSize>
    struct size_resolver
    {
        [[nodiscard]] static std::size_t resolve(const ResolutionMap &m);
    };

    template <auto TSize>
    struct size_unifier
    {
        static void unify(std::size_t concrete, ResolutionMap &m);
    };

    template <typename T>
    struct scalar_resolver
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &) { return scalar_descriptor<T>::value_meta(); }
    };

    template <fixed_string Name, typename... C>
    struct scalar_resolver<ScalarVar<Name, C...>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m) { return m.scalar(Name.sv()); }
    };

    template <typename... TElements>
    struct scalar_resolver<UnknownTuple<TElements...>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &) noexcept { return nullptr; }
    };

    template <typename TElement>
    struct scalar_resolver<HomogeneousTuple<TElement>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().list(scalar_resolver<TElement>::resolve(m), 0, true);
        }
    };

    template <typename TElement, auto... TDimensions>
    struct scalar_resolver<ArrayOf<TElement, TDimensions...>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            if constexpr (sizeof...(TDimensions) == 0)
            {
                return TypeRegistry::instance().array(
                    scalar_resolver<TElement>::resolve(m), 0);
            }
            else
            {
                const std::vector<std::size_t> dimensions{
                    size_resolver<TDimensions>::resolve(m)...};
                return TypeRegistry::instance().array(
                    scalar_resolver<TElement>::resolve(m), dimensions);
            }
        }
    };

    template <typename... TElements>
    struct scalar_resolver<FixedTuple<TElements...>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            std::vector<const ValueTypeMetaData *> elements;
            elements.reserve(sizeof...(TElements));
            (elements.push_back(scalar_resolver<TElements>::resolve(m)), ...);
            return TypeRegistry::instance().tuple(elements);
        }
    };

    template <typename TElement>
    struct scalar_resolver<Set<TElement>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().set(scalar_resolver<TElement>::resolve(m));
        }
    };

    template <typename TKey, typename TValue>
    struct scalar_resolver<Map<TKey, TValue>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().map(scalar_resolver<TKey>::resolve(m),
                                                scalar_resolver<TValue>::resolve(m));
        }
    };

    template <typename TElement>
    struct scalar_resolver<SeriesOf<TElement>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().series(scalar_resolver<TElement>::resolve(m));
        }
    };

    template <typename TSchema, typename TMetadata>
    struct scalar_resolver<FrameOf<TSchema, TMetadata>>
    {
        [[nodiscard]] static const ValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            const auto *row = scalar_resolver<TSchema>::resolve(m);
            if constexpr (std::is_void_v<TMetadata>) { return TypeRegistry::instance().frame(row); }
            else { return TypeRegistry::instance().frame(row, scalar_resolver<TMetadata>::resolve(m)); }
        }
    };

    template <auto TSize>
    std::size_t size_resolver<TSize>::resolve(const ResolutionMap &m)
    {
        using size = static_schema_detail::size_parameter_descriptor<TSize>;
        if constexpr (size::is_concrete()) { return size::concrete_size(); }
        else { return m.size(size::name()); }
    }

    // -----------------------------------------------------------------
    // ts_resolver<S> — resolve a time-series schema type to its TS meta,
    // substituting any ``TsVar`` / ``ScalarVar`` leaves. The primary template
    // is the concrete fall-back (used for SIGNAL / TSB / anything without a
    // var); the composite kinds are specialised so they recurse.
    // -----------------------------------------------------------------
    template <typename S>
    struct ts_resolver
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &) { return schema_descriptor<S>::ts_meta(); }
    };

    template <fixed_string Name, typename... C>
    struct ts_resolver<TsVar<Name, C...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m) { return m.ts(Name.sv()); }
    };

    template <typename V>
    struct ts_resolver<TS<V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().ts(scalar_resolver<V>::resolve(m));
        }
    };

    template <typename V>
    struct ts_resolver<TSS<V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tss(scalar_resolver<V>::resolve(m));
        }
    };

    template <typename C, auto N>
    struct ts_resolver<TSL<C, N>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsl(ts_resolver<C>::resolve(m), size_resolver<N>::resolve(m));
        }
    };

    template <typename C>
    struct ts_resolver<Args<C>> : ts_resolver<TSL<C, SIZE<"args_len">>>
    {
    };

    template <typename V, std::size_t Period, std::size_t MinPeriod>
    struct ts_resolver<TSW<V, Period, MinPeriod>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsw(scalar_resolver<V>::resolve(m), Period, MinPeriod);
        }
    };

    template <typename V>
    struct ts_resolver<TSWAny<V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &) noexcept { return nullptr; }
    };

    template <typename K, typename V>
    struct ts_resolver<TSD<K, V>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().tsd(scalar_resolver<K>::resolve(m), ts_resolver<V>::resolve(m));
        }
    };

    template <typename... Fields>
    struct ts_resolver<UnNamedTSB<Fields...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(sizeof...(Fields));
            (fields.emplace_back(ts_field_descriptor<Fields>::field_name(),
                                 ts_resolver<typename ts_field_descriptor<Fields>::schema>::resolve(m)),
             ...);
            return TypeRegistry::instance().un_named_tsb(fields);
        }
    };

    template <fixed_string VarName, typename... C>
    struct ts_resolver<UnNamedTSB<TsVar<VarName, C...>>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return m.ts(VarName.sv());
        }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_resolver<TSB<Name, Fields...>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(sizeof...(Fields));
            (fields.emplace_back(ts_field_descriptor<Fields>::field_name(),
                                 ts_resolver<typename ts_field_descriptor<Fields>::schema>::resolve(m)),
             ...);
            return TypeRegistry::instance().tsb(Name.sv(), fields);
        }
    };

    template <fixed_string Name, fixed_string VarName, typename... C>
    struct ts_resolver<TSB<Name, TsVar<VarName, C...>>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return m.ts(VarName.sv());
        }
    };

    template <typename... Fields>
    struct ts_resolver<Kwargs<Fields...>> : ts_resolver<UnNamedTSB<Fields...>>
    {
    };

    template <>
    struct ts_resolver<Kwargs<>> : ts_resolver<UnNamedTSB<TsVar<"kwargs">>>
    {
    };

    template <typename S>
    struct ts_resolver<REF<S>>
    {
        [[nodiscard]] static const TSValueTypeMetaData *resolve(const ResolutionMap &m)
        {
            return TypeRegistry::instance().ref(ts_resolver<S>::resolve(m));
        }
    };

    // -----------------------------------------------------------------
    // scalar_unifier<T> / ts_unifier<S> — bind variables by matching a
    // pattern schema type against a concrete runtime meta (e.g. a connected
    // input port's schema). A concrete leaf is a no-op (structural match is
    // already enforced at compile time on the concrete wiring path).
    // -----------------------------------------------------------------
    template <typename T>
    struct scalar_unifier
    {
        static void unify(const ValueTypeMetaData *, ResolutionMap &) noexcept {}
    };

    template <fixed_string Name, typename... C>
    struct scalar_unifier<ScalarVar<Name, C...>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            if constexpr (sizeof...(C) > 0)
            {
                if (!((concrete == scalar_descriptor<C>::value_meta()) || ...))
                {
                    throw std::logic_error(
                        fmt::format("scalar variable '{}' resolved outside its constraints", Name.sv()));
                }
            }
            m.bind_scalar(Name.sv(), concrete);
        }
    };

    template <typename TElement>
    struct scalar_unifier<SeriesOf<TElement>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            auto &registry = TypeRegistry::instance();
            if (!registry.is_series(concrete) || concrete->element_type == nullptr)
            {
                throw std::logic_error("expected a typed Series scalar");
            }
            scalar_unifier<TElement>::unify(concrete->element_type, m);
        }
    };

    template <typename TSchema, typename TMetadata>
    struct scalar_unifier<FrameOf<TSchema, TMetadata>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            auto &registry = TypeRegistry::instance();
            if (!registry.is_frame(concrete) || concrete->element_type == nullptr)
            {
                throw std::logic_error("expected a typed Frame scalar");
            }
            scalar_unifier<TSchema>::unify(concrete->element_type, m);
            if constexpr (std::is_void_v<TMetadata>)
            {
                if (concrete->key_type != nullptr)
                {
                    throw std::logic_error("expected a metadata-free Frame scalar");
                }
            }
            else
            {
                if (concrete->key_type == nullptr)
                {
                    throw std::logic_error("expected a metadata-bearing Frame scalar");
                }
                scalar_unifier<TMetadata>::unify(concrete->key_type, m);
            }
        }
    };

    template <typename TElement, auto... TDimensions>
    struct scalar_unifier<ArrayOf<TElement, TDimensions...>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            if (!TypeRegistry::is_array(concrete))
            {
                throw std::logic_error("expected a shaped Array scalar");
            }
            const auto dimensions = TypeRegistry::array_dimensions(concrete);
            if constexpr (sizeof...(TDimensions) != 0)
            {
                if (dimensions.size() != sizeof...(TDimensions))
                {
                    throw std::logic_error("Array rank does not match its static schema");
                }
                std::size_t index = 0;
                (size_unifier<TDimensions>::unify(dimensions[index++], m), ...);
            }
            scalar_unifier<TElement>::unify(TypeRegistry::array_element(concrete), m);
        }
    };

    namespace type_resolution_detail
    {
        [[nodiscard]] inline const ValueTypeMetaData *homogeneous_tuple_element(
            const ValueTypeMetaData *concrete) noexcept
        {
            if (concrete == nullptr || concrete->try_value_kind() != ValueTypeKind::Tuple || concrete->field_count == 0)
            {
                return nullptr;
            }
            const ValueTypeMetaData *first = concrete->fields[0].type;
            for (std::size_t index = 1; index < concrete->field_count; ++index)
            {
                if (concrete->fields[index].type != first) { return nullptr; }
            }
            return first;
        }

        template <typename... TElements, std::size_t... I>
        void unify_fixed_tuple_fields(const ValueTypeMetaData *concrete,
                                      ResolutionMap &m,
                                      std::index_sequence<I...>)
        {
            (scalar_unifier<TElements>::unify(concrete->fields[I].type, m), ...);
        }
    }  // namespace type_resolution_detail

    template <>
    struct scalar_unifier<UnknownTuple<>>
    {
        static void unify(const ValueTypeMetaData *, ResolutionMap &) noexcept {}
    };

    template <typename TElement>
    struct scalar_unifier<UnknownTuple<TElement>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            const ValueTypeMetaData *element = nullptr;
            if (concrete != nullptr && concrete->value_kind() == ValueTypeKind::List) { element = concrete->element_type; }
            else { element = type_resolution_detail::homogeneous_tuple_element(concrete); }
            scalar_unifier<TElement>::unify(element, m);
        }
    };

    template <typename TElement>
    struct scalar_unifier<HomogeneousTuple<TElement>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            const ValueTypeMetaData *element = nullptr;
            if (concrete != nullptr && concrete->value_kind() == ValueTypeKind::List) { element = concrete->element_type; }
            else { element = type_resolution_detail::homogeneous_tuple_element(concrete); }
            scalar_unifier<TElement>::unify(element, m);
        }
    };

    template <typename... TElements>
    struct scalar_unifier<FixedTuple<TElements...>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            if (concrete == nullptr || concrete->value_kind() != ValueTypeKind::Tuple ||
                concrete->field_count != sizeof...(TElements))
            {
                return;
            }
            type_resolution_detail::unify_fixed_tuple_fields<TElements...>(
                concrete, m, std::make_index_sequence<sizeof...(TElements)>{});
        }
    };

    template <typename TElement>
    struct scalar_unifier<Set<TElement>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            if (concrete == nullptr || concrete->value_kind() != ValueTypeKind::Set) { return; }
            scalar_unifier<TElement>::unify(concrete->element_type, m);
        }
    };

    template <typename TKey, typename TValue>
    struct scalar_unifier<Map<TKey, TValue>>
    {
        static void unify(const ValueTypeMetaData *concrete, ResolutionMap &m)
        {
            if (concrete == nullptr || concrete->value_kind() != ValueTypeKind::Map) { return; }
            scalar_unifier<TKey>::unify(concrete->key_type, m);
            scalar_unifier<TValue>::unify(concrete->element_type, m);
        }
    };

    template <auto TSize>
    void size_unifier<TSize>::unify(std::size_t concrete, ResolutionMap &m)
    {
        using size = static_schema_detail::size_parameter_descriptor<TSize>;
        if constexpr (!size::is_concrete())
        {
            const std::vector<std::size_t> constraints = size::constraints();
            if (!constraints.empty() &&
                std::find(constraints.begin(), constraints.end(), concrete) == constraints.end())
            {
                throw std::logic_error(fmt::format("size variable '{}' resolved outside its constraints",
                                                   size::name()));
            }
            m.bind_size(size::name(), concrete);
        }
    }

    /**
     * REF transparency (Python parity: ``REF[X]`` is type-compatible with
     * ``X``; consumers bind through the reference at runtime): a non-``REF``
     * unifier sees the *dereferenced* schema — so a bare variable binds the
     * referenced type, never the reference shape. The result port's schema is
     * NOT rewritten anywhere; only matching/unification looks through it. The
     * runtime matcher applies the same rule (``ts_pattern_match``).
     */
    [[nodiscard]] inline const TSValueTypeMetaData *unify_dereference(const TSValueTypeMetaData *c) noexcept
    {
        while (c != nullptr && c->kind == TSTypeKind::REF) { c = c->referenced_ts(); }
        return c;
    }

    template <typename S>
    struct ts_unifier
    {
        static void unify(const TSValueTypeMetaData *, ResolutionMap &) noexcept {}
    };

    template <fixed_string Name, typename... C>
    struct ts_unifier<TsVar<Name, C...>>
    {
        static void unify(const TSValueTypeMetaData *concrete, ResolutionMap &m)
        {
            concrete = unify_dereference(concrete);
            if constexpr (sizeof...(C) > 0)
            {
                if (!((concrete == schema_descriptor<C>::ts_meta()) || ...))
                {
                    throw std::logic_error(
                        fmt::format("type variable '{}' resolved outside its constraints", Name.sv()));
                }
            }
            m.bind_ts(Name.sv(), concrete);
        }
    };

    template <typename V>
    struct ts_unifier<TS<V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            scalar_unifier<V>::unify(c != nullptr ? c->value_schema : nullptr, m);
        }
    };

    template <typename V>
    struct ts_unifier<TSS<V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            const ValueTypeMetaData *elem = (c != nullptr && c->value_schema != nullptr) ? c->value_schema->element_type : nullptr;
            scalar_unifier<V>::unify(elem, m);
        }
    };

    template <typename C, auto N>
    struct ts_unifier<TSL<C, N>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            size_unifier<N>::unify(c != nullptr ? c->fixed_size() : 0, m);
            ts_unifier<C>::unify(c != nullptr ? c->element_ts() : nullptr, m);
        }
    };

    template <typename C>
    struct ts_unifier<Args<C>> : ts_unifier<TSL<C, SIZE<"args_len">>>
    {
    };

    template <typename K, typename V>
    struct ts_unifier<TSD<K, V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            scalar_unifier<K>::unify(c != nullptr ? c->key_type() : nullptr, m);
            ts_unifier<V>::unify(c != nullptr ? c->element_ts() : nullptr, m);
        }
    };

    template <typename S>
    struct ts_unifier<REF<S>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            if (c != nullptr && c->kind == TSTypeKind::REF) { c = c->referenced_ts(); }
            ts_unifier<S>::unify(c, m);
        }
    };

    template <typename V, std::size_t Period, std::size_t MinPeriod>
    struct ts_unifier<TSW<V, Period, MinPeriod>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            scalar_unifier<V>::unify(c != nullptr && c->kind == TSTypeKind::TSW ? c->value_type : nullptr, m);
        }
    };

    template <typename V>
    struct ts_unifier<TSWAny<V>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            scalar_unifier<V>::unify(c != nullptr && c->kind == TSTypeKind::TSW ? c->value_type : nullptr, m);
        }
    };

    namespace type_resolution_detail
    {
        template <fixed_string VarName>
        void unify_tsb_field_pack(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            m.bind_ts(VarName.sv(), c != nullptr && c->kind == TSTypeKind::TSB ? c : nullptr);
        }

        template <typename Field>
        void unify_tsb_field(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            c = unify_dereference(c);
            if (c == nullptr || c->kind != TSTypeKind::TSB) { return; }
            const std::string expected_name = ts_field_descriptor<Field>::field_name();
            for (std::size_t i = 0; i < c->field_count(); ++i)
            {
                const TSFieldMetaData &field = c->fields()[i];
                if (field.name != nullptr && expected_name == field.name)
                {
                    ts_unifier<typename ts_field_descriptor<Field>::schema>::unify(field.type, m);
                    return;
                }
            }
        }
    }  // namespace type_resolution_detail

    template <typename... Fields>
    struct ts_unifier<UnNamedTSB<Fields...>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            (type_resolution_detail::unify_tsb_field<Fields>(c, m), ...);
        }
    };

    template <fixed_string VarName, typename... C>
    struct ts_unifier<UnNamedTSB<TsVar<VarName, C...>>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            type_resolution_detail::unify_tsb_field_pack<VarName>(c, m);
        }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_unifier<TSB<Name, Fields...>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            (type_resolution_detail::unify_tsb_field<Fields>(c, m), ...);
        }
    };

    template <fixed_string Name, fixed_string VarName, typename... C>
    struct ts_unifier<TSB<Name, TsVar<VarName, C...>>>
    {
        static void unify(const TSValueTypeMetaData *c, ResolutionMap &m)
        {
            type_resolution_detail::unify_tsb_field_pack<VarName>(c, m);
        }
    };

    template <typename... Fields>
    struct ts_unifier<Kwargs<Fields...>> : ts_unifier<UnNamedTSB<Fields...>>
    {
    };

    template <>
    struct ts_unifier<Kwargs<>> : ts_unifier<UnNamedTSB<TsVar<"kwargs">>>
    {
    };

    // -----------------------------------------------------------------
    // Explicit type-argument helpers (source-side resolution): supply a
    // concrete schema for a variable that cannot be inferred from inputs
    // (e.g. a replay source's output). ``wire<stdlib::replay_impl, TS<Int>>(w, key)``.
    // -----------------------------------------------------------------
    template <typename S>
    [[nodiscard]] inline const TSValueTypeMetaData *ts_type()
    {
        return schema_descriptor<S>::ts_meta();
    }

    template <typename T>
    [[nodiscard]] inline const ValueTypeMetaData *scalar_type()
    {
        return scalar_descriptor<T>::value_meta();
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TYPE_RESOLUTION_H
