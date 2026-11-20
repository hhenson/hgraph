#ifndef HGRAPH_TYPES_NODE_DELTA_BUILDERS_H
#define HGRAPH_TYPES_NODE_DELTA_BUILDERS_H

#include <hgraph/types/static_node_core.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/value/value_builder.h>

#include <map>
#include <set>

namespace hgraph
{
    // -----------------------------------------------------------------
    // Time-series deltas — canonical type-erased Values
    //
    // The per-cycle delta of any time-series is the canonical ``Value`` whose schema
    // is the runtime ``delta_value_schema`` (``TS<T>`` / ``SIGNAL`` /
    // ``TSW<T,...>`` -> window change set; ``TSS<T>`` ->
    // ``Bundle{added: Set<T>, removed: Set<T>}``; ``TSD<K,V>`` ->
    // ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``; fixed ``TSL<C,N>`` ->
    // ``Map<int, delta(C)>``; unbounded ``TSL<C>`` ->
    // ``Bundle{removed: Set<int>, modified: Map<int, delta(C)>}``;
    // ``TSB{f...}`` -> ``Bundle{f: delta(f)...}``,
    // recursive). These builders *produce that exact canonical Value*, so a built
    // delta compares to a runtime-produced one via ``Value::equals`` — there is no
    // parallel wrapper type. These are the *test-authoring* builders (construct an
    // expected delta); the runtime capture / apply of a live delta is the type-erased
    // ``capture_delta`` / ``apply_delta`` (``<hgraph/types/time_series/ts_delta.h>``).
    // -----------------------------------------------------------------

    namespace static_node_detail
    {
        /** True when a schema's delta input is a scalar value rather than a child ``Value``. */
        template <typename S> struct is_scalar_ts : std::false_type {};
        template <typename V> struct is_scalar_ts<TS<V>> : std::true_type {};

        template <> struct is_scalar_ts<SIGNAL> : std::true_type
        {};

        /** The user-supplied per-entry delta input for child schema ``C``: the bare
         *  scalar for ``TS`` / ``SIGNAL``, otherwise a prebuilt child-delta
         *  ``Value``. */
        template <typename C> struct delta_input { using type = Value; };
        template <typename V> struct delta_input<TS<V>> { using type = V; };
        template <typename V> struct delta_input<TS<SeriesOf<V>>> { using type = Series; };
        template <typename V, typename M> struct delta_input<TS<FrameOf<V, M>>> { using type = Frame; };
        template <> struct delta_input<SIGNAL> { using type = bool; };
        template <typename C> using delta_input_t = typename delta_input<C>::type;

        template <typename T> struct is_optional : std::false_type {};
        template <typename T> struct is_optional<std::optional<T>> : std::true_type {};

    }  // namespace static_node_detail

    /** The canonical delta-value schema for time-series schema ``S`` (recursive). */
    template <typename S>
    [[nodiscard]] inline const ValueTypeMetaData *delta_value_schema()
    {
        const TSValueTypeMetaData *ts = schema_descriptor<S>::ts_meta();
        return ts != nullptr ? ts->delta_value_schema : nullptr;
    }

    /** The binding for ``S``'s canonical delta value. */
    template <typename S>
    [[nodiscard]] inline ValueTypeRef delta_value_binding()
    {
        const auto binding = ValuePlanFactory::instance().type_for(delta_value_schema<S>());
        if (!binding) { throw std::logic_error("delta_value_binding: unresolved delta schema"); }
        return binding;
    }

    /** Build a portable window change set. The period and minimum affect
        window membership, but the delta schema depends only on the element. */
    template <typename T>
    [[nodiscard]] Value window_delta(std::optional<T> added,
                                     const std::vector<std::pair<T, DateTime>> &removed = {},
                                     bool cleared = false)
    {
        BundleBuilder delta{delta_value_binding<TSW<T, 1, 1>>()};
        delta.set(0, Value{cleared});
        const auto added_type = delta.field_binding(1);
        ListBuilder arrival{compact_element_binding(added_type)};
        if (added) { arrival.push_back(Value{*added}); }
        auto added_storage = arrival.build_storage();
        delta.set(1, ValueView{added_type, &added_storage});

        const auto removed_type = delta.field_binding(2);
        const auto sample_type = compact_element_binding(removed_type);
        ListBuilder departures{sample_type};
        for (const auto &[value, time] : removed)
        {
            BundleBuilder sample{sample_type};
            sample.set(0, Value{value});
            sample.set(1, Value{time});
            departures.push_back(sample.build());
        }
        auto removed_storage = departures.build_storage();
        delta.set(2, ValueView{removed_type, &removed_storage});
        return delta.build();
    }

    /**
     * Build the canonical ``TSS<T>`` delta value ``Bundle{added: Set<T>, removed:
     * Set<T>}`` from typed elements. The immutable ``Set`` fields are assembled with
     * ``SetBuilder`` + ``BundleBuilder`` so the result matches the runtime
     * ``delta_value_schema`` exactly (and compares via ``Value::equals``).
     */
    template <typename TValue>
    [[nodiscard]] inline Value set_delta(std::vector<TValue> added, std::vector<TValue> removed)
    {
        auto       &registry       = TypeRegistry::instance();
        const auto *t_meta         = scalar_descriptor<TValue>::value_meta();
        const auto t_binding      = ValuePlanFactory::instance().type_for(t_meta);
        const auto *set_meta       = registry.set(t_meta);
        const auto *bundle_schema  = registry.un_named_bundle({{"added", set_meta}, {"removed", set_meta}});
        const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_schema);
        if (!t_binding || !bundle_binding) { throw std::logic_error("set_delta: unresolved binding"); }

        // SetBuilder::insert copies from the argument's address. A std::vector<bool>
        // element is a bit proxy, not a bool, so a reference to it would copy the
        // proxy's bytes (every member read back as true); copy those to a bool.
        const auto insert_all = [](SetBuilder &set, const std::vector<TValue> &values) {
            if constexpr (std::is_same_v<TValue, bool>) {
                for (const bool e : values) { (void)set.insert(e); }
            } else {
                for (const TValue &e : values) { (void)set.insert(e); }
            }
        };
        SetBuilder added_set{t_binding};
        insert_all(added_set, added);
        SetBuilder removed_set{t_binding};
        insert_all(removed_set, removed);

        BundleBuilder bundle{bundle_binding};
        bundle.set("added", added_set.build());
        bundle.set("removed", removed_set.build());
        return bundle.build();
    }

    namespace static_node_detail
    {
        /** Build ``Map<int, delta(C)>`` from a sparse ``index -> child-delta`` map. */
        template <typename C>
        [[nodiscard]] inline Value build_list_modifications(const std::map<std::size_t, delta_input_t<C>> &entries)
        {
            const auto key_binding =
                ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
            const auto val_binding = delta_value_binding<C>();
            if (!key_binding) { throw std::logic_error("list_delta: unresolved key binding"); }

            MapBuilder builder{key_binding, val_binding};
            for (const auto &[index, input] : entries)
            {
                const Int key = static_cast<Int>(index);
                if constexpr (is_scalar_ts<C>::value)
                {
                    builder.set_item_copy(std::addressof(key), std::addressof(input));  // scalar child delta == value
                }
                else
                {
                    builder.set_item_copy(std::addressof(key), input.view().data());    // container child delta Value
                }
            }
            return builder.build();
        }

        /** Build a dynamic ``TSL`` delta ``Bundle{removed: Set<int>, modified:
            Map<int, delta(C)>}`` (RFC 0031). */
        template <typename C>
        [[nodiscard]] inline Value build_list_delta(
            const std::map<std::size_t, delta_input_t<C>> &modified,
            const std::vector<std::size_t>                &removed,
            std::optional<std::size_t> size = std::nullopt)
        {
            auto       &registry        = TypeRegistry::instance();
            const auto *key_meta        = scalar_descriptor<Int>::value_meta();
            const auto  key_binding     = ValuePlanFactory::instance().type_for(key_meta);
            const auto *removed_schema  = registry.set(key_meta);
            const auto *modified_schema = registry.map(key_meta, delta_value_schema<C>());
            const auto *bundle_schema   = registry.un_named_bundle(
                {{"removed", removed_schema}, {"modified", modified_schema}, {"size_modified", key_meta}});
            const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_schema);
            if (!key_binding || !bundle_binding)
            {
                throw std::logic_error("dynamic_list_delta: unresolved binding");
            }

            SetBuilder removed_set{key_binding};
            for (const auto index : removed)
            {
                const Int key = static_cast<Int>(index);
                (void)removed_set.insert_copy(std::addressof(key));
            }

            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", removed_set.build());
            bundle.set("modified", build_list_modifications<C>(modified));
            if (size) { bundle.set("size_modified", Value{static_cast<Int>(*size)}); }
            return bundle.build();
        }

        template <typename K, typename V>
        [[nodiscard]] inline Value build_dict_delta(const std::map<K, delta_input_t<V>> &modified,
                                                    const std::vector<K>                &removed,
                                                    const std::vector<K>                &removed_strict = {})
        {
            const auto *key_meta = scalar_descriptor<K>::value_meta();
            const auto key_binding = ValuePlanFactory::instance().type_for(key_meta);
            const auto &val_binding = delta_value_binding<V>();
            if (key_binding == nullptr) { throw std::logic_error("dict_delta: unresolved key binding"); }

            auto       &registry      = TypeRegistry::instance();
            const auto *removed_schema = registry.set(key_meta);
            const auto *modified_schema = registry.map(key_meta, delta_value_schema<V>());
            // Authoring helper: emits the OBSERVED two-field delta unless the
            // caller asserts strict removals, which only an author can.
            const bool  authored      = !removed_strict.empty();
            const auto *bundle_schema =
                authored ? registry.un_named_bundle({{"removed", removed_schema},
                                                     {"modified", modified_schema},
                                                     {"removed_strict", removed_schema}})
                         : registry.un_named_bundle({{"removed", removed_schema},
                                                     {"modified", modified_schema}});
            const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_schema);
            if (bundle_binding == nullptr) { throw std::logic_error("dict_delta: unresolved bundle binding"); }

            SetBuilder removed_set{key_binding};
            for (const auto &key : removed) { (void)removed_set.insert_copy(std::addressof(key)); }
            SetBuilder removed_strict_set{key_binding};
            for (const auto &key : removed_strict) { (void)removed_strict_set.insert_copy(std::addressof(key)); }

            MapBuilder modified_map{key_binding, val_binding};
            for (const auto &[key, input] : modified)
            {
                if constexpr (is_scalar_ts<V>::value)
                {
                    modified_map.set_item_copy(std::addressof(key), std::addressof(input));
                }
                else
                {
                    modified_map.set_item_copy(std::addressof(key), input.view().data());
                }
            }

            BundleBuilder bundle{bundle_binding};
            bundle.set("removed", removed_set.build());
            bundle.set("modified", modified_map.build());
            if (authored) { bundle.set("removed_strict", removed_strict_set.build()); }
            return bundle.build();
        }

        template <typename S>
        struct empty_delta_builder;

        template <typename V>
        struct empty_delta_builder<TSS<V>>
        {
            [[nodiscard]] static Value build() { return set_delta<V>({}, {}); }
        };

        template <typename K, typename V>
        struct empty_delta_builder<TSD<K, V>>
        {
            [[nodiscard]] static Value build() { return build_dict_delta<K, V>({}, {}); }
        };

        template <typename C, auto N>
        struct empty_delta_builder<TSL<C, N>>
        {
            [[nodiscard]] static Value build()
            {
                return build_list_delta<C>({}, {});
            }
        };

        template <typename C, typename Arg>
        void set_tsb_delta_field(BundleBuilder &builder, std::size_t index, Arg &&arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (std::is_same_v<A, std::nullopt_t>)
            {
                return;
            }
            else if constexpr (is_optional<A>::value)
            {
                if (arg.has_value()) { set_tsb_delta_field<C>(builder, index, *arg); }
            }
            else if constexpr (is_scalar_ts<C>::value)
            {
                using V = delta_input_t<C>;
                static_assert(std::is_convertible_v<A, V>, "tsb_delta: field scalar delta has the wrong value type");
                V     value = static_cast<V>(std::forward<Arg>(arg));
                Value delta{std::move(value)};
                builder.set(index, std::move(delta));
            }
            else
            {
                static_assert(requires(const A &value) { value.view(); },
                              "tsb_delta: container field delta must be a Value-like object");
                if constexpr (std::is_same_v<A, Value>)
                {
                    builder.set(index, std::forward<Arg>(arg));
                }
                else
                {
                    builder.set(index, std::forward<Arg>(arg).view());
                }
            }
        }

        template <typename S>
        struct tsb_delta_builder;

        template <typename... Fields>
        struct tsb_delta_builder<UnNamedTSB<Fields...>>
        {
            // A TSB delta is sparse: omitted fields did not tick. An empty
            // delta validates the parent only when it is invalid.
            static void initialize(BundleBuilder &) {}

            template <typename... Args>
            static void fill(BundleBuilder &builder, Args &&...args)
            {
                static_assert(sizeof...(Args) == sizeof...(Fields), "tsb_delta: argument count must match TSB fields");
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (set_tsb_delta_field<typename std::tuple_element_t<I, std::tuple<Fields...>>::schema>(
                         builder, I, std::get<I>(std::forward_as_tuple(std::forward<Args>(args)...))),
                     ...);
                }(std::make_index_sequence<sizeof...(Fields)>{});
            }
        };

        template <fixed_string Name, typename... Fields>
        struct tsb_delta_builder<TSB<Name, Fields...>> : tsb_delta_builder<UnNamedTSB<Fields...>>
        {};

        template <typename ValueBundle, typename... Fields>
        struct tsb_delta_builder<NominalTSB<ValueBundle, Fields...>> : tsb_delta_builder<UnNamedTSB<Fields...>>
        {};

        template <typename... Fields> struct empty_delta_builder<UnNamedTSB<Fields...>>
        {
            [[nodiscard]] static Value build()
            {
                BundleBuilder builder{delta_value_binding<UnNamedTSB<Fields...>>()};
                return builder.build();
            }
        };

        template <fixed_string Name, typename... Fields>
        struct empty_delta_builder<TSB<Name, Fields...>> : empty_delta_builder<UnNamedTSB<Fields...>>
        {};

        template <typename ValueBundle, typename... Fields>
        struct empty_delta_builder<NominalTSB<ValueBundle, Fields...>> : empty_delta_builder<UnNamedTSB<Fields...>>
        {};
    }  // namespace static_node_detail

    /**
     * Build a list delta from indexed child deltas, cleared positions, and
     * an optional new length. For a scalar child, pass its value. For a
     * collection child, pass its own delta.
     */
    template <typename C>
    [[nodiscard]] inline Value
    list_delta(std::initializer_list<std::pair<std::size_t, static_node_detail::delta_input_t<C>>> entries,
               std::vector<std::size_t> removed = {}, std::optional<std::size_t> size = std::nullopt)
    {
        std::map<std::size_t, static_node_detail::delta_input_t<C>> map;
        for (const auto &[index, input] : entries) { map.insert_or_assign(index, input); }
        return static_node_detail::build_list_delta<C>(map, removed, size);
    }

    /** Positional ``TSL<C,N>`` delta: position is the child index, ``none`` skips it. */
    template <typename C>
    [[nodiscard]] inline Value list_delta(std::vector<std::optional<static_node_detail::delta_input_t<C>>> positional,
                                                 std::vector<std::size_t> removed = {}, std::optional<std::size_t> size = std::nullopt)
    {
        std::map<std::size_t, static_node_detail::delta_input_t<C>> map;
        for (std::size_t i = 0; i < positional.size(); ++i)
        {
            if (positional[i].has_value()) { map.emplace(i, *positional[i]); }
        }
        return static_node_detail::build_list_delta<C>(map, removed, size);
    }

    /** Unbounded-list spelling of the positional delta builder. */
    template <typename C>
    [[nodiscard]] inline Value dynamic_list_delta(
        std::initializer_list<std::pair<std::size_t, static_node_detail::delta_input_t<C>>> entries,
        std::vector<std::size_t> removed = {}, std::optional<std::size_t> size = std::nullopt)
    {
        return list_delta<C>(entries, std::move(removed), size);
    }

    template <typename C>
    [[nodiscard]] inline Value dynamic_list_delta(
        std::vector<std::optional<static_node_detail::delta_input_t<C>>> positional,
        std::vector<std::size_t> removed = {}, std::optional<std::size_t> size = std::nullopt)
    {
        return list_delta<C>(std::move(positional), std::move(removed), size);
    }

    /**
     * Build the canonical ``TSD<K,V>`` delta value
     * ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``.
     */
    template <typename K, typename V>
    [[nodiscard]] inline Value
    dict_delta(std::initializer_list<std::pair<K, static_node_detail::delta_input_t<V>>> modified,
               std::vector<K> removed = {})
    {
        std::map<K, static_node_detail::delta_input_t<V>> map;
        for (const auto &[key, input] : modified) { map.insert_or_assign(key, input); }
        return static_node_detail::build_dict_delta<K, V>(map, removed);
    }

    /**
     * Build the canonical ``TSB`` delta value ``Bundle{field: delta(field_schema)...}``.
     * Pass one argument per field in schema order. A scalar child field takes the bare
     * scalar delta; a container child field takes a child-delta ``Value``. Passing
     * ``std::nullopt`` leaves the field unset: that field has no news (TS-24). A
     * collection field that ticked empty takes an explicit empty delta
     * (``set_delta<T>({}, {})``, ``dict_delta<K, V>({})``); "no news" and "ticked
     * empty" are different statements (#835).
     */
    template <typename S, typename... Args>
    [[nodiscard]] inline Value tsb_delta(Args &&...args)
    {
        BundleBuilder builder{delta_value_binding<S>()};
        static_node_detail::tsb_delta_builder<S>::fill(builder, std::forward<Args>(args)...);
        return builder.build();
    }

}  // namespace hgraph

#endif  // HGRAPH_TYPES_NODE_DELTA_BUILDERS_H
