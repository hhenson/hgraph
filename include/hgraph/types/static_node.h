#ifndef HGRAPH_CPP_ROOT_STATIC_NODE_H
#define HGRAPH_CPP_ROOT_STATIC_NODE_H

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/call_args.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/time_series/ts_data/set_view.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/runtime/prepared_input_routes.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/util/scope.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_input/list_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>
#include <hgraph/types/time_series/ts_input/window_view.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output/bundle_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/list_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/time_series/ts_output/window_view.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace hgraph
{
    /**
     * Node-authoring selectors and the compile-time machinery that turns a
     * static node implementation into a runtime ``NodeBuilder``.
     *
     * A node implementation is a stateless struct with a static ``eval`` (and
     * optional ``start`` / ``stop``) whose parameters are the selectors below.
     * ``NodeBuilder::implementation<T>()`` reflects that signature and builds
     * the same ``(NodeTypeMetaData, NodeCallbacks, TSEndpointSchema)`` triple
     * that ``NodeBuilder::native`` already consumes, so static authoring is a
     * typed front-end over the one runtime node model rather than a parallel
     * mechanism.
     *
     * See ``data_structures`` developer guide: *Wiring* and
     * *Schemas > Static Schema*.
     *
     * The typed selector surface covers the supported time-series shapes (``TS`` /
     * ``SIGNAL`` / ``REF`` / ``TSS`` / ``TSD`` / ``TSL`` / ``TSB`` / tick-count
     * ``TSW``), scalar ``State<T>``, wiring-time ``Scalar<Name, T>`` values,
     * graph-level ``GlobalState<T>``, input activity/validity policy flags,
     * output-backed ``RecordableState<TSchema>``, evaluation-clock and engine-
     * control injection, node-self inspection, and scheduler injection.
     * Push-source nodes are intentionally outside this generic static-node path:
     * they use a specialized builder/node implementation that owns the message
     * queue and sender.
     */

    // -----------------------------------------------------------------
    // Time-series deltas — canonical type-erased Values
    //
    // The per-cycle delta of any time-series is the canonical ``Value`` whose schema
    // is the runtime ``delta_value_schema`` (``TS<T>`` / ``SIGNAL`` /
    // ``TSW<T,...>`` -> scalar; ``TSS<T>`` ->
    // ``Bundle{added: Set<T>, removed: Set<T>}``; ``TSD<K,V>`` ->
    // ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``; ``TSL<C,N>`` ->
    // ``Map<int, delta(C)>``; ``TSB{f...}`` -> ``Bundle{f: delta(f)...}``,
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
        template <typename V, std::size_t P, std::size_t M> struct is_scalar_ts<TSW<V, P, M>> : std::true_type {};
        template <> struct is_scalar_ts<SIGNAL> : std::true_type {};

        /** The user-supplied per-entry delta input for child schema ``C``: the bare
         *  scalar for ``TS`` / ``SIGNAL`` / ``TSW``, otherwise a prebuilt child-delta
         *  ``Value``. */
        template <typename C> struct delta_input { using type = Value; };
        template <typename V> struct delta_input<TS<V>> { using type = V; };
        template <typename V> struct delta_input<TS<SeriesOf<V>>> { using type = Series; };
        template <typename V, typename M> struct delta_input<TS<FrameOf<V, M>>> { using type = Frame; };
        template <typename V, std::size_t P, std::size_t M> struct delta_input<TSW<V, P, M>> { using type = V; };
        template <> struct delta_input<SIGNAL> { using type = bool; };
        template <typename C> using delta_input_t = typename delta_input<C>::type;

        template <typename T> struct is_optional : std::false_type {};
        template <typename T> struct is_optional<std::optional<T>> : std::true_type {};

        template <fixed_string Wanted, typename... Fields>
        struct tsb_field_lookup
        {
            static constexpr bool found = false;
            using type                  = void;
            static constexpr std::size_t index = 0;
        };

        template <fixed_string Wanted, fixed_string Name, typename Schema, typename... Rest>
        struct tsb_field_lookup<Wanted, Field<Name, Schema>, Rest...>
        {
          private:
            using next = tsb_field_lookup<Wanted, Rest...>;
            static constexpr bool matches = Wanted.sv() == Name.sv();

          public:
            static constexpr bool found = matches || next::found;
            using type                  = std::conditional_t<matches, Schema, typename next::type>;
            static constexpr std::size_t index = matches ? 0 : 1 + next::index;
        };

        template <typename S> struct is_static_tsb_schema : std::false_type {};
        template <typename... Fields> struct is_static_tsb_schema<UnNamedTSB<Fields...>> : std::true_type {};
        template <typename... Fields> struct is_static_tsb_schema<Kwargs<Fields...>> : std::true_type {};
        template <fixed_string Name, typename... Fields>
        struct is_static_tsb_schema<TSB<Name, Fields...>> : std::true_type {};

        /**
         * Named so the constraint below is a concept rather than a bare
         * ``::value`` atomic constraint: MSVC before 14.51 reports
         * "C7607: atomic constraint should be a constant expression of type
         * 'bool', not 'unknown'" for the latter on a constrained partial
         * specialization, which makes the whole tree unbuildable on VS 17.14.
         * Normalization is identical, so this is purely a portability spelling.
         */
        template <typename S>
        concept static_tsb_schema = is_static_tsb_schema<S>::value;

        template <typename S> struct static_tsb_schema_traits;
        template <typename... Fields>
        struct static_tsb_schema_traits<UnNamedTSB<Fields...>>
        {
            template <fixed_string FieldName>
            using lookup = tsb_field_lookup<FieldName, Fields...>;
        };
        template <typename... Fields>
        struct static_tsb_schema_traits<Kwargs<Fields...>>
        {
            template <fixed_string FieldName>
            using lookup = tsb_field_lookup<FieldName, Fields...>;
        };
        template <fixed_string Name, typename... Fields>
        struct static_tsb_schema_traits<TSB<Name, Fields...>>
        {
            template <fixed_string FieldName>
            using lookup = tsb_field_lookup<FieldName, Fields...>;
        };
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

        SetBuilder added_set{t_binding};
        for (const auto &e : added) { (void)added_set.insert(e); }
        SetBuilder removed_set{t_binding};
        for (const auto &e : removed) { (void)removed_set.insert(e); }

        BundleBuilder bundle{bundle_binding};
        bundle.set("added", added_set.build());
        bundle.set("removed", removed_set.build());
        return bundle.build();
    }

    namespace static_node_detail
    {
        /** Build ``Map<int, delta(C)>`` from a sparse ``index -> child-delta`` map. */
        template <typename C>
        [[nodiscard]] inline Value build_list_delta(const std::map<std::size_t, delta_input_t<C>> &entries)
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
            [[nodiscard]] static Value build() { return build_list_delta<C>({}); }
        };

        template <typename Field>
        void initialize_tsb_delta_field(BundleBuilder &builder, std::size_t index)
        {
            using C = typename Field::schema;
            if constexpr (!is_scalar_ts<C>::value)
            {
                Value empty = empty_delta_builder<C>::build();
                builder.set(index, std::move(empty));
            }
        }

        template <typename... Fields>
        void initialize_tsb_delta_defaults(BundleBuilder &builder)
        {
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (initialize_tsb_delta_field<std::tuple_element_t<I, std::tuple<Fields...>>>(builder, I), ...);
            }(std::make_index_sequence<sizeof...(Fields)>{});
        }

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
            static void initialize(BundleBuilder &builder) { initialize_tsb_delta_defaults<Fields...>(builder); }

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

        template <typename... Fields>
        struct empty_delta_builder<UnNamedTSB<Fields...>>
        {
            [[nodiscard]] static Value build()
            {
                BundleBuilder builder{delta_value_binding<UnNamedTSB<Fields...>>()};
                initialize_tsb_delta_defaults<Fields...>(builder);
                return builder.build();
            }
        };

        template <fixed_string Name, typename... Fields>
        struct empty_delta_builder<TSB<Name, Fields...>> : empty_delta_builder<UnNamedTSB<Fields...>>
        {};
    }  // namespace static_node_detail

    /**
     * Build the canonical ``TSL<C,N>`` delta value ``Map<int, delta(C)>`` from a
     * sparse ``index -> child-delta`` list. For a scalar child the entry value is the
     * bare ``T``; for a container child it is a prebuilt child-delta ``Value`` (from an
     * inner ``set_delta`` / ``list_delta``) — so construction is recursive. The result
     * matches the runtime ``delta_value_schema`` (immutable compact ``Map``).
     */
    template <typename C>
    [[nodiscard]] inline Value
    list_delta(std::initializer_list<std::pair<std::size_t, static_node_detail::delta_input_t<C>>> entries)
    {
        std::map<std::size_t, static_node_detail::delta_input_t<C>> map;
        for (const auto &[index, input] : entries) { map.insert_or_assign(index, input); }
        return static_node_detail::build_list_delta<C>(map);
    }

    /** Positional ``TSL<C,N>`` delta: position is the child index, ``none`` skips it. */
    template <typename C>
    [[nodiscard]] inline Value list_delta(std::vector<std::optional<static_node_detail::delta_input_t<C>>> positional)
    {
        std::map<std::size_t, static_node_detail::delta_input_t<C>> map;
        for (std::size_t i = 0; i < positional.size(); ++i)
        {
            if (positional[i].has_value()) { map.emplace(i, *positional[i]); }
        }
        return static_node_detail::build_list_delta<C>(map);
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
     * ``std::nullopt`` leaves the field at its canonical default delta: typed-null for
     * scalar children, empty delta for collection children.
     */
    template <typename S, typename... Args>
    [[nodiscard]] inline Value tsb_delta(Args &&...args)
    {
        BundleBuilder builder{delta_value_binding<S>()};
        static_node_detail::tsb_delta_builder<S>::initialize(builder);
        static_node_detail::tsb_delta_builder<S>::fill(builder, std::forward<Args>(args)...);
        return builder.build();
    }

    // -----------------------------------------------------------------
    // Selector markers
    // -----------------------------------------------------------------

    /**
     * Typed input selector — a zero-overhead facade that **derives from** the
     * type-erased input view for its kind (``TSInputView`` / ``TSSInputView`` /
     * ``TSLInputView`` …), inheriting the whole erased API and adding the compile-time
     * schema plus typed sugar. Constructed from a ``TSInputView`` (by ``arg_provider``
     * or, for a child, from ``TSLInputView::at(i)``).
     */
    enum class InputActivity
    {
        Active,
        Passive,
        /** Wake on collection membership changes, not nested value ticks. */
        Structural,
    };

    enum class InputValidity
    {
        Valid,
        AllValid,
        Unchecked,
    };

    /**
     * How an input's source is bound at wiring. ``Caller`` (the default): the
     * caller passes a port. ``Context``: the source resolves from the nearest
     * enclosing ``context::scope`` with a matching name — the caller passes
     * nothing (an explicit ``arg<"name">(port)`` keyword overrides the
     * ambient context). Use via the ``Context<"name", S>`` alias.
     */
    enum class InputBinding
    {
        Caller,
        Context,
    };

    namespace static_node_detail
    {
        template <auto TPolicy>
        consteval bool is_input_policy_flag()
        {
            using policy_type = std::remove_cv_t<decltype(TPolicy)>;
            return std::is_same_v<policy_type, InputActivity> || std::is_same_v<policy_type, InputValidity> ||
                   std::is_same_v<policy_type, InputBinding>;
        }

        template <auto... TPolicies>
        consteval void validate_input_policy_flags()
        {
            static_assert((is_input_policy_flag<TPolicies>() && ...),
                          "In<> only accepts InputActivity / InputValidity / InputBinding policy flags");
            static_assert((0U + ... +
                           static_cast<unsigned>(
                               std::is_same_v<std::remove_cv_t<decltype(TPolicies)>, InputActivity>)) <= 1U,
                          "In<> accepts at most one InputActivity policy flag");
            static_assert((0U + ... +
                           static_cast<unsigned>(
                               std::is_same_v<std::remove_cv_t<decltype(TPolicies)>, InputValidity>)) <= 1U,
                          "In<> accepts at most one InputValidity policy flag");
            static_assert((0U + ... +
                           static_cast<unsigned>(
                               std::is_same_v<std::remove_cv_t<decltype(TPolicies)>, InputBinding>)) <= 1U,
                          "In<> accepts at most one InputBinding policy flag");
        }

        template <auto... TPolicies>
        consteval InputBinding resolved_input_binding()
        {
            validate_input_policy_flags<TPolicies...>();
            InputBinding result = InputBinding::Caller;
            (
                [&] {
                    using policy_type = std::remove_cv_t<decltype(TPolicies)>;
                    if constexpr (std::is_same_v<policy_type, InputBinding>) { result = TPolicies; }
                }(),
                ...);
            return result;
        }

        template <auto... TPolicies>
        consteval InputActivity resolved_input_activity()
        {
            validate_input_policy_flags<TPolicies...>();
            InputActivity result = InputActivity::Active;
            (
                [&] {
                    using policy_type = std::remove_cv_t<decltype(TPolicies)>;
                    if constexpr (std::is_same_v<policy_type, InputActivity>) { result = TPolicies; }
                }(),
                ...);
            return result;
        }

        template <auto... TPolicies>
        consteval InputValidity resolved_input_validity()
        {
            validate_input_policy_flags<TPolicies...>();
            InputValidity result = InputValidity::Valid;
            (
                [&] {
                    using policy_type = std::remove_cv_t<decltype(TPolicies)>;
                    if constexpr (std::is_same_v<policy_type, InputValidity>) { result = TPolicies; }
                }(),
                ...);
            return result;
        }
    }  // namespace static_node_detail

    template <fixed_string Name, typename TSchema, auto... TPolicies>
    class In;

    /**
     * A context-bound time-series input: an ordinary ``In`` whose source is
     * resolved at wiring time from the nearest enclosing
     * ``context::scope<"name">`` (see *Contexts* in services.rst) instead of
     * being passed by the caller. The context name IS the field name. An
     * explicit keyword argument (``arg<"name">(port)``) overrides the ambient
     * context. Everything else — input schema, activity/validity policies,
     * eval-side access — is exactly ``In``.
     */
    template <fixed_string Name, typename TSchema, auto... TPolicies>
    using Context = In<Name, TSchema, InputBinding::Context, TPolicies...>;

    namespace call_args_detail
    {
        // Context-bound inputs are auto-bound call parameters: no positional
        // slot, never "missing", keyword-overridable (see call_args.h).
        template <fixed_string N, typename S, auto... P>
        inline constexpr bool auto_context_param_v<In<N, S, P...>> =
            static_node_detail::resolved_input_binding<P...>() == InputBinding::Context;
    }  // namespace call_args_detail

    template <fixed_string Name, typename TValue, auto... TPolicies>
    class In<Name, TS<TValue>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<TValue>;
        using value_type                 = TValue;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        /** Current value of the input (typed; shadows the erased ``value() -> ValueView``). */
        [[nodiscard]] value_type value() const
        {
            if (const auto *fast = static_cast<const TValue *>(
                    this->try_native_value_memory(&ops_for<TValue>())))
            {
                return *fast;
            }
            return TSInputView::value().template checked_as<TValue>();
        }
        /** The underlying erased input view (the container typed views expose the same via the CRTP base). */
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
        // modified() / valid() / delta_value() inherited from TSInputView.
    };

    template <fixed_string Name, typename TElement, auto... TPolicies>
    class In<Name, TS<SeriesOf<TElement>>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<SeriesOf<TElement>>;
        using value_type                 = Series;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] const Series &value() const { return TSInputView::value().template checked_as<Series>(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    template <fixed_string Name, typename TSchema, typename TMetadata, auto... TPolicies>
    class In<Name, TS<FrameOf<TSchema, TMetadata>>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<FrameOf<TSchema, TMetadata>>;
        using value_type                 = Frame;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] const Frame &value() const { return TSInputView::value().template checked_as<Frame>(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    /** ``TS<AnyValue>`` input: expose the contained erased value without
        pretending the Any box is an ordinary C++ atomic scalar. */
    template <fixed_string Name, auto... TPolicies>
    class In<Name, TS<AnyValue>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<AnyValue>;
        using value_type                 = AnyValue;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] AnyView value() const { return TSInputView::value().as_any(); }
        [[nodiscard]] ValueView contained_value() const { return value().get(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    template <fixed_string Name, auto... TPolicies>
    class In<Name, SIGNAL, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = SIGNAL;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] bool ticked() const { return modified(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    /**
     * Reference time-series input (``REF<S>``): reads the current reference token.
     * The reference points at ``S``; ``base()`` remains the erased view over the REF
     * time-series itself.
     */
    template <fixed_string Name, typename TSchema, auto... TPolicies>
    class In<Name, REF<TSchema>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = REF<TSchema>;
        using target_schema              = TSchema;
        using value_type                 = TimeSeriesReference;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] TimeSeriesReference value() const
        {
            return TSInputView::value().template checked_as<TimeSeriesReference>();
        }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    /**
     * Set time-series input (``TSS<T>``): inherits ``TSSInputView`` and adds typed
     * ``added`` / ``removed`` / ``values`` / ``contains``; ``delta()`` is the canonical
     * delta ``Value`` (the inherited ``delta_value()``).
     */
    template <fixed_string Name, typename TValue, auto... TPolicies>
    class In<Name, TSS<TValue>, TPolicies...> : public TSSInputView
    {
      public:
        using schema                     = TSS<TValue>;
        using value_type                 = TValue;  // the set's element type
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSSInputView(std::move(view)) {}

        [[nodiscard]] bool contains(const TValue &key) const
        {
            return TSSInputView::contains(ValueView{data_view().layout().key_binding, &key});
        }

        /** This cycle's added / removed / current elements as typed vectors. */
        [[nodiscard]] std::vector<TValue> values() const { return collect(TSSInputView::values()); }
        [[nodiscard]] std::vector<TValue> added() const { return collect(TSSInputView::added()); }
        [[nodiscard]] std::vector<TValue> removed() const { return collect(TSSInputView::removed()); }

        /** This cycle's delta as the canonical ``Bundle{added, removed}`` value view. */
        [[nodiscard]] ValueView delta() const { return delta_value(); }
        // size() / empty() / modified() / valid() inherited from TSSInputView.

      private:
        template <typename RangeT>
        static std::vector<TValue> collect(RangeT range)
        {
            std::vector<TValue> out;
            for (const auto &v : range) { out.push_back(v.template checked_as<TValue>()); }
            return out;
        }
    };

    /**
     * List time-series input (``TSL<C, N>``): inherits ``TSLInputView`` for the
     * fixed-size collection. The child schema ``C`` is **any** time-series type
     * (``TS`` / ``TSS`` / ``TSL`` …); ``operator[](i)`` / ``at(i)`` return the typed
     * child selector ``In<"", C>`` (recursive). ``delta()`` is the canonical
     * ``Map<int, delta(C)>`` value view; ``size()`` / ``modified_items()`` /
     * ``modified()`` / ``valid()`` are inherited.
     */
    template <fixed_string Name, typename TElementSchema, auto N, auto... TPolicies>
    class In<Name, TSL<TElementSchema, N>, TPolicies...> : public TSLInputView
    {
      public:
        using schema                            = TSL<TElementSchema, N>;
        using element_schema                    = TElementSchema;
        static constexpr auto        field_name = Name;
        static constexpr std::size_t fixed_size =
            static_schema_detail::size_parameter_descriptor<N>::is_concrete()
                ? static_schema_detail::size_parameter_descriptor<N>::concrete_size()
                : std::size_t{0};
        static constexpr auto        activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto        validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSLInputView(std::move(view)) {}

        /** Typed child selector for index ``i`` (recursive). */
        [[nodiscard]] In<"", TElementSchema> operator[](std::size_t i) const
        {
            return In<"", TElementSchema>{TSLInputView::at(i)};
        }
        [[nodiscard]] In<"", TElementSchema> at(std::size_t i) const { return (*this)[i]; }

        /** This cycle's delta as the canonical ``Map<int, delta(C)>`` value view. */
        [[nodiscard]] ValueView delta() const { return delta_value(); }
        // size() / modified_items() / modified() / valid() inherited from TSLInputView.
    };

    /**
     * ``Args<C>`` is a node-authoring marker for call-site positional packing.
     * At runtime it is the same view and metadata shape as ``TSL<C, SIZE<"args_len">>``.
     */
    template <fixed_string Name, typename TElementSchema, auto... TPolicies>
    class In<Name, Args<TElementSchema>, TPolicies...>
        : public In<Name, TSL<TElementSchema, SIZE<"args_len">>, TPolicies...>
    {
      public:
        using base           = In<Name, TSL<TElementSchema, SIZE<"args_len">>, TPolicies...>;
        using schema         = Args<TElementSchema>;
        using element_schema = TElementSchema;
        using base::base;
    };

    /**
     * Dict time-series input (``TSD<K, V>``): inherits ``TSDInputView`` and adds
     * typed key lookup. ``at(key)`` / ``operator[](key)`` return ``In<"", V>``.
     * Iteration helpers return typed child selectors while preserving ``ValueView``
     * keys.
     */
    template <fixed_string Name, typename TKey, typename TValueSchema, auto... TPolicies>
    class In<Name, TSD<TKey, TValueSchema>, TPolicies...> : public TSDInputView
    {
      public:
        using schema                     = TSD<TKey, TValueSchema>;
        using key_type                   = TKey;
        using value_schema               = TValueSchema;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSDInputView(std::move(view)) {}

        [[nodiscard]] bool contains(const TKey &key) const
        {
            return TSDInputView::contains(ValueView{data_view().layout().key_binding, &key});
        }
        [[nodiscard]] std::size_t find_slot(const TKey &key) const
        {
            return TSDInputView::find_slot(ValueView{data_view().layout().key_binding, &key});
        }

        [[nodiscard]] In<"", TValueSchema> at_slot(std::size_t slot) const
        {
            return In<"", TValueSchema>{TSDInputView::at_slot(slot)};
        }

        [[nodiscard]] In<"", TValueSchema> at(const TKey &key) const
        {
            return In<"", TValueSchema>{
                TSDInputView::at(ValueView{data_view().layout().key_binding, &key})};
        }
        [[nodiscard]] In<"", TValueSchema> operator[](const TKey &key) const { return at(key); }

        [[nodiscard]] Range<In<"", TValueSchema>> values() const
        {
            return typed_values(&live_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> items() const
        {
            return typed_items(&live_slot);
        }
        [[nodiscard]] Range<In<"", TValueSchema>> valid_values() const
        {
            return typed_values(&valid_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> valid_items() const
        {
            return typed_items(&valid_slot);
        }
        [[nodiscard]] Range<In<"", TValueSchema>> modified_values() const
        {
            if (!modified()) { return empty_values(); }
            return typed_values(&modified_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> modified_items() const
        {
            if (!modified()) { return empty_items(); }
            return typed_items(&modified_slot);
        }
        [[nodiscard]] Range<In<"", TValueSchema>> added_values() const
        {
            return typed_values(&added_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> added_items() const
        {
            return typed_items(&added_slot);
        }
        [[nodiscard]] Range<In<"", TValueSchema>> removed_values() const
        {
            return typed_values(&removed_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> removed_items() const
        {
            return typed_items(&removed_slot);
        }

        /** This cycle's delta as the canonical ``Bundle{removed, modified}`` value view. */
        [[nodiscard]] ValueView delta() const { return delta_value(); }

      private:
        static bool live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const In *>(context)->slot_live(slot);
        }
        static bool valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const In *>(context);
            return self->slot_live(slot) && self->TSDInputView::at_slot(slot).valid();
        }
        static bool modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const In *>(context);
            return self->slot_live(slot) && self->slot_modified(slot);
        }
        static bool added_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const In *>(context);
            return self->slot_occupied(slot) && self->slot_added(slot);
        }
        static bool removed_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const In *>(context);
            return self->slot_occupied(slot) && self->slot_removed(slot);
        }
        static In<"", TValueSchema> project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const In *>(context)->at_slot(slot);
        }
        static std::pair<ValueView, In<"", TValueSchema>> project_item(const void *context,
                                                                       const void *,
                                                                       std::size_t slot)
        {
            const auto *self = static_cast<const In *>(context);
            return {self->key_at_slot(slot), self->at_slot(slot)};
        }

        [[nodiscard]] Range<In<"", TValueSchema>> typed_values(
            typename Range<In<"", TValueSchema>>::predicate_fn predicate) const
        {
            return Range<In<"", TValueSchema>>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                               .predicate = predicate, .projector = &project_value};
        }
        [[nodiscard]] KeyValueRange<ValueView, In<"", TValueSchema>> typed_items(
            typename KeyValueRange<ValueView, In<"", TValueSchema>>::predicate_fn predicate) const
        {
            return KeyValueRange<ValueView, In<"", TValueSchema>>{.context = this, .memory = nullptr,
                                                                  .limit = slot_capacity(), .predicate = predicate,
                                                                  .projector = &project_item};
        }
        [[nodiscard]] static Range<In<"", TValueSchema>> empty_values() noexcept
        {
            return Range<In<"", TValueSchema>>{.context = nullptr, .memory = nullptr, .limit = 0,
                                               .predicate = nullptr, .projector = nullptr};
        }
        [[nodiscard]] static KeyValueRange<ValueView, In<"", TValueSchema>> empty_items() noexcept
        {
            return KeyValueRange<ValueView, In<"", TValueSchema>>{.context = nullptr, .memory = nullptr,
                                                                  .limit = 0, .predicate = nullptr,
                                                                  .projector = nullptr};
        }
    };

    /**
     * Bundle time-series input (``TSB`` / ``UnNamedTSB``): inherits
     * ``TSBInputView`` and adds compile-time field selection via
     * ``field<"name">()``.
     */
    template <fixed_string Name, typename TSchema, auto... TPolicies>
        requires static_node_detail::static_tsb_schema<TSchema>
    class In<Name, TSchema, TPolicies...> : public TSBInputView
    {
      public:
        using schema                     = TSchema;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSBInputView(std::move(view)) {}

        using TSBInputView::at;
        using TSBInputView::field;
        using TSBInputView::operator[];

        template <fixed_string FieldName>
        [[nodiscard]] auto field() const
        {
            using lookup = typename static_node_detail::static_tsb_schema_traits<TSchema>::template lookup<FieldName>;
            static_assert(lookup::found, "In<TSB>::field requested an unknown field");
            return In<FieldName, typename lookup::type>{TSBInputView::at(lookup::index)};
        }
        template <fixed_string FieldName>
        [[nodiscard]] auto get() const { return field<FieldName>(); }

        [[nodiscard]] ValueView delta() const { return delta_value(); }
    };

    /**
     * Window time-series input (``TSW<T>``): inherits ``TSWInputView`` and adds
     * typed accessors for the scalar window elements.
     */
    template <fixed_string Name, typename TValue, std::size_t Period, std::size_t MinPeriod, auto... TPolicies>
    class In<Name, TSW<TValue, Period, MinPeriod>, TPolicies...> : public TSWInputView
    {
      public:
        using schema     = TSW<TValue, Period, MinPeriod>;
        using value_type = TValue;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSWInputView(std::move(view)) {}

        [[nodiscard]] TValue at(std::size_t index) const { return TSWInputView::at(index).template checked_as<TValue>(); }
        [[nodiscard]] TValue operator[](std::size_t index) const { return at(index); }
        [[nodiscard]] TValue front() const { return TSWInputView::front().template checked_as<TValue>(); }
        [[nodiscard]] TValue back() const { return TSWInputView::back().template checked_as<TValue>(); }
        [[nodiscard]] ValueView delta() const { return delta_value(); }
    };

    /**
     * **Deferred** (generic) input selector — the schema is a ``TsVar`` type variable
     * resolved at wiring time, not a concrete time-series. It IS a ``TSInputView``
     * (no typed sugar, since there is no concrete element type); the node drives it
     * through the erased API and the runtime ``capture_delta`` / ``apply_delta``.
     * ``base()`` returns the erased view uniformly (matching ``In<TS<T>>``).
     */
    template <fixed_string Name, fixed_string VarName, typename... TConstraints, auto... TPolicies>
    class In<Name, TsVar<VarName, TConstraints...>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TsVar<VarName, TConstraints...>;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        /** The erased input view (uniform with ``In<TS<T>>::base()``). */
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
        // schema() / value() / delta_value() / modified() / valid() / as_set() /
        // as_list() / as_bundle() all inherited from TSInputView.
    };

    /**
     * Typed output selector — derives from the type-erased output view for its kind
     * (``TSOutputView`` / ``TSSOutputView`` / ``TSLOutputView`` …). The output view
     * already carries the current ``evaluation_time``, so the selector adds no data.
     */
    template <typename TSchema>
    class Out;

    template <typename TValue>
    class Out<TS<TValue>> : public TSOutputView
    {
      public:
        using schema     = TS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        /** Write ``value`` into the output and tick it at the current evaluation time. */
        template <typename U>
        void set(U &&value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            // Typed fast path: a pure-native atomic slot whose realized ops
            // match TValue assigns in place and commits via mark_modified
            // (observer notification and parent bubbling included) — the
            // erased copy ceremony below exists for representation variants
            // (python caches, polymorphic realizations), which fall through.
            if (auto destination = mutation.mutable_value(); destination.valid())
            {
                if (TValue *slot = destination.template try_mutable_as<TValue>())
                {
                    *slot = TValue{std::forward<U>(value)};
                    mutation.mark_modified();
                    return;
                }
            }
            TValue resolved{std::forward<U>(value)};
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&resolved)};
            static_cast<void>(mutation.copy_value_from(source));
        }

        /** Type-erased set: copy a value (matching the output schema) and tick it. */
        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
        // modified() / valid() / evaluation_time() inherited from TSOutputView.
    };

    template <typename TElement>
    class Out<TS<SeriesOf<TElement>>> : public TSOutputView
    {
      public:
        using schema     = TS<SeriesOf<TElement>>;
        using value_type = Series;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(Series value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&value)};
            static_cast<void>(mutation.copy_value_from(source));
        }

        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
    };

    template <typename TSchema, typename TMetadata>
    class Out<TS<FrameOf<TSchema, TMetadata>>> : public TSOutputView
    {
      public:
        using schema     = TS<FrameOf<TSchema, TMetadata>>;
        using value_type = Frame;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(Frame value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&value)};
            static_cast<void>(mutation.copy_value_from(source));
        }

        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
    };

    /** ``TS<AnyValue>`` output: each write is boxed with its concrete native
        schema and then applied through the ordinary output mutation path. */
    template <>
    class Out<TS<AnyValue>> : public TSOutputView
    {
      public:
        using schema     = TS<AnyValue>;
        using value_type = AnyValue;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(const ValueView &value) const
        {
            Value boxed{ValuePlanFactory::instance().type_for(TypeRegistry::instance().any())};
            boxed.as_any().begin_mutation().set(value);
            apply_box(std::move(boxed));
        }

        void set(Value value) const
        {
            Value boxed{ValuePlanFactory::instance().type_for(TypeRegistry::instance().any())};
            boxed.as_any().begin_mutation().set(std::move(value));
            apply_box(std::move(boxed));
        }

        void apply(const ValueView &boxed) const
        {
            if (!boxed.is_any()) { throw std::invalid_argument("Out<TS<AnyValue>>::apply requires an Any value"); }
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(boxed));
        }

      private:
        void apply_box(Value boxed) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            if (!mutation.move_value_from(std::move(boxed)))
            {
                throw std::logic_error("Out<TS<AnyValue>>::set failed to move the boxed value");
            }
        }
    };

    template <>
    class Out<SIGNAL> : public TSOutputView
    {
      public:
        using schema = SIGNAL;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void tick() const
        {
            Value value{true};
            auto  mutation = TSOutputView::begin_mutation(evaluation_time());
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("Out<SIGNAL>::tick failed to move the signal tick");
            }
        }

        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            if (!mutation.copy_value_from(value))
            {
                throw std::logic_error("Out<SIGNAL>::apply failed to copy the signal tick");
            }
        }
    };

    template <typename TSchema>
    class Out<REF<TSchema>> : public TSOutputView
    {
      public:
        using schema        = REF<TSchema>;
        using target_schema = TSchema;
        using value_type    = TimeSeriesReference;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(const TimeSeriesReference &reference) const
        {
            TimeSeriesReference typed_reference = normalize(reference);
            Value               wrapped{std::move(typed_reference)};
            move_reference_value(std::move(wrapped));
        }

        void apply(const ValueView &value) const
        {
            if (value.valid())
            {
                set(value.checked_as<TimeSeriesReference>());
                return;
            }
            copy_reference_value(value);
        }

      private:
        void copy_reference_value(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            if (!mutation.copy_value_from(value))
            {
                throw std::logic_error("Out<REF<S>>::apply failed to copy the reference into the output");
            }
        }

        void move_reference_value(Value &&value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("Out<REF<S>>::set failed to move the reference into the output");
            }
        }

        [[nodiscard]] TimeSeriesReference normalize(const TimeSeriesReference &reference) const
        {
            // The runtime view carries the resolved output schema — authoritative
            // for a generic target (``REF<TsVar<...>>``, where the static
            // descriptor is non-concrete) and identical to it when concrete.
            const auto *output_schema = TSOutputView::schema();
            const auto *expected      = output_schema != nullptr ? output_schema->referenced_ts()
                                                                 : schema_descriptor<TSchema>::ts_meta();
            const auto *actual   = reference.target_schema();
            if (actual == nullptr)
            {
                if (reference.is_empty()) { return TimeSeriesReference::empty(expected); }
                throw std::invalid_argument("Out<REF<S>>::set reference has no target schema");
            }

            auto &registry = TypeRegistry::instance();
            if (!time_series_schema_equivalent(registry.dereference(actual), registry.dereference(expected)))
            {
                throw std::invalid_argument("Out<REF<S>>::set reference target schema does not match output schema");
            }
            return reference;
        }
    };

    /**
     * Output-backed recordable node state. This is not the scalar ``State<T>``
     * slot and it is not a normal node output selector: it is feedback-like
     * node-local state stored as a hidden output so system-level record/replay
     * code can observe and restore it without letting it activate the node.
     */
    template <typename TSchema>
    class RecordableState : public Out<TSchema>
    {
      public:
        using schema = TSchema;

        RecordableState(TSOutputView view, DateTime evaluation_time) noexcept
            : Out<TSchema>(std::move(view), evaluation_time)
        {
        }
    };

    /**
     * Set time-series output (``TSS<T>``): inherits ``TSSOutputView``. Mutate with
     * add / remove / clear; the delta accumulates across calls within the cycle.
     */
    template <typename TValue>
    class Out<TSS<TValue>> : public TSSOutputView
    {
      public:
        using schema     = TSS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSSOutputView(std::move(view)) {}

        /** Add ``key`` to the set; returns whether the set delta changed. */
        bool add(const TValue &key) const
        {
            auto &mutation = this->mutation();
            return mutation.add(ValueView{mutation.layout().key_binding, &key});
        }
        /** Remove ``key`` from the set; returns whether the set delta changed. */
        bool remove(const TValue &key) const
        {
            auto &mutation = this->mutation();
            return mutation.remove(ValueView{mutation.layout().key_binding, &key});
        }
        /** Remove all elements. */
        void clear() const { this->mutation().clear(); }
        // modified() / valid() / evaluation_time() inherited from TSSOutputView.

      private:
        /** One mutation scope per selector (audit O3): scope validation and
            the container-kind revalidation run once per invocation instead of
            per call; marking is idempotent and the scope has no close action,
            so holding it across the eval is semantics-identical. */
        [[nodiscard]] TSSDataMutationView &mutation() const
        {
            if (!mutation_) { mutation_.emplace(TSSOutputView::begin_mutation(evaluation_time())); }
            return *mutation_;
        }
        mutable std::optional<TSSDataMutationView> mutation_{};
    };

    /**
     * List time-series output (``TSL<C, N>``): inherits ``TSLOutputView``. Tick a
     * child by index through the per-child sub-selector ``out[i]`` (an ``Out<C>``,
     * recursive): ``out[i].set(v)`` for a scalar child, ``out[i].add(...)`` for a set
     * child, ``out[i][j]...`` for a nested list. ``set(i,v)`` / ``apply(i,v)`` are
     * scalar-child conveniences over ``out[i]``.
     */
    template <typename TElementSchema, auto N>
    class Out<TSL<TElementSchema, N>> : public TSLOutputView
    {
      public:
        using schema                            = TSL<TElementSchema, N>;
        using element_schema                    = TElementSchema;
        static constexpr std::size_t fixed_size =
            static_schema_detail::size_parameter_descriptor<N>::is_concrete()
                ? static_schema_detail::size_parameter_descriptor<N>::concrete_size()
                : std::size_t{0};

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSLOutputView(std::move(view)) {}

        /** Per-child output selector: ``out[i]`` is an ``Out<C>`` (recursive). */
        [[nodiscard]] Out<TElementSchema> operator[](std::size_t index) const
        {
            return Out<TElementSchema>{TSLOutputView::at(index), evaluation_time()};
        }

        /** Scalar-child convenience: set child ``index`` to ``value`` and tick it. */
        template <typename U>
        void set(std::size_t index, U &&value) const
        {
            (*this)[index].set(std::forward<U>(value));
        }

        /** Scalar-child convenience: type-erased per-child set. */
        void apply(std::size_t index, const ValueView &value) const { (*this)[index].apply(value); }
        // size() / modified() / valid() / evaluation_time() inherited from TSLOutputView.
    };

    /**
     * Dict time-series output (``TSD<K, V>``): inherits ``TSDOutputView`` and adds
     * typed key lookup. ``out[key]`` creates the key when needed and returns
     * ``Out<V>`` for the child.
     */
    template <typename TKey, typename TValueSchema>
    class Out<TSD<TKey, TValueSchema>> : public TSDOutputView
    {
      public:
        using schema       = TSD<TKey, TValueSchema>;
        using key_type     = TKey;
        using value_schema = TValueSchema;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSDOutputView(std::move(view)) {}

        [[nodiscard]] bool contains(const TKey &key) const
        {
            return TSDOutputView::contains(ValueView{data_view().layout().key_binding, &key});
        }
        [[nodiscard]] std::size_t find_slot(const TKey &key) const
        {
            return TSDOutputView::find_slot(ValueView{data_view().layout().key_binding, &key});
        }

        [[nodiscard]] Out<TValueSchema> at_slot(std::size_t slot) const
        {
            return Out<TValueSchema>{TSDOutputView::at_slot(slot), evaluation_time()};
        }

        [[nodiscard]] Out<TValueSchema> at(const TKey &key) const
        {
            auto &mutation = this->mutation();
            auto  child    = mutation.at(ValueView{mutation.layout().key_binding, &key});
            return Out<TValueSchema>{TSOutputView{base().output(), child, evaluation_time()}, evaluation_time()};
        }
        [[nodiscard]] Out<TValueSchema> operator[](const TKey &key) const { return at(key); }

        template <typename U>
        void set(const TKey &key, U &&value) const
        {
            (*this)[key].set(std::forward<U>(value));
        }

        void apply(const TKey &key, const ValueView &value) const { (*this)[key].apply(value); }

        /** Remove ``key`` if it is live; same-cycle additions are cancelled by the slot protocol. */
        [[nodiscard]] bool erase(const TKey &key) const
        {
            auto &mutation = this->mutation();
            return mutation.erase(ValueView{mutation.layout().key_binding, &key});
        }

        /** Remove every live entry while retaining slots until normal end-of-cycle cleanup. */
        void clear() const { this->mutation().clear(); }

        [[nodiscard]] Range<Out<TValueSchema>> values() const
        {
            return typed_values(&live_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, Out<TValueSchema>> items() const
        {
            return typed_items(&live_slot);
        }
        [[nodiscard]] Range<Out<TValueSchema>> valid_values() const
        {
            return typed_values(&valid_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, Out<TValueSchema>> valid_items() const
        {
            return typed_items(&valid_slot);
        }
        [[nodiscard]] Range<Out<TValueSchema>> modified_values() const
        {
            if (!modified()) { return empty_values(); }
            return typed_values(&modified_slot);
        }
        [[nodiscard]] KeyValueRange<ValueView, Out<TValueSchema>> modified_items() const
        {
            if (!modified()) { return empty_items(); }
            return typed_items(&modified_slot);
        }

      private:
        static bool live_slot(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const Out *>(context)->slot_live(slot);
        }
        static bool valid_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const Out *>(context);
            return self->slot_live(slot) && self->TSDOutputView::at_slot(slot).valid();
        }
        static bool modified_slot(const void *context, const void *, std::size_t slot)
        {
            const auto *self = static_cast<const Out *>(context);
            return self->slot_live(slot) && self->slot_modified(slot);
        }
        static Out<TValueSchema> project_value(const void *context, const void *, std::size_t slot)
        {
            return static_cast<const Out *>(context)->at_slot(slot);
        }
        static std::pair<ValueView, Out<TValueSchema>> project_item(const void *context,
                                                                    const void *,
                                                                    std::size_t slot)
        {
            const auto *self = static_cast<const Out *>(context);
            return {self->key_at_slot(slot), self->at_slot(slot)};
        }

        [[nodiscard]] Range<Out<TValueSchema>> typed_values(
            typename Range<Out<TValueSchema>>::predicate_fn predicate) const
        {
            return Range<Out<TValueSchema>>{.context = this, .memory = nullptr, .limit = slot_capacity(),
                                            .predicate = predicate, .projector = &project_value};
        }
        [[nodiscard]] KeyValueRange<ValueView, Out<TValueSchema>> typed_items(
            typename KeyValueRange<ValueView, Out<TValueSchema>>::predicate_fn predicate) const
        {
            return KeyValueRange<ValueView, Out<TValueSchema>>{.context = this, .memory = nullptr,
                                                               .limit = slot_capacity(), .predicate = predicate,
                                                               .projector = &project_item};
        }
        [[nodiscard]] static Range<Out<TValueSchema>> empty_values() noexcept
        {
            return Range<Out<TValueSchema>>{.context = nullptr, .memory = nullptr, .limit = 0,
                                            .predicate = nullptr, .projector = nullptr};
        }
        [[nodiscard]] static KeyValueRange<ValueView, Out<TValueSchema>> empty_items() noexcept
        {
            return KeyValueRange<ValueView, Out<TValueSchema>>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                               .predicate = nullptr, .projector = nullptr};
        }

      private:
        /** One mutation scope per selector (audit O3): scope validation and
            the container-kind revalidation run once per invocation instead of
            per call; marking is idempotent and the scope has no close action,
            so holding it across the eval is semantics-identical. */
        [[nodiscard]] TSDDataMutationView &mutation() const
        {
            if (!mutation_) { mutation_.emplace(TSDOutputView::begin_mutation(evaluation_time())); }
            return *mutation_;
        }
        mutable std::optional<TSDDataMutationView> mutation_{};
    };

    /**
     * Bundle time-series output (``TSB`` / ``UnNamedTSB``): inherits
     * ``TSBOutputView`` and adds compile-time field selection via
     * ``field<"name">()``.
     */
    template <typename... TFields>
    class Out<UnNamedTSB<TFields...>> : public TSBOutputView
    {
      public:
        using schema = UnNamedTSB<TFields...>;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSBOutputView(std::move(view)) {}

        template <fixed_string FieldName>
        [[nodiscard]] auto field() const
        {
            using lookup = static_node_detail::tsb_field_lookup<FieldName, TFields...>;
            static_assert(lookup::found, "Out<TSB>::field requested an unknown field");
            return Out<typename lookup::type>{TSBOutputView::at(lookup::index), evaluation_time()};
        }
        template <fixed_string FieldName>
        [[nodiscard]] auto get() const { return field<FieldName>(); }
    };

    template <fixed_string Name, typename... TFields>
    class Out<TSB<Name, TFields...>> : public TSBOutputView
    {
      public:
        using schema = TSB<Name, TFields...>;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSBOutputView(std::move(view)) {}

        template <fixed_string FieldName>
        [[nodiscard]] auto field() const
        {
            using lookup = static_node_detail::tsb_field_lookup<FieldName, TFields...>;
            static_assert(lookup::found, "Out<TSB>::field requested an unknown field");
            return Out<typename lookup::type>{TSBOutputView::at(lookup::index), evaluation_time()};
        }
        template <fixed_string FieldName>
        [[nodiscard]] auto get() const { return field<FieldName>(); }
    };

    /**
     * Window time-series output (``TSW<T>``): inherits ``TSWOutputView`` and adds
     * typed ``push``.
     */
    template <typename TValue, std::size_t Period, std::size_t MinPeriod>
    class Out<TSW<TValue, Period, MinPeriod>> : public TSWOutputView
    {
      public:
        using schema     = TSW<TValue, Period, MinPeriod>;
        using value_type = TValue;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSWOutputView(std::move(view)) {}

        template <typename U>
        void push(U &&value) const
        {
            Value wrapped{std::forward<U>(value)};
            TSWOutputView::begin_mutation(evaluation_time()).push(wrapped.view());
        }

        void clear() const { TSWOutputView::begin_mutation(evaluation_time()).clear(); }

        void apply(const ValueView &value) const { TSWOutputView::begin_mutation(evaluation_time()).push(value); }
    };

    /**
     * **Deferred** (generic) output selector — the schema is a ``TsVar`` resolved at
     * wiring time. It IS a ``TSOutputView``; the node drives it through the erased
     * API. ``apply`` copies a whole current value matching the resolved output's
     * value schema and ticks it; per-cycle replay deltas go through ``apply_delta``.
     */
    template <fixed_string VarName, typename... TConstraints>
    class Out<TsVar<VarName, TConstraints...>> : public TSOutputView
    {
      public:
        using schema = TsVar<VarName, TConstraints...>;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        /** Type-erased set: copy a value (matching the resolved output schema) and tick it. */
        void apply(const ValueView &value) const
        {
            apply_current_value(*this, value);
        }
        // schema() / begin_mutation() / evaluation_time() / as_set() / as_list() inherited.
    };

    // The per-cycle delta capture / apply that ``replay`` / ``record`` and the harness
    // use is now the runtime, type-erased ``capture_delta`` / ``apply_delta``
    // (``<hgraph/types/time_series/ts_delta.h>``) — schema-as-data, dispatched through
    // the live endpoint's ``TSDataOps`` table — which replaced the compile-time
    // ``ts_delta<S>`` driver.
    // The ``set_delta`` / ``list_delta`` / ``dict_delta`` builders above remain as the test-authoring
    // way to construct an *expected* canonical delta ``Value`` (compared via
    // ``Value::equals`` against a captured one).

    /** Typed handle into node-local (value-layer) state. One state slot per node. */
    template <typename TValue>
    class State
    {
      public:
        using value_type = TValue;

        explicit State(ValueView view) noexcept : view_(std::move(view)) {}

        [[nodiscard]] value_type get() const { return view_.template checked_as<TValue>(); }

        /** Borrowed read — no copy. Valid until the next set()/modify().
            Prefer this over get() when the state holds owning containers:
            get() deep-copies the whole value out per call. */
        [[nodiscard]] const value_type &ref() const
        {
            return view_.template checked_as<TValue>();
        }

        /** In-place mutation: a typed reference into the state storage.
            The heavy-state alternative to the get()/set() round trip, which
            deep-copies the whole value out and back — an O(buffer) per-tick
            copy for every node holding a deque/vector in state (std-operator
            audit 2026-08-15). The reference stays valid for the remainder of
            the current hook invocation. */
        [[nodiscard]] value_type &modify()
        {
            auto mutation = view_.begin_mutation();
            return mutation.template checked_mutable_as<TValue>();
        }

        template <typename U>
        void set(U &&value)
        {
            auto mutation = view_.begin_mutation();
            mutation.set_scalar(std::forward<U>(value));
        }

        [[nodiscard]] ValueView       &view() noexcept { return view_; }
        [[nodiscard]] const ValueView &view() const noexcept { return view_; }

      private:
        ValueView view_;
    };

    /**
     * Typed handle to one of a graph or node's scalar inputs (wiring-time
     * configuration). Read-only: scalars are fixed at wiring time and do not change
     * during evaluation. The same marker is used in a node's ``eval`` (where the
     * value is read from the node's compound scalar configuration) and in a graph's
     * ``compose`` (where the value is supplied by the caller); it carries its value
     * directly so both paths are uniform.
     */
    template <fixed_string Name, typename TValue>
    class Scalar
    {
      public:
        using value_type                 = TValue;
        static constexpr auto field_name = Name;

        /** Supplied directly (graph ``compose`` parameter / wiring-time value). */
        explicit Scalar(TValue value)
            : storage_(std::in_place_index<0>, std::move(value))
        {
        }

        /** Borrow from the immutable node scalar storage (node hook path). */
        explicit Scalar(const ValueView &view)
            : storage_(std::in_place_index<1>,
                       std::addressof(view.template checked_as<TValue>()))
        {
        }

        /** The configured value of this scalar input. */
        [[nodiscard]] const value_type &value() const noexcept
        {
            return storage_.index() == 0 ? std::get<0>(storage_) : *std::get<1>(storage_);
        }

      private:
        std::variant<TValue, const TValue *> storage_;
    };

    /**
     * **Deferred** (generic) scalar selector — the value type is a ``ScalarVar``
     * resolved at wiring time. It holds the configured value type-erased as an owned
     * ``Value``; ``value()`` returns the erased ``ValueView`` (a node typically forwards
     * it through ``apply_delta`` / ``copy_value_from`` against its resolved output).
     */
    template <fixed_string Name, fixed_string VarName, typename... TConstraints>
    class Scalar<Name, ScalarVar<VarName, TConstraints...>>
    {
      public:
        using schema                     = ScalarVar<VarName, TConstraints...>;
        static constexpr auto field_name = Name;

        /** Borrow from the immutable node scalar storage (node hook path). */
        explicit Scalar(const ValueView &view) noexcept
            : type_(view.type()), data_(view.data())
        {
        }

        /** The configured value, type-erased. */
        [[nodiscard]] ValueView value() const noexcept { return ValueView{type_, data_}; }

      private:
        ValueTypeRef type_{};
        const void  *data_{nullptr};
    };

    // The ``GlobalStateView`` injectable selector is the runtime view type from
    // ``<hgraph/runtime/global_state.h>`` (a node declares a ``GlobalStateView``
    // parameter to read/write the graph's shared state). It is *not* part of the
    // node's data contract: it adds no input field, scalar, or state, and does
    // not affect node-kind inference. Its ``arg_provider`` is below.

    // -----------------------------------------------------------------
    // Compile-time machinery
    // -----------------------------------------------------------------

    namespace static_node_detail
    {
        template <typename T>
        inline constexpr bool always_false_v = false;

        template <typename T>
        using selector_of = std::remove_cv_t<std::remove_reference_t<T>>;

        // ---- selector detection ----
        template <typename T> struct is_input_selector : std::false_type {};
        template <fixed_string N, typename S, auto... P> struct is_input_selector<In<N, S, P...>> : std::true_type {};

        template <typename T> struct is_output_selector : std::false_type {};
        template <typename S> struct is_output_selector<Out<S>> : std::true_type {};

        template <typename T> struct is_state_selector : std::false_type {};
        template <typename V> struct is_state_selector<State<V>> : std::true_type {};

        template <typename T> struct is_recordable_state_selector : std::false_type {};
        template <typename S> struct is_recordable_state_selector<RecordableState<S>> : std::true_type {};

        template <typename T> struct is_scalar_selector : std::false_type {};
        template <fixed_string N, typename V> struct is_scalar_selector<Scalar<N, V>> : std::true_type {};

        template <typename A, typename B> struct same_input_name : std::false_type {};
        template <fixed_string AName, typename ASchema, auto... AP, fixed_string BName, typename BSchema, auto... BP>
        struct same_input_name<In<AName, ASchema, AP...>, In<BName, BSchema, BP...>>
            : std::bool_constant<AName.sv() == BName.sv()>
        {
        };

        template <typename A, typename B> struct same_scalar_name : std::false_type {};
        template <fixed_string AName, typename AValue, fixed_string BName, typename BValue>
        struct same_scalar_name<Scalar<AName, AValue>, Scalar<BName, BValue>>
            : std::bool_constant<AName.sv() == BName.sv()>
        {
        };

        template <template <typename, typename> typename SameName, typename... Es>
        struct has_duplicate_selector_names_pack : std::false_type
        {
        };

        template <template <typename, typename> typename SameName, typename E, typename... Rest>
        struct has_duplicate_selector_names_pack<SameName, E, Rest...>
            : std::bool_constant<(SameName<selector_of<E>, selector_of<Rest>>::value || ...) ||
                                 has_duplicate_selector_names_pack<SameName, Rest...>::value>
        {
        };

        template <template <typename, typename> typename SameName, typename Tuple>
        struct has_duplicate_selector_names;

        template <template <typename, typename> typename SameName, typename... Es>
        struct has_duplicate_selector_names<SameName, std::tuple<Es...>>
            : has_duplicate_selector_names_pack<SameName, Es...>
        {
        };

        // The scheduler is an injectable, not part of the data contract; it only
        // flips the node's ``uses_scheduler`` flag so a per-node scheduler-state
        // slot is allocated.
        template <typename T> struct is_scheduler_selector : std::false_type {};
        template <> struct is_scheduler_selector<NodeScheduler> : std::true_type {};

        // The evaluation clock is also an injectable. Static nodes that request it
        // get a cached clock-ref slot; other nodes pay no storage cost.
        template <typename T> struct is_global_state_selector : std::false_type {};
        template <> struct is_global_state_selector<GlobalStateView> : std::true_type {};

        template <typename T> struct is_evaluation_clock_selector : std::false_type {};
        template <> struct is_evaluation_clock_selector<EvaluationClockView> : std::true_type {};

        // ---- per-selector runtime metadata ----
        // ``schema`` / ``value_schema`` expose the selector's compile-time schema type
        // so the generic-wiring path can resolve type variables (see type_resolution.h);
        // the ``*_meta()`` accessors stay the concrete-path (var schemas return nullptr).
        template <typename T> struct input_selector_traits;
        template <fixed_string N, typename S, auto... P>
        struct input_selector_traits<In<N, S, P...>>
        {
            using schema = S;
            static std::string                name() { return std::string{N.sv()}; }
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct output_selector_traits;
        template <typename S>
        struct output_selector_traits<Out<S>>
        {
            using schema = S;
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct state_selector_traits;
        template <typename V>
        struct state_selector_traits<State<V>>
        {
            using value_schema = V;
            static const ValueTypeMetaData *value_meta() { return scalar_descriptor<V>::value_meta(); }
        };

        template <typename T> struct recordable_state_selector_traits;
        template <typename S>
        struct recordable_state_selector_traits<RecordableState<S>>
        {
            using schema = S;
            static const TSValueTypeMetaData *ts_meta() { return schema_descriptor<S>::ts_meta(); }
        };

        template <typename T> struct scalar_selector_traits;
        template <fixed_string N, typename V>
        struct scalar_selector_traits<Scalar<N, V>>
        {
            using value_schema = V;
            static std::string              name() { return std::string{N.sv()}; }
            static const ValueTypeMetaData *value_meta() { return scalar_descriptor<V>::value_meta(); }
        };

        // ---- per-selector genericity (a var-bearing schema is not concrete) ----
        template <typename E> struct selector_is_generic : std::false_type {};
        template <fixed_string N, typename S, auto... P>
        struct selector_is_generic<In<N, S, P...>> : std::bool_constant<!schema_descriptor<S>::is_concrete()> {};
        template <typename S>
        struct selector_is_generic<Out<S>> : std::bool_constant<!schema_descriptor<S>::is_concrete()> {};
        template <fixed_string N, typename V>
        struct selector_is_generic<Scalar<N, V>> : std::bool_constant<!scalar_descriptor<V>::is_concrete()> {};
        template <typename V>
        struct selector_is_generic<State<V>> : std::bool_constant<!scalar_descriptor<V>::is_concrete()> {};
        template <typename S>
        struct selector_is_generic<RecordableState<S>> : std::bool_constant<!schema_descriptor<S>::is_concrete()> {};

        // ---- compile-time output schema type extraction ----
        // output_schema_of<E> is void for any non-output selector and S for Out<S>,
        // so picking the single non-void result yields the node's output schema type.
        template <typename T> struct output_schema_of { using type = void; };
        template <typename S> struct output_schema_of<Out<S>> { using type = S; };

        template <typename A, typename B> struct pick_non_void { using type = A; };
        template <typename B> struct pick_non_void<void, B> { using type = B; };

        template <typename... Es> struct output_type_of_pack { using type = void; };
        template <typename E, typename... Rest>
        struct output_type_of_pack<E, Rest...>
        {
            using type = typename pick_non_void<typename output_schema_of<E>::type,
                                                typename output_type_of_pack<Rest...>::type>::type;
        };

        template <typename Tuple> struct output_type_of_tuple;
        template <typename... Es>
        struct output_type_of_tuple<std::tuple<Es...>>
        {
            using type = typename output_type_of_pack<selector_of<Es>...>::type;
        };

        // ---- compile-time list of the input schema types (In args' ``S``, in order) ----
        // in_schema_tuple<E> is the empty tuple for non-input selectors and tuple<S>
        // for In<Name, S>, so tuple_cat over the args yields the input schema list.
        template <typename E> struct in_schema_tuple { using type = std::tuple<>; };
        template <fixed_string N, typename S, auto... P>
        struct in_schema_tuple<In<N, S, P...>>
        {
            using type = std::tuple<S>;
        };

        template <typename Tuple> struct input_schemas_of_tuple;
        template <typename... Es>
        struct input_schemas_of_tuple<std::tuple<Es...>>
        {
            using type = decltype(std::tuple_cat(std::declval<typename in_schema_tuple<selector_of<Es>>::type>()...));
        };

        // ---- compile-time list of the "wireable" parameters, in eval order ----
        // These are the parameters supplied at wiring time: the time-series inputs
        // (In) and the scalar inputs (Scalar). State / clock / scheduler parameters
        // are injected by the runtime and never appear at a wiring call site.
        template <typename E> struct wire_param_tuple { using type = std::tuple<>; };
        template <fixed_string N, typename S, auto... P>
        struct wire_param_tuple<In<N, S, P...>>
        {
            using type = std::tuple<In<N, S, P...>>;
        };
        template <fixed_string N, typename V> struct wire_param_tuple<Scalar<N, V>> { using type = std::tuple<Scalar<N, V>>; };

        template <typename Tuple> struct wire_params_of_tuple;
        template <typename... Es>
        struct wire_params_of_tuple<std::tuple<Es...>>
        {
            using type = decltype(std::tuple_cat(std::declval<typename wire_param_tuple<selector_of<Es>>::type>()...));
        };

        // ---- function-pointer traits over a static hook ----
        template <typename F> struct fn_traits;
        template <typename R, typename... A>
        struct fn_traits<R (*)(A...)>
        {
            using return_type = R;
            using args_tuple  = std::tuple<A...>;
        };

        template <typename Wanted, typename CanonicalArgs, std::size_t ArgIndex = 0,
                  std::size_t Slot = 0>
        consteval std::size_t canonical_input_slot()
        {
            if constexpr (ArgIndex == std::tuple_size_v<CanonicalArgs>)
            {
                static_assert(always_false_v<Wanted>,
                              "Static node hook input is absent from the eval signature");
                return 0;
            }
            else
            {
                using current = selector_of<std::tuple_element_t<ArgIndex, CanonicalArgs>>;
                if constexpr (is_input_selector<current>::value)
                {
                    if constexpr (same_input_name<Wanted, current>::value) { return Slot; }
                    else
                    {
                        return canonical_input_slot<Wanted, CanonicalArgs, ArgIndex + 1,
                                                    Slot + 1>();
                    }
                }
                else
                {
                    return canonical_input_slot<Wanted, CanonicalArgs, ArgIndex + 1, Slot>();
                }
            }
        }

        template <typename Wanted, typename CanonicalArgs, std::size_t ArgIndex = 0,
                  std::size_t Slot = 0>
        consteval std::size_t canonical_scalar_slot()
        {
            if constexpr (ArgIndex == std::tuple_size_v<CanonicalArgs>)
            {
                static_assert(always_false_v<Wanted>,
                              "Static node hook scalar is absent from the eval signature");
                return 0;
            }
            else
            {
                using current = selector_of<std::tuple_element_t<ArgIndex, CanonicalArgs>>;
                if constexpr (is_scalar_selector<current>::value)
                {
                    if constexpr (same_scalar_name<Wanted, current>::value) { return Slot; }
                    else
                    {
                        return canonical_scalar_slot<Wanted, CanonicalArgs, ArgIndex + 1,
                                                     Slot + 1>();
                    }
                }
                else
                {
                    return canonical_scalar_slot<Wanted, CanonicalArgs, ArgIndex + 1, Slot>();
                }
            }
        }

        // Per-hook scratch only. Keeping the roots beyond this frame would
        // retain input-route state across rebind and dynamic-slot lifecycles.
        // ``routes`` (RFC 0008 stage 5) is the node's planned prepared-slot
        // array: per-tick slot views rebuild from the cached route instead of
        // re-walking the projection; an unready route falls back.
        class StaticNodeInvocationFrame
        {
          public:
            StaticNodeInvocationFrame(const NodeView &view, DateTime evaluation_time,
                                      const detail::PreparedInputSlotRoute *routes = nullptr)
                : view_(view), evaluation_time_(evaluation_time), routes_(routes)
            {
            }

            [[nodiscard]] const NodeView &view() const noexcept { return view_; }
            [[nodiscard]] DateTime evaluation_time() const noexcept { return evaluation_time_; }

            [[nodiscard]] TSInputView input_at(std::size_t slot)
            {
                if (!input_root_.has_value())
                {
                    input_root_.emplace(view_.input(evaluation_time_));
                }
                if (routes_ != nullptr && routes_[slot].ready())
                {
                    return input_root_->child_from_prepared(routes_[slot]);
                }
                return input_root_->indexed_child_at(slot);
            }

            [[nodiscard]] ValueView scalar_at(std::size_t slot)
            {
                if (!scalar_root_.has_value())
                {
                    scalar_root_.emplace(view_.scalars().as_bundle());
                }
                return (*scalar_root_)[slot];
            }

            [[nodiscard]] TSOutputView output() const
            {
                return view_.output(evaluation_time_);
            }

          private:
            const NodeView            &view_;
            DateTime                   evaluation_time_;
            const detail::PreparedInputSlotRoute *routes_{nullptr};
            std::optional<TSInputView> input_root_{};
            std::optional<BundleView>  scalar_root_{};
        };

        // ---- arg providers: build a selector from one invocation frame ----
        template <typename T>
        struct arg_provider
        {
            static_assert(always_false_v<T>, "Unsupported static node hook parameter type");
        };

        template <fixed_string N, typename V, auto... P>
        struct arg_provider<In<N, V, P...>>
        {
            template <std::size_t Slot, typename Frame>
            static In<N, V, P...> get_indexed(Frame &frame)
            {
                return In<N, V, P...>{frame.input_at(Slot)};
            }
        };

        template <typename V>
        struct arg_provider<Out<V>>
        {
            template <typename Frame>
            static Out<V> get_prepared(Frame &frame)
            {
                return Out<V>{frame.output(), frame.evaluation_time()};
            }
        };

        template <typename V>
        struct arg_provider<State<V>>
        {
            static State<V> get(const NodeView &view, DateTime) { return State<V>{view.state()}; }
        };

        template <typename S>
        struct arg_provider<RecordableState<S>>
        {
            static RecordableState<S> get(const NodeView &view, DateTime evaluation_time)
            {
                return RecordableState<S>{view.recordable_state(evaluation_time), evaluation_time};
            }
        };

        // The native node-self injectable is the existing compact type-erased
        // handle. Recreate the move-only borrowed view from the current pointer;
        // it contributes no schema fields or planned storage.
        template <>
        struct arg_provider<NodeView>
        {
            static NodeView get(const NodeView &view, DateTime) noexcept
            {
                return NodeView{view.pointer()};
            }
        };

        template <fixed_string N, typename V>
        struct arg_provider<Scalar<N, V>>
        {
            template <std::size_t Slot, typename Frame>
            static Scalar<N, V> get_indexed(Frame &frame)
            {
                return Scalar<N, V>{frame.scalar_at(Slot)};
            }
        };

        template <>
        struct arg_provider<GlobalStateView>
        {
            static GlobalStateView get(const NodeView &view, DateTime)
            {
                return view.global_state();
            }
        };

        // Transparent, stateless injectable: no signature footprint, no scheduler
        // component (unlike NodeScheduler it is not an ``is_scheduler_selector``).
        template <>
        struct arg_provider<SingleShotScheduler>
        {
            static SingleShotScheduler get(const NodeView &view, DateTime evaluation_time)
            {
                return SingleShotScheduler{view.graph_value(), view.node_index(), evaluation_time};
            }
        };

        template <>
        struct arg_provider<NodeScheduler>
        {
            static NodeScheduler get(const NodeView &view, DateTime evaluation_time)
            {
                // ``started()`` is false while the node's ``start`` hook runs, which
                // is what lets a source schedule its first evaluation at the start
                // time (schedule(now())); during ``eval`` it is true (future only).
                auto       executor            = view.graph().executor();
                const bool supports_wall_clock = executor.valid() &&
                                                 executor.schema()->mode == GraphExecutorMode::RealTime;
                return NodeScheduler{view.scheduler_state(), view.graph_value(), view.node_index(), evaluation_time,
                                     view.started(), view.evaluation_clock(), supports_wall_clock};
            }
        };

        // Transparent, stateless injectable (see TraitsView in runtime/graph.h).
        template <>
        struct arg_provider<TraitsView>
        {
            static TraitsView get(const NodeView &view, DateTime)
            {
                GraphValue *graph = view.graph_value();
                if (graph == nullptr) { return TraitsView{}; }
                GraphView graph_view = graph->view();
                return TraitsView{graph_view.pointer()};
            }
        };

        template <>
        struct arg_provider<EvaluationClockView>
        {
            static EvaluationClockView get(const NodeView &view, DateTime)
            {
                return view.evaluation_clock();
            }
        };

        // Copyable borrowed projection over the owning root executor. Nested
        // graphs resolve the same root executor through GraphView::executor().
        template <>
        struct arg_provider<EngineControlView>
        {
            static EngineControlView get(const NodeView &view, DateTime)
            {
                return view.graph().executor().engine_control();
            }
        };

        template <>
        struct arg_provider<DateTime>
        {
            static DateTime get(const NodeView &, DateTime evaluation_time) noexcept
            {
                return evaluation_time;
            }
        };

        template <typename Selector, typename CanonicalArgs, typename Frame>
        decltype(auto) provide_arg(Frame &frame)
        {
            if constexpr (is_input_selector<Selector>::value)
            {
                constexpr std::size_t slot = canonical_input_slot<Selector, CanonicalArgs>();
                return arg_provider<Selector>::template get_indexed<slot>(frame);
            }
            else if constexpr (is_scalar_selector<Selector>::value)
            {
                constexpr std::size_t slot = canonical_scalar_slot<Selector, CanonicalArgs>();
                return arg_provider<Selector>::template get_indexed<slot>(frame);
            }
            else if constexpr (is_output_selector<Selector>::value)
            {
                return arg_provider<Selector>::get_prepared(frame);
            }
            else
            {
                return arg_provider<Selector>::get(frame.view(), frame.evaluation_time());
            }
        }

        // ---- invoke a static hook by injecting each parameter by type ----
        template <auto Fn, typename CanonicalArgs, std::size_t... I>
        void invoke_impl(const NodeView &view,
                         DateTime evaluation_time,
                         const detail::PreparedInputSlotRoute *routes,
                         std::index_sequence<I...>)
        {
            using args = typename fn_traits<decltype(Fn)>::args_tuple;
            StaticNodeInvocationFrame frame{view, evaluation_time, routes};
            Fn(provide_arg<selector_of<std::tuple_element_t<I, args>>, CanonicalArgs>(
                frame)...);
        }

        // True when some eval parameter is an input selector occupying
        // ``Slot`` in the canonical signature — i.e. the slot's validity is
        // already gated through the materialised argument views.
        template <std::size_t Slot, typename EvalArgs, typename CanonicalArgs, std::size_t... J>
        consteval bool slot_covered_by_eval_impl(std::index_sequence<J...>)
        {
            return (([] {
                        using eval_selector = selector_of<std::tuple_element_t<J, EvalArgs>>;
                        if constexpr (is_input_selector<eval_selector>::value)
                        {
                            return canonical_input_slot<eval_selector, CanonicalArgs>() == Slot;
                        }
                        else { return false; }
                    }()) ||
                    ...);
        }

        template <std::size_t Slot, typename EvalArgs, typename CanonicalArgs>
        consteval bool slot_covered_by_eval()
        {
            return slot_covered_by_eval_impl<Slot, EvalArgs, CanonicalArgs>(
                std::make_index_sequence<std::tuple_size_v<EvalArgs>>{});
        }

        // Gate the canonical input selectors ABSENT from eval's parameter
        // list (the explicit ``signature_args`` contract lets eval omit an
        // In used only by start/stop — review finding on #489: the schema
        // gate covered those, so the frame gate must too). Compiles to
        // ``true`` when the canonical and eval signatures coincide.
        template <typename CanonicalArgs, typename EvalArgs, std::size_t... K>
        [[nodiscard]] bool residual_canonical_inputs_ready_impl(StaticNodeInvocationFrame &frame,
                                                                std::index_sequence<K...>)
        {
            return ([&] {
                       using selector = selector_of<std::tuple_element_t<K, CanonicalArgs>>;
                       if constexpr (is_input_selector<selector>::value)
                       {
                           if constexpr (selector::validity == InputValidity::Unchecked)
                           {
                               return true;
                           }
                           else
                           {
                               if constexpr (!slot_covered_by_eval<
                                                 canonical_input_slot<selector, CanonicalArgs>(),
                                                 EvalArgs, CanonicalArgs>())
                               {
                                   constexpr std::size_t slot =
                                       canonical_input_slot<selector, CanonicalArgs>();
                                   auto view = frame.input_at(slot);
                                   if constexpr (selector::validity == InputValidity::Valid)
                                   {
                                       return view.valid();
                                   }
                                   else { return view.all_valid(); }
                               }
                               else { return true; }
                           }
                       }
                       else { return true; }
                   }() &&
                   ...);
        }

        template <typename CanonicalArgs, typename EvalArgs>
        [[nodiscard]] bool residual_canonical_inputs_ready(StaticNodeInvocationFrame &frame)
        {
            return residual_canonical_inputs_ready_impl<CanonicalArgs, EvalArgs>(
                frame, std::make_index_sequence<std::tuple_size_v<CanonicalArgs>>{});
        }

        // ---- gated invoke: build args once, fold In validity, then call ----
        //
        // The eval twin of ``invoke``: materialise every argument ONCE, gate
        // on the input selectors' compile-time validity policies using the
        // SAME views the hook receives, then apply. Replaces the generic
        // ``ready_to_evaluate`` slot pass for static nodes, which built and
        // resolved each slot view a second time per tick (audit O4). The
        // fold is definitionally identical to the schema gate: valid_inputs /
        // all_valid_inputs are collected from the same ``E::validity``
        // constants in the same canonical slot order.
        template <auto Fn, typename CanonicalArgs, std::size_t... I>
        void invoke_gated_impl(const NodeView &view,
                               DateTime evaluation_time,
                               const detail::PreparedInputSlotRoute *routes,
                               std::index_sequence<I...>)
        {
            using args = typename fn_traits<decltype(Fn)>::args_tuple;
            StaticNodeInvocationFrame frame{view, evaluation_time, routes};
            std::tuple<std::tuple_element_t<I, args>...> hook_args{
                provide_arg<selector_of<std::tuple_element_t<I, args>>, CanonicalArgs>(frame)...};
            const bool inputs_ready =
                ([&] {
                    using selector = selector_of<std::tuple_element_t<I, args>>;
                    if constexpr (is_input_selector<selector>::value)
                    {
                        if constexpr (selector::validity == InputValidity::Valid)
                        {
                            return std::get<I>(hook_args).valid();
                        }
                        else if constexpr (selector::validity == InputValidity::AllValid)
                        {
                            return std::get<I>(hook_args).all_valid();
                        }
                        else { return true; }
                    }
                    else { return true; }
                }() && ...);
            if (!inputs_ready ||
                !residual_canonical_inputs_ready<CanonicalArgs, args>(frame))
            {
                return;
            }
            std::apply(
                [](auto &&...hook_arg) { Fn(std::forward<decltype(hook_arg)>(hook_arg)...); },
                std::move(hook_args));
        }

        template <auto Fn,
                  typename CanonicalArgs = typename fn_traits<decltype(Fn)>::args_tuple>
        void invoke_gated(const NodeView &view, DateTime evaluation_time,
                          const detail::PreparedInputSlotRoute *routes)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::is_same_v<typename traits::return_type, void>,
                          "Static node hooks must return void");
            invoke_gated_impl<Fn, CanonicalArgs>(
                view, evaluation_time, routes,
                std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        template <auto Fn,
                  typename CanonicalArgs = typename fn_traits<decltype(Fn)>::args_tuple>
        void invoke(const NodeView &view, DateTime evaluation_time,
                    const detail::PreparedInputSlotRoute *routes = nullptr)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::is_same_v<typename traits::return_type, void>,
                          "Static node hooks must return void");
            invoke_impl<Fn, CanonicalArgs>(
                view, evaluation_time, routes,
                std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        // ---- hook / trait detection ----
        template <typename T> concept has_start     = requires { &T::start; };
        template <typename T> concept has_stop      = requires { &T::stop; };
        template <typename T> concept has_name      = requires { T::name; };
        template <typename T> concept has_implementation_label = requires { T::implementation_label; };

        template <typename T>
            requires has_name<T>
        [[nodiscard]] constexpr std::string_view name_view() noexcept
        {
            if constexpr (requires { T::name.sv(); }) { return T::name.sv(); }
            else { return std::string_view{T::name}; }
        }

        template <typename T>
        [[nodiscard]] std::string diagnostic_name()
        {
            if constexpr (has_name<T>) { return std::string{name_view<T>()}; }
            else { return typeid(T).name(); }
        }

        template <typename T, typename = void>
        struct signature_args_of
        {
            using type = typename fn_traits<decltype(&T::eval)>::args_tuple;
        };

        template <typename T>
        struct signature_args_of<T, std::void_t<typename T::signature_args>>
        {
            using type = typename T::signature_args;
        };

        // Optional ``static constexpr bool schedule_on_start`` attribute: when true
        // the framework schedules the node for the start cycle (see node.cpp).
        template <typename T> concept has_schedule_on_start = requires { T::schedule_on_start; };
        template <typename T> concept has_uses_python_values = requires { T::uses_python_values; };
        template <typename T> concept has_requires_phase_runner = requires { T::requires_phase_runner; };

        /** Process-stable token for one stateless static-node implementation.
         */
        template <typename TImplementation>
        [[nodiscard]] const void *static_node_runtime_type_id() noexcept {
          static const std::byte token{};
          return &token;
        }
    }  // namespace static_node_detail

    // -----------------------------------------------------------------
    // Static node signature
    // -----------------------------------------------------------------

    /**
     * Compile-time reflection of a static node implementation. Derives the
     * runtime node contract (input/output/state schema, node kind, input
     * endpoint annotation) from ``TImplementation::signature_args`` when
     * supplied, otherwise from the parameter list of ``TImplementation::eval``.
     * An explicit signature lets lifecycle-only fields remain part of the
     * node schema without injecting them into the hot evaluation callback.
     */
    template <typename TImplementation>
    struct StaticNodeSignature
    {
      public:
        using canonical_args =
            typename static_node_detail::signature_args_of<TImplementation>::type;

      private:
        using eval_args = canonical_args;

        static constexpr std::size_t arg_count = std::tuple_size_v<eval_args>;
        using indices                          = std::make_index_sequence<arg_count>;

        template <std::size_t... I>
        static void collect_inputs(std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                   std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_input_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::input_selector_traits<E>::name(),
                                            static_node_detail::input_selector_traits<E>::ts_meta());
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_output(std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *out = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_output_selector<E>::value)
                    {
                        out = static_node_detail::output_selector_traits<E>::ts_meta();
                    }
                }(),
                ...);
            return out;
        }

        template <std::size_t... I>
        static const ValueTypeMetaData *find_state(std::index_sequence<I...>)
        {
            const ValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_state_selector<E>::value)
                    {
                        state = static_node_detail::state_selector_traits<E>::value_meta();
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_recordable_state(std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_recordable_state_selector<E>::value)
                    {
                        state = static_node_detail::recordable_state_selector_traits<E>::ts_meta();
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static void collect_scalars(std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields,
                                    std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_scalar_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::scalar_selector_traits<E>::name(),
                                            static_node_detail::scalar_selector_traits<E>::value_meta());
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static constexpr std::size_t count_inputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_input_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_outputs(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_output_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_scalars(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_scalar_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_states(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_state_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_recordable_states(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_recordable_state_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <typename E>
        static constexpr bool input_has_non_default_activity()
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                return E::activity != InputActivity::Active;
            }
            else
            {
                return false;
            }
        }

        template <typename E>
        static constexpr bool input_has_non_default_validity()
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                return E::validity != InputValidity::Valid;
            }
            else
            {
                return false;
            }
        }

        template <std::size_t... I>
        static constexpr bool has_explicit_activity_policy(std::index_sequence<I...>)
        {
            return (false || ... ||
                    input_has_non_default_activity<
                        static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>());
        }

        template <std::size_t... I>
        static constexpr bool has_explicit_validity_policy(std::index_sequence<I...>)
        {
            return (false || ... ||
                    input_has_non_default_validity<
                        static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>());
        }

        template <typename E>
        static void collect_active_input_slot(std::vector<std::size_t> &slots, std::size_t &input_index)
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                if constexpr (E::activity == InputActivity::Active) { slots.push_back(input_index); }
                ++input_index;
            }
        }

        template <std::size_t... I>
        static std::vector<std::size_t> collect_active_input_slots(std::index_sequence<I...>)
        {
            std::vector<std::size_t> slots;
            slots.reserve(input_count());
            std::size_t input_index = 0;
            (collect_active_input_slot<static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>(
                 slots, input_index),
             ...);
            return slots;
        }

        template <typename E>
        static void collect_structural_input_slot(std::vector<std::size_t> &slots,
                                                  std::size_t &input_index)
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                if constexpr (E::activity == InputActivity::Structural) { slots.push_back(input_index); }
                ++input_index;
            }
        }

        template <std::size_t... I>
        static std::vector<std::size_t> collect_structural_input_slots(std::index_sequence<I...>)
        {
            std::vector<std::size_t> slots;
            slots.reserve(input_count());
            std::size_t input_index = 0;
            (collect_structural_input_slot<
                 static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>(slots, input_index),
             ...);
            return slots;
        }

        template <typename E>
        static void collect_valid_input_slot(std::vector<std::size_t> &slots, std::size_t &input_index)
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                if constexpr (E::validity == InputValidity::Valid) { slots.push_back(input_index); }
                ++input_index;
            }
        }

        template <std::size_t... I>
        static std::vector<std::size_t> collect_valid_input_slots(std::index_sequence<I...>)
        {
            std::vector<std::size_t> slots;
            slots.reserve(input_count());
            std::size_t input_index = 0;
            (collect_valid_input_slot<static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>(
                 slots, input_index),
             ...);
            return slots;
        }

        template <typename E>
        static void collect_all_valid_input_slot(std::vector<std::size_t> &slots, std::size_t &input_index)
        {
            if constexpr (static_node_detail::is_input_selector<E>::value)
            {
                if constexpr (E::validity == InputValidity::AllValid) { slots.push_back(input_index); }
                ++input_index;
            }
        }

        template <std::size_t... I>
        static std::vector<std::size_t> collect_all_valid_input_slots(std::index_sequence<I...>)
        {
            std::vector<std::size_t> slots;
            slots.reserve(input_count());
            std::size_t input_index = 0;
            (collect_all_valid_input_slot<static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>(
                 slots, input_index),
             ...);
            return slots;
        }

        template <template <typename> typename Predicate, typename ArgsTuple, std::size_t... I>
        static constexpr bool tuple_has_selector(std::index_sequence<I...>)
        {
            return (false || ... ||
                    Predicate<static_node_detail::selector_of<std::tuple_element_t<I, ArgsTuple>>>::value);
        }

        template <template <typename> typename Predicate, typename ArgsTuple>
        static constexpr bool args_have_selector()
        {
            return tuple_has_selector<Predicate, ArgsTuple>(
                std::make_index_sequence<std::tuple_size_v<ArgsTuple>>{});
        }

        template <template <typename> typename Predicate>
        static constexpr bool any_hook_uses_selector()
        {
            bool result = args_have_selector<Predicate, eval_args>();
            if constexpr (static_node_detail::has_start<TImplementation>)
            {
                result = result || args_have_selector<
                                       Predicate,
                                       typename static_node_detail::fn_traits<
                                           decltype(&TImplementation::start)>::args_tuple>();
            }
            if constexpr (static_node_detail::has_stop<TImplementation>)
            {
                result = result || args_have_selector<
                                       Predicate,
                                       typename static_node_detail::fn_traits<
                                           decltype(&TImplementation::stop)>::args_tuple>();
            }
            return result;
        }

      public:
        /** The static output schema type (the ``Out<S>``'s ``S``), or ``void`` if no output. */
        using output_schema_type = typename static_node_detail::output_type_of_tuple<eval_args>::type;

        /** Tuple of the input schema *types* (each ``In<Name, S>``'s ``S``), in argument order. */
        using input_schema_types = typename static_node_detail::input_schemas_of_tuple<eval_args>::type;

        /** Tuple of the wiring-time parameter selector types (``In`` and ``Scalar``), in eval order. */
        using wire_param_types = typename static_node_detail::wire_params_of_tuple<eval_args>::type;

        [[nodiscard]] static constexpr std::size_t input_count() { return count_inputs(indices{}); }
        [[nodiscard]] static constexpr std::size_t output_count() { return count_outputs(indices{}); }
        [[nodiscard]] static constexpr std::size_t scalar_count() { return count_scalars(indices{}); }
        [[nodiscard]] static constexpr std::size_t state_count() { return count_states(indices{}); }
        [[nodiscard]] static constexpr std::size_t recordable_state_count()
        {
            return count_recordable_states(indices{});
        }
        [[nodiscard]] static constexpr bool input_names_unique()
        {
            return !static_node_detail::has_duplicate_selector_names<static_node_detail::same_input_name,
                                                                     eval_args>::value;
        }
        [[nodiscard]] static constexpr bool scalar_names_unique()
        {
            return !static_node_detail::has_duplicate_selector_names<static_node_detail::same_scalar_name,
                                                                     eval_args>::value;
        }
        [[nodiscard]] static constexpr bool        has_output() { return output_count() > 0; }
        /** Whether the node declares ``static constexpr bool schedule_on_start = true``. */
        [[nodiscard]] static constexpr bool schedule_on_start()
        {
            if constexpr (static_node_detail::has_schedule_on_start<TImplementation>)
            {
                return TImplementation::schedule_on_start;
            }
            else
            {
                return false;
            }
        }
        /**
         * Whether any hook (``eval`` / ``start`` / ``stop``) injects a
         * ``NodeScheduler`` — so a scheduler-state slot is allocated. A source
         * that only schedules itself in ``start`` still needs the slot.
         */
        [[nodiscard]] static constexpr bool uses_scheduler()
        {
            return any_hook_uses_selector<static_node_detail::is_scheduler_selector>();
        }

        /**
         * Whether any hook injects ``GlobalStateView`` — so a cached root-state
         * view slot is allocated for this node only.
         */
        [[nodiscard]] static constexpr bool uses_global_state()
        {
            return any_hook_uses_selector<static_node_detail::is_global_state_selector>();
        }

        /**
         * Whether any hook injects ``EvaluationClockView`` — so a cached clock-ref
         * slot is allocated for this node only.
         */
        [[nodiscard]] static constexpr bool uses_evaluation_clock()
        {
            return any_hook_uses_selector<static_node_detail::is_evaluation_clock_selector>();
        }

        [[nodiscard]] static std::optional<std::vector<std::size_t>> active_inputs()
        {
            if constexpr (has_explicit_activity_policy(indices{}))
            {
                return collect_active_input_slots(indices{});
            }
            else
            {
                return std::nullopt;
            }
        }

        [[nodiscard]] static std::vector<std::size_t> structural_inputs()
        {
            return collect_structural_input_slots(indices{});
        }

        [[nodiscard]] static std::optional<std::vector<std::size_t>> valid_inputs()
        {
            if constexpr (has_explicit_validity_policy(indices{}))
            {
                return collect_valid_input_slots(indices{});
            }
            else
            {
                return std::nullopt;
            }
        }

        [[nodiscard]] static std::vector<std::size_t> all_valid_inputs()
        {
            return collect_all_valid_input_slots(indices{});
        }

        // ---- resolved (generic) collectors: substitute type-var bindings ----
        template <std::size_t... I>
        static void collect_inputs_resolved(std::vector<std::pair<std::string, const TSValueTypeMetaData *>> &fields,
                                            const ResolutionMap &m, std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_input_selector<E>::value)
                    {
                        using S = typename static_node_detail::input_selector_traits<E>::schema;
                        fields.emplace_back(static_node_detail::input_selector_traits<E>::name(),
                                            ts_resolver<S>::resolve(m));
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_output_resolved(const ResolutionMap &m, std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *out = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_output_selector<E>::value)
                    {
                        out = ts_resolver<typename static_node_detail::output_selector_traits<E>::schema>::resolve(m);
                    }
                }(),
                ...);
            return out;
        }

        template <std::size_t... I>
        static const ValueTypeMetaData *find_state_resolved(const ResolutionMap &m, std::index_sequence<I...>)
        {
            const ValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_state_selector<E>::value)
                    {
                        state = scalar_resolver<typename static_node_detail::state_selector_traits<E>::value_schema>::resolve(m);
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static const TSValueTypeMetaData *find_recordable_state_resolved(const ResolutionMap &m, std::index_sequence<I...>)
        {
            const TSValueTypeMetaData *state = nullptr;
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_recordable_state_selector<E>::value)
                    {
                        state = ts_resolver<
                            typename static_node_detail::recordable_state_selector_traits<E>::schema>::resolve(m);
                    }
                }(),
                ...);
            return state;
        }

        template <std::size_t... I>
        static void collect_scalars_resolved(std::vector<std::pair<std::string, const ValueTypeMetaData *>> &fields,
                                             const ResolutionMap &m, std::index_sequence<I...>)
        {
            (
                [&] {
                    using E = static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>;
                    if constexpr (static_node_detail::is_scalar_selector<E>::value)
                    {
                        fields.emplace_back(static_node_detail::scalar_selector_traits<E>::name(),
                                            scalar_resolver<typename static_node_detail::scalar_selector_traits<E>::value_schema>::resolve(m));
                    }
                }(),
                ...);
        }

        template <std::size_t... I>
        static constexpr bool compute_is_generic(std::index_sequence<I...>)
        {
            return (false || ... ||
                    static_node_detail::selector_is_generic<
                        static_node_detail::selector_of<std::tuple_element_t<I, eval_args>>>::value);
        }

        /** True when any In / Out / Scalar / State selector carries an unresolved type variable. */
        [[nodiscard]] static constexpr bool is_generic() { return compute_is_generic(indices{}); }

        [[nodiscard]] static const TSValueTypeMetaData *input_schema()
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_tsb(fields);
        }

        [[nodiscard]] static const TSValueTypeMetaData *output_schema() { return find_output(indices{}); }
        [[nodiscard]] static const ValueTypeMetaData   *state_schema() { return find_state(indices{}); }
        [[nodiscard]] static const TSValueTypeMetaData *recordable_state_schema()
        {
            return find_recordable_state(indices{});
        }

        /** Resolved input TSB schema, substituting type-var bindings from ``m``. */
        [[nodiscard]] static const TSValueTypeMetaData *input_schema(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs_resolved(fields, m, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_tsb(fields);
        }

        [[nodiscard]] static const TSValueTypeMetaData *output_schema(const ResolutionMap &m)
        {
            return find_output_resolved(m, indices{});
        }

        [[nodiscard]] static const ValueTypeMetaData *state_schema(const ResolutionMap &m)
        {
            return find_state_resolved(m, indices{});
        }

        [[nodiscard]] static const TSValueTypeMetaData *recordable_state_schema(const ResolutionMap &m)
        {
            return find_recordable_state_resolved(m, indices{});
        }

        /** Resolved scalar-configuration bundle schema (or nullptr when none). */
        [[nodiscard]] static const ValueTypeMetaData *scalar_schema(const ResolutionMap &m)
        {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            collect_scalars_resolved(fields, m, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_bundle(fields);
        }

        /**
         * The node's scalar-configuration schema: a compound (un-named bundle)
         * value type with one field per ``Scalar<Name, T>`` argument, or
         * ``nullptr`` when the node takes no scalar inputs. Scalars are *not*
         * time-series inputs and never appear in the input TSB.
         */
        [[nodiscard]] static const ValueTypeMetaData *scalar_schema()
        {
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
            collect_scalars(fields, indices{});
            if (fields.empty()) { return nullptr; }
            return TypeRegistry::instance().un_named_bundle(fields);
        }

        [[nodiscard]] static NodeKind node_kind()
        {
            // The kind is always determined from the node's shape; there is no
            // override for ordinary static nodes. Push sources use a specialized
            // builder/node implementation and do not add a generic hook here.
            const bool has_in  = input_count() > 0;
            const bool has_out = output_count() > 0;
            if (has_in && has_out) { return NodeKind::Compute; }
            if (has_out) { return NodeKind::PullSource; }
            if (has_in) { return NodeKind::Sink; }
            return NodeKind::Compute;
        }

        /** Input endpoint annotation: a non-peered TSB of peered terminals. */
        [[nodiscard]] static TSEndpointSchema input_endpoint()
        {
            const TSValueTypeMetaData *tsb = input_schema();
            if (tsb == nullptr) { return TSEndpointSchema{}; }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs(fields, indices{});
            std::vector<TSEndpointSchema> children;
            children.reserve(fields.size());
            for (const auto &field : fields) { children.push_back(TSEndpointSchema::peered(field.second)); }
            return TSEndpointSchema::non_peered(tsb, std::move(children));
        }

        /** Resolved input endpoint annotation (type-var bindings from ``m``). */
        [[nodiscard]] static TSEndpointSchema input_endpoint(const ResolutionMap &m)
        {
            const TSValueTypeMetaData *tsb = input_schema(m);
            if (tsb == nullptr) { return TSEndpointSchema{}; }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            collect_inputs_resolved(fields, m, indices{});
            std::vector<TSEndpointSchema> children;
            children.reserve(fields.size());
            for (const auto &field : fields) { children.push_back(TSEndpointSchema::peered(field.second)); }
            return TSEndpointSchema::non_peered(tsb, std::move(children));
        }
    };

    // -----------------------------------------------------------------
    // NodeBuilder front-end
    // -----------------------------------------------------------------

    namespace static_node_detail
    {
        template <typename TImplementation>
        struct StaticNodeBuilderParts
        {
            NodeTypeMetaData schema{};
            TSEndpointSchema input_endpoint{};
            std::string_view implementation_label{};
        };

        template <typename TImplementation>
        [[nodiscard]] constexpr std::string_view static_node_implementation_label() noexcept
        {
            if constexpr (has_implementation_label<TImplementation>)
            {
                return std::string_view{TImplementation::implementation_label};
            }
            return {};
        }

        /**
         * A reference input's initial binding exists before graph start and is
         * not a runtime modification. Active C++ reference consumers therefore
         * need one explicit startup sample; later reference rebinds arrive via
         * the normal subscription path. Passive reference inputs deliberately
         * do not opt into this policy.
         */
        [[nodiscard]] inline bool has_active_reference_input(const NodeTypeMetaData &schema)
        {
            const auto *input = schema.input_schema;
            if (input == nullptr) { return false; }

            const auto active = [&](std::size_t slot) {
                if (!schema.active_inputs.has_value()) { return true; }
                for (const std::size_t active_slot : *schema.active_inputs)
                {
                    if (active_slot == slot) { return true; }
                }
                return false;
            };

            if (input->kind != TSTypeKind::TSB)
            {
                return active(0) && TypeRegistry::contains_ref(input);
            }
            for (std::size_t slot = 0; slot < input->field_count(); ++slot)
            {
                if (active(slot) && TypeRegistry::contains_ref(input->fields()[slot].type)) { return true; }
            }
            return false;
        }

        template <typename TImplementation>
        [[nodiscard]] NodeTypeMetaData static_node_schema_base()
        {
            static_assert(std::is_class_v<TImplementation>, "Static node implementations must be class/struct types");
            static_assert(std::is_empty_v<TImplementation>, "Static node implementations must be stateless");

            using signature = StaticNodeSignature<TImplementation>;
            static_assert(signature::output_count() <= 1, "Static nodes support at most one Out<...> parameter");
            static_assert(signature::state_count() <= 1, "Static nodes support at most one State<...> parameter");
            static_assert(signature::recordable_state_count() <= 1,
                          "Static nodes support at most one RecordableState<...> parameter");
            static_assert(signature::state_count() == 0 || signature::recordable_state_count() == 0,
                          "Static nodes cannot mix State<...> and RecordableState<...>; recordable state replaces local state");
            static_assert(signature::input_names_unique(), "Static node In<> selector names must be unique");
            static_assert(signature::scalar_names_unique(), "Static node Scalar<> selector names must be unique");

            NodeTypeMetaData schema;
            if constexpr (has_name<TImplementation>) { schema.display_name = TImplementation::name; }
            schema.node_kind             = signature::node_kind();
            schema.uses_scheduler        = signature::uses_scheduler();
            schema.uses_global_state     = signature::uses_global_state();
            schema.uses_evaluation_clock = signature::uses_evaluation_clock();
            if constexpr (has_uses_python_values<TImplementation>)
            {
                schema.uses_python_values = TImplementation::uses_python_values;
            }
            if constexpr (has_requires_phase_runner<TImplementation>)
            {
                schema.requires_phase_runner = TImplementation::requires_phase_runner;
            }
            schema.schedule_on_start     = signature::schedule_on_start();
            schema.active_inputs         = signature::active_inputs();
            schema.structural_inputs     = signature::structural_inputs();
            schema.valid_inputs          = signature::valid_inputs();
            schema.all_valid_inputs      = signature::all_valid_inputs();
            return schema;
        }

        // ---- prepared input routes (RFC 0008 stage 5) ----
        // Shared machinery in <hgraph/runtime/prepared_input_routes.h>;
        // acquired after the user start hook, cleared after the user stop
        // hook, offset resolved through the node's own runtime layout.
        template <typename TImplementation>
        [[nodiscard]] NodeCallbacks static_node_callbacks()
        {
            using signature_args =
                typename StaticNodeSignature<TImplementation>::canonical_args;
            constexpr std::size_t slot_count =
                StaticNodeSignature<TImplementation>::input_count();
            NodeCallbacks callbacks;
            if constexpr (slot_count == 0)
            {
                callbacks.evaluate = [](const NodeView &view, DateTime evaluation_time) {
                    invoke<&TImplementation::eval, signature_args>(view, evaluation_time);
                };
                if constexpr (has_start<TImplementation>)
                {
                    callbacks.start = [](const NodeView &view, DateTime evaluation_time) {
                        invoke<&TImplementation::start, signature_args>(view, evaluation_time);
                    };
                }
                if constexpr (has_stop<TImplementation>)
                {
                    callbacks.stop = [](const NodeView &view, DateTime evaluation_time) {
                        invoke<&TImplementation::stop, signature_args>(view, evaluation_time);
                    };
                }
                return callbacks;
            }
            else
            {
                callbacks.evaluate = [](const NodeView &view, DateTime evaluation_time) {
                    invoke_gated<&TImplementation::eval, signature_args>(
                        view, evaluation_time, prepared_input_routes_for(view));
                };
                callbacks.input_validity_in_evaluate = true;
#if defined(_MSC_VER)
                // A deliberately throwing start hook makes the route-acquire
                // success path unreachable in that specialization.
#pragma warning(push)
#pragma warning(disable: 4702)
#endif
                callbacks.start = [](const NodeView &view, DateTime evaluation_time) {
                    // The user hook runs with the slow path (routes not yet
                    // acquired); a throwing start leaves nothing behind.
                    if constexpr (has_start<TImplementation>)
                    {
                        invoke<&TImplementation::start, signature_args>(view, evaluation_time);
                    }
                    acquire_prepared_input_routes<slot_count>(view, evaluation_time);
                };
#if defined(_MSC_VER)
#pragma warning(pop)
#endif
                callbacks.stop = [](const NodeView &view, DateTime evaluation_time) {
                    static_cast<void>(evaluation_time);
                    auto clear_routes = UnwindCleanupGuard([&]() noexcept {
                        clear_prepared_input_routes<slot_count>(view);
                    });
                    if constexpr (has_stop<TImplementation>)
                    {
                        invoke<&TImplementation::stop, signature_args>(view, evaluation_time,
                                                                       prepared_input_routes_for(view));
                    }
                    clear_routes.complete();
                };
                return callbacks;
            }
        }

        template <typename TImplementation>
        [[nodiscard]] StaticNodeBuilderParts<TImplementation> static_node_builder_parts()
        {
            using signature = StaticNodeSignature<TImplementation>;
            auto schema = static_node_schema_base<TImplementation>();
            schema.input_schema            = signature::input_schema();
            schema.output_schema           = signature::output_schema();
            schema.state_schema            = signature::state_schema();
            schema.scalar_schema           = signature::scalar_schema();
            schema.recordable_state_schema = signature::recordable_state_schema();
            schema.schedule_on_start = schema.schedule_on_start || has_active_reference_input(schema);
            return StaticNodeBuilderParts<TImplementation>{
                .schema         = std::move(schema),
                .input_endpoint = signature::input_endpoint(),
                .implementation_label = static_node_implementation_label<TImplementation>(),
            };
        }

        template <typename TImplementation>
        [[nodiscard]] StaticNodeBuilderParts<TImplementation> static_node_builder_parts(
            const ResolutionMap &resolution)
        {
            // Resolved schema pointers substitute wiring-time type-var bindings.
            // The callbacks stay schema-agnostic and read resolved endpoints from NodeView.
            using signature = StaticNodeSignature<TImplementation>;
            auto schema = static_node_schema_base<TImplementation>();
            schema.input_schema            = signature::input_schema(resolution);
            schema.output_schema           = signature::output_schema(resolution);
            schema.state_schema            = signature::state_schema(resolution);
            schema.scalar_schema           = signature::scalar_schema(resolution);
            schema.recordable_state_schema = signature::recordable_state_schema(resolution);
            schema.schedule_on_start = schema.schedule_on_start || has_active_reference_input(schema);
            return StaticNodeBuilderParts<TImplementation>{
                .schema         = std::move(schema),
                .input_endpoint = signature::input_endpoint(resolution),
                .implementation_label = static_node_implementation_label<TImplementation>(),
            };
        }
        /**
         * Assemble the node builder from parts (RFC 0008 stage 5): nodes
         * with inputs take the descriptor path so their storage plan carries
         * the prepared-slot array, whose offset the callbacks capture.
         */
        template <typename TImplementation>
        [[nodiscard]] NodeBuilder make_static_node_builder(StaticNodeBuilderParts<TImplementation> parts)
        {
            using signature = StaticNodeSignature<TImplementation>;
            NodeTypeDescriptor descriptor;
            descriptor.schema               = std::move(parts.schema);
            descriptor.implementation_label = parts.implementation_label;
            constexpr std::size_t slot_count = signature::input_count();
            if constexpr (slot_count > 0)
            {
                if (descriptor.schema.input_schema != nullptr)
                {
                    const std::array fields{prepared_routes_storage_field<slot_count>()};
                    descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);
                }
            }
            descriptor.callbacks = static_node_callbacks<TImplementation>();
            return NodeBuilder::from_canonical_descriptor(
                std::move(descriptor),
                static_node_runtime_type_id<TImplementation>(),
                std::move(parts.input_endpoint));
        }
    }  // namespace static_node_detail

    template <typename TImplementation>
    NodeBuilder &NodeBuilder::implementation()
    {
        auto parts = static_node_detail::static_node_builder_parts<TImplementation>();
        std::string saved_label{label_};
        Value       saved_scalars{std::move(scalars_)};
        const auto  saved_output_storage = output_value_storage_;
        *this = static_node_detail::make_static_node_builder<TImplementation>(std::move(parts));
        if (!saved_label.empty()) { label(std::move(saved_label)); }
        if (saved_scalars.has_value()) { scalars(std::move(saved_scalars)); }
        output_value_storage(saved_output_storage);
        return *this;
    }

    template <typename TImplementation>
    NodeBuilder &NodeBuilder::implementation(const ResolutionMap &resolution)
    {
        auto parts = static_node_detail::static_node_builder_parts<TImplementation>(resolution);
        std::string saved_label{label_};
        Value       saved_scalars{std::move(scalars_)};
        const auto  saved_output_storage = output_value_storage_;
        *this = static_node_detail::make_static_node_builder<TImplementation>(std::move(parts));
        if (!saved_label.empty()) { label(std::move(saved_label)); }
        if (saved_scalars.has_value()) { scalars(std::move(saved_scalars)); }
        output_value_storage(saved_output_storage);
        return *this;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_STATIC_NODE_H
