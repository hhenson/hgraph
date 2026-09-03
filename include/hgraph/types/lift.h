#ifndef HGRAPH_TYPES_LIFT_H
#define HGRAPH_TYPES_LIFT_H

#include <hgraph/runtime/node.h>
#include <hgraph/runtime/prepared_input_routes.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/types/value_callable.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace lift_detail
    {
        struct function_identity_t
        {
        };

        inline constexpr function_identity_t function_identity{};
    }  // namespace lift_detail

    template <typename F, auto Identity = lift_detail::function_identity>
    struct lift;

    namespace lift_detail
    {
        template <typename T>
        struct is_function_identity : std::false_type
        {
        };

        template <>
        struct is_function_identity<function_identity_t> : std::true_type
        {
        };

        template <auto Identity>
        inline constexpr bool uses_function_identity_v =
            is_function_identity<std::remove_cvref_t<decltype(Identity)>>::value;

        template <typename T>
        struct is_fixed_string : std::false_type
        {
        };

        template <std::size_t N>
        struct is_fixed_string<fixed_string<N>> : std::true_type
        {
        };

        template <typename T>
        struct function_signature;

        template <typename R, typename... Args>
        struct function_signature<R (*)(Args...)>
        {
            using result_type = std::remove_cvref_t<R>;
            using arg_tuple   = std::tuple<std::remove_cvref_t<Args>...>;
            static constexpr std::size_t arity = sizeof...(Args);
        };

        template <typename R, typename... Args>
        struct function_signature<R(Args...)> : function_signature<R (*)(Args...)>
        {
        };

        template <typename C, typename R, typename... Args>
        struct function_signature<R (C::*)(Args...)> : function_signature<R (*)(Args...)>
        {
        };

        template <typename C, typename R, typename... Args>
        struct function_signature<R (C::*)(Args...) const> : function_signature<R (*)(Args...)>
        {
        };

        template <typename C, typename R, typename... Args>
        struct function_signature<R (C::*)(Args...) noexcept> : function_signature<R (*)(Args...)>
        {
        };

        template <typename C, typename R, typename... Args>
        struct function_signature<R (C::*)(Args...) const noexcept> : function_signature<R (*)(Args...)>
        {
        };

        template <typename F>
        concept has_static_apply = requires { &F::apply; };

        template <typename F, bool = has_static_apply<F>>
        struct callable_pointer;

        template <typename F>
        struct callable_pointer<F, true>
        {
            using type = decltype(&F::apply);
        };

        template <typename F>
        struct callable_pointer<F, false>
        {
            using type = decltype(&F::operator());
        };

        template <typename F>
        struct callable_traits : function_signature<typename callable_pointer<F>::type>
        {
        };

        template <typename F>
        using result_t = typename callable_traits<F>::result_type;

        template <typename F>
        using arg_tuple_t = typename callable_traits<F>::arg_tuple;

        template <typename F>
        inline constexpr std::size_t arity_v = callable_traits<F>::arity;

        template <typename F>
        concept has_name = requires { F::name; };

        template <typename F>
        concept has_parameter_names = requires { F::parameter_names; };

        template <typename F>
        concept has_identity_value = requires {
            { F::identity() } -> std::convertible_to<result_t<F>>;
        };

        template <typename F>
        concept has_associative_flag = requires { F::associative; };

        template <typename F>
        concept has_commutative_flag = requires { F::commutative; };

        template <typename F, typename... Args>
        decltype(auto) invoke(Args &&...args)
        {
            if constexpr (has_static_apply<F>) { return F::apply(std::forward<Args>(args)...); }
            else { return F{}(std::forward<Args>(args)...); }
        }

        template <typename Tuple, std::size_t I>
        using tuple_arg_t = std::tuple_element_t<I, Tuple>;

        template <typename F, std::size_t... I>
        [[nodiscard]] Value eval_values_impl(std::span<const ValueView> args, std::index_sequence<I...>)
        {
            using R = result_t<F>;
            if (args.size() != sizeof...(I))
            {
                throw std::invalid_argument("lifted function argument count does not match the kernel arity");
            }
            if constexpr (std::is_void_v<R>)
            {
                static_assert(!std::is_void_v<R>, "lift<F>: lifted functions must return a scalar value");
            }
            else
            {
                using tuple = arg_tuple_t<F>;
                return Value{invoke<F>(args[I].template checked_as<tuple_arg_t<tuple, I>>()...)};
            }
        }

        template <typename F, std::size_t... I>
        [[nodiscard]] Value eval_bound_values_impl(ValueTypeRef output_binding,
                                                   std::span<const ValueView> args,
                                                   std::index_sequence<I...>)
        {
            using R = result_t<F>;
            using tuple = arg_tuple_t<F>;
            if (args.size() != sizeof...(I))
            {
                throw std::invalid_argument("lifted function argument count does not match the kernel arity");
            }
            R result = invoke<F>(args[I].template checked_as<tuple_arg_t<tuple, I>>()...);
            return Value{output_binding, &result};
        }

        template <typename F>
        [[nodiscard]] Value eval_values(std::span<const ValueView> args)
        {
            return eval_values_impl<F>(args, std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F>
        [[nodiscard]] Value eval_bound_values(ValueTypeRef output_binding,
                                              std::span<const ValueView> args)
        {
            return eval_bound_values_impl<F>(output_binding, args,
                                             std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F, std::size_t... I>
        void eval_into_impl(TSDataMutationView &destination, std::span<const ValueView> args,
                            std::index_sequence<I...>)
        {
            using R = result_t<F>;
            using tuple = arg_tuple_t<F>;
            if (args.size() != sizeof...(I))
            {
                throw std::invalid_argument("lifted function argument count does not match the kernel arity");
            }
            R result = invoke<F>(args[I].template checked_as<tuple_arg_t<tuple, I>>()...);
            const ValueView source{destination.value().binding(),
                                   static_cast<const void *>(&result)};
            static_cast<void>(destination.copy_value_from(source));
        }

        template <typename F>
        void eval_into(TSDataMutationView &destination, std::span<const ValueView> args)
        {
            eval_into_impl<F>(destination, args,
                              std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F, std::size_t... I>
        [[nodiscard]] const TSValueTypeMetaData *input_schema_impl(std::size_t index, std::index_sequence<I...>)
        {
            using tuple = arg_tuple_t<F>;
            const std::array<const TSValueTypeMetaData *, sizeof...(I)> schemas{
                TypeRegistry::instance().ts(scalar_descriptor<tuple_arg_t<tuple, I>>::value_meta())...};
            if (index >= schemas.size()) { throw std::out_of_range("LiftedKernel input schema index out of range"); }
            return schemas[index];
        }

        template <typename F>
        [[nodiscard]] const TSValueTypeMetaData *input_schema(std::size_t index)
        {
            return input_schema_impl<F>(index, std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F>
        [[nodiscard]] const TSValueTypeMetaData *output_schema()
        {
            return TypeRegistry::instance().ts(scalar_descriptor<result_t<F>>::value_meta());
        }

        template <typename F>
        [[nodiscard]] std::span<const std::string_view> param_names();

        template <typename F, std::size_t... I>
        [[nodiscard]] std::vector<std::pair<std::string, const TSValueTypeMetaData *>> input_fields_impl(
            std::span<const std::string_view> names,
            std::index_sequence<I...>)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(sizeof...(I));
            (
                fields.emplace_back(
                    I < names.size() && !names[I].empty() ? std::string{names[I]} : std::to_string(I),
                    input_schema<F>(I)),
                ...);
            return fields;
        }

        template <typename F>
        [[nodiscard]] std::vector<std::pair<std::string, const TSValueTypeMetaData *>> input_fields()
        {
            return input_fields_impl<F>(param_names<F>(), std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F>
        [[nodiscard]] std::span<const std::string_view> param_names()
        {
            if constexpr (has_parameter_names<F>)
            {
                return std::span<const std::string_view>{F::parameter_names.data(), F::parameter_names.size()};
            }
            else
            {
                static const auto empty_names = [] {
                    std::array<std::string_view, arity_v<F>> names{};
                    return names;
                }();
                return std::span<const std::string_view>{empty_names.data(), empty_names.size()};
            }
        }

        template <typename F>
        [[nodiscard]] Value function_identity_value()
        {
            return Value{F::identity()};
        }

        template <typename R, auto Identity>
        [[nodiscard]] R explicit_identity_as_result()
        {
            using identity_t = std::remove_cvref_t<decltype(Identity)>;
            if constexpr (is_fixed_string<identity_t>::value)
            {
                static_assert(std::constructible_from<R, std::string_view>,
                              "lift<F, Identity>: fixed_string identity must construct the lifted result type");
                return R{Identity.sv()};
            }
            else if constexpr (std::constructible_from<R, decltype(Identity)>)
            {
                return R{Identity};
            }
            else
            {
                static_assert(std::convertible_to<decltype(Identity), R>,
                              "lift<F, Identity>: identity must be convertible to the lifted result type");
                return static_cast<R>(Identity);
            }
        }

        template <typename F, auto Identity>
        [[nodiscard]] Value explicit_identity_value()
        {
            return Value{explicit_identity_as_result<result_t<F>, Identity>()};
        }

        template <typename F, auto Identity>
        [[nodiscard]] constexpr Value (*identity_value_thunk())()
        {
            if constexpr (!uses_function_identity_v<Identity>) { return &explicit_identity_value<F, Identity>; }
            else if constexpr (has_identity_value<F>) { return &function_identity_value<F>; }
            else { return nullptr; }
        }

        template <typename F>
        [[nodiscard]] const char *name()
        {
            if constexpr (has_name<F>) { return F::name; }
            else { return typeid(F).name(); }
        }

        template <typename F, auto Identity>
        struct lifted_identity
        {
        };

        template <typename F, auto Identity>
        struct lifted_node_identity
        {
        };

        template <typename F>
        [[nodiscard]] bool associative()
        {
            if constexpr (has_associative_flag<F>) { return static_cast<bool>(F::associative); }
            else { return false; }
        }

        template <typename F>
        [[nodiscard]] bool commutative()
        {
            if constexpr (has_commutative_flag<F>) { return static_cast<bool>(F::commutative); }
            else { return false; }
        }

        template <typename F, auto Identity>
        [[nodiscard]] const LiftedKernel &kernel()
        {
            static_assert(!std::is_void_v<result_t<F>>, "lift<F>: lifted functions must return a scalar value");
            static const LiftedKernel value{
                .name              = name<F>(),
                .identity          = &typeid(lifted_identity<F, Identity>),
                .arity             = arity_v<F>,
                .input_schema_fn   = &input_schema<F>,
                .output_schema_fn  = &output_schema<F>,
                .param_names_fn    = &param_names<F>,
                .eval_values_fn    = &eval_values<F>,
                .eval_bound_values_fn = &eval_bound_values<F>,
                .eval_into_fn      = &eval_into<F>,
                .identity_value_fn = identity_value_thunk<F, Identity>(),
                .associative       = associative<F>(),
                .commutative       = commutative<F>(),
            };
            return value;
        }

        /** Typed argument read sharing the ``In<TS<T>>::value`` fast path:
            direct native payload load when the resolved target qualifies,
            erased ``checked_as`` otherwise. */
        template <typename T>
        T read_lifted_arg(const TSInputView &child)
        {
            if (const auto *fast =
                    static_cast<const T *>(child.try_native_value_memory(&ops_for<T>())))
            {
                return *fast;
            }
            return child.value().template checked_as<T>();
        }

        template <typename F, std::size_t... I>
        bool evaluate_lifted_children(const NodeView &view, TSInputView &root, TSBInputView &input,
                                      DateTime evaluation_time, std::index_sequence<I...>)
        {
            // Project each argument child ONCE per evaluation: at(I) walks
            // the input child projection, and the validity pass plus the
            // value pass previously each paid it — the lifted twin of the
            // static-node invocation frame (RFC 0008 profile). Prepared
            // routes (stage 5) replace even that single walk with the
            // planned per-slot route cache when acquired.
            const auto *routes = prepared_input_routes_for(view);
            std::array<TSInputView, sizeof...(I)> children{
                (routes != nullptr && routes[I].ready() ? root.child_from_prepared(routes[I])
                                                        : input.at(I))...};
            if (!(children[I].valid() && ...)) { return true; }
            auto output = view.output(evaluation_time);

            using tuple = arg_tuple_t<F>;
            result_t<F> result =
                invoke<F>(read_lifted_arg<tuple_arg_t<tuple, I>>(children[I])...);
            auto mutation = output.begin_mutation(evaluation_time);
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&result)};
            static_cast<void>(mutation.copy_value_from(source));
            return true;
        }

        template <typename F, auto Identity>
        bool evaluate_lifted_node(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto root_input = view.input(evaluation_time);
            auto input      = root_input.as_bundle();
            return evaluate_lifted_children<F>(view, root_input, input, evaluation_time,
                                               std::make_index_sequence<arity_v<F>>{});
        }

        template <typename F, auto Identity>
        void evaluate_lifted_node_callback(const NodeView &view, DateTime evaluation_time)
        {
            static_cast<void>(evaluate_lifted_node<F, Identity>(nullptr, view, evaluation_time));
        }

        template <typename F, auto Identity>
        [[nodiscard]] WiringPortRef wire_lifted(Wiring &w, std::span<const WiringPortRef> raw_args)
        {
            const LiftedKernel &k = kernel<F, Identity>();
            if (raw_args.size() != k.arity)
            {
                throw std::invalid_argument("lift<F>: wired argument count does not match the function arity");
            }

            std::vector<WiringPortRef> inputs;
            inputs.reserve(raw_args.size());
            for (std::size_t i = 0; i < raw_args.size(); ++i)
            {
                const TSValueTypeMetaData *expected = k.input_schema(i);
                if (raw_args[i].schema == nullptr)
                {
                    throw std::invalid_argument("lift<F>: arguments must have concrete time-series schemas");
                }
                if (!graph_wiring_detail::input_accepts_output_schema(expected, raw_args[i].schema))
                {
                    throw std::invalid_argument("lift<F>: argument schema does not match the lifted function signature");
                }
                inputs.push_back(graph_wiring_detail::adapt_source_for_input(w, expected, raw_args[i]));
            }

            auto       &registry     = TypeRegistry::instance();
            const auto  fields       = input_fields<F>();
            const auto *input_schema = fields.empty() ? nullptr : registry.un_named_tsb(fields);
            const auto *out_schema   = k.output_schema();

            NodeTypeMetaData schema;
            schema.display_name     = k.name;
            schema.input_schema     = input_schema;
            schema.output_schema    = out_schema;
            schema.node_kind        = NodeKind::Compute;

            NodeTypeDescriptor descriptor;
            descriptor.schema             = std::move(schema);
            // The lifted implementation and its callbacks are fixed by this
            // template specialization; values specific to one wired instance
            // live in the node scalars, not in the callback objects.
            static const std::byte runtime_type_token{};
            descriptor.callbacks.evaluate = &evaluate_lifted_node_callback<F, Identity>;
            descriptor.ops.evaluate_impl  = &evaluate_lifted_node<F, Identity>;
            if constexpr (arity_v<F> > 0)
            {
                // Prepared routes (RFC 0008 stage 5): same planned field and
                // acquire/clear lifecycle as static nodes.
                if (input_schema != nullptr)
                {
                    const std::array storage_fields{prepared_routes_storage_field<arity_v<F>>()};
                    descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, storage_fields);
                    descriptor.callbacks.start = [](const NodeView &node, DateTime evaluation_time) {
                        acquire_prepared_input_routes<arity_v<F>>(node, evaluation_time);
                    };
                    descriptor.callbacks.stop = [](const NodeView &node, DateTime) {
                        clear_prepared_input_routes<arity_v<F>>(node);
                    };
                }
            }

            NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
                std::move(descriptor), &runtime_type_token);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            return w.add_node(std::type_index(typeid(lifted_node_identity<F, Identity>)),
                              std::move(builder),
                              std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                              Value{static_cast<WiredFn>(lift<F, Identity>{})});
        }

        template <typename F, auto Identity>
        [[nodiscard]] CompiledSubGraph compile_lifted(
            std::span<const TSValueTypeMetaData *const> input_schemas,
            Wiring *parent = nullptr)
        {
            const LiftedKernel &k = kernel<F, Identity>();
            if (input_schemas.size() != k.arity)
            {
                throw std::invalid_argument("lift<F>: compiled input schema count does not match the function arity");
            }

            Wiring w = parent != nullptr ? parent->child_wiring()
                                         : Wiring{WiringKind::SubGraph};
            std::vector<WiringPortRef> args;
            args.reserve(input_schemas.size());
            std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
            for (std::size_t i = 0; i < input_schemas.size(); ++i)
            {
                args.push_back(WiringPortRef::boundary_source(i, {}, input_schemas[i]));
            }

            WiringPortRef out = wire_lifted<F, Identity>(w, std::span<const WiringPortRef>{args.data(), args.size()});
            return std::move(w).finish_subgraph(std::move(out), std::move(schemas));
        }

        template <typename F, auto Identity>
        [[nodiscard]] WiringPortRef wire_thunk(Wiring &w, std::span<const WiringPortRef> args)
        {
            return wire_lifted<F, Identity>(w, args);
        }

        template <typename F, auto Identity>
        [[nodiscard]] CompiledSubGraph compile_thunk(
            Wiring *parent,
            std::span<const TSValueTypeMetaData *const> input_schemas)
        {
            return compile_lifted<F, Identity>(input_schemas, parent);
        }
    }  // namespace lift_detail

    template <typename F, auto Identity>
    struct lift
    {
        const LiftedKernel *lifted{lifted_kernel()};
        std::size_t         arity{lifted_kernel()->arity};
        bool                has_output{true};

        [[nodiscard]] static const LiftedKernel &kernel()
        {
            return lift_detail::kernel<F, Identity>();
        }

        [[nodiscard]] static const LiftedKernel *lifted_kernel()
        {
            return &kernel();
        }

        [[nodiscard]] static WiringPortRef wire_lifted(Wiring &w, std::span<const WiringPortRef> args)
        {
            return lift_detail::wire_lifted<F, Identity>(w, args);
        }

        [[nodiscard]] static CompiledSubGraph compile_lifted(
            std::span<const TSValueTypeMetaData *const> input_schemas)
        {
            return lift_detail::compile_lifted<F, Identity>(input_schemas);
        }

        [[nodiscard]] static const WiredFnOps &wired_ops()
        {
            static constexpr WiredFnOps ops{
                [](const void *, Wiring &w, std::span<const WiringPortRef> args) {
                    return lift_detail::wire_thunk<F, Identity>(w, args);
                },
                [](const void *, Wiring *parent,
                   std::span<const TSValueTypeMetaData *const> schemas) {
                    return lift_detail::compile_thunk<F, Identity>(parent, schemas);
                },
                nullptr,
                [](const void *) { return lift_detail::param_names<F>(); },
                nullptr,
                nullptr,
                nullptr,
                nullptr,
                nullptr,
                [](const void *) -> std::string_view {
                    const char *name = lift_detail::kernel<F, Identity>().name;
                    return name != nullptr ? std::string_view{name}
                                           : std::string_view{"lift"};
                },
            };
            return ops;
        }

        [[nodiscard]] static WiredFn fn()
        {
            const LiftedKernel &k = kernel();
            return WiredFn{
                .ops            = &wired_ops(),
                .identity       = &typeid(lift_detail::lifted_identity<F, Identity>),
                .lifted         = &k,
                .arity          = k.arity,
                .has_output     = true,
            };
        }

        [[nodiscard]] std::span<const std::string_view> param_names() const
        {
            return lifted != nullptr ? lifted->param_names() : std::span<const std::string_view>{};
        }

        [[nodiscard]] bool valid() const noexcept
        {
            return lifted != nullptr && lifted->valid();
        }

        [[nodiscard]] WiringPortRef wire(Wiring &w, std::span<const WiringPortRef> args) const
        {
            return wire_lifted(w, args);
        }

        [[nodiscard]] CompiledSubGraph compile(std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            return compile_lifted(input_schemas);
        }

        template <typename Other>
        [[nodiscard]] bool operator==(const Other &other) const
            requires(std::is_convertible_v<Other, WiredFn>)
        {
            return static_cast<WiredFn>(*this) == static_cast<WiredFn>(other);
        }

        [[nodiscard]] operator WiredFn() const
        {
            return fn();
        }
    };

    /** Erase a native lifted scalar kernel for runtime value invocation. */
    template <typename F, auto Identity = lift_detail::function_identity>
    [[nodiscard]] ValueCallable value_fn()
    {
        const LiftedKernel *kernel = lift<F, Identity>::lifted_kernel();
        return ValueCallable{
            .identity = kernel->identity,
            .lifted   = kernel,
            .arity    = kernel->arity,
        };
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_LIFT_H
