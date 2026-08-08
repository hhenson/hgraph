#ifndef HGRAPH_TYPES_CALL_ARGS_H
#define HGRAPH_TYPES_CALL_ARGS_H

#include <hgraph/types/static_schema.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hgraph
{
    template <typename T>
    struct signature_defaults
    {
        [[nodiscard]] static auto values() { return std::tuple{}; }
    };

    /**
     * Call-site keyword argument: ``arg<"name">(value)`` — the C++ spelling of
     * Python's ``name=value``. Erased dispatch uses the runtime ``name`` field;
     * static call paths can still unwrap the payload when they remain positional.
     */
    template <typename T>
    struct NamedArg
    {
        std::string_view name{};
        T                value;
    };

    template <fixed_string Name, typename T>
    struct StaticNamedArg
    {
        static constexpr auto field_name = Name;

        std::string_view name{Name.sv()};
        T                value;
    };

    template <fixed_string Name, typename T>
    [[nodiscard]] StaticNamedArg<Name, std::decay_t<T>> arg(T &&value)
    {
        return StaticNamedArg<Name, std::decay_t<T>>{Name.sv(), std::forward<T>(value)};
    }

    namespace call_args_detail
    {
        inline constexpr std::size_t npos = static_cast<std::size_t>(-1);

        /**
         * Auto-bound parameter customisation point. ``Context<"name", S>``
         * inputs (see static_node.h) are resolved from the wiring-time
         * context stack rather than supplied by the caller: they never
         * consume a POSITIONAL argument and are never "missing", but MAY be
         * overridden by a keyword argument (``arg<"name">(port)``), matching
         * Python's explicit-context-override behaviour. The primary template
         * is false; static_node.h specialises it for context-tagged ``In``.
         */
        template <typename P>
        inline constexpr bool auto_context_param_v = false;

        /**
         * The positional rank of ``ParamIndex`` among the caller-visible
         * (non-auto) parameters, or ``npos`` when the parameter is auto-bound
         * (it has no positional slot).
         */
        template <std::size_t ParamIndex, typename ParamsTuple>
        [[nodiscard]] consteval std::size_t caller_positional_rank()
        {
            if constexpr (auto_context_param_v<
                              std::remove_cvref_t<std::tuple_element_t<ParamIndex, ParamsTuple>>>)
            {
                return npos;
            }
            else
            {
                std::size_t rank = 0;
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            if constexpr (I < ParamIndex &&
                                          !auto_context_param_v<std::remove_cvref_t<
                                              std::tuple_element_t<I, ParamsTuple>>>)
                            {
                                ++rank;
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
                return rank;
            }
        }

        template <typename ParamsTuple>
        [[nodiscard]] consteval std::size_t caller_visible_param_count()
        {
            std::size_t count = 0;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        if constexpr (!auto_context_param_v<std::remove_cvref_t<
                                          std::tuple_element_t<I, ParamsTuple>>>)
                        {
                            ++count;
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
            return count;
        }

        /** Map a positional slot (rank among caller-visible params) to its param index. */
        template <typename ParamsTuple>
        [[nodiscard]] consteval std::array<std::size_t, std::tuple_size_v<ParamsTuple>>
        positional_slot_to_param_index()
        {
            std::array<std::size_t, std::tuple_size_v<ParamsTuple>> map{};
            std::size_t slot = 0;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        if constexpr (!auto_context_param_v<std::remove_cvref_t<
                                          std::tuple_element_t<I, ParamsTuple>>>)
                        {
                            map[slot++] = I;
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
            for (; slot < map.size(); ++slot) { map[slot] = npos; }
            return map;
        }

        /**
         * Always-false trait used to fail compilation when a required call
         * argument is neither supplied nor defaulted. It exists (instead of a
         * plain dependent-false) so the compiler error NAMES the missing
         * parameter: the failed requirement reads
         * ``missing_required_argument<Scalar<{"delta"}, long long>>::value``,
         * pointing straight at the ``In<"name", …>`` / ``Scalar<"name", T>``
         * that was not provided.
         */
        template <typename MissingParam>
        struct missing_required_argument : std::false_type
        {
        };

        template <typename T>
        struct is_named_arg : std::false_type
        {
        };
        template <typename T>
        struct is_named_arg<NamedArg<T>> : std::true_type
        {
        };
        template <fixed_string Name, typename T>
        struct is_named_arg<StaticNamedArg<Name, T>> : std::true_type
        {
        };

        template <typename T>
        struct is_static_named_arg : std::false_type
        {
        };
        template <fixed_string Name, typename T>
        struct is_static_named_arg<StaticNamedArg<Name, T>> : std::true_type
        {
        };

        template <typename A>
        struct named_payload
        {
            using type                  = A;
            static constexpr bool named = false;
        };
        template <typename T>
        struct named_payload<NamedArg<T>>
        {
            using type                  = T;
            static constexpr bool named = true;
        };
        template <fixed_string Name, typename T>
        struct named_payload<StaticNamedArg<Name, T>>
        {
            using type                  = T;
            static constexpr bool named = true;
        };

        template <typename A>
        using payload_t = typename named_payload<std::remove_cvref_t<A>>::type;
        template <typename A>
        inline constexpr bool is_named_arg_v = named_payload<std::remove_cvref_t<A>>::named;
        template <typename A>
        inline constexpr bool is_static_named_arg_v = is_static_named_arg<std::remove_cvref_t<A>>::value;

        template <typename T>
        concept has_static_defaults = requires { T::defaults(); };

        template <typename T>
        [[nodiscard]] auto default_args_for()
        {
            if constexpr (has_static_defaults<T>) { return T::defaults(); }
            else { return signature_defaults<T>::values(); }
        }

        template <typename Arg>
        [[nodiscard]] const auto &payload_of(const Arg &argument)
        {
            if constexpr (is_named_arg_v<Arg>) { return argument.value; }
            else { return argument; }
        }

        template <std::size_t I, typename Tuple>
        [[nodiscard]] const auto &payload_at(const Tuple &arguments)
        {
            return payload_of(std::get<I>(arguments));
        }

        template <typename P>
        concept has_field_name = requires {
            { std::remove_cvref_t<P>::field_name.sv() } -> std::convertible_to<std::string_view>;
        };

        template <typename P>
        [[nodiscard]] consteval std::string_view parameter_name()
        {
            if constexpr (has_field_name<P>) { return std::remove_cvref_t<P>::field_name.sv(); }
            else { return {}; }
        }

        template <typename A>
        [[nodiscard]] consteval std::string_view static_arg_name()
        {
            using Arg = std::remove_cvref_t<A>;
            if constexpr (is_static_named_arg_v<Arg>) { return Arg::field_name.sv(); }
            else { return {}; }
        }

        template <typename ParamsTuple, std::size_t... I>
        [[nodiscard]] std::size_t find_parameter_index(std::string_view name, std::index_sequence<I...>)
        {
            constexpr std::size_t count = sizeof...(I);
            std::size_t           found = count;
            (
                [&] {
                    using P = std::tuple_element_t<I, ParamsTuple>;
                    constexpr std::string_view param_name = parameter_name<P>();
                    if (found == count && !param_name.empty() && param_name == name) { found = I; }
                }(),
                ...);
            return found;
        }

        template <typename ParamsTuple>
        [[nodiscard]] std::size_t find_parameter_index(std::string_view name)
        {
            return find_parameter_index<ParamsTuple>(
                name, std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
        }

        [[nodiscard]] inline std::string missing_parameter_name(std::string_view parameter_name, std::size_t index)
        {
            if (!parameter_name.empty()) { return "'" + std::string{parameter_name} + "'"; }
            return "at position " + std::to_string(index);
        }

        enum class named_parameter_binding
        {
            default_value,
            call_argument,
        };

        template <named_parameter_binding Binding>
        [[noreturn]] inline void throw_unknown_named_parameter(std::string_view call_name,
                                                               std::string_view parameter_kind,
                                                               std::string_view name)
        {
            if constexpr (Binding == named_parameter_binding::default_value)
            {
                throw std::logic_error(std::string{call_name} + ": default names an unknown " +
                                       std::string{parameter_kind} + " '" + std::string{name} + "'");
            }
            else
            {
                throw std::invalid_argument(std::string{call_name} + ": got an unexpected keyword argument '" +
                                            std::string{name} + "'");
            }
        }

        template <named_parameter_binding Binding>
        [[noreturn]] inline void throw_duplicate_named_parameter(std::string_view call_name,
                                                                 std::string_view parameter_kind,
                                                                 std::string_view name)
        {
            if constexpr (Binding == named_parameter_binding::default_value)
            {
                throw std::logic_error(std::string{call_name} + ": multiple defaults for " +
                                       std::string{parameter_kind} + " '" + std::string{name} + "'");
            }
            else
            {
                throw std::invalid_argument(std::string{call_name} + ": got multiple values for " +
                                            std::string{parameter_kind} + " '" + std::string{name} + "'");
            }
        }

        template <named_parameter_binding Binding, typename ParamsTuple, std::size_t ParamCount>
        void mark_named_parameter(std::array<bool, ParamCount> &filled,
                                  std::string_view name,
                                  std::string_view call_name,
                                  std::string_view parameter_kind)
        {
            static_assert(ParamCount == std::tuple_size_v<ParamsTuple>);

            constexpr std::size_t param_count = std::tuple_size_v<ParamsTuple>;
            const std::size_t     param       = find_parameter_index<ParamsTuple>(name);
            if (param == param_count)
            {
                throw_unknown_named_parameter<Binding>(call_name, parameter_kind, name);
            }
            if (filled[param])
            {
                throw_duplicate_named_parameter<Binding>(call_name, parameter_kind, name);
            }
            filled[param] = true;
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename Arg>
        [[nodiscard]] consteval bool static_named_arg_matches_parameter()
        {
            using A0 = std::remove_cvref_t<Arg>;
            if constexpr (is_static_named_arg_v<A0>)
            {
                using Param = std::tuple_element_t<ParamIndex, ParamsTuple>;
                constexpr std::string_view param_name = parameter_name<Param>();
                constexpr std::string_view arg_name   = static_arg_name<A0>();
                return !param_name.empty() && param_name == arg_name;
            }
            else
            {
                return false;
            }
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename DefaultsTuple, std::size_t... I>
        [[nodiscard]] consteval std::size_t default_arg_index(std::index_sequence<I...>)
        {
            std::size_t found = npos;
            (
                [&] {
                    if (found != npos) { return; }
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, DefaultsTuple>>;
                    if constexpr (static_named_arg_matches_parameter<ParamIndex, ParamsTuple, A0>()) { found = I; }
                }(),
                ...);
            return found;
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename DefaultsTuple>
        [[nodiscard]] consteval std::size_t default_arg_index()
        {
            using defaults_tuple = std::remove_reference_t<DefaultsTuple>;
            return default_arg_index<ParamIndex, ParamsTuple, defaults_tuple>(
                std::make_index_sequence<std::tuple_size_v<defaults_tuple>>{});
        }

        template <typename ParamsTuple, typename DefaultsTuple, std::size_t... I>
        void validate_default_args([[maybe_unused]] std::string_view call_name,
                                   const DefaultsTuple &defaults,
                                   [[maybe_unused]] std::string_view parameter_kind,
                                   std::index_sequence<I...>)
        {
            constexpr std::size_t param_count = std::tuple_size_v<ParamsTuple>;
            [[maybe_unused]] std::array<bool, param_count> filled{};

            (
                [&] {
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, DefaultsTuple>>;
                    const auto &argument = std::get<I>(defaults);
                    if constexpr (!is_static_named_arg_v<A0>)
                    {
                        throw std::logic_error(std::string{call_name} + ": default " +
                                               std::string{parameter_kind} +
                                               " values must use arg<\"name\">(...)");
                    }
                    else
                    {
                        mark_named_parameter<named_parameter_binding::default_value, ParamsTuple>(
                            filled, argument.name, call_name, parameter_kind);
                    }
                }(),
                ...);
        }

        template <typename ParamsTuple, typename DefaultsTuple>
        void validate_default_args(std::string_view call_name,
                                   const DefaultsTuple &defaults,
                                   std::string_view parameter_kind)
        {
            using defaults_tuple = std::remove_reference_t<DefaultsTuple>;
            validate_default_args<ParamsTuple>(call_name,
                                               defaults,
                                               parameter_kind,
                                               std::make_index_sequence<std::tuple_size_v<defaults_tuple>>{});
        }

        template <typename ParamsTuple, typename ArgsTuple, typename DefaultsTuple, std::size_t... I>
        void validate_call_args_impl(std::string_view call_name,
                                     const ArgsTuple &args,
                                     const DefaultsTuple &defaults,
                                     std::string_view parameter_kind,
                                     std::index_sequence<I...>)
        {
            validate_default_args<ParamsTuple>(call_name, defaults, parameter_kind);

            constexpr std::size_t param_count = std::tuple_size_v<ParamsTuple>;
            // Auto-bound (context) params have no positional slot: positional
            // arguments fill the caller-visible params in declaration order.
            constexpr std::size_t caller_params = caller_visible_param_count<ParamsTuple>();
            [[maybe_unused]] constexpr auto        slot_to_param = positional_slot_to_param_index<ParamsTuple>();
            [[maybe_unused]] std::array<bool, param_count> filled{};
            [[maybe_unused]] bool seen_named = false;
            [[maybe_unused]] std::size_t positional = 0;

            (
                [&] {
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
                    if constexpr (is_named_arg_v<A0>)
                    {
                        const auto &argument = std::get<I>(args);
                        seen_named = true;
                        if constexpr (!is_static_named_arg_v<A0>)
                        {
                            throw std::invalid_argument(
                                std::string{call_name} +
                                ": static call binding requires arg<\"name\">(...) keyword wrappers");
                        }

                        mark_named_parameter<named_parameter_binding::call_argument, ParamsTuple>(
                            filled, argument.name, call_name, parameter_kind);
                    }
                    else
                    {
                        if (seen_named)
                        {
                            throw std::invalid_argument(std::string{call_name} +
                                                        ": positional argument follows a named argument");
                        }
                        if (positional >= caller_params)
                        {
                            throw std::invalid_argument(std::string{call_name} + ": too many arguments");
                        }
                        filled[slot_to_param[positional++]] = true;
                    }
                }(),
                ...);

            [&]<std::size_t... P>(std::index_sequence<P...>) {
                (
                    [&] {
                        using Param = std::tuple_element_t<P, ParamsTuple>;
                        if constexpr (!auto_context_param_v<std::remove_cvref_t<Param>>)
                        {
                            constexpr std::size_t default_index = default_arg_index<P, ParamsTuple, DefaultsTuple>();
                            if (!filled[P] && default_index == npos)
                            {
                                throw std::invalid_argument(std::string{call_name} + ": missing " +
                                                            std::string{parameter_kind} + " " +
                                                            missing_parameter_name(parameter_name<Param>(), P));
                            }
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<param_count>{});
        }

        template <typename ParamsTuple, typename ArgsTuple>
        void validate_call_args(std::string_view call_name,
                                const ArgsTuple &args,
                                std::string_view parameter_kind = "argument")
        {
            using args_tuple = std::remove_reference_t<ArgsTuple>;
            const auto defaults = std::tuple{};
            validate_call_args_impl<ParamsTuple>(call_name,
                                                 args,
                                                 defaults,
                                                 parameter_kind,
                                                 std::make_index_sequence<std::tuple_size_v<args_tuple>>{});
        }

        template <typename ParamsTuple, typename ArgsTuple, typename DefaultsTuple>
        void validate_call_args(std::string_view call_name,
                                const ArgsTuple &args,
                                const DefaultsTuple &defaults,
                                std::string_view parameter_kind = "argument")
        {
            using args_tuple = std::remove_reference_t<ArgsTuple>;
            validate_call_args_impl<ParamsTuple>(call_name,
                                                 args,
                                                 defaults,
                                                 parameter_kind,
                                                 std::make_index_sequence<std::tuple_size_v<args_tuple>>{});
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] consteval std::size_t bound_arg_index(std::index_sequence<I...>)
        {
            std::size_t found      = npos;
            [[maybe_unused]] bool        seen_named = false;
            [[maybe_unused]] std::size_t positional = 0;

            (
                [&] {
                    if (found != npos) { return; }

                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
                    if constexpr (is_named_arg_v<A0>)
                    {
                        seen_named = true;
                        if constexpr (is_static_named_arg_v<A0>)
                        {
                            if constexpr (static_named_arg_matches_parameter<ParamIndex, ParamsTuple, A0>())
                            {
                                found = I;
                            }
                        }
                    }
                    else
                    {
                        // Auto-bound params have rank npos and never match a
                        // positional argument.
                        if (!seen_named && positional == caller_positional_rank<ParamIndex, ParamsTuple>())
                        {
                            found = I;
                        }
                        ++positional;
                    }
                }(),
                ...);

            return found;
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple>
        [[nodiscard]] consteval std::size_t bound_arg_index()
        {
            using args_tuple = std::remove_reference_t<ArgsTuple>;
            return bound_arg_index<ParamIndex, ParamsTuple, args_tuple>(
                std::make_index_sequence<std::tuple_size_v<args_tuple>>{});
        }
    }  // namespace call_args_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_CALL_ARGS_H
