#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/static_schema.h>

#include <algorithm>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    namespace detail
    {
        template <typename T>
        struct static_fn_traits;

        template <typename R, typename... Args>
        struct static_fn_traits<R (*)(Args...)>
        {
            using return_type = R;
            using args_tuple = std::tuple<Args...>;
        };

        template <typename R, typename... Args>
        struct static_fn_traits<R (*)(Args...) noexcept>
        {
            using return_type = R;
            using args_tuple = std::tuple<Args...>;
        };

        template <typename T>
        struct input_selector_traits;

        template <fixed_string Name, typename TSchema, InputActivity Activity, InputValidity Validity>
        struct input_selector_traits<In<Name, TSchema, Activity, Validity>>
        {
            using schema = TSchema;
            static constexpr auto activity = Activity;
            static constexpr auto validity = Validity;

            [[nodiscard]] static std::string name()
            {
                return std::string{Name.sv()};
            }
        };

        template <typename T>
        struct output_selector_traits;

        template <typename TSchema>
        struct output_selector_traits<Out<TSchema>>
        {
            using schema = TSchema;
        };

        template <typename T>
        struct state_selector_traits;

        template <typename TSchema, fixed_string Name>
        struct state_selector_traits<State<TSchema, Name>>
        {
            using schema = TSchema;

            [[nodiscard]] static std::string name()
            {
                return std::string{Name.sv()};
            }
        };

        template <typename T>
        struct recordable_state_selector_traits;

        template <typename TSchema, fixed_string Name>
        struct recordable_state_selector_traits<RecordableState<TSchema, Name>>
        {
            using schema = TSchema;

            [[nodiscard]] static std::string name()
            {
                return std::string{Name.sv()};
            }
        };

        template <typename T>
        struct injectable_selector_traits;

        template <>
        struct injectable_selector_traits<EvaluationClock>
        {
            [[nodiscard]] static std::string name()
            {
                return std::string{default_clock_name.sv()};
            }

            [[nodiscard]] static nb::object py_type()
            {
                return nb::module_::import_("hgraph").attr("EvaluationClock");
            }

            [[nodiscard]] static const char *enum_name() noexcept
            {
                return "CLOCK";
            }
        };

        template <typename T, typename = void>
        struct start_args_tuple_or_empty
        {
            using type = std::tuple<>;
        };

        template <typename T>
        struct start_args_tuple_or_empty<T, std::void_t<decltype(&T::start)>>
        {
            using type = typename static_fn_traits<decltype(&T::start)>::args_tuple;
        };

        template <typename T>
        using start_args_tuple_or_empty_t = typename start_args_tuple_or_empty<T>::type;

        template <typename T, typename = void>
        struct stop_args_tuple_or_empty
        {
            using type = std::tuple<>;
        };

        template <typename T>
        struct stop_args_tuple_or_empty<T, std::void_t<decltype(&T::stop)>>
        {
            using type = typename static_fn_traits<decltype(&T::stop)>::args_tuple;
        };

        template <typename T>
        using stop_args_tuple_or_empty_t = typename stop_args_tuple_or_empty<T>::type;

        template <typename T>
        struct is_injectable_selector : std::false_type
        {};

        template <>
        struct is_injectable_selector<EvaluationClock> : std::true_type
        {};

        template <typename T>
        struct is_evaluation_clock_selector : std::false_type
        {};

        template <>
        struct is_evaluation_clock_selector<EvaluationClock> : std::true_type
        {};

        template <typename TArgsTuple, template <typename> class TPredicate, size_t... I>
        [[nodiscard]] constexpr size_t selector_count_impl(std::index_sequence<I...>)
        {
            return (static_cast<size_t>(TPredicate<std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>>::value) + ... + 0U);
        }

        template <typename TArgsTuple, template <typename> class TPredicate>
        [[nodiscard]] constexpr size_t selector_count()
        {
            return selector_count_impl<TArgsTuple, TPredicate>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <template <typename> class TPredicate, typename... Ts>
        struct first_matching_type_impl
        {
            using type = void;
        };

        template <template <typename> class TPredicate, typename TFirst, typename... TRest>
        struct first_matching_type_impl<TPredicate, TFirst, TRest...>
        {
            using candidate = std::remove_cvref_t<TFirst>;
            using type = std::conditional_t<TPredicate<candidate>::value,
                                            candidate,
                                            typename first_matching_type_impl<TPredicate, TRest...>::type>;
        };

        template <typename TArgsTuple, template <typename> class TPredicate, size_t... I>
        [[nodiscard]] auto first_matching_type_helper(std::index_sequence<I...>)
            -> typename first_matching_type_impl<TPredicate, std::tuple_element_t<I, TArgsTuple>...>::type;

        template <typename TArgsTuple, template <typename> class TPredicate>
        using first_matching_tuple_type =
            decltype(first_matching_type_helper<TArgsTuple, TPredicate>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{}));

        template <typename... Ts>
        struct first_non_void;

        template <>
        struct first_non_void<>
        {
            using type = void;
        };

        template <typename TFirst, typename... TRest>
        struct first_non_void<TFirst, TRest...>
        {
            using type =
                std::conditional_t<!std::is_void_v<TFirst>, TFirst, typename first_non_void<TRest...>::type>;
        };

        template <typename... Ts>
        using first_non_void_t = typename first_non_void<Ts...>::type;

        template <typename TLhs, typename TRhs>
        [[nodiscard]] constexpr bool compatible_optional_selector_types()
        {
            return std::is_void_v<TLhs> || std::is_void_v<TRhs> || std::is_same_v<TLhs, TRhs>;
        }

        template <typename T>
        [[nodiscard]] std::string node_name_or(std::string_view fallback)
        {
            if (!fallback.empty()) { return std::string{fallback}; }

            if constexpr (requires { T::name; }) {
                if constexpr (std::is_same_v<std::remove_cvref_t<decltype(T::name)>, std::string_view>) {
                    return std::string{T::name};
                } else if constexpr (std::is_same_v<std::remove_cvref_t<decltype(T::name)>, const char *>) {
                    return std::string{T::name};
                } else if constexpr (requires { T::name.sv(); }) {
                    return std::string{T::name.sv()};
                }
            }

            return std::string{typeid(T).name()};
        }

        [[nodiscard]] inline nb::object input_names_to_frozenset(const std::vector<std::string> &names)
        {
            nb::list items;
            for (const auto &name : names) { items.append(name); }
            return py_call(nb::module_::import_("builtins").attr("frozenset"), nb::make_tuple(items));
        }

        [[nodiscard]] inline nb::tuple list_to_tuple(const nb::list &items)
        {
            return nb::steal<nb::tuple>(PySequence_Tuple(items.ptr()));
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> active_input_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        if constexpr (input_selector_traits<arg_type>::activity == InputActivity::Active) {
                            names.push_back(input_selector_traits<arg_type>::name());
                        }
                    }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> active_input_names()
        {
            return active_input_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> valid_input_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        if constexpr (input_selector_traits<arg_type>::validity == InputValidity::Valid) {
                            names.push_back(input_selector_traits<arg_type>::name());
                        }
                    }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> valid_input_names()
        {
            return valid_input_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> all_valid_input_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        if constexpr (input_selector_traits<arg_type>::validity == InputValidity::AllValid) {
                            names.push_back(input_selector_traits<arg_type>::name());
                        }
                    }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> all_valid_input_names()
        {
            return all_valid_input_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> unresolved_argument_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        if constexpr (!schema_descriptor<typename input_selector_traits<arg_type>::schema>::is_concrete()) {
                            names.push_back(input_selector_traits<arg_type>::name());
                        }
                    } else if constexpr (is_state_selector<arg_type>::value) {
                        if constexpr (!state_descriptor<typename state_selector_traits<arg_type>::schema>::is_concrete()) {
                            names.push_back(state_selector_traits<arg_type>::name());
                        }
                    } else if constexpr (is_recordable_state_selector<arg_type>::value) {
                        if constexpr (!recordable_state_descriptor<typename recordable_state_selector_traits<arg_type>::schema>::is_concrete()) {
                            names.push_back(recordable_state_selector_traits<arg_type>::name());
                        }
                    }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> unresolved_argument_names()
        {
            return unresolved_argument_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        [[nodiscard]] inline nb::object names_as_frozenset_or_none(const std::vector<std::string> &names)
        {
            return names.empty() ? nb::none() : input_names_to_frozenset(names);
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] constexpr size_t output_selector_count_impl(std::index_sequence<I...>)
        {
            return (static_cast<size_t>(is_output_selector<std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>>::value) + ... + 0U);
        }

        template <typename TArgsTuple>
        [[nodiscard]] constexpr size_t output_selector_count()
        {
            return output_selector_count_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> time_series_argument_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) { names.push_back(input_selector_traits<arg_type>::name()); }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> time_series_argument_names()
        {
            return time_series_argument_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::string> argument_names_impl(std::index_sequence<I...>)
        {
            std::vector<std::string> names;
            names.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        names.push_back(input_selector_traits<arg_type>::name());
                    } else if constexpr (is_state_selector<arg_type>::value) {
                        names.push_back(state_selector_traits<arg_type>::name());
                    } else if constexpr (is_recordable_state_selector<arg_type>::value) {
                        names.push_back(recordable_state_selector_traits<arg_type>::name());
                    } else if constexpr (is_injectable_selector<arg_type>::value) {
                        names.push_back(injectable_selector_traits<arg_type>::name());
                    }
                }(),
                ...);

            return names;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::string> argument_names()
        {
            return argument_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] std::vector<std::pair<std::string, const TSMeta *>> input_fields_impl(std::index_sequence<I...>)
        {
            std::vector<std::pair<std::string, const TSMeta *>> fields;
            fields.reserve(sizeof...(I));

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        fields.emplace_back(
                            input_selector_traits<arg_type>::name(),
                            schema_descriptor<typename input_selector_traits<arg_type>::schema>::ts_meta());
                    }
                }(),
                ...);

            return fields;
        }

        template <typename TArgsTuple>
        [[nodiscard]] std::vector<std::pair<std::string, const TSMeta *>> input_fields()
        {
            return input_fields_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] const TSMeta *output_schema_impl(std::index_sequence<I...>)
        {
            const TSMeta *output_schema = nullptr;

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_output_selector<arg_type>::value) {
                        output_schema = schema_descriptor<typename output_selector_traits<arg_type>::schema>::ts_meta();
                    }
                }(),
                ...);

            return output_schema;
        }

        template <typename TArgsTuple>
        [[nodiscard]] const TSMeta *output_schema()
        {
            return output_schema_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        template <typename TNamedArg>
        void append_python_input_type(nb::dict &input_types, const nb::object &parse_type, PythonTypeVarContext &context)
        {
            if constexpr (is_input_selector<TNamedArg>::value) {
                input_types[input_selector_traits<TNamedArg>::name().c_str()] =
                    parse_type(schema_descriptor<typename input_selector_traits<TNamedArg>::schema>::py_type(context));
            } else if constexpr (is_state_selector<TNamedArg>::value) {
                input_types[state_selector_traits<TNamedArg>::name().c_str()] =
                    parse_type(state_descriptor<typename state_selector_traits<TNamedArg>::schema>::py_type(context));
            } else if constexpr (is_recordable_state_selector<TNamedArg>::value) {
                input_types[recordable_state_selector_traits<TNamedArg>::name().c_str()] =
                    parse_type(recordable_state_descriptor<typename recordable_state_selector_traits<TNamedArg>::schema>::py_type(context));
            } else if constexpr (is_injectable_selector<TNamedArg>::value) {
                input_types[injectable_selector_traits<TNamedArg>::name().c_str()] =
                    parse_type(injectable_selector_traits<TNamedArg>::py_type());
            }
        }

        template <typename TArgsTuple, size_t... I>
        void append_python_input_types(nb::dict &input_types,
                                       const nb::object &parse_type,
                                       PythonTypeVarContext &context,
                                       std::index_sequence<I...>)
        {
            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    append_python_input_type<arg_type>(input_types, parse_type, context);
                }(),
                ...);
        }

        template <typename TArgsTuple>
        [[nodiscard]] nb::dict python_input_types(PythonTypeVarContext &context)
        {
            nb::object parse_type = hgraph_module().attr("HgTypeMetaData").attr("parse_type");
            nb::dict input_types;
            append_python_input_types<TArgsTuple>(
                input_types, parse_type, context, std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
            return input_types;
        }

        template <typename TNamedArg>
        void append_optional_python_input_type(nb::dict &input_types, PythonTypeVarContext &context)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                const std::string name = [&]() {
                    if constexpr (is_state_selector<TNamedArg>::value) {
                        return state_selector_traits<TNamedArg>::name();
                    } else {
                        return recordable_state_selector_traits<TNamedArg>::name();
                    }
                }();
                if (PyMapping_HasKeyString(input_types.ptr(), name.c_str()) != 1) {
                    nb::object parse_type = hgraph_module().attr("HgTypeMetaData").attr("parse_type");
                    append_python_input_type<TNamedArg>(input_types, parse_type, context);
                }
            }
        }

        template <typename TNamedArg>
        void append_optional_argument_name(std::vector<std::string> &names)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                const std::string name = [&]() {
                    if constexpr (is_state_selector<TNamedArg>::value) {
                        return state_selector_traits<TNamedArg>::name();
                    } else {
                        return recordable_state_selector_traits<TNamedArg>::name();
                    }
                }();
                if (std::find(names.begin(), names.end(), name) == names.end()) { names.push_back(name); }
            }
        }

        template <typename TNamedArg>
        void append_optional_unresolved_name(std::vector<std::string> &names)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                if constexpr (is_state_selector<TNamedArg>::value) {
                    if constexpr (!state_descriptor<typename state_selector_traits<TNamedArg>::schema>::is_concrete()) {
                        append_optional_argument_name<TNamedArg>(names);
                    }
                } else if constexpr (is_recordable_state_selector<TNamedArg>::value) {
                    if constexpr (!recordable_state_descriptor<typename recordable_state_selector_traits<TNamedArg>::schema>::is_concrete()) {
                        append_optional_argument_name<TNamedArg>(names);
                    }
                }
            }
        }

        template <typename TNamedArg>
        void append_optional_injectable_argument_name(std::vector<std::string> &names)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                const std::string name = injectable_selector_traits<TNamedArg>::name();
                if (std::find(names.begin(), names.end(), name) == names.end()) { names.push_back(name); }
            }
        }

        template <typename TNamedArg>
        void append_optional_injectable_python_input_type(nb::dict &input_types, PythonTypeVarContext &context)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                const std::string name = injectable_selector_traits<TNamedArg>::name();
                if (PyMapping_HasKeyString(input_types.ptr(), name.c_str()) != 1) {
                    nb::object parse_type = hgraph_module().attr("HgTypeMetaData").attr("parse_type");
                    append_python_input_type<TNamedArg>(input_types, parse_type, context);
                }
            }
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] nb::object python_output_type_impl(PythonTypeVarContext &context, std::index_sequence<I...>)
        {
            nb::object parse_type = hgraph_module().attr("HgTimeSeriesTypeMetaData").attr("parse_type");
            nb::object output_type = nb::none();

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_output_selector<arg_type>::value) {
                        output_type =
                            parse_type(schema_descriptor<typename output_selector_traits<arg_type>::schema>::py_type(context));
                    }
                }(),
                ...);

            return output_type;
        }

        template <typename TArgsTuple>
        [[nodiscard]] nb::object python_output_type(PythonTypeVarContext &context)
        {
            return python_output_type_impl<TArgsTuple>(context, std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
        }

        [[nodiscard]] inline nb::object make_source_code_details()
        {
            nb::object hgraph = hgraph_module();
            nb::object path = py_call(nb::module_::import_("pathlib").attr("Path"));
            return py_call(hgraph.attr("SourceCodeDetails"), nb::make_tuple(path, nb::int_(0)));
        }

        [[nodiscard]] inline nb::object make_empty_frozendict()
        {
            return py_call(nb::module_::import_("frozendict").attr("frozendict"), nb::make_tuple(nb::dict()));
        }

        [[nodiscard]] inline nb::object make_frozendict(const nb::dict &value)
        {
            return py_call(nb::module_::import_("frozendict").attr("frozendict"), nb::make_tuple(value));
        }

        template <typename TNamedArg>
        void append_optional_default(nb::dict &defaults)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                const std::string name = [&]() {
                    if constexpr (is_state_selector<TNamedArg>::value) {
                        return state_selector_traits<TNamedArg>::name();
                    } else {
                        return recordable_state_selector_traits<TNamedArg>::name();
                    }
                }();
                defaults[name.c_str()] = nb::none();
            }
        }

        template <typename TNamedArg>
        void append_optional_injectable_default(nb::dict &defaults)
        {
            if constexpr (!std::is_void_v<TNamedArg>) {
                defaults[injectable_selector_traits<TNamedArg>::name().c_str()] = nb::none();
            }
        }

        template <bool HasState, bool HasRecordableState, bool HasClock>
        [[nodiscard]] nb::object python_injectables()
        {
            nb::object enum_type = nb::module_::import_("hgraph._runtime._node").attr("InjectableTypesEnum");
            nb::object value = enum_type.attr("NONE");
            if constexpr (HasState) { value = nb::steal<nb::object>(PyNumber_Or(value.ptr(), enum_type.attr("STATE").ptr())); }
            if constexpr (HasRecordableState) {
                value = nb::steal<nb::object>(PyNumber_Or(value.ptr(), enum_type.attr("RECORDABLE_STATE").ptr()));
            }
            if constexpr (HasClock) { value = nb::steal<nb::object>(PyNumber_Or(value.ptr(), enum_type.attr("CLOCK").ptr())); }
            return value;
        }
    }  // namespace detail

    template <typename TImplementation>
    struct StaticNodeSignature
    {
      private:
        using eval_traits = detail::static_fn_traits<decltype(&TImplementation::eval)>;
        using eval_args_tuple = typename eval_traits::args_tuple;
        using start_args_tuple = detail::start_args_tuple_or_empty_t<TImplementation>;
        using stop_args_tuple = detail::stop_args_tuple_or_empty_t<TImplementation>;

        using eval_state_arg = detail::first_matching_tuple_type<eval_args_tuple, detail::is_state_selector>;
        using start_state_arg = detail::first_matching_tuple_type<start_args_tuple, detail::is_state_selector>;
        using stop_state_arg = detail::first_matching_tuple_type<stop_args_tuple, detail::is_state_selector>;
        using state_arg = detail::first_non_void_t<eval_state_arg, start_state_arg, stop_state_arg>;

        using eval_recordable_state_arg =
            detail::first_matching_tuple_type<eval_args_tuple, detail::is_recordable_state_selector>;
        using start_recordable_state_arg =
            detail::first_matching_tuple_type<start_args_tuple, detail::is_recordable_state_selector>;
        using stop_recordable_state_arg =
            detail::first_matching_tuple_type<stop_args_tuple, detail::is_recordable_state_selector>;
        using recordable_state_arg =
            detail::first_non_void_t<eval_recordable_state_arg, start_recordable_state_arg, stop_recordable_state_arg>;

        using eval_clock_arg = detail::first_matching_tuple_type<eval_args_tuple, detail::is_evaluation_clock_selector>;
        using start_clock_arg = detail::first_matching_tuple_type<start_args_tuple, detail::is_evaluation_clock_selector>;
        using stop_clock_arg = detail::first_matching_tuple_type<stop_args_tuple, detail::is_evaluation_clock_selector>;
        using clock_arg = detail::first_non_void_t<eval_clock_arg, start_clock_arg, stop_clock_arg>;

      public:
        static_assert(std::is_same_v<typename eval_traits::return_type, void>, "Static node eval hooks must return void");
        static_assert(detail::output_selector_count<eval_args_tuple>() <= 1, "Static compute nodes support at most one Out<...> parameter");
        static_assert(detail::selector_count<eval_args_tuple, detail::is_state_selector>() <= 1, "Static node eval supports at most one State<...> parameter");
        static_assert(detail::selector_count<start_args_tuple, detail::is_state_selector>() <= 1, "Static node start supports at most one State<...> parameter");
        static_assert(detail::selector_count<stop_args_tuple, detail::is_state_selector>() <= 1, "Static node stop supports at most one State<...> parameter");
        static_assert(detail::compatible_optional_selector_types<eval_state_arg, start_state_arg>(), "State<...> must match across eval/start");
        static_assert(detail::compatible_optional_selector_types<eval_state_arg, stop_state_arg>(), "State<...> must match across eval/stop");
        static_assert(detail::compatible_optional_selector_types<start_state_arg, stop_state_arg>(), "State<...> must match across start/stop");
        static_assert(detail::selector_count<eval_args_tuple, detail::is_recordable_state_selector>() <= 1,
                      "Static node eval supports at most one RecordableState<...> parameter");
        static_assert(detail::selector_count<start_args_tuple, detail::is_recordable_state_selector>() <= 1,
                      "Static node start supports at most one RecordableState<...> parameter");
        static_assert(detail::selector_count<stop_args_tuple, detail::is_recordable_state_selector>() <= 1,
                      "Static node stop supports at most one RecordableState<...> parameter");
        static_assert(detail::compatible_optional_selector_types<eval_recordable_state_arg, start_recordable_state_arg>(),
                      "RecordableState<...> must match across eval/start");
        static_assert(detail::compatible_optional_selector_types<eval_recordable_state_arg, stop_recordable_state_arg>(),
                      "RecordableState<...> must match across eval/stop");
        static_assert(detail::compatible_optional_selector_types<start_recordable_state_arg, stop_recordable_state_arg>(),
                      "RecordableState<...> must match across start/stop");
        static_assert(detail::selector_count<eval_args_tuple, detail::is_evaluation_clock_selector>() <= 1,
                      "Static node eval supports at most one EvaluationClock parameter");
        static_assert(detail::selector_count<start_args_tuple, detail::is_evaluation_clock_selector>() <= 1,
                      "Static node start supports at most one EvaluationClock parameter");
        static_assert(detail::selector_count<stop_args_tuple, detail::is_evaluation_clock_selector>() <= 1,
                      "Static node stop supports at most one EvaluationClock parameter");

        [[nodiscard]] static std::vector<std::string> input_names()
        {
            return detail::time_series_argument_names<eval_args_tuple>();
        }

        [[nodiscard]] static std::vector<std::string> argument_names()
        {
            auto names = detail::argument_names<eval_args_tuple>();
            detail::append_optional_argument_name<state_arg>(names);
            detail::append_optional_argument_name<recordable_state_arg>(names);
            detail::append_optional_injectable_argument_name<clock_arg>(names);
            return names;
        }

        [[nodiscard]] static std::vector<std::string> active_input_names()
        {
            return detail::active_input_names<eval_args_tuple>();
        }

        [[nodiscard]] static std::vector<std::string> valid_input_names()
        {
            return detail::valid_input_names<eval_args_tuple>();
        }

        [[nodiscard]] static std::vector<std::string> all_valid_input_names()
        {
            return detail::all_valid_input_names<eval_args_tuple>();
        }

        [[nodiscard]] static std::vector<std::string> unresolved_input_names()
        {
            auto names = detail::unresolved_argument_names<eval_args_tuple>();
            detail::append_optional_unresolved_name<state_arg>(names);
            detail::append_optional_unresolved_name<recordable_state_arg>(names);
            return names;
        }

        [[nodiscard]] static const TSMeta *input_schema(std::string_view name = {})
        {
            const auto fields = detail::input_fields<eval_args_tuple>();
            if (fields.empty()) { return nullptr; }
            for (const auto &[_, schema] : fields) {
                if (schema == nullptr) { return nullptr; }
            }
            return TSTypeRegistry::instance().tsb(fields, detail::node_name_or<TImplementation>(name) + ".inputs");
        }

        [[nodiscard]] static const TSMeta *output_schema()
        {
            return detail::output_schema<eval_args_tuple>();
        }

        [[nodiscard]] static constexpr bool has_state()
        {
            return !std::is_void_v<state_arg>;
        }

        [[nodiscard]] static constexpr bool has_recordable_state()
        {
            return !std::is_void_v<recordable_state_arg>;
        }

        [[nodiscard]] static constexpr bool has_clock()
        {
            return !std::is_void_v<clock_arg>;
        }

        [[nodiscard]] static const value::TypeMeta *state_schema()
        {
            if constexpr (has_state()) {
                return detail::state_descriptor<typename detail::state_selector_traits<state_arg>::schema>::type_meta();
            } else {
                return nullptr;
            }
        }

        [[nodiscard]] static const TSMeta *recordable_state_schema()
        {
            if constexpr (has_recordable_state()) {
                return detail::recordable_state_descriptor<typename detail::recordable_state_selector_traits<recordable_state_arg>::schema>::ts_meta();
            } else {
                return nullptr;
            }
        }

        [[nodiscard]] static nb::object wiring_signature(std::string_view name = {})
        {
            const std::string node_name = detail::node_name_or<TImplementation>(name);
            const std::vector<std::string> args = argument_names();
            const std::vector<std::string> unresolved_args = unresolved_input_names();
            detail::PythonTypeVarContext type_var_context;

            nb::list arg_list;
            for (const auto &arg : args) { arg_list.append(arg); }

            auto input_types = detail::python_input_types<eval_args_tuple>(type_var_context);
            detail::append_optional_python_input_type<state_arg>(input_types, type_var_context);
            detail::append_optional_python_input_type<recordable_state_arg>(input_types, type_var_context);
            detail::append_optional_injectable_python_input_type<clock_arg>(input_types, type_var_context);
            nb::dict defaults;
            detail::append_optional_default<state_arg>(defaults);
            detail::append_optional_default<recordable_state_arg>(defaults);
            detail::append_optional_injectable_default<clock_arg>(defaults);

            nb::dict kwargs;
            kwargs["node_type"] = nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeType").attr("COMPUTE_NODE");
            kwargs["name"] = node_name;
            kwargs["args"] = detail::list_to_tuple(arg_list);
            kwargs["defaults"] = detail::make_frozendict(defaults);
            kwargs["input_types"] = detail::make_frozendict(input_types);
            kwargs["output_type"] = detail::python_output_type<eval_args_tuple>(type_var_context);
            kwargs["src_location"] = detail::make_source_code_details();
            kwargs["active_inputs"] = detail::names_as_frozenset_or_none(active_input_names());
            kwargs["valid_inputs"] = detail::names_as_frozenset_or_none(valid_input_names());
            kwargs["all_valid_inputs"] = detail::names_as_frozenset_or_none(all_valid_input_names());
            kwargs["context_inputs"] = nb::none();
            kwargs["unresolved_args"] = detail::input_names_to_frozenset(unresolved_args);
            kwargs["time_series_args"] = detail::input_names_to_frozenset(input_names());
            kwargs["injectables"] = detail::python_injectables<has_state(), has_recordable_state(), has_clock()>();

            return detail::py_call(
                nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeSignature"),
                nb::tuple(),
                kwargs);
        }
    };
}  // namespace hgraph::v2
