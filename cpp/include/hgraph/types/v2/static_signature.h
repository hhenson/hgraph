#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/static_schema.h>

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
        [[nodiscard]] std::vector<std::string> input_argument_names_impl(std::index_sequence<I...>)
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
        [[nodiscard]] std::vector<std::string> input_argument_names()
        {
            return input_argument_names_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
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

        template <typename TArgsTuple, size_t... I>
        void append_python_input_types(nb::dict &input_types, const nb::object &parse_type, std::index_sequence<I...>)
        {
            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_input_selector<arg_type>::value) {
                        input_types[input_selector_traits<arg_type>::name().c_str()] =
                            parse_type(schema_descriptor<typename input_selector_traits<arg_type>::schema>::py_type());
                    }
                }(),
                ...);
        }

        template <typename TArgsTuple>
        [[nodiscard]] nb::dict python_input_types()
        {
            nb::object parse_type = hgraph_module().attr("HgTypeMetaData").attr("parse_type");
            nb::dict input_types;
            append_python_input_types<TArgsTuple>(
                input_types, parse_type, std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
            return input_types;
        }

        template <typename TArgsTuple, size_t... I>
        [[nodiscard]] nb::object python_output_type_impl(std::index_sequence<I...>)
        {
            nb::object parse_type = hgraph_module().attr("HgTimeSeriesTypeMetaData").attr("parse_type");
            nb::object output_type = nb::none();

            (
                [&] {
                    using arg_type = std::remove_cvref_t<std::tuple_element_t<I, TArgsTuple>>;
                    if constexpr (is_output_selector<arg_type>::value) {
                        output_type = parse_type(schema_descriptor<typename output_selector_traits<arg_type>::schema>::py_type());
                    }
                }(),
                ...);

            return output_type;
        }

        template <typename TArgsTuple>
        [[nodiscard]] nb::object python_output_type()
        {
            return python_output_type_impl<TArgsTuple>(std::make_index_sequence<std::tuple_size_v<TArgsTuple>>{});
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
    }  // namespace detail

    template <typename TImplementation>
    struct StaticNodeSignature
    {
      private:
        using eval_traits = detail::static_fn_traits<decltype(&TImplementation::eval)>;
        using eval_args_tuple = typename eval_traits::args_tuple;

      public:
        static_assert(std::is_same_v<typename eval_traits::return_type, void>, "Static node eval hooks must return void");
        static_assert(detail::output_selector_count<eval_args_tuple>() <= 1, "Static compute nodes support at most one Out<...> parameter");

        [[nodiscard]] static std::vector<std::string> input_names()
        {
            return detail::input_argument_names<eval_args_tuple>();
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

        [[nodiscard]] static const TSMeta *input_schema(std::string_view name = {})
        {
            const auto fields = detail::input_fields<eval_args_tuple>();
            if (fields.empty()) { return nullptr; }
            return TSTypeRegistry::instance().tsb(fields, detail::node_name_or<TImplementation>(name) + ".inputs");
        }

        [[nodiscard]] static const TSMeta *output_schema()
        {
            return detail::output_schema<eval_args_tuple>();
        }

        [[nodiscard]] static nb::object wiring_signature(std::string_view name = {})
        {
            const std::string node_name = detail::node_name_or<TImplementation>(name);
            const std::vector<std::string> args = input_names();

            nb::list arg_list;
            for (const auto &arg : args) { arg_list.append(arg); }

            nb::dict kwargs;
            kwargs["node_type"] = nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeType").attr("COMPUTE_NODE");
            kwargs["name"] = node_name;
            kwargs["args"] = detail::list_to_tuple(arg_list);
            kwargs["defaults"] = detail::make_empty_frozendict();
            kwargs["input_types"] = detail::make_frozendict(detail::python_input_types<eval_args_tuple>());
            kwargs["output_type"] = detail::python_output_type<eval_args_tuple>();
            kwargs["src_location"] = detail::make_source_code_details();
            kwargs["active_inputs"] = detail::names_as_frozenset_or_none(active_input_names());
            kwargs["valid_inputs"] = detail::names_as_frozenset_or_none(valid_input_names());
            kwargs["all_valid_inputs"] = detail::names_as_frozenset_or_none(all_valid_input_names());
            kwargs["context_inputs"] = nb::none();
            kwargs["unresolved_args"] = detail::input_names_to_frozenset(std::vector<std::string>{});
            kwargs["time_series_args"] = detail::input_names_to_frozenset(args);

            return detail::py_call(
                nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeSignature"),
                nb::tuple(),
                kwargs);
        }
    };
}  // namespace hgraph::v2
