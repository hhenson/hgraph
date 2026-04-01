#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph::v2
{
    template <size_t N>
    struct fixed_string
    {
        char value[N]{};

        constexpr fixed_string(const char (&src)[N])
        {
            for (size_t i = 0; i < N; ++i) { value[i] = src[i]; }
        }

        [[nodiscard]] constexpr std::string_view sv() const noexcept
        {
            return std::string_view{value, N - 1};
        }
    };

    template <size_t N>
    fixed_string(const char (&)[N]) -> fixed_string<N>;

    template <typename TValue>
    struct TS
    {
        using value_type = TValue;
    };

    template <typename TValue>
    struct TSS
    {
        using value_type = TValue;
    };

    template <typename TKey, typename TValueSchema>
    struct TSD
    {
        using key_type = TKey;
        using value_schema = TValueSchema;
    };

    template <typename TElementSchema, size_t FixedSize = 0>
    struct TSL
    {
        using element_schema = TElementSchema;
        static constexpr size_t fixed_size = FixedSize;
    };

    template <typename TSchema>
    struct REF
    {
        using target_schema = TSchema;
    };

    struct SIGNAL
    {};

    template <fixed_string Name, typename TSchema>
    struct Field
    {
        using schema = TSchema;
        static constexpr auto name = Name;
    };

    template <typename... TFields>
    struct TSB
    {};

    // TODO: The reflected C++ signature now carries selector-level
    // activity/validity policy metadata. We still need to extend the static
    // node model with node-level overloads, resolvers, and requires to reach
    // feature parity with Python wiring.
    enum class InputActivity
    {
        Active,
        Passive,
    };

    enum class InputValidity
    {
        Valid,
        AllValid,
        Unchecked,
    };

    template <fixed_string Name,
              typename TSchema,
              InputActivity Activity = InputActivity::Active,
              InputValidity Validity = InputValidity::Valid>
    class In
    {
      public:
        using schema = TSchema;
        static constexpr auto name = Name;
        static constexpr auto activity = Activity;
        static constexpr auto validity = Validity;

        explicit In(TSInputView view) : m_view(std::move(view)) {}

        [[nodiscard]] const TSInputView &view() const noexcept { return m_view; }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }

        operator const TSInputView &() const noexcept { return m_view; }

      protected:
        TSInputView m_view;
    };

    template <fixed_string Name, typename TValue, InputActivity Activity, InputValidity Validity>
    class In<Name, TS<TValue>, Activity, Validity>
    {
      public:
        using schema = TS<TValue>;
        using value_type = TValue;
        static constexpr auto name = Name;
        static constexpr auto activity = Activity;
        static constexpr auto validity = Validity;

        explicit In(TSInputView view) : m_view(std::move(view)) {}

        [[nodiscard]] const TSInputView &view() const noexcept { return m_view; }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }
        [[nodiscard]] const TValue &value() const { return m_view.value().as_atomic().template checked_as<TValue>(); }

        operator const TSInputView &() const noexcept { return m_view; }

      private:
        TSInputView m_view;
    };

    template <typename TSchema>
    class Out
    {
      public:
        using schema = TSchema;

        Out(TSOutputView view, engine_time_t evaluation_time)
            : m_view(std::move(view)), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const TSOutputView &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }

        operator const TSOutputView &() const noexcept { return m_view; }

      protected:
        TSOutputView m_view;
        engine_time_t m_evaluation_time{MIN_DT};
    };

    template <typename TValue>
    class Out<TS<TValue>>
    {
      public:
        using schema = TS<TValue>;
        using value_type = TValue;

        Out(TSOutputView view, engine_time_t evaluation_time)
            : m_view(std::move(view)), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const TSOutputView &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }
        [[nodiscard]] const TValue *try_value() const { return m_view.value().as_atomic().template try_as<TValue>(); }

        template <typename T>
        void set(T &&value) const
        {
            using TValueArg = std::remove_cvref_t<T>;
            m_view.value().template set_scalar<TValueArg>(std::forward<T>(value));

            LinkedTSContext context = m_view.linked_context();
            if (context.ts_state == nullptr) {
                throw std::logic_error("Out<TS<...>>::set requires an output state");
            }
            context.ts_state->mark_modified(m_evaluation_time);
        }

        operator const TSOutputView &() const noexcept { return m_view; }

      private:
        TSOutputView m_view;
        engine_time_t m_evaluation_time{MIN_DT};
    };

    namespace detail
    {
        template <typename T>
        inline constexpr bool schema_always_false_v = false;

        [[nodiscard]] inline nb::object py_call(nb::handle callable, nb::tuple args = nb::tuple(), nb::dict kwargs = nb::dict())
        {
            return nb::steal<nb::object>(PyObject_Call(callable.ptr(), args.ptr(), kwargs.ptr()));
        }

        [[nodiscard]] inline nb::object py_getitem(nb::handle object, nb::handle item)
        {
            return nb::steal<nb::object>(PyObject_GetItem(object.ptr(), item.ptr()));
        }

        [[nodiscard]] inline nb::module_ hgraph_module()
        {
            return nb::module_::import_("hgraph");
        }

        template <typename T>
        [[nodiscard]] inline nb::object python_scalar_type()
        {
            if constexpr (std::is_same_v<T, bool>) {
                return nb::module_::import_("builtins").attr("bool");
            } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
                return nb::module_::import_("builtins").attr("int");
            } else if constexpr (std::is_floating_point_v<T>) {
                return nb::module_::import_("builtins").attr("float");
            } else if constexpr (std::is_same_v<T, std::string>) {
                return nb::module_::import_("builtins").attr("str");
            } else if constexpr (std::is_same_v<T, engine_date_t>) {
                return nb::module_::import_("datetime").attr("date");
            } else if constexpr (std::is_same_v<T, engine_time_t>) {
                return nb::module_::import_("datetime").attr("datetime");
            } else if constexpr (std::is_same_v<T, engine_time_delta_t>) {
                return nb::module_::import_("datetime").attr("timedelta");
            } else {
                return nb::borrow<nb::object>(nb::type<T>());
            }
        }

        template <typename TSchema>
        struct schema_descriptor
        {
            static const TSMeta *ts_meta()
            {
                static_assert(schema_always_false_v<TSchema>, "Unsupported static time-series schema");
            }

            static nb::object py_type()
            {
                static_assert(schema_always_false_v<TSchema>, "Unsupported static time-series schema");
            }
        };

        template <typename TValue>
        struct schema_descriptor<TS<TValue>>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().ts(value::scalar_type_meta<TValue>());
            }

            [[nodiscard]] static nb::object py_type()
            {
                return py_getitem(hgraph_module().attr("TS"), python_scalar_type<TValue>());
            }
        };

        template <typename TValue>
        struct schema_descriptor<TSS<TValue>>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().tss(value::scalar_type_meta<TValue>());
            }

            [[nodiscard]] static nb::object py_type()
            {
                return py_getitem(hgraph_module().attr("TSS"), python_scalar_type<TValue>());
            }
        };

        template <typename TKey, typename TValueSchema>
        struct schema_descriptor<TSD<TKey, TValueSchema>>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().tsd(value::scalar_type_meta<TKey>(), schema_descriptor<TValueSchema>::ts_meta());
            }

            [[nodiscard]] static nb::object py_type()
            {
                return py_getitem(
                    hgraph_module().attr("TSD"),
                    nb::make_tuple(python_scalar_type<TKey>(), schema_descriptor<TValueSchema>::py_type()));
            }
        };

        template <typename TElementSchema, size_t FixedSize>
        struct schema_descriptor<TSL<TElementSchema, FixedSize>>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().tsl(schema_descriptor<TElementSchema>::ts_meta(), FixedSize);
            }

            [[nodiscard]] static nb::object py_type()
            {
                nb::object size_type = FixedSize == 0
                                           ? hgraph_module().attr("Size")
                                           : py_getitem(hgraph_module().attr("Size"), nb::int_(FixedSize));
                return py_getitem(hgraph_module().attr("TSL"), nb::make_tuple(schema_descriptor<TElementSchema>::py_type(), size_type));
            }
        };

        template <typename TSchema>
        struct schema_descriptor<REF<TSchema>>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().ref(schema_descriptor<TSchema>::ts_meta());
            }

            [[nodiscard]] static nb::object py_type()
            {
                return py_getitem(hgraph_module().attr("REF"), schema_descriptor<TSchema>::py_type());
            }
        };

        template <>
        struct schema_descriptor<SIGNAL>
        {
            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().signal();
            }

            [[nodiscard]] static nb::object py_type()
            {
                return hgraph_module().attr("SIGNAL");
            }
        };

        template <typename TField>
        struct field_descriptor;

        template <fixed_string Name, typename TSchema>
        struct field_descriptor<Field<Name, TSchema>>
        {
            using schema = TSchema;

            [[nodiscard]] static std::string name()
            {
                return std::string{Name.sv()};
            }

            [[nodiscard]] static nb::object py_type()
            {
                return schema_descriptor<TSchema>::py_type();
            }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return schema_descriptor<TSchema>::ts_meta();
            }
        };

        template <typename... TFields>
        struct schema_descriptor<TSB<TFields...>>
        {
            [[nodiscard]] static std::string bundle_name()
            {
                std::string out{"TSB["};
                bool first = true;
                ((out += (first ? "" : ",") + field_descriptor<TFields>::name(), first = false), ...);
                out += "]";
                return out;
            }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                static const TSMeta *meta = [] {
                    std::vector<std::pair<std::string, const TSMeta *>> fields;
                    fields.reserve(sizeof...(TFields));
                    (fields.emplace_back(field_descriptor<TFields>::name(), field_descriptor<TFields>::ts_meta()), ...);
                    return TSTypeRegistry::instance().tsb(fields, bundle_name());
                }();
                return meta;
            }

            [[nodiscard]] static nb::object py_type()
            {
                nb::dict kwargs;
                ((kwargs[field_descriptor<TFields>::name().c_str()] = field_descriptor<TFields>::py_type()), ...);
                nb::object schema = py_call(hgraph_module().attr("ts_schema"), nb::tuple(), kwargs);
                return py_getitem(hgraph_module().attr("TSB"), schema);
            }
        };

        template <typename T>
        struct is_input_selector : std::false_type
        {};

        template <fixed_string Name, typename TSchema, InputActivity Activity, InputValidity Validity>
        struct is_input_selector<In<Name, TSchema, Activity, Validity>> : std::true_type
        {};

        template <typename T>
        struct is_output_selector : std::false_type
        {};

        template <typename TSchema>
        struct is_output_selector<Out<TSchema>> : std::true_type
        {};
    }  // namespace detail
}  // namespace hgraph::v2
