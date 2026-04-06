#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/evaluation_clock.h>
#include <hgraph/types/v2/ref.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/type_registry.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    /**
     * Compile-time schema vocabulary for the v2 static node model.
     *
     * A node implementation describes its contract entirely through the
     * signatures of static start/stop/eval hooks. The schema types in this
     * header are the C++ equivalent of the Python wiring annotations:
     *
     * @code
     * struct SumNode
     * {
     *     static constexpr auto name = "sum";
     *
     *     static void eval(
     *         In<"lhs", TS<int>> lhs,
     *         In<"rhs", TS<int>> rhs,
     *         Out<TS<int>> out)
     *     {
     *         out.set(lhs.value() + rhs.value());
     *     }
     * };
     * @endcode
     *
     * The same type information is used for three things:
     * - selecting the injected view type seen by the implementation
     * - deriving runtime schemas for TSInput / TSOutput / local state
     * - exporting a matching Python wiring signature
     */

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

    // Default Python-facing argument names for non-time-series injectables.
    inline constexpr fixed_string default_state_name{"_state"};
    inline constexpr fixed_string default_recordable_state_name{"_recordable_state"};
    inline constexpr fixed_string default_scheduler_name{"_scheduler"};
    inline constexpr fixed_string default_clock_name{"_clock"};

    /** Scalar time-series schema, equivalent to Python TS[T]. */
    template <typename TValue>
    struct TS
    {
        using value_type = TValue;
    };

    /** Set time-series schema, equivalent to Python TSS[T]. */
    template <typename TValue>
    struct TSS
    {
        using value_type = TValue;
    };

    /** Dict time-series schema, equivalent to Python TSD[K, V]. */
    template <typename TKey, typename TValueSchema>
    struct TSD
    {
        using key_type = TKey;
        using value_schema = TValueSchema;
    };

    /** List time-series schema, equivalent to Python TSL[T, N?]. */
    template <typename TElementSchema, size_t FixedSize = 0>
    struct TSL
    {
        using element_schema = TElementSchema;
        static constexpr size_t fixed_size = FixedSize;
    };

    /** REF schema marker for v2 time-series references. */
    template <typename TSchema>
    struct REF
    {
        using target_schema = TSchema;
    };

    /** Signal schema marker. */
    struct SIGNAL
    {};

    /** Named TSB field descriptor. */
    template <fixed_string Name, typename TSchema>
    struct Field
    {
        using schema = TSchema;
        static constexpr auto name = Name;
    };

    /** Bundle time-series schema, equivalent to Python TSB[...] / ts_schema(...). */
    template <typename... TFields>
    struct TSB
    {};

    template <typename TSchema, fixed_string Name = default_state_name>
    class State;

    template <typename TSchema, fixed_string Name = default_recordable_state_name>
    class RecordableState;

    template <fixed_string Name, typename... TConstraints>
    struct ScalarVar
    {
        static constexpr auto name = Name;
    };

    template <fixed_string Name, typename... TConstraints>
    struct TsVar
    {
        static constexpr auto name = Name;
    };

    /**
     * Named scalar argument injected from Python wiring scalars.
     *
     * This is intentionally separate from time-series selectors. The node
     * implementation names the scalar explicitly so runtime injection can
     * recover the corresponding entry from the captured Python scalar dict.
     */
    template <fixed_string Name, typename TValue>
    class ScalarArg
    {
      public:
        using value_type = TValue;
        static constexpr auto name = Name;

        explicit ScalarArg(TValue value) : m_value(std::move(value)) {}

        [[nodiscard]] const TValue &value() const noexcept { return m_value; }
        operator const TValue &() const noexcept { return m_value; }

      private:
        TValue m_value;
    };

    /** Named raw Python scalar argument injected without conversion. */
    template <fixed_string Name>
    class PythonScalarArg
    {
      public:
        static constexpr auto name = Name;

        explicit PythonScalarArg(nb::object value) : m_value(std::move(value)) {}

        [[nodiscard]] const nb::object &value() const noexcept { return m_value; }
        [[nodiscard]] const nb::object &object() const noexcept { return m_value; }
        operator const nb::object &() const noexcept { return m_value; }

      private:
        nb::object m_value;
    };

    // TODO: The reflected C++ signature now carries selector-level
    // activity/validity policy metadata. We still need node-level overloads,
    // resolvers, and requires to reach feature parity with Python wiring.
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

    namespace detail
    {
        template <typename TSchema>
        struct schema_view_traits
        {
            using input_view_type = TSInputView;
            using output_view_type = TSOutputView;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view; }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view; }
        };

        template <typename TValue>
        struct schema_view_traits<TS<TValue>>
        {
            using input_view_type = TSInputView;
            using output_view_type = TSOutputView;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view; }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view; }
        };

        template <typename... TFields>
        struct schema_view_traits<TSB<TFields...>>
        {
            using input_view_type = TSBView<TSInputView>;
            using output_view_type = TSBView<TSOutputView>;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view.as_bundle(); }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view.as_bundle(); }
        };

        template <typename TElementSchema, size_t FixedSize>
        struct schema_view_traits<TSL<TElementSchema, FixedSize>>
        {
            using input_view_type = TSLView<TSInputView>;
            using output_view_type = TSLView<TSOutputView>;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view.as_list(); }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view.as_list(); }
        };

        template <typename TKey, typename TValueSchema>
        struct schema_view_traits<TSD<TKey, TValueSchema>>
        {
            using input_view_type = TSDView<TSInputView>;
            using output_view_type = TSDView<TSOutputView>;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view.as_dict(); }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view.as_dict(); }
        };

        template <typename TValue>
        struct schema_view_traits<TSS<TValue>>
        {
            using input_view_type = TSSView<TSInputView>;
            using output_view_type = TSSView<TSOutputView>;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view.as_set(); }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view.as_set(); }
        };

        template <typename TValueSchema>
        struct schema_view_traits<REF<TValueSchema>>
        {
            using input_view_type = TSInputView;
            using output_view_type = TSOutputView;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view; }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view; }
        };

        template <>
        struct schema_view_traits<SIGNAL>
        {
            using input_view_type = SignalView<TSInputView>;
            using output_view_type = SignalView<TSOutputView>;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view.as_signal(); }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view.as_signal(); }
        };

        template <fixed_string Name, typename... TConstraints>
        struct schema_view_traits<TsVar<Name, TConstraints...>>
        {
            using input_view_type = TSInputView;
            using output_view_type = TSOutputView;

            [[nodiscard]] static input_view_type input_view(TSInputView view) { return view; }
            [[nodiscard]] static output_view_type output_view(TSOutputView view) { return view; }
        };

        inline void mark_ts_output_modified(const TSOutputView &view, engine_time_t evaluation_time)
        {
            LinkedTSContext context = view.linked_context();
            if (context.ts_state == nullptr) {
                throw std::logic_error("Output mutation requires a linked output state");
            }
            context.ts_state->mark_modified(evaluation_time);
        }
    }  // namespace detail

    template <fixed_string Name,
              typename TSchema,
              InputActivity Activity = InputActivity::Active,
              InputValidity Validity = InputValidity::Valid>
    class In
    {
      public:
        /**
         * Named input selector.
         *
         * The schema controls both the runtime TSInputView shape injected into
         * the implementation and the exported Python signature. Activity and
         * validity policies are reflected into builder metadata so the runtime
         * can activate and gate the appropriate top-level inputs.
         */
        using schema = TSchema;
        using view_type = typename detail::schema_view_traits<TSchema>::input_view_type;
        static constexpr auto name = Name;
        static constexpr auto activity = Activity;
        static constexpr auto validity = Validity;

        explicit In(TSInputView view) : m_view(detail::schema_view_traits<TSchema>::input_view(std::move(view))) {}

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] View delta_value() const noexcept { return m_view.delta_value(); }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }

        operator const view_type &() const noexcept { return m_view; }

      protected:
        view_type m_view;
    };

    template <fixed_string Name, typename TValue, InputActivity Activity, InputValidity Validity>
    class In<Name, TS<TValue>, Activity, Validity>
    {
      public:
        using schema = TS<TValue>;
        using value_type = TValue;
        using view_type = TSInputView;
        static constexpr auto name = Name;
        static constexpr auto activity = Activity;
        static constexpr auto validity = Validity;

        explicit In(TSInputView view) : m_view(std::move(view)) {}

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] const TValue &delta_value() const
        {
            return m_view.delta_value().as_atomic().template checked_as<TValue>();
        }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }
        [[nodiscard]] const TValue &value() const { return m_view.value().as_atomic().template checked_as<TValue>(); }

        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
    };

    template <fixed_string Name, typename TSchema, InputActivity Activity, InputValidity Validity>
    class In<Name, REF<TSchema>, Activity, Validity>
    {
      public:
        using schema = REF<TSchema>;
        using value_type = v2::TimeSeriesReference;
        using view_type = TSInputView;
        static constexpr auto name = Name;
        static constexpr auto activity = Activity;
        static constexpr auto validity = Validity;

        explicit In(TSInputView view) : m_view(std::move(view)) {}

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] const value_type &delta_value() const
        {
            return m_view.delta_value().as_atomic().template checked_as<value_type>();
        }
        [[nodiscard]] bool modified() const noexcept { return m_view.modified(); }
        [[nodiscard]] bool valid() const noexcept { return m_view.valid(); }
        [[nodiscard]] bool all_valid() const noexcept { return m_view.all_valid(); }
        [[nodiscard]] const value_type &value() const
        {
            return m_view.value().as_atomic().template checked_as<value_type>();
        }

        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
    };

    template <typename TSchema>
    class Out
    {
      public:
        /**
         * Named output selector.
         *
         * Outputs wrap a TSOutputView plus the current evaluation time so a
         * mutation can immediately mark the linked TS state modified.
         */
        using schema = TSchema;
        using view_type = typename detail::schema_view_traits<TSchema>::output_view_type;

        Out(TSOutputView view, engine_time_t evaluation_time)
            : m_view(detail::schema_view_traits<TSchema>::output_view(std::move(view))), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }

        operator const view_type &() const noexcept { return m_view; }

      protected:
        view_type m_view;
        engine_time_t m_evaluation_time{MIN_DT};
    };

    template <typename TValue>
    class Out<TS<TValue>>
    {
      public:
        using schema = TS<TValue>;
        using value_type = TValue;
        using view_type = TSOutputView;

        Out(TSOutputView view, engine_time_t evaluation_time)
            : m_view(std::move(view)), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }
        [[nodiscard]] const TValue *try_value() const { return m_view.value().as_atomic().template try_as<TValue>(); }

        template <typename T>
        void set(T &&value) const
        {
            m_view.value().set_scalar(std::forward<T>(value));
            detail::mark_ts_output_modified(m_view, m_evaluation_time);
        }

        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
        engine_time_t m_evaluation_time{MIN_DT};
    };

    template <typename TSchema>
    class Out<REF<TSchema>>
    {
      public:
        using schema = REF<TSchema>;
        using value_type = v2::TimeSeriesReference;
        using view_type = TSOutputView;

        Out(TSOutputView view, engine_time_t evaluation_time)
            : m_view(std::move(view)), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }
        [[nodiscard]] const value_type *try_value() const { return m_view.value().as_atomic().template try_as<value_type>(); }

        template <typename T>
        void set(T &&value) const
        {
            m_view.value().set_scalar(std::forward<T>(value));
            detail::mark_ts_output_modified(m_view, m_evaluation_time);
        }

        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
        engine_time_t m_evaluation_time{MIN_DT};
    };

    template <typename TSchema, fixed_string Name>
    class State
    {
      public:
        /** Typed node-local state stored inside the node's slab allocation. */
        using schema = TSchema;
        using view_type = View;
        static constexpr auto name = Name;

        explicit State(view_type view) : m_view(std::move(view)) {}

        [[nodiscard]] view_type &view() noexcept { return m_view; }
        [[nodiscard]] const view_type &view() const noexcept { return m_view; }

        operator view_type &() noexcept { return m_view; }
        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
    };

    template <typename TSchema, fixed_string Name>
    class RecordableState
    {
      public:
        /**
         * Recordable state backed by a local TSOutput.
         *
         * The wrapped output behaves like any other TS output view and must be
         * marked modified explicitly when child leaves are updated.
         */
        using schema = TSchema;
        using view_type = typename detail::schema_view_traits<TSchema>::output_view_type;
        static constexpr auto name = Name;

        RecordableState(TSOutputView view, engine_time_t evaluation_time)
            : m_view(detail::schema_view_traits<TSchema>::output_view(std::move(view))), m_evaluation_time(evaluation_time)
        {
        }

        [[nodiscard]] const view_type &view() const noexcept { return m_view; }
        [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }

        void mark_modified(const TSOutputView &view) const { detail::mark_ts_output_modified(view, m_evaluation_time); }

        operator const view_type &() const noexcept { return m_view; }

      private:
        view_type m_view;
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

        struct PythonTypeVarContext
        {
            nb::object typing{nb::module_::import_("typing")};
            std::unordered_map<std::string, nb::object> scalar_type_vars;
            std::unordered_map<std::string, nb::object> ts_type_vars;

            void ensure_unique_name(std::string_view name, bool time_series) const
            {
                const std::string key{name};
                if (time_series) {
                    if (scalar_type_vars.contains(key)) {
                        throw std::logic_error("C++ static schema reuses the same type variable name for scalar and time-series roles");
                    }
                } else if (ts_type_vars.contains(key)) {
                    throw std::logic_error("C++ static schema reuses the same type variable name for scalar and time-series roles");
                }
            }
        };

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

        template <typename TScalar>
        struct scalar_descriptor
        {
            [[nodiscard]] static constexpr bool is_concrete() { return true; }

            [[nodiscard]] static const value::TypeMeta *type_meta()
            {
                return value::scalar_type_meta<TScalar>();
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &)
            {
                return python_scalar_type<TScalar>();
            }
        };

        template <fixed_string Name, typename... TConstraints>
        struct scalar_descriptor<ScalarVar<Name, TConstraints...>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return false; }

            [[nodiscard]] static const value::TypeMeta *type_meta()
            {
                return nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                const std::string key{Name.sv()};
                context.ensure_unique_name(key, false);

                if (const auto it = context.scalar_type_vars.find(key); it != context.scalar_type_vars.end()) {
                    return it->second;
                }

                nb::object type_var = [&]() -> nb::object {
                    if constexpr (sizeof...(TConstraints) == 0) {
                        nb::dict kwargs;
                        kwargs["bound"] = nb::module_::import_("builtins").attr("object");
                        return py_call(context.typing.attr("TypeVar"), nb::make_tuple(key), kwargs);
                    } else if constexpr (sizeof...(TConstraints) == 1) {
                        nb::dict kwargs;
                        kwargs["bound"] = scalar_descriptor<std::tuple_element_t<0, std::tuple<TConstraints...>>>::py_type(context);
                        return py_call(
                            context.typing.attr("TypeVar"),
                            nb::make_tuple(key),
                            kwargs);
                    } else {
                        return py_call(context.typing.attr("TypeVar"), nb::make_tuple(key, scalar_descriptor<TConstraints>::py_type(context)...));
                    }
                }();

                auto [it, _] = context.scalar_type_vars.emplace(key, type_var);
                return it->second;
            }
        };

        template <typename TSchema>
        struct schema_descriptor
        {
            [[nodiscard]] static constexpr bool is_concrete() { return false; }

            static const TSMeta *ts_meta()
            {
                static_assert(schema_always_false_v<TSchema>, "Unsupported static time-series schema");
            }

            static nb::object py_type(PythonTypeVarContext &)
            {
                static_assert(schema_always_false_v<TSchema>, "Unsupported static time-series schema");
            }

            static nb::object py_type()
            {
                PythonTypeVarContext context;
                return py_type(context);
            }
        };

        template <typename TField>
        struct field_descriptor;

        template <typename TSchema>
        struct state_descriptor
        {
            [[nodiscard]] static constexpr bool is_concrete() { return scalar_descriptor<TSchema>::is_concrete(); }

            [[nodiscard]] static const value::TypeMeta *type_meta()
            {
                return scalar_descriptor<TSchema>::type_meta();
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("STATE"), scalar_descriptor<TSchema>::py_type(context));
            }
        };

        template <typename TSchema>
        struct recordable_state_schema_descriptor
        {
            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return schema_descriptor<TSchema>::py_type(context);
            }
        };

        template <typename... TFields>
        struct recordable_state_schema_descriptor<TSB<TFields...>>
        {
            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                nb::dict kwargs;
                ((kwargs[field_descriptor<TFields>::name().c_str()] = field_descriptor<TFields>::py_type(context)), ...);
                return py_call(hgraph_module().attr("ts_schema"), nb::tuple(), kwargs);
            }
        };

        template <typename TSchema>
        struct recordable_state_descriptor
        {
            [[nodiscard]] static constexpr bool is_concrete() { return schema_descriptor<TSchema>::is_concrete(); }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return schema_descriptor<TSchema>::ts_meta();
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("RECORDABLE_STATE"),
                                  recordable_state_schema_descriptor<TSchema>::py_type(context));
            }
        };

        template <typename TValue>
        struct schema_descriptor<TS<TValue>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return scalar_descriptor<TValue>::is_concrete(); }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                const value::TypeMeta *scalar_meta = scalar_descriptor<TValue>::type_meta();
                return scalar_meta != nullptr ? TSTypeRegistry::instance().ts(scalar_meta) : nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("TS"), scalar_descriptor<TValue>::py_type(context));
            }
        };

        template <typename TValue>
        struct schema_descriptor<TSS<TValue>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return scalar_descriptor<TValue>::is_concrete(); }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                const value::TypeMeta *scalar_meta = scalar_descriptor<TValue>::type_meta();
                return scalar_meta != nullptr ? TSTypeRegistry::instance().tss(scalar_meta) : nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("TSS"), scalar_descriptor<TValue>::py_type(context));
            }
        };

        template <typename TKey, typename TValueSchema>
        struct schema_descriptor<TSD<TKey, TValueSchema>>
        {
            [[nodiscard]] static constexpr bool is_concrete()
            {
                return scalar_descriptor<TKey>::is_concrete() && schema_descriptor<TValueSchema>::is_concrete();
            }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                const value::TypeMeta *key_meta = scalar_descriptor<TKey>::type_meta();
                const TSMeta *value_meta = schema_descriptor<TValueSchema>::ts_meta();
                return key_meta != nullptr && value_meta != nullptr ? TSTypeRegistry::instance().tsd(key_meta, value_meta) : nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("TSD"),
                                  nb::make_tuple(scalar_descriptor<TKey>::py_type(context),
                                                 schema_descriptor<TValueSchema>::py_type(context)));
            }
        };

        template <typename TElementSchema, size_t FixedSize>
        struct schema_descriptor<TSL<TElementSchema, FixedSize>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return schema_descriptor<TElementSchema>::is_concrete(); }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                const TSMeta *element_meta = schema_descriptor<TElementSchema>::ts_meta();
                return element_meta != nullptr ? TSTypeRegistry::instance().tsl(element_meta, FixedSize) : nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                nb::object size_type = FixedSize == 0
                                           ? hgraph_module().attr("Size")
                                           : py_getitem(hgraph_module().attr("Size"), nb::int_(FixedSize));
                return py_getitem(hgraph_module().attr("TSL"), nb::make_tuple(schema_descriptor<TElementSchema>::py_type(context), size_type));
            }
        };

        template <typename TSchema>
        struct schema_descriptor<REF<TSchema>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return schema_descriptor<TSchema>::is_concrete(); }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                const TSMeta *target_meta = schema_descriptor<TSchema>::ts_meta();
                return target_meta != nullptr ? TSTypeRegistry::instance().ref(target_meta) : nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return py_getitem(hgraph_module().attr("REF"), schema_descriptor<TSchema>::py_type(context));
            }
        };

        template <>
        struct schema_descriptor<SIGNAL>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return true; }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return TSTypeRegistry::instance().signal();
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &)
            {
                return hgraph_module().attr("SIGNAL");
            }
        };

        template <fixed_string Name, typename... TConstraints>
        struct schema_descriptor<TsVar<Name, TConstraints...>>
        {
            [[nodiscard]] static constexpr bool is_concrete() { return false; }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return nullptr;
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                const std::string key{Name.sv()};
                context.ensure_unique_name(key, true);

                if (const auto it = context.ts_type_vars.find(key); it != context.ts_type_vars.end()) {
                    return it->second;
                }

                nb::object type_var = [&]() -> nb::object {
                    if constexpr (sizeof...(TConstraints) == 0) {
                        nb::dict kwargs;
                        kwargs["bound"] = hgraph_module().attr("TimeSeries");
                        return py_call(context.typing.attr("TypeVar"), nb::make_tuple(key), kwargs);
                    } else if constexpr (sizeof...(TConstraints) == 1) {
                        nb::dict kwargs;
                        kwargs["bound"] =
                            schema_descriptor<std::tuple_element_t<0, std::tuple<TConstraints...>>>::py_type(context);
                        return py_call(
                            context.typing.attr("TypeVar"),
                            nb::make_tuple(key),
                            kwargs);
                    } else {
                        return py_call(context.typing.attr("TypeVar"), nb::make_tuple(key, schema_descriptor<TConstraints>::py_type(context)...));
                    }
                }();

                auto [it, _] = context.ts_type_vars.emplace(key, type_var);
                return it->second;
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

            [[nodiscard]] static constexpr bool is_concrete()
            {
                return schema_descriptor<TSchema>::is_concrete();
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                return schema_descriptor<TSchema>::py_type(context);
            }

            [[nodiscard]] static const TSMeta *ts_meta()
            {
                return schema_descriptor<TSchema>::ts_meta();
            }
        };

        template <typename... TFields>
        struct schema_descriptor<TSB<TFields...>>
        {
            [[nodiscard]] static constexpr bool is_concrete()
            {
                return (field_descriptor<TFields>::is_concrete() && ...);
            }

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
                if constexpr (is_concrete()) {
                    static const TSMeta *meta = [] {
                        std::vector<std::pair<std::string, const TSMeta *>> fields;
                        fields.reserve(sizeof...(TFields));
                        (fields.emplace_back(field_descriptor<TFields>::name(), field_descriptor<TFields>::ts_meta()), ...);
                        return TSTypeRegistry::instance().tsb(fields, bundle_name());
                    }();
                    return meta;
                } else {
                    return nullptr;
                }
            }

            [[nodiscard]] static nb::object py_type(PythonTypeVarContext &context)
            {
                nb::dict kwargs;
                ((kwargs[field_descriptor<TFields>::name().c_str()] = field_descriptor<TFields>::py_type(context)), ...);
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

        template <typename T>
        struct is_state_selector : std::false_type
        {};

        template <typename TSchema, fixed_string Name>
        struct is_state_selector<State<TSchema, Name>> : std::true_type
        {};

        template <typename T>
        struct is_recordable_state_selector : std::false_type
        {};

        template <typename TSchema, fixed_string Name>
        struct is_recordable_state_selector<RecordableState<TSchema, Name>> : std::true_type
        {};
    }  // namespace detail
}  // namespace hgraph::v2
