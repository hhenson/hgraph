#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/v2/static_schema.h>
#include <hgraph/types/value/value.h>

#include <sstream>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hgraph::v2
{
    struct Graph;
    [[nodiscard]] TSInputView input_view_for(Node &node, engine_time_t evaluation_time);
    [[nodiscard]] TSOutputView output_view_for(Node &node, engine_time_t evaluation_time);

    namespace detail
    {
        /**
         * Runtime payload for the default static node family.
         *
         * This payload is embedded in the node's slab chunk immediately after
         * the BuiltNodeSpec and is interpreted through NodeRuntimeOps.
         */
        struct StaticNodeRuntimeData
        {
            TSInput *input{nullptr};
            TSOutput *output{nullptr};
            const ValueBuilder *state_builder{nullptr};
            void *state_memory{nullptr};
            TSOutput *recordable_state{nullptr};
        };

        template <typename T>
        [[nodiscard]] inline T &runtime_data(Node &node);

        template <typename T>
        [[nodiscard]] inline const T &runtime_data(const Node &node);

        template <typename T>
        inline constexpr bool always_false_v = false;

        template <typename T>
        struct fn_traits;

        template <typename R, typename... Args>
        struct fn_traits<R (*)(Args...)>
        {
            using return_type = R;
            using args_tuple = std::tuple<Args...>;
        };

        template <typename R, typename... Args>
        struct fn_traits<R (*)(Args...) noexcept>
        {
            using return_type = R;
            using args_tuple = std::tuple<Args...>;
        };

        template <typename T>
        struct is_static_hook_function : std::false_type
        {};

        template <typename R, typename... Args>
        struct is_static_hook_function<R (*)(Args...)> : std::true_type
        {};

        template <typename R, typename... Args>
        struct is_static_hook_function<R (*)(Args...) noexcept> : std::true_type
        {};

        template <typename T>
        concept StaticHook = is_static_hook_function<std::remove_cv_t<T>>::value;

        template <typename T>
        concept HasStart = requires { &T::start; } && StaticHook<decltype(&T::start)>;

        template <typename T>
        concept HasStop = requires { &T::stop; } && StaticHook<decltype(&T::stop)>;

        template <typename T>
        concept HasEval = requires { &T::eval; } && StaticHook<decltype(&T::eval)>;

        template <typename T>
        concept HasApplyMessage = requires { &T::apply_message; } && StaticHook<decltype(&T::apply_message)>;

        template <typename T>
        struct arg_provider
        {
            static decltype(auto) get(Node &, engine_time_t)
            {
                static_assert(always_false_v<T>, "Unsupported injected node implementation parameter");
            }
        };

        template <>
        struct arg_provider<Node>
        {
            static Node &get(Node &node, engine_time_t) noexcept { return node; }
        };

        template <>
        struct arg_provider<Node *>
        {
            static Node *get(Node &node, engine_time_t) noexcept { return &node; }
        };

        template <>
        struct arg_provider<Graph>
        {
            static Graph &get(Node &node, engine_time_t);
        };

        template <>
        struct arg_provider<Graph *>
        {
            static Graph *get(Node &node, engine_time_t);
        };

        template <>
        struct arg_provider<engine_time_t>
        {
            static engine_time_t get(Node &, engine_time_t evaluation_time) noexcept { return evaluation_time; }
        };

        template <>
        struct arg_provider<EvaluationClock>
        {
            static EvaluationClock get(Node &node, engine_time_t);
        };

        template <>
        struct arg_provider<TSInputView>
        {
            static TSInputView get(Node &node, engine_time_t evaluation_time);
        };

        template <>
        struct arg_provider<TSOutputView>
        {
            static TSOutputView get(Node &node, engine_time_t evaluation_time);
        };

        template <fixed_string Name, typename TSchema, InputActivity Activity, InputValidity Validity>
        struct arg_provider<In<Name, TSchema, Activity, Validity>>
        {
            static In<Name, TSchema, Activity, Validity> get(Node &node, engine_time_t evaluation_time)
            {
                return In<Name, TSchema, Activity, Validity>{
                    input_view_for(node, evaluation_time).as_bundle().field(Name.sv())};
            }
        };

        template <typename TSchema>
        struct arg_provider<Out<TSchema>>
        {
            static Out<TSchema> get(Node &node, engine_time_t evaluation_time)
            {
                return Out<TSchema>{output_view_for(node, evaluation_time), evaluation_time};
            }
        };

        template <typename TSchema, fixed_string Name>
        struct arg_provider<State<TSchema, Name>>
        {
            static State<TSchema, Name> get(Node &node, engine_time_t)
            {
                auto &runtime = runtime_data<StaticNodeRuntimeData>(node);
                if (runtime.state_builder == nullptr || runtime.state_memory == nullptr) {
                    throw std::logic_error("State<...> requested but no typed local state was constructed");
                }
                return State<TSchema, Name>{
                    View{&runtime.state_builder->dispatch(), runtime.state_memory, &runtime.state_builder->schema()}};
            }
        };

        template <typename TSchema, fixed_string Name>
        struct arg_provider<RecordableState<TSchema, Name>>
        {
            static RecordableState<TSchema, Name> get(Node &node, engine_time_t evaluation_time)
            {
                auto &runtime = runtime_data<StaticNodeRuntimeData>(node);
                if (runtime.recordable_state == nullptr) {
                    throw std::logic_error("RecordableState<...> requested but no recordable state output was constructed");
                }
                return RecordableState<TSchema, Name>{runtime.recordable_state->view(evaluation_time), evaluation_time};
            }
        };

        template <auto Fn, size_t... I>
        void invoke_impl(Node &node, engine_time_t evaluation_time, std::index_sequence<I...>)
        {
            using traits = fn_traits<decltype(Fn)>;
            using args_tuple = typename traits::args_tuple;

            // Injection is by parameter type, not position. The static hook
            // only asks for the values it needs and the runtime synthesizes
            // them from the current Node and evaluation time.
            Fn(arg_provider<std::remove_cvref_t<std::tuple_element_t<I, args_tuple>>>::get(node, evaluation_time)...);
        }

        template <auto Fn>
        void invoke(Node &node, engine_time_t evaluation_time)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::same_as<typename traits::return_type, void>, "Node implementation hooks must return void");
            invoke_impl<Fn>(node, evaluation_time, std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        template <typename T>
        struct push_message_arg_provider
        {
            static decltype(auto) get(Node &node, engine_time_t evaluation_time, const value::Value &message)
            {
                using value_type = std::remove_cvref_t<T>;

                if constexpr (std::is_same_v<value_type, value::Value> || std::is_same_v<value_type, Value>) {
                    return value::Value::copy(message);
                } else if constexpr (std::is_same_v<value_type, value::View> || std::is_same_v<value_type, View>) {
                    return message.view();
                } else if constexpr (std::is_same_v<value_type, value::AtomicView> || std::is_same_v<value_type, AtomicView>) {
                    return message.atomic_view();
                } else if constexpr (std::is_same_v<value_type, value::TupleView> || std::is_same_v<value_type, TupleView>) {
                    return message.tuple_view();
                } else if constexpr (std::is_same_v<value_type, value::BundleView> || std::is_same_v<value_type, BundleView>) {
                    return message.bundle_view();
                } else if constexpr (std::is_same_v<value_type, value::ListView> || std::is_same_v<value_type, ListView>) {
                    return message.list_view();
                } else if constexpr (std::is_same_v<value_type, value::SetView> || std::is_same_v<value_type, SetView>) {
                    return message.set_view();
                } else if constexpr (std::is_same_v<value_type, value::MapView> || std::is_same_v<value_type, MapView>) {
                    return message.map_view();
                } else if constexpr (std::is_same_v<value_type, value::CyclicBufferView> || std::is_same_v<value_type, CyclicBufferView>) {
                    return message.cyclic_buffer_view();
                } else if constexpr (std::is_same_v<value_type, value::QueueView> || std::is_same_v<value_type, QueueView>) {
                    return message.queue_view();
                } else if constexpr (is_input_selector<value_type>::value || is_output_selector<value_type>::value ||
                                     is_state_selector<value_type>::value || is_recordable_state_selector<value_type>::value ||
                                     std::is_same_v<value_type, Node> || std::is_same_v<value_type, Node *> ||
                                     std::is_same_v<value_type, Graph> || std::is_same_v<value_type, Graph *> ||
                                     std::is_same_v<value_type, engine_time_t> || std::is_same_v<value_type, EvaluationClock> ||
                                     std::is_same_v<value_type, TSInputView> || std::is_same_v<value_type, TSOutputView>) {
                    return arg_provider<value_type>::get(node, evaluation_time);
                } else {
                    return message.atomic_view().template checked_as<value_type>();
                }
            }
        };

        template <auto Fn, size_t... I>
        [[nodiscard]] bool invoke_push_message_impl(Node &node,
                                                    engine_time_t evaluation_time,
                                                    const value::Value &message,
                                                    std::index_sequence<I...>)
        {
            using traits = fn_traits<decltype(Fn)>;
            using args_tuple = typename traits::args_tuple;

            return static_cast<bool>(
                Fn(push_message_arg_provider<std::tuple_element_t<I, args_tuple>>::get(node, evaluation_time, message)...));
        }

        template <auto Fn>
        [[nodiscard]] bool invoke_push_message(Node &node, engine_time_t evaluation_time, const value::Value &message)
        {
            using traits = fn_traits<decltype(Fn)>;
            static_assert(std::same_as<typename traits::return_type, bool>,
                          "Static node apply_message hooks must return bool");
            return invoke_push_message_impl<Fn>(
                node, evaluation_time, message, std::make_index_sequence<std::tuple_size_v<typename traits::args_tuple>>{});
        }

        [[nodiscard]] inline TSInputView invalid_input_view(engine_time_t evaluation_time)
        {
            return TSInputView{TSViewContext::none(), TSViewContext::none(), evaluation_time};
        }

        [[nodiscard]] inline TSOutputView invalid_output_view(engine_time_t evaluation_time)
        {
            return TSOutputView{TSViewContext::none(), TSViewContext::none(), evaluation_time};
        }

        template <typename T>
        [[nodiscard]] inline T &runtime_data(Node &node)
        {
            return *static_cast<T *>(node.data());
        }

        template <typename T>
        [[nodiscard]] inline const T &runtime_data(const Node &node)
        {
            return *static_cast<const T *>(node.data());
        }

        [[nodiscard]] inline bool default_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && runtime_data<StaticNodeRuntimeData>(node).input != nullptr;
        }

        [[nodiscard]] inline bool default_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && runtime_data<StaticNodeRuntimeData>(node).output != nullptr;
        }

        [[nodiscard]] inline TSInputView default_input_view(Node &node, engine_time_t evaluation_time)
        {
            if (!default_has_input(node)) { return invalid_input_view(evaluation_time); }
            return runtime_data<StaticNodeRuntimeData>(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] inline TSOutputView default_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!default_has_output(node)) { return invalid_output_view(evaluation_time); }
            return runtime_data<StaticNodeRuntimeData>(node).output->view(evaluation_time);
        }

        [[nodiscard]] inline std::string default_runtime_label(const Node &node)
        {
            std::ostringstream stream;
            const std::string_view label = node.label();
            if (!label.empty()) { stream << label << '@'; }
            stream << static_cast<const void *>(&node);
            return stream.str();
        }

        template <typename TImplementation>
        struct runtime_ops_for
        {
            static void start(Node &node, engine_time_t evaluation_time)
            {
                static_cast<void>(node);
                static_cast<void>(evaluation_time);
                if constexpr (HasStart<TImplementation>) { invoke<&TImplementation::start>(node, evaluation_time); }
            }

            static void stop(Node &node, engine_time_t evaluation_time)
            {
                static_cast<void>(node);
                static_cast<void>(evaluation_time);
                if constexpr (HasStop<TImplementation>) { invoke<&TImplementation::stop>(node, evaluation_time); }
            }

            static void eval(Node &node, engine_time_t evaluation_time)
            {
                static_cast<void>(node);
                static_cast<void>(evaluation_time);
                if constexpr (HasEval<TImplementation>) { invoke<&TImplementation::eval>(node, evaluation_time); }
            }

            static bool apply_push_message(Node &node, const value::Value &message, engine_time_t evaluation_time)
            {
                if constexpr (HasApplyMessage<TImplementation>) {
                    return invoke_push_message<&TImplementation::apply_message>(node, evaluation_time, message);
                } else {
                    throw std::logic_error("v2 push-source nodes require a static bool apply_message(...) hook");
                }
            }

            static constexpr PushSourceNodeRuntimeOps push_source_value{
                &apply_push_message,
            };

            static constexpr NodeRuntimeOps value{
                &start,
                &stop,
                &eval,
                &default_has_input,
                &default_has_output,
                &default_input_view,
                &default_output_view,
                &default_runtime_label,
            };
        };
    }  // namespace detail
}  // namespace hgraph::v2
