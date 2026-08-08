#ifndef HGRAPH_TYPES_VALUE_CALLABLE_H
#define HGRAPH_TYPES_VALUE_CALLABLE_H

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/wired_fn.h>

#include <array>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace hgraph
{
    /** One argument to a runtime value callable. An empty name denotes a
        positional argument; named entries preserve Python-style kwargs. */
    struct ValueCallArg
    {
        std::string_view name{};
        ValueTypeRef     binding{};
        const void      *data{nullptr};

        ValueCallArg() = default;

        explicit ValueCallArg(const ValueView &value)
            : binding(value.binding()), data(value.data())
        {
        }

        ValueCallArg(std::string_view argument_name, const ValueView &value)
            : name(argument_name), binding(value.binding()), data(value.data())
        {
        }

        [[nodiscard]] ValueView view() const { return ValueView{binding, data}; }
    };

    /** Backend operations for a type-erased runtime value callable. */
    struct ValueCallableOps
    {
        Value (*invoke)(const void *context, std::span<const ValueCallArg> args,
                        const ValueTypeMetaData *output_schema){nullptr};
        const ValueTypeMetaData *(*input_schema)(const void *context, std::size_t index){nullptr};
        const ValueTypeMetaData *(*output_schema)(const void *context){nullptr};
    };

    /**
     * A callable scalar evaluated against runtime values.
     *
     * This is deliberately separate from ``WiredFn``: ``WiredFn`` creates
     * graph structure during wiring, while ``ValueCallable`` invokes code
     * when a node evaluates. Native lifted kernels use their existing static
     * operation table; bridge backends retain their context through
     * ``owner``. The value therefore remains C++-owned and usable without the
     * Python bridge.
     */
    struct ValueCallable
    {
        const ValueCallableOps       *ops{nullptr};
        std::shared_ptr<const void>    owner{};
        const void                   *context{nullptr};
        const void                   *identity{nullptr};
        const LiftedKernel           *lifted{nullptr};
        std::size_t                   arity{0};
        bool                          variadic{false};

        [[nodiscard]] bool valid() const noexcept
        {
            return lifted != nullptr || (ops != nullptr && ops->invoke != nullptr);
        }

        [[nodiscard]] const ValueTypeMetaData *input_schema(std::size_t index) const
        {
            if (lifted != nullptr)
            {
                const auto *ts = lifted->input_schema(index);
                return ts != nullptr ? ts->value_schema : nullptr;
            }
            return ops != nullptr && ops->input_schema != nullptr
                       ? ops->input_schema(context, index)
                       : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *output_schema() const
        {
            if (lifted != nullptr)
            {
                const auto *ts = lifted->output_schema();
                return ts != nullptr ? ts->value_schema : nullptr;
            }
            return ops != nullptr && ops->output_schema != nullptr
                       ? ops->output_schema(context)
                       : nullptr;
        }

        [[nodiscard]] Value invoke(std::span<const ValueCallArg> args,
                                   const ValueTypeMetaData *requested_output = nullptr) const
        {
            if (!valid()) { throw std::logic_error("ValueCallable::invoke called on an empty callable"); }
            if (!variadic && args.size() != arity)
            {
                throw std::invalid_argument("runtime callable argument count does not match its arity");
            }
            if (lifted != nullptr)
            {
                constexpr std::size_t inline_arity = 8;
                std::array<ValueView, inline_arity> inline_positional{};
                std::vector<ValueView> overflow_positional;
                ValueView *positional = inline_positional.data();
                if (args.size() > inline_arity)
                {
                    overflow_positional.resize(args.size());
                    positional = overflow_positional.data();
                }
                for (std::size_t index = 0; index < args.size(); ++index)
                {
                    if (!args[index].name.empty())
                    {
                        throw std::invalid_argument("native lifted value callables do not accept keyword arguments");
                    }
                    positional[index] = args[index].view();
                }
                const auto *declared_output = output_schema();
                if (requested_output != nullptr && declared_output != requested_output)
                {
                    throw std::invalid_argument("runtime callable output schema does not match the requested output");
                }
                return lifted->eval(std::span<const ValueView>{positional, args.size()});
            }
            return ops->invoke(context, args, requested_output);
        }

        [[nodiscard]] bool operator==(const ValueCallable &other) const noexcept
        {
            return identity == other.identity && ops == other.ops && lifted == other.lifted;
        }
    };

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    template <>
    struct python_conversion_traits<ValueCallable>
    {
        using ToPythonHook   = nanobind::object (*)(const ValueCallable &);
        using FromPythonHook = ValueCallable (*)(nanobind::handle);

        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook &to_python_hook() noexcept;
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;
        HGRAPH_EXPORT static nanobind::object to_python(const ValueCallable &value);
        HGRAPH_EXPORT static ValueCallable from_python(nanobind::handle source);
    };
#endif

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<ValueCallable>
        {
            static constexpr std::string_view value{"callable"};
        };
    }  // namespace static_schema_detail
}  // namespace hgraph

template <>
struct std::hash<hgraph::ValueCallable>
{
    [[nodiscard]] std::size_t operator()(const hgraph::ValueCallable &fn) const noexcept
    {
        const std::size_t identity = std::hash<const void *>{}(fn.identity);
        return identity ^ (std::hash<const void *>{}(fn.ops) << 1U) ^
               (std::hash<const void *>{}(fn.lifted) << 2U);
    }
};

#endif  // HGRAPH_TYPES_VALUE_CALLABLE_H
