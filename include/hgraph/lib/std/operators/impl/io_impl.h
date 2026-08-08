#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H

#include <hgraph/lib/std/operators/io.h>        // debug_print / null_sink / record / replay / log_
#include <hgraph/runtime/logger.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <fmt/core.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the I/O / debug operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/io.h>``; this file provides the concrete sinks and
     * ``register_io_operators`` to register them.
     */

    /** The diagnostic-sink WRITER hook: defaults to C stdout/stderr; the
        python module re-points it at ``sys.stdout``/``sys.stderr`` so
        redirection (and pytest capture) behaves like hgraph's python
        prints. One line per call (no trailing newline in ``line``). */
    using IoWriteFn = void (*)(std::string_view line, bool to_stdout);
    [[nodiscard]] HGRAPH_EXPORT IoWriteFn &io_write_slot() noexcept;
    void io_write(std::string_view line, bool to_stdout);

    /**
     * ``debug_print`` implementation: a single generic sink that prints ``label: value`` on
     * each tick of ``ts`` (the value renders through the type-erased view ``to_string``).
     * ``sample=N`` prints every N-th tick with an ``[N]`` prefix (hgraph's
     * shape); ``print_delta`` is not yet modelled.
     */
    struct debug_print_impl
    {
        static void eval(Scalar<"label", Str> label, In<"ts", TsVar<"S">> ts, Scalar<"sample", Int> sample,
                         State<Int> ticks)
        {
            if (sample.value() > 1)
            {
                const Int seen = ticks.get() + 1;
                ticks.set(seen);
                if (seen % sample.value() != 0) { return; }
                io_write(fmt::format("[{}] {}: {}", sample.value(), label.value(), ts.value().to_string()), true);
                return;
            }
            io_write(fmt::format("{}: {}", label.value(), ts.value().to_string()), true);
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"sample", Value{Int{1}}}};
        }
    };

    /** Stop requests are observed by the executor after the graph completes
        the current evaluation cycle. */
    struct stop_engine_impl
    {
        static auto defaults() { return std::tuple{arg<"msg">(Str{"Stopping"})}; }

        static void eval(In<"ts", SIGNAL> ts, Scalar<"msg", Str> msg,
                         EngineControlView engine, LoggerView log)
        {
            static_cast<void>(ts);
            log.info("stop_engine: {}", msg.value());
            engine.request_stop();
        }
    };

    namespace io_impl_detail
    {
        /** python-style ``{}`` / ``{name}`` formatting over a packed
            structural bundle of arguments (positional entries are the
            leading unnamed fields in call order; kwargs carry names). */
        [[nodiscard]] std::string format_bundle(std::string_view format, const TSInputView &packed);
    }  // namespace io_impl_detail

    /** ``__print_sink``: the runtime half of ``print_`` — formats the packed
        argument bundle into ``fmt`` and writes one line to stdout/stderr. */
    struct print_sink_impl
    {
        static constexpr auto name = "print_sink";

        static void eval(In<"fmt", TS<Str>> format, In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"to_stdout", Bool> to_stdout)
        {
            io_write(io_impl_detail::format_bundle(format.value(), args.base()), to_stdout.value());
        }
    };

    /** ``__assert_sink``: the runtime half of the format-args ``assert_``. */
    struct assert_fmt_sink_impl
    {
        static constexpr auto name = "assert_fmt_sink";

        static void eval(In<"condition", TS<Bool>> condition, Scalar<"error_msg", Str> format,
                         In<"args", TsVar<"A">, InputValidity::Unchecked> args)
        {
            if (condition.value()) { return; }
            throw std::runtime_error(io_impl_detail::format_bundle(format.value(), args.base()));
        }
    };

    /** ``__log_sink``: formats the packed arguments and logs through the
        LOGGER injectable. Native levels use the spdlog 0..5 scale; Python's
        standard 10..50 levels are normalized onto the same scale. */
    struct log_sink_impl
    {
        static constexpr auto name = "log_sink";

        static void eval(In<"fmt", TS<Str>> format, In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"level", Int> level, Scalar<"sample_count", Int> sample_count,
                         State<Int> ticks, LoggerView log)
        {
            const Int seen = ticks.get() + 1;
            ticks.set(seen);
            if (sample_count.value() > 1 && seen % sample_count.value() != 0) { return; }

            const Int raw_level = level.value();
            const auto lvl = static_cast<int>(raw_level >= 10 && raw_level <= 50 && raw_level % 10 == 0
                                                  ? raw_level / 10
                                                  : raw_level);
            if (!log.should_log(lvl)) { return; }
            log.log(lvl, io_impl_detail::format_bundle(format.value(), args.base()));
        }
    };

    // The in-memory record/replay/compare backends live in their own
    // discoverable home (``impl/record_replay_memory_impl.h``), the sibling of
    // the frame backend (``impl/record_replay_frame_impl.h``).

    /** ``null_sink`` implementation: a single generic sink that consumes ``ts`` and does nothing. */
    struct null_sink_impl
    {
        static void eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };

    namespace io_impl_detail
    {
        /** Build callable arguments from a packed structural bundle. The
            explicit count keeps keyword names independent of internal field
            labels. */
        [[nodiscard]] inline std::optional<std::vector<ValueCallArg>> callable_args(
            const TSInputView &packed, std::size_t positional_count)
        {
            auto bundle = const_cast<TSInputView &>(packed).as_bundle();
            if (positional_count > bundle.size())
            {
                throw std::logic_error("runtime callable positional count exceeds its packed arguments");
            }
            std::vector<ValueCallArg> result;
            result.reserve(bundle.size());
            std::size_t index = 0;
            for (auto [name, child] : bundle.items())
            {
                if (index < positional_count)
                {
                    if (!child.valid()) { return std::nullopt; }
                    result.emplace_back(child.value());
                }
                else if (child.valid()) { result.emplace_back(name, child.value()); }
                ++index;
            }
            return result;
        }
    }  // namespace io_impl_detail

    /** Invoke a ticking value callable and publish its typed result. */
    struct apply_value_callable_impl
    {
        static constexpr auto name = "apply_value_callable";

        static void eval(In<"fn", TS<ValueCallable>> fn,
                         In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"positional_count", Int> positional_count,
                         Out<TsVar<"O">> out)
        {
            const auto call_args = io_impl_detail::callable_args(
                args.base(), static_cast<std::size_t>(positional_count.value()));
            if (!call_args) { return; }
            const auto &erased = static_cast<const TSOutputView &>(out);
            Value result = fn.value().invoke(std::span<const ValueCallArg>{*call_args},
                                             erased.schema()->value_schema);
            if (result.has_value()) { out.apply(result.view()); }
        }
    };

    /** Invoke a ticking value callable for its side effect. */
    struct call_value_callable_impl
    {
        static constexpr auto name = "call_value_callable";

        static void eval(In<"fn", TS<ValueCallable>> fn,
                         In<"args", TsVar<"A">, InputValidity::Unchecked> args,
                         Scalar<"positional_count", Int> positional_count)
        {
            const auto call_args = io_impl_detail::callable_args(
                args.base(), static_cast<std::size_t>(positional_count.value()));
            if (!call_args) { return; }
            static_cast<void>(fn.value().invoke(std::span<const ValueCallArg>{*call_args}));
        }
    };

    namespace io_impl_detail
    {
        /** Pack positional format args (unnamed leading fields, call order)
            and kwargs (named fields) into one structural bundle. */
        [[nodiscard]] WiringPortRef pack_format_args(std::vector<WiringPortRef> positional,
                                                     std::vector<std::pair<std::string, WiringPortRef>> named);
    }  // namespace io_impl_detail

    /** Signature-only graph used to reflect the public variadic ``apply``
        shape. Registration replaces its wire closure because the resolved
        output schema must be passed to the packed runtime node. */
    struct apply_value_callable_signature
    {
        static constexpr auto name = "apply_value_callable_signature";

        static Port<void> compose(Wiring &, NamedPort<"fn", TS<ValueCallable>>,
                                  VarIn<"args", TsVar<"S">>, VarKwIn<"kwargs">)
        {
            throw std::logic_error("apply signature graph is not executable");
        }
    };

    /** ``call`` needs no output resolution and can use the ordinary graph
        compose path after packing its positional and keyword inputs. */
    struct call_value_callable_compose
    {
        static constexpr auto name = "call_value_callable_compose";

        static void compose(Wiring &w, NamedPort<"fn", TS<ValueCallable>> fn,
                            VarIn<"args", TsVar<"S">> positional,
                            VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            wire<call_value_callable_op>(w, fn, Port<void>{w, std::move(packed)},
                                         static_cast<Int>(positional.size()));
        }
    };

    /** ``print_(fmt, *args, **kwargs)`` — python-style formatting to
        stdout (``__std_out__=False`` writes stderr). */
    struct print_compose
    {
        static constexpr auto name = "print_compose";

        static auto compose(Wiring &w, NamedPort<"fmt", TS<Str>> fmt, VarIn<"args", TsVar<"B">> positional,
                            Scalar<"__std_out__", Bool> to_stdout, VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            return wire<print_sink_op>(w, fmt, Port<void>{w, std::move(packed)}, to_stdout.value());
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"__std_out__", Value{true}}};
        }
    };

    /** ``log_(fmt, *args, level=..., sample_count=..., **kwargs)`` — pack the
        format inputs once at wiring time and delegate runtime work to the
        native logging sink. */
    struct log_compose
    {
        static constexpr auto name = "log_compose";

        static auto compose(Wiring &w, NamedPort<"fmt", TS<Str>> fmt,
                            VarIn<"args", TsVar<"B">> positional,
                            Scalar<"level", Int> level,
                            Scalar<"sample_count", Int> sample_count,
                            VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            return wire<log_sink_op>(w, fmt, Port<void>{w, std::move(packed)},
                                     level.value(), sample_count.value());
        }

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"level", Value{Int{2}}}, {"sample_count", Value{Int{1}}}};
        }
    };

    /** ``assert_(condition, error_msg)`` — throw ``error_msg`` when the
        condition ticks false. */
    struct assert_plain_impl
    {
        static constexpr auto name = "assert_plain";

        static void eval(In<"condition", TS<Bool>> condition, Scalar<"error_msg", Str> message)
        {
            if (!condition.value()) { throw std::runtime_error(std::string{message.value()}); }
        }
    };

    /** ``assert_(condition, fmt, *args, **kwargs)`` — the format-args form. */
    struct assert_fmt_compose
    {
        static constexpr auto name = "assert_fmt";

        static auto compose(Wiring &w, NamedPort<"condition", TS<Bool>> condition,
                            Scalar<"error_msg", Str> message, VarIn<"args", TsVar<"B">> positional,
                            VarKwIn<"kwargs"> kwargs)
        {
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::vector<WiringPortRef>{positional.begin(), positional.end()},
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});
            return wire<assert_fmt_op>(w, condition, message.value(), Port<void>{w, std::move(packed)});
        }
    };

    /** Register the I/O / debug operator overloads. */
    void register_io_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_IO_IMPL_H
