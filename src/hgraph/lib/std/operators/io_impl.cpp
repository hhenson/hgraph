#include <hgraph/lib/std/operators/impl/io_impl.h>

namespace hgraph::stdlib
{
    void register_io_operators()
    {
        register_overload<debug_print, debug_print_impl>();
        register_overload<null_sink, null_sink_impl>();
        register_overload<stop_engine, stop_engine_impl>();
        register_overload<apply_value_callable_op, apply_value_callable_impl>();
        register_overload<call_value_callable_op, call_value_callable_impl>();

        OperatorImpl apply = make_operator_graph_impl<apply_value_callable_signature>(
            std::string{apply_op::name});
        const TypePattern output_pattern = apply.output;
        apply.wire = [output_pattern](
                         Wiring &w, const ResolutionMap &resolution,
                         std::span<const WiringArg> args,
                         std::span<const std::pair<std::string, WiringPortRef>> kwargs)
            -> OperatorWireResult {
            if (args.empty()) { throw std::logic_error("apply resolved without a callable input"); }

            auto as_port = [&](const WiringArg &arg) {
                if (arg.kind == WiringArg::Kind::TimeSeries) { return arg.port; }
                const auto *schema = TypeRegistry::instance().ts(arg.scalar_value.schema());
                return operator_dispatch_detail::wire_scalar_const(w, arg, schema);
            };

            WiringPortRef fn = as_port(args.front());
            std::vector<WiringPortRef> positional;
            positional.reserve(args.size() - 1);
            for (std::size_t index = 1; index < args.size(); ++index)
            {
                positional.push_back(as_port(args[index]));
            }
            const auto positional_count = static_cast<Int>(positional.size());
            WiringPortRef packed = io_impl_detail::pack_format_args(
                std::move(positional),
                std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()});

            const auto *output = ts_pattern_resolve(output_pattern, resolution);
            if (output == nullptr) { throw std::logic_error("apply output type is unresolved"); }
            const WiringArg inner[]{
                WiringArg{.kind = WiringArg::Kind::TimeSeries, .port = std::move(fn)},
                WiringArg{.kind = WiringArg::Kind::TimeSeries, .port = std::move(packed)},
                WiringArg{.kind = WiringArg::Kind::Scalar,
                          .scalar_value = Value{positional_count},
                          .scalar_meta = scalar_descriptor<Int>::value_meta()},
            };
            return wire_operator(w, apply_value_callable_op::name,
                                 std::span<const WiringArg>{inner}, true, output);
        };
        OperatorRegistry::instance().register_overload(std::move(apply));
        register_graph_overload<call_op, call_value_callable_compose>();
        // The in-memory record/replay/compare overloads (dense harness + sparse
        // absolute-time backends) are registered by
        // register_record_replay_memory_operators (impl/record_replay_memory_impl.h).
        register_overload<log_sink_op, log_sink_impl>();
        register_graph_overload<log_, log_compose>();
        register_overload<print_sink_op, print_sink_impl>();
        register_graph_overload<print_, print_compose>();
        register_overload<assert_, assert_plain_impl>();
        register_overload<assert_fmt_op, assert_fmt_sink_impl>();
        register_graph_overload<assert_, assert_fmt_compose>();
    }

    IoWriteFn &io_write_slot() noexcept
    {
        static IoWriteFn writer = [](std::string_view line, bool to_stdout) {
            if (to_stdout) { fmt::print("{}\n", line); }
            else { fmt::print(stderr, "{}\n", line); }
        };
        return writer;
    }

    void io_write(std::string_view line, bool to_stdout) { io_write_slot()(line, to_stdout); }

    namespace io_impl_detail
    {
        std::string format_bundle(std::string_view format, const TSInputView &packed)
        {
            auto bundle = const_cast<TSInputView &>(packed).as_bundle();
            std::string result;
            result.reserve(format.size());
            std::size_t positional = 0;
            for (std::size_t i = 0; i < format.size(); ++i)
            {
                if (format[i] != '{')
                {
                    result.push_back(format[i]);
                    continue;
                }
                const auto close = format.find('}', i);
                if (close == std::string_view::npos)
                {
                    result.append(format.substr(i));
                    break;
                }
                const std::string_view name = format.substr(i + 1, close - i - 1);
                auto child = name.empty() ? bundle.at(positional++) : bundle.at(name);
                // A Str renders WITHOUT quoting (python print semantics).
                if (child.valid())
                {
                    const ValueView value = child.value();
                    if (const auto *text = value.try_as<Str>(); text != nullptr) { result.append(*text); }
                    else { result.append(value.to_string()); }
                }
                else { result.append("<n/a>"); }
                i = close;
            }
            return result;
        }

        WiringPortRef pack_format_args(std::vector<WiringPortRef> positional,
                                       std::vector<std::pair<std::string, WiringPortRef>> named)
        {
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            std::vector<WiringPortRef> children;
            fields.reserve(positional.size() + named.size());
            children.reserve(positional.size() + named.size());
            std::size_t index = 0;
            for (WiringPortRef &port : positional)
            {
                fields.emplace_back(fmt::format("${}", index++), port.schema);
                children.push_back(std::move(port));
            }
            for (auto &[name, port] : named)
            {
                fields.emplace_back(name, port.schema);
                children.push_back(std::move(port));
            }
            const auto *schema = TypeRegistry::instance().un_named_tsb(fields);
            return WiringPortRef::structural_source(schema, std::move(children));
        }
    }  // namespace io_impl_detail
}  // namespace hgraph::stdlib
