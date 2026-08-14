#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H

#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/string.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <algorithm>
#include <cstddef>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace string_impl_detail
    {
        [[nodiscard]] inline std::size_t normalize_slice_index(Int index, std::size_t size)
        {
            const Int signed_size = static_cast<Int>(size);
            Int       normalized  = index < 0 ? signed_size + index : index;
            normalized            = std::clamp(normalized, Int{0}, signed_size);
            return static_cast<std::size_t>(normalized);
        }

        [[nodiscard]] inline std::vector<Str> split_parts(const Str &value, const Str &separator, std::size_t max_parts)
        {
            if (separator.empty()) { throw std::invalid_argument("split: separator must not be empty"); }

            std::vector<Str> parts;
            std::size_t      start = 0;
            if (max_parts != 0) { parts.reserve(max_parts); }

            while ((max_parts == 0 || parts.size() + 1 < max_parts))
            {
                const std::size_t pos = value.find(separator, start);
                if (pos == Str::npos) { break; }
                parts.push_back(value.substr(start, pos - start));
                start = pos + separator.size();
            }
            parts.push_back(value.substr(start));
            return parts;
        }

        [[nodiscard]] inline const std::string *find_named_format_value(
            std::string_view name,
            const std::vector<std::pair<std::string, std::string>> &named_values)
        {
            for (const auto &[key, value] : named_values)
            {
                if (key == name) { return &value; }
            }
            return nullptr;
        }

        [[nodiscard]] inline std::string placeholder_name(std::string_view placeholder)
        {
            const std::size_t end = placeholder.find_first_of(":!");
            return std::string{placeholder.substr(0, end)};
        }

        [[nodiscard]] inline Str render_format_string(
            std::string_view fmt,
            const std::vector<std::string> &positional_values,
            const std::vector<std::pair<std::string, std::string>> &named_values)
        {
            Str         rendered;
            std::size_t next_positional = 0;
            for (std::size_t i = 0; i < fmt.size();)
            {
                const char c = fmt[i];
                if (c == '{')
                {
                    if (i + 1 < fmt.size() && fmt[i + 1] == '{')
                    {
                        rendered.push_back('{');
                        i += 2;
                        continue;
                    }

                    const std::size_t close = fmt.find('}', i + 1);
                    if (close == std::string_view::npos)
                    {
                        throw std::invalid_argument("format_: unmatched '{' in format string");
                    }

                    const std::string name = placeholder_name(fmt.substr(i + 1, close - i - 1));
                    if (name.empty())
                    {
                        if (next_positional >= positional_values.size())
                        {
                            throw std::invalid_argument("format_: not enough positional arguments");
                        }
                        rendered += positional_values[next_positional++];
                    }
                    else
                    {
                        const std::string *value = find_named_format_value(name, named_values);
                        if (value == nullptr)
                        {
                            throw std::invalid_argument("format_: missing keyword argument '" + name + "'");
                        }
                        rendered += *value;
                    }
                    i = close + 1;
                    continue;
                }

                if (c == '}')
                {
                    if (i + 1 < fmt.size() && fmt[i + 1] == '}')
                    {
                        rendered.push_back('}');
                        i += 2;
                        continue;
                    }
                    throw std::invalid_argument("format_: unmatched '}' in format string");
                }

                rendered.push_back(c);
                ++i;
            }
            return rendered;
        }

        struct FormatValues
        {
            std::vector<std::string>                         positional;
            std::vector<std::pair<std::string, std::string>> named;
        };

        [[nodiscard]] inline FormatValues collect_format_values(TSBInputView &bundle, std::size_t positional_count,
                                                                bool strict)
        {
            FormatValues values;
            values.positional.reserve(std::min(positional_count, bundle.size()));
            values.named.reserve(bundle.size() > positional_count ? bundle.size() - positional_count : 0);

            std::size_t index = 0;
            for (auto [name, child] : bundle.items())
            {
                if (!strict && !child.valid())
                {
                    if (index < positional_count) { values.positional.emplace_back("None"); }
                    else { values.named.emplace_back(std::string{name}, "None"); }
                    ++index;
                    continue;
                }

                if (index < positional_count) { values.positional.push_back(child.value().format_string()); }
                else { values.named.emplace_back(std::string{name}, child.value().format_string()); }
                ++index;
            }
            return values;
        }

        [[nodiscard]] inline WiringNamedStructuralSourceArg format_bundle_arg(
            const std::vector<std::pair<std::string, WiringPortRef>> &fields)
        {
            std::vector<WiringNamedPortRef> refs;
            refs.reserve(fields.size());
            for (const auto &[name, port] : fields)
            {
                refs.emplace_back(name, port);
            }
            return WiringNamedStructuralSourceArg{std::move(refs)};
        }
    }  // namespace string_impl_detail

    struct replace_impl
    {
        static void eval(In<"pattern", TS<Str>> pattern, In<"repl", TS<Str>> repl, In<"s", TS<Str>> s,
                         Out<TS<Str>> out)
        {
            out.set(std::regex_replace(s.value(), std::regex{pattern.value()}, repl.value()));
        }
    };

    struct match_impl
    {
        static constexpr auto name = "match_";

        /** hgraph shape: TSB{is_match: TS[bool], groups: TS[tuple[str, ...]]}
            - groups is a TUPLE SCALAR, not a TSL. */
        [[nodiscard]] static const TSValueTypeMetaData *result_schema()
        {
            auto &registry = TypeRegistry::instance();
            const auto *groups_meta = registry.list(scalar_descriptor<Str>::value_meta(), 0, true);
            return registry.un_named_tsb({
                {"is_match", registry.ts(scalar_descriptor<Bool>::value_meta())},
                {"groups", registry.ts(groups_meta)},
            });
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            resolution.bind_ts("O", result_schema());
        }

        static void eval(In<"pattern", TS<Str>> pattern, In<"s", TS<Str>> s, Out<TsVar<"O">> out)
        {
            const Str   value = s.value();
            std::smatch match;
            const bool  is_match = std::regex_search(value, match, std::regex{pattern.value()});

            const auto &erased = static_cast<const TSOutputView &>(out);
            auto        bundle = erased.as_bundle();
            {
                auto flag = bundle.at(0);
                auto mutation = flag.begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(Value{Bool{is_match}}));
            }
            if (!is_match) { return; }

            const auto *groups_meta = bundle.at(1).schema()->value_schema;
            const auto element_binding = ValuePlanFactory::instance().type_for(groups_meta->element_type);
            ListBuilder builder{element_binding};
            for (std::size_t i = 1; i < match.size(); ++i)
            {
                const Str group = match[i].str();
                builder.push_back(group);
            }
            auto groups = bundle.at(1);
            auto mutation = groups.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(builder.build()));
        }
    };

    struct substr_impl
    {
        static void eval(In<"s", TS<Str>> s, In<"start", TS<Int>> start, In<"end", TS<Int>> end, Out<TS<Str>> out)
        {
            const Str        value       = s.value();
            const std::size_t begin      = string_impl_detail::normalize_slice_index(start.value(), value.size());
            const std::size_t finish     = string_impl_detail::normalize_slice_index(end.value(), value.size());
            const std::size_t safe_end   = std::max(begin, finish);
            const std::size_t char_count = safe_end - begin;
            out.set(value.substr(begin, char_count));
        }
    };

    /** hgraph's default split shape: TS[tuple[str, ...]] (all splits), or a
        FIXED tuple via subscript - split[TS[Tuple[str, str]]] keeps the
        remainder in the last slot (python str.split maxsplit semantics). */
    struct split_tuple_impl
    {
        static constexpr auto name = "split_tuple";

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            const auto *out = resolution.find_ts("O");
            if (out == nullptr) { return true; }   // default form resolves the variadic tuple
            if (out->kind != TSTypeKind::TS || out->value_schema == nullptr) { return false; }
            const auto *value_meta = out->value_schema;
            if (value_meta->value_kind() == ValueTypeKind::List)
            {
                return value_meta->element_type == scalar_descriptor<Str>::value_meta();
            }
            if (value_meta->value_kind() == ValueTypeKind::Tuple)
            {
                // Tuple[str, str, ...]: every field must be str.
                for (std::size_t index = 0; index < value_meta->field_count; ++index)
                {
                    if (value_meta->fields[index].type != scalar_descriptor<Str>::value_meta()) { return false; }
                }
                return value_meta->field_count > 0;
            }
            return false;
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            auto &registry = TypeRegistry::instance();
            resolution.bind_ts("O", registry.ts(registry.list(scalar_descriptor<Str>::value_meta(), 0, true)));
        }

        static void eval(In<"s", TS<Str>> s, Scalar<"separator", Str> separator, Out<TsVar<"O">> out)
        {
            const auto &erased = static_cast<const TSOutputView &>(out);
            const auto *value_meta = erased.schema()->value_schema;
            const auto  fixed = value_meta->value_kind() == ValueTypeKind::Tuple
                                    ? static_cast<std::size_t>(value_meta->field_count)
                                    : static_cast<std::size_t>(value_meta->fixed_size);

            const Str value = s.value();
            const Str sep   = separator.value();
            std::vector<Str> parts;
            std::size_t      begin = 0;
            while (true)
            {
                if (fixed != 0 && parts.size() + 1 == fixed) { parts.push_back(value.substr(begin)); break; }
                const auto at = value.find(sep, begin);
                if (at == Str::npos) { parts.push_back(value.substr(begin)); break; }
                parts.push_back(value.substr(begin, at - begin));
                begin = at + sep.size();
            }

            Value result;
            if (fixed == 0)
            {
                const auto element_binding =
                    ValuePlanFactory::instance().type_for(value_meta->element_type);
                ListBuilder builder{element_binding};
                for (const Str &part : parts) { builder.push_back(part); }
                result = builder.build();
            }
            else
            {
                if (parts.size() != fixed)
                {
                    throw std::invalid_argument("split: input does not produce the fixed tuple arity");
                }
                if (value_meta->value_kind() == ValueTypeKind::Tuple)
                {
                    BundleBuilder builder{ValuePlanFactory::instance().type_for(value_meta)};
                    for (std::size_t index = 0; index < fixed; ++index)
                    {
                        builder.set(index, Value{parts[index]});
                    }
                    result = builder.build();
                }
                else
                {
                    const auto binding = ValuePlanFactory::instance().type_for(value_meta);
                    result = Value{binding};
                    auto mutation = result.begin_mutation();
                    auto *base = static_cast<std::byte *>(mutation.mutable_data());
                    const auto stride = ValuePlanFactory::instance()
                                            .type_for(value_meta->element_type)
                                            .checked_plan()
                                            .layout.size;
                    for (std::size_t index = 0; index < fixed; ++index)
                    {
                        *reinterpret_cast<Str *>(base + index * stride) = parts[index];
                    }
                }
            }
            auto mutation = erased.begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(result)));
        }
    };

    struct split_tsl_impl
    {
        /*
         * ``N`` is intentionally resolved from the caller's requested output
         * schema. There is no TSL size in the inputs, so generic calls must use:
         *
         *     wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","})
         */
        static void eval(In<"s", TS<Str>> s, Scalar<"separator", Str> separator,
                         Out<TSL<TS<Str>, SIZE<"N">>> out)
        {
            const std::vector<Str> parts = string_impl_detail::split_parts(s.value(), separator.value(), out.size());
            for (std::size_t i = 0; i < parts.size(); ++i)
            {
                auto child = out[i];
                if (!child.valid() || child.value().template checked_as<Str>() != parts[i]) { child.set(parts[i]); }
            }
        }
    };

    /** join over the VARIADIC call shape (hgraph: ``join(*ts, separator=)``). */
    struct join_multi_impl
    {
        static constexpr auto name = "join_multi";

        static auto defaults()
        {
            return std::tuple{arg<"__strict__">(Bool{false})};
        }

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
        {
            higher_order_impl_detail::bind_graph_output(
                resolution, TypeRegistry::instance().ts(scalar_descriptor<Str>::value_meta()), "O");
        }

        static WiringPortRef compose(Wiring &w, VarIn<"ts", TS<Str>> ts, Scalar<"separator", Str> separator,
                                     Scalar<"__strict__", Bool> strict)
        {
            if (ts.empty()) { throw std::invalid_argument("join requires at least one input"); }
            auto &registry = TypeRegistry::instance();
            const auto *element = registry.ts(scalar_descriptor<Str>::value_meta());
            std::vector<WiringPortRef> children;
            children.reserve(ts.size());
            for (const WiringPortRef &port : ts) { children.push_back(port); }
            WiringPortRef packed =
                WiringPortRef::structural_source(registry.tsl(element, ts.size()), std::move(children));

            WiringArg ts_arg;
            ts_arg.kind = WiringArg::Kind::TimeSeries;
            ts_arg.port = packed;
            WiringArg sep_arg;
            sep_arg.kind         = WiringArg::Kind::Scalar;
            sep_arg.scalar_value = Value{separator.value()};
            sep_arg.scalar_meta  = sep_arg.scalar_value.schema();
            sep_arg.name         = "separator";
            WiringArg strict_arg;
            strict_arg.kind         = WiringArg::Kind::Scalar;
            strict_arg.scalar_value = Value{strict.value()};
            strict_arg.scalar_meta  = strict_arg.scalar_value.schema();
            strict_arg.name         = "__strict__";
            std::array<WiringArg, 3> args{ts_arg, sep_arg, strict_arg};
            return wire_operator(w, "join", args).output.erased();
        }
    };

    /** join over a tuple[str, ...] SCALAR value. */
    struct join_tuple_impl
    {
        static constexpr auto name = "join_tuple";

        static auto defaults()
        {
            return std::tuple{arg<"__strict__">(Bool{false})};
        }

        static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
        {
            const auto *meta = resolution.find_scalar("T");
            return meta != nullptr && meta->value_kind() == ValueTypeKind::List && !meta->is_mutable() &&
                   meta->element_type == scalar_descriptor<Str>::value_meta();
        }

        static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Scalar<"separator", Str> separator,
                         Scalar<"__strict__", Bool> strict, Out<TS<Str>> out)
        {
            static_cast<void>(strict);
            Str  joined;
            bool first = true;
            const auto values = ts.base().value().as_list();
            for (const ValueView &item : values)
            {
                if (!first) { joined += separator.value(); }
                joined += item.checked_as<Str>();
                first = false;
            }
            out.set(joined);
        }
    };

    struct join_tsl_impl
    {
        static void eval(In<"strings", TSL<TS<Str>, SIZE<"N">>, InputValidity::Unchecked> strings,
                         Scalar<"separator", Str> separator, Scalar<"__strict__", Bool> strict, Out<TS<Str>> out)
        {
            Str  joined;
            bool first = true;
            for (std::size_t i = 0; i < strings.size(); ++i)
            {
                auto item = strings[i];
                if (!item.valid())
                {
                    if (strict.value()) { return; }
                    continue;
                }
                if (!first) { joined += separator.value(); }
                joined += item.value();
                first = false;
            }
            out.set(joined);
        }

        static auto defaults()
        {
            return std::tuple{arg<"__strict__">(Bool{false})};
        }
    };

    struct format_bundle_impl
    {
        static constexpr auto name = "format";

        static void eval(In<"fmt", TS<Str>> fmt,
                         In<"__args__", Kwargs<>, InputValidity::Unchecked> args,
                         Scalar<"__pos_count__", Int> pos_count, Scalar<"__sample__", Int> sample,
                         Scalar<"__strict__", Bool> strict, State<Int> count, Out<TS<Str>> out)
        {
            const bool require_all = strict.value();
            if (require_all && !args.all_valid()) { return; }

            if (sample.value() > 1)
            {
                const Int next = count.get() + 1;
                count.set(next);
                if (next % sample.value() != 0) { return; }
            }

            const string_impl_detail::FormatValues values =
                string_impl_detail::collect_format_values(args, static_cast<std::size_t>(pos_count.value()), require_all);
            out.set(string_impl_detail::render_format_string(fmt.value(), values.positional, values.named));
        }
    };

    struct format_no_args_impl
    {
        static constexpr auto name = "format";

        static void eval(In<"fmt", TS<Str>> fmt, Scalar<"__sample__", Int> sample, Scalar<"__strict__", Bool> strict,
                         State<Int> count, Out<TS<Str>> out)
        {
            static_cast<void>(strict);
            if (sample.value() > 1)
            {
                const Int next = count.get() + 1;
                count.set(next);
                if (next % sample.value() != 0) { return; }
            }
            out.set(string_impl_detail::render_format_string(fmt.value(), {}, {}));
        }
    };

    struct format_graph_impl
    {
        static constexpr auto name = "format";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> fmt, VarIn<"args", TsVar<"A">> args,
                                     Scalar<"__sample__", Int> sample, Scalar<"__strict__", Bool> strict,
                                     VarKwIn<"kwargs"> kwargs)
        {
            std::vector<std::pair<std::string, WiringPortRef>> positional_fields;
            positional_fields.reserve(args.size());
            for (std::size_t i = 0; i < args.size(); ++i)
            {
                positional_fields.emplace_back(
                    std::to_string(i), args[i]);
            }

            std::vector<std::pair<std::string, WiringPortRef>> fields;
            fields.reserve(args.size() + kwargs.size());
            fields.insert(fields.end(), positional_fields.begin(), positional_fields.end());
            for (const auto &[field_name, field_port] : kwargs)
            {
                fields.emplace_back(field_name, field_port);
            }

            if (fields.empty())
            {
                return wire<format_no_args_impl>(w,
                                                 fmt,
                                                 arg<"__sample__">(sample.value()),
                                                 arg<"__strict__">(strict.value()));
            }

            return wire<format_bundle_impl, TS<Str>>(w,
                                                     fmt,
                                                     arg<"__args__">(string_impl_detail::format_bundle_arg(fields)),
                                                     arg<"__pos_count__">(static_cast<Int>(args.size())),
                                                     arg<"__sample__">(sample.value()),
                                                     arg<"__strict__">(strict.value()));
        }

        static auto defaults()
        {
            return std::tuple{arg<"__sample__">(Int{-1}), arg<"__strict__">(Bool{true})};
        }
    };

    void register_string_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_STRING_IMPL_H
