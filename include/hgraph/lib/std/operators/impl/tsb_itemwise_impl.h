#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TSB_ITEMWISE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TSB_ITEMWISE_IMPL_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>

#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib::tsb_itemwise_impl_detail
{
    using namespace hgraph::operator_type_resolution;

    [[nodiscard]] inline const TSValueTypeMetaData *tsb_schema(const WiringArg &arg)
    {
        const TSValueTypeMetaData *schema = time_series_schema(arg);
        return schema != nullptr && schema->kind == TSTypeKind::TSB ? schema : nullptr;
    }

    [[nodiscard]] inline std::string_view field_name_at(const TSValueTypeMetaData &schema,
                                                        std::size_t index) noexcept
    {
        const char *name = schema.fields()[index].name;
        return name != nullptr ? std::string_view{name} : std::string_view{};
    }

    [[nodiscard]] inline bool same_field_layout(const TSValueTypeMetaData &lhs,
                                                const TSValueTypeMetaData &rhs) noexcept
    {
        if (lhs.field_count() != rhs.field_count()) { return false; }
        for (std::size_t index = 0; index < lhs.field_count(); ++index)
        {
            if (field_name_at(lhs, index) != field_name_at(rhs, index)) { return false; }
        }
        return true;
    }

    [[nodiscard]] inline WiringPortRef project_field(Wiring &w,
                                                     const WiringPortRef &input,
                                                     const TSValueTypeMetaData &bundle,
                                                     std::size_t index)
    {
        if (input.schema != nullptr && input.schema->kind == TSTypeKind::REF)
        {
            return wire<getattr_>(w, Port<void>{w, input},
                                  Str{field_name_at(bundle, index)})
                .erased();
        }
        return subgraph_wiring_detail::tsb_field_ref(
            input, index, bundle.fields()[index].type);
    }

    [[nodiscard]] inline const TSValueTypeMetaData *resolve_field_operator_output(std::string_view op_name,
                                                                                  std::span<const WiringArg> args)
    {
        return fallback_on_exception<const TSValueTypeMetaData *>(nullptr, [&] {
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(op_name, args, true);
            return ts_pattern_resolve(resolved.impl->output, resolved.map);
        });
    }

    template <typename Op>
    [[nodiscard]] const TSValueTypeMetaData *resolve_unary_output_schema(const TSValueTypeMetaData &input)
    {
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        fields.reserve(input.field_count());
        bool preserves_input = true;

        for (std::size_t index = 0; index < input.field_count(); ++index)
        {
            WiringArg field_arg;
            field_arg.kind        = WiringArg::Kind::TimeSeries;
            field_arg.port.schema = input.fields()[index].type;

            const TSValueTypeMetaData *field_output =
                resolve_field_operator_output(Op::name, std::span<const WiringArg>{&field_arg, 1});
            if (field_output == nullptr) { return nullptr; }
            preserves_input = preserves_input &&
                              time_series_schema_equivalent(
                                  field_output, input.fields()[index].type);
            fields.emplace_back(std::string{field_name_at(input, index)}, field_output);
        }

        if (preserves_input) { return &input; }
        return TypeRegistry::instance().un_named_tsb(fields);
    }

    template <typename Op>
    [[nodiscard]] const TSValueTypeMetaData *resolve_binary_output_schema(const TSValueTypeMetaData &lhs,
                                                                          const TSValueTypeMetaData &rhs)
    {
        if (!same_field_layout(lhs, rhs)) { return nullptr; }

        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        fields.reserve(lhs.field_count());
        bool preserves_lhs = true;

        for (std::size_t index = 0; index < lhs.field_count(); ++index)
        {
            std::array<WiringArg, 2> field_args{};
            field_args[0].kind        = WiringArg::Kind::TimeSeries;
            field_args[0].port.schema = lhs.fields()[index].type;
            field_args[1].kind        = WiringArg::Kind::TimeSeries;
            field_args[1].port.schema = rhs.fields()[index].type;

            const TSValueTypeMetaData *field_output =
                resolve_field_operator_output(Op::name,
                                              std::span<const WiringArg>{field_args.data(), field_args.size()});
            if (field_output == nullptr) { return nullptr; }
            preserves_lhs = preserves_lhs &&
                            time_series_schema_equivalent(
                                field_output, lhs.fields()[index].type);
            fields.emplace_back(std::string{field_name_at(lhs, index)}, field_output);
        }

        if (preserves_lhs) { return &lhs; }
        return TypeRegistry::instance().un_named_tsb(fields);
    }

    template <typename Op>
    [[nodiscard]] WiringPortRef wire_unary(Wiring &w, const WiringPortRef &input)
    {
        const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(input.schema);
        if (schema == nullptr || schema->kind != TSTypeKind::TSB)
        {
            throw std::logic_error("TSB itemwise operator requires a TSB or REF[TSB] input");
        }
        std::vector<WiringPortRef> children;
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        children.reserve(schema->field_count());
        fields.reserve(schema->field_count());
        bool preserves_input = true;

        for (std::size_t index = 0; index < schema->field_count(); ++index)
        {
            WiringPortRef field_ref = project_field(w, input, *schema, index);
            Port<void> field_out = wire<Op>(w, Port<void>{w, std::move(field_ref)});
            WiringPortRef child = field_out.erased();
            if (child.schema == nullptr)
            {
                throw std::logic_error("TSB itemwise operator field output schema is unresolved");
            }
            preserves_input = preserves_input &&
                              time_series_schema_equivalent(
                                  child.schema, schema->fields()[index].type);
            fields.emplace_back(std::string{field_name_at(*schema, index)}, child.schema);
            children.push_back(std::move(child));
        }

        const TSValueTypeMetaData *output_schema =
            preserves_input ? schema : TypeRegistry::instance().un_named_tsb(fields);
        return WiringPortRef::structural_source(output_schema, std::move(children));
    }

    template <typename Op>
    [[nodiscard]] WiringPortRef wire_binary(Wiring &w, const WiringPortRef &lhs, const WiringPortRef &rhs)
    {
        const TSValueTypeMetaData *schema = TypeRegistry::instance().dereference(lhs.schema);
        const TSValueTypeMetaData *rhs_schema = TypeRegistry::instance().dereference(rhs.schema);
        if (schema == nullptr || rhs_schema == nullptr || schema->kind != TSTypeKind::TSB ||
            rhs_schema->kind != TSTypeKind::TSB || !same_field_layout(*schema, *rhs_schema))
        {
            throw std::logic_error("TSB itemwise operator requires compatible TSB inputs");
        }
        std::vector<WiringPortRef> children;
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        children.reserve(schema->field_count());
        fields.reserve(schema->field_count());
        bool preserves_lhs = true;

        for (std::size_t index = 0; index < schema->field_count(); ++index)
        {
            WiringPortRef lhs_field = project_field(w, lhs, *schema, index);
            WiringPortRef rhs_field = project_field(w, rhs, *rhs_schema, index);
            Port<void> field_out = wire<Op>(w, Port<void>{w, std::move(lhs_field)},
                                            Port<void>{w, std::move(rhs_field)});
            WiringPortRef child = field_out.erased();
            if (child.schema == nullptr)
            {
                throw std::logic_error("TSB itemwise operator field output schema is unresolved");
            }
            preserves_lhs = preserves_lhs &&
                            time_series_schema_equivalent(
                                child.schema, schema->fields()[index].type);
            fields.emplace_back(std::string{field_name_at(*schema, index)}, child.schema);
            children.push_back(std::move(child));
        }

        const TSValueTypeMetaData *output_schema =
            preserves_lhs ? schema : TypeRegistry::instance().un_named_tsb(fields);
        return WiringPortRef::structural_source(output_schema, std::move(children));
    }

    template <typename Op>
    struct tsb_unary_map
    {
        static constexpr auto name = "tsb_unary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution) || context.args.size() != 1) { return; }
            const TSValueTypeMetaData *schema = tsb_schema(context.args[0]);
            if (schema == nullptr) { return; }
            const TSValueTypeMetaData *output = resolve_unary_output_schema<Op>(*schema);
            bind_output(resolution, output);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 1) { return false; }
            const TSValueTypeMetaData *schema = tsb_schema(context.args[0]);
            return schema != nullptr && resolve_unary_output_schema<Op>(*schema) != nullptr;
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"ts", TsVar<"S">> ts)
        {
            return wire_unary<Op>(w, ts.erased());
        }
    };

    template <typename Op>
    struct tsb_binary_map
    {
        static constexpr auto name = "tsb_binary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution) || context.args.size() != 2) { return; }
            const TSValueTypeMetaData *lhs = tsb_schema(context.args[0]);
            const TSValueTypeMetaData *rhs = tsb_schema(context.args[1]);
            if (lhs == nullptr || rhs == nullptr) { return; }
            const TSValueTypeMetaData *output = resolve_binary_output_schema<Op>(*lhs, *rhs);
            bind_output(resolution, output);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            if (context.args.size() != 2) { return false; }
            const TSValueTypeMetaData *lhs = tsb_schema(context.args[0]);
            const TSValueTypeMetaData *rhs = tsb_schema(context.args[1]);
            return lhs != nullptr && rhs != nullptr && resolve_binary_output_schema<Op>(*lhs, *rhs) != nullptr;
        }

        static WiringPortRef compose(Wiring &w, NamedPort<"lhs", TsVar<"L">> lhs,
                                     NamedPort<"rhs", TsVar<"R">> rhs)
        {
            return wire_binary<Op>(w, lhs.erased(), rhs.erased());
        }
    };
}  // namespace hgraph::stdlib::tsb_itemwise_impl_detail

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TSB_ITEMWISE_IMPL_H
