#include "wiring/type_bridge.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cstddef>
#include <exception>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgl::wiring
{
    namespace
    {
        namespace hir = ir::hir;

        const hgraph::ValueTypeMetaData *scalar_meta(hir::ScalarType                                type,
                                                     const hgraph::stdlib::RegisteredStandardTypes &types) noexcept {
            switch (type) {
                case hir::ScalarType::Bool: return types.bool_type;
                case hir::ScalarType::I64: return types.int_type;
                case hir::ScalarType::F64: return types.float_type;
                case hir::ScalarType::Str: return types.str_type;
                case hir::ScalarType::Date: return types.date_type;
                case hir::ScalarType::Time: return types.time_type;
                case hir::ScalarType::DateTime: return types.datetime_type;
                case hir::ScalarType::Duration: return types.timedelta_type;
                case hir::ScalarType::CivilDateTime: return types.civil_datetime_type;
                case hir::ScalarType::ZonedDateTime: return types.zoned_datetime_type;
                case hir::ScalarType::TimeZone: return types.zone_id_type;
                case hir::ScalarType::ZonedTime: return nullptr;
            }
            std::unreachable();
        }

        std::pair<std::string, std::string> split_identity(std::string_view identity) {
            const std::size_t separator = identity.rfind('.');
            if (separator == std::string_view::npos) { return {{}, std::string{identity}}; }
            return {std::string{identity.substr(0, separator)}, std::string{identity.substr(separator + 1U)}};
        }
    }  // namespace

    TypeBridge::TypeBridge(const hgraph_ir::Module &module, syntax::DiagnosticSink &diagnostics)
        : module_{module}, diagnostics_{diagnostics}, registry_{hgraph::TypeRegistry::instance()},
          types_{hgraph::stdlib::register_standard_types(registry_)}, generation_{registry_.reset_generation()} {}

    void TypeBridge::refresh_registry() {
        const std::uint64_t current = registry_.reset_generation();
        if (generation_ == current) { return; }
        values_.clear();
        schemas_.clear();
        types_      = hgraph::stdlib::register_standard_types(registry_);
        generation_ = registry_.reset_generation();
    }

    void TypeBridge::report(syntax::SourceRange range, std::string message) {
        diagnostics_.report(syntax::Category::Backend, range, std::move(message));
    }

    const hgraph_ir::StructContract *TypeBridge::structure(std::string_view identity) const noexcept {
        for (const hgraph_ir::StructContract &candidate : module_.structures) {
            if (candidate.identity == identity) { return &candidate; }
        }
        return nullptr;
    }

    std::optional<TypeBridge::Bindings> TypeBridge::bind(const hgraph_ir::Type &type, const hgraph_ir::StructContract &contract,
                                                         const Bindings &outer) {
        if (type.arguments.size() != contract.generics.size()) {
            report(type.range, "nominal type '" + type.nominal_identity + "' has an incomplete generic application");
            return std::nullopt;
        }
        Bindings result = outer;
        for (std::size_t index = 0; index < contract.generics.size(); ++index) {
            const hgraph_ir::GenericParameter &generic  = contract.generics[index];
            const hgraph_ir::TypeArgument     &argument = type.arguments[index];
            if (generic.is_const) {
                if (!generic.binding.valid() || !argument.value) {
                    report(type.range, "const generic '" + generic.name + "' requires a value argument");
                    return std::nullopt;
                }
                result.values[generic.binding.value] = *argument.value;
            } else {
                if (!generic.binding.valid() || !argument.type) {
                    report(type.range, "type generic '" + generic.name + "' requires a type argument");
                    return std::nullopt;
                }
                result.types[generic.binding.value] = *argument.type;
            }
        }
        return result;
    }

    std::optional<hgraph::Value> TypeBridge::literal(hgraph_ir::ConstExprId expression) {
        refresh_registry();
        if (!expression.valid() || expression.value >= module_.const_exprs.size()) { return std::nullopt; }
        const hgraph_ir::ConstExpr &source = module_.const_exprs[expression.value];
        if (source.kind != hgraph_ir::ConstExprKind::Literal || !source.literal) {
            report(source.range, "the hgraph type bridge requires a folded constant literal");
            return std::nullopt;
        }
        return std::visit(
            [&](const auto &item) -> std::optional<hgraph::Value> {
                using T = std::decay_t<decltype(item)>;
                if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
                              std::is_same_v<T, std::string>) {
                    return hgraph::Value{item};
                } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                    using syntax::TemporalKind;
                    switch (item.kind) {
                        case TemporalKind::Date:
                            return hgraph::Value{hgraph::Date{std::chrono::sys_days{std::chrono::days{item.micros}}}};
                        case TemporalKind::Time: return hgraph::Value{hgraph::Time{item.micros}};
                        case TemporalKind::DateTime: return hgraph::Value{hgraph::DateTime{std::chrono::microseconds{item.micros}}};
                        case TemporalKind::Duration: return hgraph::Value{hgraph::TimeDelta{item.micros}};
                        case TemporalKind::CivilDateTime:
                        case TemporalKind::ZonedDateTime:
                        case TemporalKind::ZonedTime:
                        case TemporalKind::TimeZone:
                            report(source.range, "zoned and civil constants are not supported by the direct backend yet");
                            return std::nullopt;
                    }
                    std::unreachable();
                } else {
                    return std::nullopt;
                }
            },
            *source.literal);
    }

    std::optional<std::int64_t> TypeBridge::integer(hgraph_ir::ConstExprId expression, syntax::SourceRange range,
                                                    std::string_view role) {
        const std::optional<hgraph::Value> value = literal(expression);
        if (value && value->schema() == types_.int_type) {
            const std::int64_t result = value->view().checked_as<hgraph::Int>();
            if (result >= 0) { return result; }
        }
        report(range, std::string{role} + " must be a non-negative i64 constant");
        return std::nullopt;
    }

    const hgraph::ValueTypeMetaData *TypeBridge::nominal_value(const hgraph_ir::Type &type, const Bindings &outer) {
        const hgraph_ir::StructContract *contract = structure(type.nominal_identity);
        if (contract == nullptr) {
            report(type.range, "unknown nominal type '" + type.nominal_identity + "'");
            return nullptr;
        }
        const std::optional<Bindings> applied = bind(type, *contract, outer);
        if (!applied) { return nullptr; }

        std::vector<const hgraph::ValueTypeMetaData *> generic_types;
        std::string                                    local_name = split_identity(contract->identity).second;
        if (!contract->generics.empty()) { local_name += '['; }
        for (std::size_t index = 0; index < contract->generics.size(); ++index) {
            if (index != 0) { local_name += ','; }
            const hgraph_ir::GenericParameter &generic = contract->generics[index];
            if (generic.is_const) {
                report(type.range, "const generic struct arguments require typed constant Bundle metadata in hgraph");
                return nullptr;
            }
            const hgraph::ValueTypeMetaData *argument = value(applied->types.at(generic.binding.value), *applied);
            if (argument == nullptr) { return nullptr; }
            generic_types.push_back(argument);
            local_name += argument->name();
        }
        if (!contract->generics.empty()) { local_name += ']'; }

        std::vector<std::pair<std::string, const hgraph::ValueTypeMetaData *>> fields;
        fields.reserve(contract->fields.size());
        for (const hgraph_ir::StructField &field : contract->fields) {
            const hgraph::ValueTypeMetaData *field_type = value(field.type, *applied);
            if (field_type == nullptr) { return nullptr; }
            fields.emplace_back(field.name, field_type);
        }

        std::vector<const hgraph::ValueTypeMetaData *> parents;
        parents.reserve(contract->parents.size());
        for (hgraph_ir::TypeId parent : contract->parents) {
            const hgraph::ValueTypeMetaData *parent_type = value(parent, *applied);
            if (parent_type == nullptr) { return nullptr; }
            parents.push_back(parent_type);
        }

        const std::string module_name = split_identity(contract->identity).first;
        try {
            return registry_.bundle(module_name, local_name, fields, parents, contract->abstract, "__type__", generic_types);
        } catch (const std::exception &error) {
            report(type.range, "cannot register struct '" + local_name + "': " + error.what());
            return nullptr;
        }
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::nominal_schema(const hgraph_ir::Type &type, const Bindings &outer) {
        const hgraph_ir::StructContract *contract = structure(type.nominal_identity);
        if (contract == nullptr) {
            report(type.range, "unknown nominal type '" + type.nominal_identity + "'");
            return nullptr;
        }
        const std::optional<Bindings> applied = bind(type, *contract, outer);
        if (!applied) { return nullptr; }
        const hgraph::ValueTypeMetaData *value_type = nominal_value(type, outer);
        if (value_type == nullptr) { return nullptr; }

        std::vector<std::pair<std::string, const hgraph::TSValueTypeMetaData *>> fields;
        fields.reserve(contract->fields.size());
        for (const hgraph_ir::StructField &field : contract->fields) {
            const hgraph::TSValueTypeMetaData *field_type = schema(field.type, *applied);
            if (field_type == nullptr) { return nullptr; }
            fields.emplace_back(field.name, field_type);
        }
        try {
            return registry_.tsb(value_type->name(), fields);
        } catch (const std::exception &error) {
            report(type.range, "cannot register temporal struct '" + std::string{value_type->name()} + "': " + error.what());
            return nullptr;
        }
    }

    const hgraph::ValueTypeMetaData *TypeBridge::value(hgraph_ir::TypeId id) {
        refresh_registry();
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        if (const auto found = values_.find(id.value); found != values_.end()) { return found->second; }
        const hgraph::ValueTypeMetaData *result = value(id, Bindings{});
        if (result != nullptr) { values_.emplace(id.value, result); }
        return result;
    }

    const hgraph::ValueTypeMetaData *TypeBridge::value(hgraph_ir::TypeId id, const Bindings &bindings) {
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Scalar:
                {
                    const hgraph::ValueTypeMetaData *result = scalar_meta(type.scalar, types_);
                    if (result == nullptr) { report(type.range, "unsupported scalar value type"); }
                    return result;
                }
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    if (const auto generic = bindings.types.find(type.binding.value); generic != bindings.types.end()) {
                        if (generic->second == id) {
                            report(type.range, "generic type '" + type.nominal_identity + "' is not concretely bound");
                            return nullptr;
                        }
                        return value(generic->second, bindings);
                    }
                }
                return nominal_value(type, bindings);
            case hir::TypeKind::Tuple:
                {
                    std::vector<const hgraph::ValueTypeMetaData *> elements;
                    elements.reserve(type.children.size());
                    for (hgraph_ir::TypeId child : type.children) {
                        const hgraph::ValueTypeMetaData *element = value(child, bindings);
                        if (element == nullptr) { return nullptr; }
                        elements.push_back(element);
                    }
                    return registry_.tuple(elements);
                }
            case hir::TypeKind::List:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    std::size_t size = 0;
                    if (type.size.valid()) {
                        const std::optional<std::int64_t> count = integer(type.size, type.range, "a list size");
                        if (!count) { return nullptr; }
                        if (*count <= 0) {
                            report(type.range, "a fixed list size must be positive");
                            return nullptr;
                        }
                        size = static_cast<std::size_t>(*count);
                    }
                    return registry_.list(element, size);
                }
            case hir::TypeKind::Set:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings)) {
                        return registry_.set(element);
                    }
                }
                return nullptr;
            case hir::TypeKind::Map:
                if (type.children.size() == 2U) {
                    const hgraph::ValueTypeMetaData *key    = value(type.children[0], bindings);
                    const hgraph::ValueTypeMetaData *mapped = value(type.children[1], bindings);
                    if (key != nullptr && mapped != nullptr) { return registry_.map(key, mapped); }
                }
                return nullptr;
            case hir::TypeKind::Atomic:
                if (!type.children.empty()) { return value(type.children.front(), bindings); }
                return nullptr;
            case hir::TypeKind::Rolling:
                report(type.range, "'rolling' has no value type; it is a time-series window");
                return nullptr;
            case hir::TypeKind::Void:
            case hir::TypeKind::Iterator:
            case hir::TypeKind::Callable:
            case hir::TypeKind::Capability:
            case hir::TypeKind::HarnessSequence:
            case hir::TypeKind::Deferred: break;
        }
        report(type.range, "unsupported hgraph value type");
        return nullptr;
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::schema(hgraph_ir::TypeId id) {
        refresh_registry();
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        if (const auto found = schemas_.find(id.value); found != schemas_.end()) { return found->second; }
        const hgraph::TSValueTypeMetaData *result = schema(id, Bindings{});
        if (result != nullptr) { schemas_.emplace(id.value, result); }
        return result;
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::schema(hgraph_ir::TypeId id, const Bindings &bindings) {
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Scalar:
                {
                    const hgraph::ValueTypeMetaData *meta = scalar_meta(type.scalar, types_);
                    if (meta != nullptr) { return registry_.ts(meta); }
                    report(type.range, "unsupported scalar time-series type");
                    return nullptr;
                }
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    if (const auto generic = bindings.types.find(type.binding.value); generic != bindings.types.end()) {
                        if (generic->second == id) {
                            report(type.range, "generic type '" + type.nominal_identity + "' is not concretely bound");
                            return nullptr;
                        }
                        return schema(generic->second, bindings);
                    }
                }
                return nominal_schema(type, bindings);
            case hir::TypeKind::Tuple:
                report(type.range, "a structural tuple has no time-series schema; use atomic<tuple<...>> for one value");
                return nullptr;
            case hir::TypeKind::List:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::TSValueTypeMetaData *element = schema(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    std::size_t size = 0;
                    if (type.size.valid()) {
                        const std::optional<std::int64_t> count = integer(type.size, type.range, "a list size");
                        if (!count) { return nullptr; }
                        if (*count <= 0) {
                            report(type.range, "a fixed list size must be positive");
                            return nullptr;
                        }
                        size = static_cast<std::size_t>(*count);
                    }
                    return registry_.tsl(element, size);
                }
            case hir::TypeKind::Set:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings)) {
                        return registry_.tss(element);
                    }
                }
                return nullptr;
            case hir::TypeKind::Map:
                if (type.children.size() == 2U) {
                    const hgraph::ValueTypeMetaData   *key    = value(type.children[0], bindings);
                    const hgraph::TSValueTypeMetaData *mapped = schema(type.children[1], bindings);
                    if (key != nullptr && mapped != nullptr) { return registry_.tsd(key, mapped); }
                }
                return nullptr;
            case hir::TypeKind::Rolling:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    const std::optional<hgraph::Value> period_value  = literal(type.size);
                    const std::optional<hgraph::Value> minimum_value = literal(type.min_size);
                    if (!period_value || !minimum_value) { return nullptr; }
                    if (period_value->schema() == types_.timedelta_type) {
                        if (minimum_value->schema() != types_.timedelta_type) {
                            report(type.range, "a minimum rolling duration must be a duration constant");
                            return nullptr;
                        }
                        const hgraph::TimeDelta period  = period_value->view().checked_as<hgraph::TimeDelta>();
                        const hgraph::TimeDelta minimum = minimum_value->view().checked_as<hgraph::TimeDelta>();
                        if (period <= hgraph::TimeDelta{0} || minimum < hgraph::TimeDelta{0} || minimum > period) {
                            report(type.range,
                                   "rolling durations require a positive maximum and a non-negative minimum no larger than it");
                            return nullptr;
                        }
                        return registry_.tsw_duration(element, period, minimum);
                    }
                    if (period_value->schema() != types_.int_type || minimum_value->schema() != types_.int_type) {
                        report(type.range, "a rolling size must be an i64 or duration constant");
                        return nullptr;
                    }
                    const std::int64_t period  = period_value->view().checked_as<hgraph::Int>();
                    const std::int64_t minimum = minimum_value->view().checked_as<hgraph::Int>();
                    if (period <= 0 || minimum < 0 || minimum > period) {
                        report(type.range, "rolling sizes require a positive maximum and a non-negative minimum no larger than it");
                        return nullptr;
                    }
                    return registry_.tsw(element, static_cast<std::size_t>(period), static_cast<std::size_t>(minimum));
                }
            case hir::TypeKind::Atomic:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *meta = value(type.children.front(), bindings)) {
                        return registry_.ts(meta);
                    }
                }
                return nullptr;
            case hir::TypeKind::Void:
            case hir::TypeKind::Iterator:
            case hir::TypeKind::Callable:
            case hir::TypeKind::Capability:
            case hir::TypeKind::HarnessSequence:
            case hir::TypeKind::Deferred: break;
        }
        report(type.range, "unsupported hgraph time-series type");
        return nullptr;
    }
}  // namespace hgl::wiring
