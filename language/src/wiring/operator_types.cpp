#include "wiring/operator_types.h"

#include "wiring/backend.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/type_pattern.h>
#include <hgraph/types/value/value.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgl::wiring
{
    namespace
    {
        namespace hir = ir::hir;

        class NativeTypes
        {
          public:
            explicit NativeTypes(const hir::Module &module)
                : module_{module}, types_{hgraph::stdlib::register_standard_types()}, registry_{hgraph::TypeRegistry::instance()} {}

            [[nodiscard]] ir::OperatorSelection resolve(const ir::OperatorQuery &query) {
                ir::OperatorSelection selection;
                selection.result = query.expected_result;
                if (query.identity.empty()) {
                    selection.error = "operator has no registry identity";
                    return selection;
                }

                std::vector<hgraph::WiringArg> arguments;
                arguments.reserve(query.arguments.size());
                for (const ir::OperatorArgument &argument : query.arguments) {
                    if (argument.value_kind == hir::ValueKind::Function || argument.value_kind == hir::ValueKind::Operator ||
                        argument.value_kind == hir::ValueKind::Type) {
                        selection.deferred = true;
                        return selection;
                    }
                    std::optional<hgraph::WiringArg> lowered = lower_argument(argument);
                    if (!lowered) {
                        selection.deferred = true;
                        return selection;
                    }
                    arguments.push_back(std::move(*lowered));
                }

                const hgraph::TSValueTypeMetaData *expected = nullptr;
                if (query.expected_result.valid()) {
                    expected = schema(query.expected_result);
                    if (expected == nullptr) {
                        selection.deferred = true;
                        return selection;
                    }
                }
                try {
                    const auto signatures = hgraph::OperatorRegistry::instance().overload_signatures(query.identity);
                    const bool reference_output =
                        expected != nullptr && !signatures.empty() && std::ranges::all_of(signatures, [](const auto &signature) {
                            return signature.output_pattern && signature.output_pattern->starts_with("REF[");
                        });
                    const hgraph::TSValueTypeMetaData *dispatch_expected = reference_output ? registry_.ref(expected) : expected;
                    const hgraph::ResolvedOperatorCall resolved          = hgraph::OperatorRegistry::instance().resolve(
                        query.identity, arguments, dispatch_expected == nullptr ? std::nullopt : std::optional<bool>{true},
                        dispatch_expected);
                    selection.candidate_label = resolved.impl->label;
                    if (selection.candidate_label.empty()) { selection.candidate_label = resolved.impl->name; }
                    if (resolved.impl->has_output) {
                        const hgraph::TSValueTypeMetaData *output = hgraph::ts_pattern_resolve(resolved.impl->output, resolved.map);
                        if (output != nullptr) {
                            selection.result = type_for_schema(output);
                            if (!selection.result.valid()) { selection.deferred = true; }
                        } else {
                            selection.deferred = true;
                        }
                    }
                    append_substitutions(resolved.map, selection.substitutions);
                } catch (const hgraph::OperatorResolutionError &error) {
                    selection.error = query.identity + ": " + error.what();
                } catch (const std::exception &error) { selection.error = query.identity + ": " + error.what(); }
                return selection;
            }

          private:
            [[nodiscard]] hir::TypeId canonical(hir::TypeId id) const noexcept {
                if (!id.valid()) { return {}; }
                const hir::TypeId result = module_.type(id).canonical;
                return result.valid() ? result : id;
            }

            [[nodiscard]] const hgraph::ValueTypeMetaData *scalar(hir::ScalarType type) const noexcept {
                using hir::ScalarType;
                switch (type) {
                    case ScalarType::Bool: return types_.bool_type;
                    case ScalarType::I64: return types_.int_type;
                    case ScalarType::F64: return types_.float_type;
                    case ScalarType::Str: return types_.str_type;
                    case ScalarType::Date: return types_.date_type;
                    case ScalarType::Time: return types_.time_type;
                    case ScalarType::DateTime: return types_.datetime_type;
                    case ScalarType::Duration: return types_.timedelta_type;
                    case ScalarType::CivilDateTime: return types_.civil_datetime_type;
                    case ScalarType::ZonedDateTime: return types_.zoned_datetime_type;
                    case ScalarType::TimeZone: return types_.zone_id_type;
                    case ScalarType::ZonedTime: return nullptr;
                }
                std::unreachable();
            }

            [[nodiscard]] std::optional<std::int64_t> integer(hir::ExprId id) const noexcept {
                if (!id.valid()) { return std::nullopt; }
                const std::optional<hir::Constant> &constant = module_.expr(id).constant;
                if (!constant) { return std::nullopt; }
                if (const auto *value = std::get_if<std::int64_t>(&*constant)) { return *value; }
                return std::nullopt;
            }

            [[nodiscard]] std::optional<std::int64_t> duration(hir::ExprId id) const noexcept {
                if (!id.valid()) { return std::nullopt; }
                const std::optional<hir::Constant> &constant = module_.expr(id).constant;
                if (!constant) { return std::nullopt; }
                const auto *value = std::get_if<syntax::TemporalValue>(&*constant);
                if (value == nullptr || value->kind != syntax::TemporalKind::Duration) { return std::nullopt; }
                return value->micros;
            }

            [[nodiscard]] const hgraph::ValueTypeMetaData *value(hir::TypeId id) {
                id = canonical(id);
                if (!id.valid()) { return nullptr; }
                if (const auto found = values_.find(id.value); found != values_.end()) { return found->second; }
                const hir::Type                 &type   = module_.type(id);
                const hgraph::ValueTypeMetaData *result = nullptr;
                switch (type.kind) {
                    case hir::TypeKind::Scalar: result = scalar(type.scalar); break;
                    case hir::TypeKind::Tuple:
                        {
                            std::vector<const hgraph::ValueTypeMetaData *> children;
                            for (hir::TypeId child : type.children) {
                                const auto *meta = value(child);
                                if (meta == nullptr) { return nullptr; }
                                children.push_back(meta);
                            }
                            result = registry_.tuple(children);
                            break;
                        }
                    case hir::TypeKind::List:
                        {
                            if (type.children.empty()) { return nullptr; }
                            const auto *element = value(type.children.front());
                            if (element == nullptr) { return nullptr; }
                            std::size_t fixed_size = 0;
                            if (type.size.valid()) {
                                const std::optional<std::int64_t> size = integer(type.size);
                                if (!size || *size < 0) { return nullptr; }
                                fixed_size = static_cast<std::size_t>(*size);
                            }
                            result = registry_.list(element, fixed_size);
                            break;
                        }
                    case hir::TypeKind::Set:
                        if (!type.children.empty()) {
                            if (const auto *element = value(type.children.front())) { result = registry_.set(element); }
                        }
                        break;
                    case hir::TypeKind::Map:
                        if (type.children.size() == 2U) {
                            const auto *key    = value(type.children[0]);
                            const auto *mapped = value(type.children[1]);
                            if (key != nullptr && mapped != nullptr) { result = registry_.map(key, mapped); }
                        }
                        break;
                    case hir::TypeKind::Atomic:
                        if (!type.children.empty()) { result = value(type.children.front()); }
                        break;
                    case hir::TypeKind::Symbol:
                    case hir::TypeKind::Rolling:
                    case hir::TypeKind::Void:
                    case hir::TypeKind::Iterator:
                    case hir::TypeKind::Callable:
                    case hir::TypeKind::Capability:
                    case hir::TypeKind::HarnessSequence:
                    case hir::TypeKind::Deferred: break;
                }
                if (result != nullptr) {
                    values_.emplace(id.value, result);
                    value_ids_.emplace(result, id);
                }
                return result;
            }

            [[nodiscard]] const hgraph::TSValueTypeMetaData *schema(hir::TypeId id) {
                id = canonical(id);
                if (!id.valid()) { return nullptr; }
                if (const auto found = schemas_.find(id.value); found != schemas_.end()) { return found->second; }
                const hir::Type                   &type   = module_.type(id);
                const hgraph::TSValueTypeMetaData *result = nullptr;
                switch (type.kind) {
                    case hir::TypeKind::Scalar:
                        if (const auto *meta = scalar(type.scalar)) { result = registry_.ts(meta); }
                        break;
                    case hir::TypeKind::List:
                        {
                            if (type.children.empty()) { return nullptr; }
                            const auto *element = schema(type.children.front());
                            if (element == nullptr) { return nullptr; }
                            std::size_t fixed_size = 0;
                            if (type.size.valid()) {
                                const std::optional<std::int64_t> size = integer(type.size);
                                if (!size || *size < 0) { return nullptr; }
                                fixed_size = static_cast<std::size_t>(*size);
                            }
                            result = registry_.tsl(element, fixed_size);
                            break;
                        }
                    case hir::TypeKind::Set:
                        if (!type.children.empty()) {
                            if (const auto *element = value(type.children.front())) { result = registry_.tss(element); }
                        }
                        break;
                    case hir::TypeKind::Map:
                        if (type.children.size() == 2U) {
                            const auto *key    = value(type.children[0]);
                            const auto *mapped = schema(type.children[1]);
                            if (key != nullptr && mapped != nullptr) { result = registry_.tsd(key, mapped); }
                        }
                        break;
                    case hir::TypeKind::Rolling:
                        {
                            if (type.children.empty()) { return nullptr; }
                            const auto *element = value(type.children.front());
                            if (element == nullptr) { return nullptr; }
                            if (const std::optional<std::int64_t> period = duration(type.size)) {
                                const std::optional<std::int64_t> minimum = duration(type.min_size);
                                if (!minimum) { return nullptr; }
                                result = registry_.tsw_duration(element, hgraph::TimeDelta{*period}, hgraph::TimeDelta{*minimum});
                            } else {
                                const std::optional<std::int64_t> tick_period = integer(type.size);
                                const std::optional<std::int64_t> minimum     = integer(type.min_size);
                                if (!tick_period || !minimum || *tick_period <= 0 || *minimum < 0) { return nullptr; }
                                result = registry_.tsw(element, static_cast<std::size_t>(*tick_period),
                                                       static_cast<std::size_t>(*minimum));
                            }
                            break;
                        }
                    case hir::TypeKind::Atomic:
                        if (!type.children.empty()) {
                            if (const auto *meta = value(type.children.front())) { result = registry_.ts(meta); }
                        }
                        break;
                    case hir::TypeKind::Symbol:
                    case hir::TypeKind::Tuple:
                    case hir::TypeKind::Void:
                    case hir::TypeKind::Iterator:
                    case hir::TypeKind::Callable:
                    case hir::TypeKind::Capability:
                    case hir::TypeKind::HarnessSequence:
                    case hir::TypeKind::Deferred: break;
                }
                if (result != nullptr) {
                    schemas_.emplace(id.value, result);
                    schema_ids_.emplace(result, id);
                }
                return result;
            }

            [[nodiscard]] hir::TypeId type_for_schema(const hgraph::TSValueTypeMetaData *target) {
                if (target == nullptr) { return {}; }
                if (const auto found = schema_ids_.find(target); found != schema_ids_.end()) { return found->second; }

                // HIR owns its type arena, so the adapter may only return an
                // existing language type. Materialize every representable HIR
                // schema to find outputs inferred solely by hgraph (for
                // example mean(TSW[float]) -> TS[float]).
                const std::size_t count = module_.types.size();
                for (std::uint32_t index = 0; index < count; ++index) {
                    const hir::TypeId candidate{index};
                    if (schema(candidate) == target) { return canonical(candidate); }
                }
                return {};
            }

            [[nodiscard]] std::optional<hgraph::Value> constant(const ir::OperatorArgument &argument) {
                if (!argument.constant) { return std::nullopt; }
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
                                case TemporalKind::DateTime:
                                    return hgraph::Value{hgraph::DateTime{std::chrono::microseconds{item.micros}}};
                                case TemporalKind::Duration: return hgraph::Value{hgraph::TimeDelta{item.micros}};
                                case TemporalKind::CivilDateTime:
                                case TemporalKind::ZonedDateTime:
                                case TemporalKind::ZonedTime:
                                case TemporalKind::TimeZone: return std::nullopt;
                            }
                            std::unreachable();
                        } else {
                            return std::nullopt;
                        }
                    },
                    *argument.constant);
            }

            [[nodiscard]] std::optional<hgraph::WiringArg> lower_argument(const ir::OperatorArgument &argument) {
                hgraph::WiringArg result;
                result.name = argument.name;
                if (argument.value_kind == hir::ValueKind::Constant) {
                    std::optional<hgraph::Value> constant_value = constant(argument);
                    if (!constant_value) { return std::nullopt; }
                    result.kind         = hgraph::WiringArg::Kind::Scalar;
                    result.scalar_value = std::move(*constant_value);
                    result.scalar_meta  = result.scalar_value.schema();
                    value_ids_.emplace(result.scalar_meta, canonical(argument.type));
                    return result;
                }
                const hgraph::TSValueTypeMetaData *meta = schema(argument.type);
                if (meta == nullptr) { return std::nullopt; }
                result.kind = hgraph::WiringArg::Kind::TimeSeries;
                result.port = hgraph::WiringPortRef::null_source(meta);
                return result;
            }

            void append_substitutions(const hgraph::ResolutionMap &map, std::vector<hir::Substitution> &out) const {
                for (const auto &[name, meta] : map.ts_vars) {
                    hir::Substitution value;
                    value.name = name;
                    if (const auto found = schema_ids_.find(meta); found != schema_ids_.end()) { value.type = found->second; }
                    out.push_back(std::move(value));
                }
                for (const auto &[name, meta] : map.scalar_vars) {
                    hir::Substitution value;
                    value.name = name;
                    if (const auto found = value_ids_.find(meta); found != value_ids_.end()) { value.type = found->second; }
                    out.push_back(std::move(value));
                }
                for (const auto &[name, size] : map.size_vars) {
                    hir::Substitution value;
                    value.name     = name;
                    value.constant = hir::Constant{static_cast<std::int64_t>(size)};
                    out.push_back(std::move(value));
                }
                std::ranges::sort(out,
                                  [](const hir::Substitution &lhs, const hir::Substitution &rhs) { return lhs.name < rhs.name; });
            }

            const hir::Module                                                     &module_;
            const hgraph::stdlib::RegisteredStandardTypes                          types_;
            hgraph::TypeRegistry                                                  &registry_;
            std::unordered_map<std::uint32_t, const hgraph::ValueTypeMetaData *>   values_{};
            std::unordered_map<std::uint32_t, const hgraph::TSValueTypeMetaData *> schemas_{};
            std::unordered_map<const hgraph::ValueTypeMetaData *, hir::TypeId>     value_ids_{};
            std::unordered_map<const hgraph::TSValueTypeMetaData *, hir::TypeId>   schema_ids_{};
        };
    }  // namespace

    ir::OperatorSelection resolve_operator_types(const ir::hir::Module &module, const ir::OperatorQuery &query) {
        ensure_session();
        return NativeTypes{module}.resolve(query);
    }
}  // namespace hgl::wiring
