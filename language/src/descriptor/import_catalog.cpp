#include "descriptor/import_catalog.h"

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace hgl::descriptor
{
    namespace
    {
        [[nodiscard]] std::optional<semantics::ImportedScalarType> scalar_type(const ModuleDescriptor &descriptor,
                                                                               SchemaId                id) noexcept {
            if (id == no_schema_id || id >= descriptor.types.size()) { return std::nullopt; }
            const TypeRecord &type = descriptor.types[id];
            if (type.category != TypeCategory::Scalar) { return std::nullopt; }
            using semantics::ImportedScalarType;
            if (type.scalar_name == "bool") { return ImportedScalarType::Bool; }
            if (type.scalar_name == "i64") { return ImportedScalarType::I64; }
            if (type.scalar_name == "f64") { return ImportedScalarType::F64; }
            if (type.scalar_name == "str") { return ImportedScalarType::Str; }
            if (type.scalar_name == "date") { return ImportedScalarType::Date; }
            if (type.scalar_name == "time") { return ImportedScalarType::Time; }
            if (type.scalar_name == "datetime") { return ImportedScalarType::DateTime; }
            if (type.scalar_name == "duration") { return ImportedScalarType::Duration; }
            if (type.scalar_name == "civil_datetime") { return ImportedScalarType::CivilDateTime; }
            if (type.scalar_name == "zoned_datetime") { return ImportedScalarType::ZonedDateTime; }
            if (type.scalar_name == "zoned_time") { return ImportedScalarType::ZonedTime; }
            if (type.scalar_name == "timezone") { return ImportedScalarType::TimeZone; }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<semantics::ImportedConstant> imported_constant(const ModuleDescriptor &descriptor,
                                                                                   SchemaId                id) noexcept {
            using semantics::ImportedConstant;
            using semantics::ImportedConstantKind;
            if (id == no_schema_id) { return ImportedConstant{}; }
            if (id >= descriptor.constant_expressions.size()) { return std::nullopt; }
            const ConstantExpressionRecord &source = descriptor.constant_expressions[id];
            if (source.category == ConstantExpressionCategory::Parameter) {
                return ImportedConstant{.kind = ImportedConstantKind::Parameter, .binding_identity = source.parameter_identity};
            }
            if (source.category == ConstantExpressionCategory::Literal && source.literal) {
                if (const auto *value = std::get_if<std::int64_t>(&*source.literal)) {
                    return ImportedConstant{.kind = ImportedConstantKind::I64, .i64 = *value};
                }
            }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<semantics::ImportedType> imported_type(const ModuleDescriptor &descriptor,
                                                                           SchemaId                id) noexcept {
            using semantics::ImportedType;
            using semantics::ImportedTypeKind;
            if (id == no_schema_id || id >= descriptor.types.size()) { return std::nullopt; }
            const TypeRecord &source = descriptor.types[id];
            ImportedType      result;
            if (const auto scalar = scalar_type(descriptor, id)) {
                result.scalar = *scalar;
                return result;
            }
            switch (source.category) {
                case TypeCategory::Symbol:
                    if (source.binding_identity.empty()) { return std::nullopt; }
                    result.kind             = ImportedTypeKind::Symbol;
                    result.binding_identity = source.binding_identity;
                    break;
                case TypeCategory::List: result.kind = ImportedTypeKind::List; break;
                case TypeCategory::Set: result.kind = ImportedTypeKind::Set; break;
                case TypeCategory::Map: result.kind = ImportedTypeKind::Map; break;
                case TypeCategory::Rolling: result.kind = ImportedTypeKind::Rolling; break;
                case TypeCategory::Signal: result.kind = ImportedTypeKind::Signal; break;
                default: return std::nullopt;
            }
            for (SchemaId child : source.children) {
                std::optional<ImportedType> lowered = imported_type(descriptor, child);
                if (!lowered) { return std::nullopt; }
                result.children.push_back(std::move(*lowered));
            }
            const auto size     = imported_constant(descriptor, source.size);
            const auto min_size = imported_constant(descriptor, source.min_size);
            if (!size || !min_size) { return std::nullopt; }
            result.size      = *size;
            result.min_size  = *min_size;
            result.unbounded = source.unbounded;
            return result;
        }

        [[nodiscard]] std::string short_name(std::string_view module, std::string_view identity) {
            const std::string prefix = std::string{module} + "::";
            if (!identity.starts_with(prefix)) { return {}; }
            identity.remove_prefix(prefix.size());
            if (identity.empty() || identity.find("::") != std::string_view::npos) { return {}; }
            return std::string{identity};
        }

        [[nodiscard]] semantics::NativeCallPhase phase(NativePhase value) noexcept {
            using semantics::NativeCallPhase;
            switch (value) {
                case NativePhase::Wiring: return NativeCallPhase::Wiring;
                case NativePhase::Start: return NativeCallPhase::Start;
                case NativePhase::Evaluation: return NativeCallPhase::Evaluation;
                case NativePhase::Stop: return NativeCallPhase::Stop;
            }
            std::unreachable();
        }

        [[nodiscard]] semantics::NativeParameterAccess access(NativeParameterAccess value) noexcept {
            switch (value) {
                case NativeParameterAccess::Value: return semantics::NativeParameterAccess::Value;
                case NativeParameterAccess::InputView: return semantics::NativeParameterAccess::InputView;
            }
            std::unreachable();
        }
    }  // namespace

    std::optional<ReadError> add_to_catalog(const ModuleDescriptor &descriptor, semantics::ModuleCatalog &catalog) {
        if (const std::optional<ReadError> invalid = validate(descriptor)) { return invalid; }

        semantics::ImportableModule module;
        module.identity               = descriptor.module_identity;
        module.descriptor_fingerprint = descriptor.descriptor_fingerprint;
        for (std::size_t declaration_index = 0; declaration_index < descriptor.native_declarations.size(); ++declaration_index) {
            const NativeDeclaration &declaration = descriptor.native_declarations[declaration_index];
            if (declaration.category != NativeDeclarationCategory::Function) { continue; }

            semantics::ImportedFunction function;
            function.module_identity        = descriptor.module_identity;
            function.name                   = short_name(descriptor.module_identity, declaration.identity);
            function.identity               = declaration.identity;
            function.candidate_identity     = declaration.identity + "#" + std::to_string(declaration_index);
            function.cpp_symbol             = declaration.cpp_symbol;
            function.public_headers         = descriptor.build.public_headers;
            function.cmake_packages         = descriptor.build.cmake_packages;
            function.imported_targets       = descriptor.build.imported_targets;
            function.runtime_images         = descriptor.build.runtime_images;
            function.descriptor_fingerprint = descriptor.descriptor_fingerprint;
            if (function.name.empty()) {
                return ReadError{"$.native.declarations[" + std::to_string(declaration_index) + "].identity",
                                 "native function identity must be '" + descriptor.module_identity + "::<name>'"};
            }
            if (!declaration.effects.empty()) {
                function.support_error = "native value calls with declared effects are not supported yet";
            }
            if (declaration.phases.size() != 1U || declaration.phases.front() != NativePhase::Evaluation) {
                function.support_error = "native value calls currently require the evaluation phase only";
            }
            if (declaration.thread_safety == NativeThreadSafety::Serialized) {
                function.support_error = "serialized native value calls are not supported yet";
            }
            if (declaration.result.ownership != NativeOwnership::Value || declaration.result.mutable_value ||
                !declaration.result.dependent_on.empty()) {
                function.support_error = "native value call results must use value ownership";
            }
            for (const GenericParameter &generic : declaration.signature.generics) {
                semantics::ImportedGeneric lowered{
                    .name             = generic.name,
                    .binding_identity = generic.binding_identity,
                    .is_const         = generic.is_const,
                };
                if (generic.type != no_schema_id) { lowered.type = imported_type(descriptor, generic.type); }
                if (generic.type != no_schema_id && !lowered.type) {
                    function.support_error = "native generic constraints must use supported value types";
                }
                function.generics.push_back(std::move(lowered));
            }
            for (std::size_t index = 0; index < declaration.signature.parameters.size(); ++index) {
                const Parameter &parameter = declaration.signature.parameters[index];
                const auto       type      = imported_type(descriptor, parameter.type);
                if (!type) {
                    function.support_error =
                        "native calls require supported scalar, collection-view, or signal input-view parameter types";
                    continue;
                }
                const NativeValuePolicy &policy = declaration.parameters[index].value;
                if (policy.ownership != NativeOwnership::Value || policy.mutable_value || !policy.dependent_on.empty()) {
                    function.support_error = "native value call parameters must use value ownership";
                }
                function.parameters.push_back(semantics::ImportedParameter{parameter.name, *type, parameter.is_const,
                                                                           access(declaration.parameters[index].access)});
            }
            if (declaration.signature.result != no_schema_id) {
                function.result = imported_type(descriptor, declaration.signature.result);
                if (!function.result) { function.support_error = "native calls require a supported value result"; }
            }
            for (NativePhase allowed : declaration.phases) { function.phases.push_back(phase(allowed)); }
            module.functions.push_back(std::move(function));
        }

        if (const std::optional<semantics::CatalogError> error = catalog.add(std::move(module))) {
            return ReadError{error->path, error->message};
        }
        return std::nullopt;
    }
}  // namespace hgl::descriptor
