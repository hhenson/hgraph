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
                function.support_error = "native scalar calls with declared effects are not supported yet";
            }
            if (declaration.phases.size() != 1U || declaration.phases.front() != NativePhase::Evaluation) {
                function.support_error = "native scalar calls currently require the evaluation phase only";
            }
            if (declaration.thread_safety == NativeThreadSafety::Serialized) {
                function.support_error = "serialized native scalar calls are not supported yet";
            }
            if (declaration.result.ownership != NativeOwnership::Value || declaration.result.mutable_value ||
                !declaration.result.dependent_on.empty()) {
                function.support_error = "native scalar call results must use value ownership";
            }
            for (std::size_t index = 0; index < declaration.signature.parameters.size(); ++index) {
                const Parameter &parameter = declaration.signature.parameters[index];
                const auto       type      = scalar_type(descriptor, parameter.type);
                if (!type) {
                    function.support_error = "native exact calls currently require canonical scalar parameter types";
                    continue;
                }
                const NativeValuePolicy &policy = declaration.parameters[index].value;
                if (policy.ownership != NativeOwnership::Value || policy.mutable_value || !policy.dependent_on.empty()) {
                    function.support_error = "native scalar call parameters must use value ownership";
                }
                function.parameters.push_back(semantics::ImportedParameter{parameter.name, *type, parameter.is_const});
            }
            if (declaration.signature.result != no_schema_id) {
                function.result = scalar_type(descriptor, declaration.signature.result);
                if (!function.result) { function.support_error = "native exact calls currently require a canonical scalar result"; }
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
