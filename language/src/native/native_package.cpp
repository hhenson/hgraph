#include <hgl/native_package.h>

#include "descriptor/module_descriptor.h"
#include "descriptor/module_descriptor_reader.h"

#include <algorithm>
#include <fstream>
#include <ios>
#include <map>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace hgl::native
{
    namespace
    {
        using Descriptor = descriptor::ModuleDescriptor;

        template <typename T, typename Projection> void normalize(std::vector<T> &values, Projection projection) {
            std::ranges::sort(values, {}, projection);
        }

        void normalize(std::vector<std::string> &values) {
            std::ranges::sort(values);
            values.erase(std::ranges::unique(values).begin(), values.end());
        }

        [[nodiscard]] std::string_view scalar_name(ScalarType type) {
            switch (type) {
                case ScalarType::Bool: return "bool";
                case ScalarType::I64: return "i64";
                case ScalarType::F64: return "f64";
                case ScalarType::Str: return "str";
                case ScalarType::Date: return "date";
                case ScalarType::Time: return "time";
                case ScalarType::DateTime: return "datetime";
                case ScalarType::Duration: return "duration";
                case ScalarType::CivilDateTime: return "civil_datetime";
                case ScalarType::ZonedDateTime: return "zoned_datetime";
                case ScalarType::ZonedTime: return "zoned_time";
                case ScalarType::TimeZone: return "timezone";
            }
            throw std::invalid_argument{"unknown native scalar type"};
        }

        [[nodiscard]] descriptor::NativeTypeCategory type_category(TypeCategory category) {
            switch (category) {
                case TypeCategory::OpaqueState: return descriptor::NativeTypeCategory::OpaqueState;
                case TypeCategory::AtomicValue: return descriptor::NativeTypeCategory::AtomicValue;
            }
            throw std::invalid_argument{"unknown native type category"};
        }

        [[nodiscard]] descriptor::NativeDeclarationCategory declaration_category(DeclarationCategory category) {
            switch (category) {
                case DeclarationCategory::Function: return descriptor::NativeDeclarationCategory::Function;
                case DeclarationCategory::Constructor: return descriptor::NativeDeclarationCategory::Constructor;
                case DeclarationCategory::Lifecycle: return descriptor::NativeDeclarationCategory::Lifecycle;
            }
            throw std::invalid_argument{"unknown native declaration category"};
        }

        [[nodiscard]] descriptor::NativePhase phase(Phase value) {
            switch (value) {
                case Phase::Wiring: return descriptor::NativePhase::Wiring;
                case Phase::Start: return descriptor::NativePhase::Start;
                case Phase::Evaluation: return descriptor::NativePhase::Evaluation;
                case Phase::Stop: return descriptor::NativePhase::Stop;
            }
            throw std::invalid_argument{"unknown native phase"};
        }

        [[nodiscard]] descriptor::NativeEffect effect(Effect value) {
            switch (value) {
                case Effect::Mutation: return descriptor::NativeEffect::Mutation;
                case Effect::InputOutput: return descriptor::NativeEffect::InputOutput;
                case Effect::Blocking: return descriptor::NativeEffect::Blocking;
                case Effect::Allocation: return descriptor::NativeEffect::Allocation;
            }
            throw std::invalid_argument{"unknown native effect"};
        }

        [[nodiscard]] descriptor::NativeOwnership ownership(Ownership value) {
            switch (value) {
                case Ownership::Value: return descriptor::NativeOwnership::Value;
                case Ownership::Owned: return descriptor::NativeOwnership::Owned;
                case Ownership::Shared: return descriptor::NativeOwnership::Shared;
                case Ownership::Borrowed: return descriptor::NativeOwnership::Borrowed;
            }
            throw std::invalid_argument{"unknown native ownership policy"};
        }

        [[nodiscard]] descriptor::NativeExceptionPolicy exception_policy(ExceptionPolicy value) {
            switch (value) {
                case ExceptionPolicy::NoThrow: return descriptor::NativeExceptionPolicy::NoThrow;
                case ExceptionPolicy::Translated: return descriptor::NativeExceptionPolicy::Translated;
            }
            throw std::invalid_argument{"unknown native exception policy"};
        }

        [[nodiscard]] descriptor::NativeThreadSafety thread_safety(ThreadSafety value) {
            switch (value) {
                case ThreadSafety::NodeLocal: return descriptor::NativeThreadSafety::NodeLocal;
                case ThreadSafety::ThreadSafe: return descriptor::NativeThreadSafety::ThreadSafe;
                case ThreadSafety::Serialized: return descriptor::NativeThreadSafety::Serialized;
            }
            throw std::invalid_argument{"unknown native thread-safety policy"};
        }

        [[nodiscard]] descriptor::NativeValuePolicy value_policy(const ValuePolicy &value) {
            return descriptor::NativeValuePolicy{
                .ownership     = ownership(value.ownership),
                .dependent_on  = value.dependent_on,
                .mutable_value = value.mutable_value,
            };
        }

        struct Schema
        {
            std::map<ScalarType, descriptor::SchemaId>               scalar{};
            std::map<std::string, descriptor::SchemaId, std::less<>> native{};

            [[nodiscard]] descriptor::SchemaId at(const ValueType &type) const {
                switch (type.category) {
                    case ValueTypeCategory::Scalar: return scalar.at(type.scalar);
                    case ValueTypeCategory::Native:
                        {
                            const auto found = native.find(type.native_identity);
                            if (found == native.end()) {
                                throw std::invalid_argument{"native signature names undeclared type '" + type.native_identity +
                                                            "'"};
                            }
                            return found->second;
                        }
                }
                throw std::invalid_argument{"unknown native value type category"};
            }
        };

        [[nodiscard]] Schema make_schema(const Package &package, Descriptor &out) {
            Schema                  result;
            std::vector<ScalarType> scalars;
            for (const Declaration &declaration : package.declarations) {
                for (const Parameter &parameter : declaration.parameters) {
                    if (parameter.type.category == ValueTypeCategory::Scalar) { scalars.push_back(parameter.type.scalar); }
                }
                if (declaration.result_type && declaration.result_type->category == ValueTypeCategory::Scalar) {
                    scalars.push_back(declaration.result_type->scalar);
                }
            }
            std::ranges::sort(scalars);
            scalars.erase(std::ranges::unique(scalars).begin(), scalars.end());
            for (ScalarType scalar : scalars) {
                const auto id = static_cast<descriptor::SchemaId>(out.types.size());
                result.scalar.emplace(scalar, id);
                out.types.push_back(descriptor::TypeRecord{
                    .category    = descriptor::TypeCategory::Scalar,
                    .scalar_name = std::string{scalar_name(scalar)},
                });
            }

            std::vector<std::string> identities;
            identities.reserve(package.types.size());
            for (const Type &type : package.types) { identities.push_back(type.identity); }
            normalize(identities);
            for (const std::string &identity : identities) {
                if (result.native.contains(identity)) { continue; }
                const auto id = static_cast<descriptor::SchemaId>(out.types.size());
                result.native.emplace(identity, id);
                out.types.push_back(descriptor::TypeRecord{
                    .category         = descriptor::TypeCategory::Symbol,
                    .nominal_identity = identity,
                });
            }
            return result;
        }

        [[nodiscard]] Descriptor describe(const Package &package) {
            Descriptor out;
            out.module_identity           = package.module_identity;
            out.language_version          = package.language_version;
            out.provider_identity         = package.provider_identity.empty() ? package.module_identity : package.provider_identity;
            out.provider_requirements     = package.provider_requirements;
            out.build.public_headers      = package.build.public_headers;
            out.build.cmake_packages      = package.build.cmake_packages;
            out.build.imported_targets    = package.build.imported_targets;
            out.build.runtime_images      = package.build.runtime_images;
            out.build.registration_symbol = package.build.registration_symbol;
            if (package.build.lifecycle) {
                out.build.lifecycle = descriptor::LifecycleMetadata{
                    .abi_version  = package.build.lifecycle->abi_version,
                    .query_symbol = package.build.lifecycle->query_symbol,
                };
            }
            for (const Type &type : package.types) { out.build.public_headers.push_back(type.public_header); }
            normalize(out.provider_requirements);
            normalize(out.build.public_headers);
            normalize(out.build.cmake_packages);
            normalize(out.build.imported_targets);
            normalize(out.build.runtime_images);

            const Schema schema = make_schema(package, out);

            std::vector<Type> types = package.types;
            normalize(types, &Type::identity);
            for (const Type &type : types) {
                out.native_types.push_back(descriptor::NativeTypeDeclaration{
                    .category      = type_category(type.category),
                    .identity      = type.identity,
                    .cpp_type      = type.cpp_type,
                    .public_header = type.public_header,
                });
            }

            std::vector<Declaration> declarations = package.declarations;
            normalize(declarations, &Declaration::identity);
            for (const Declaration &declaration : declarations) {
                descriptor::NativeDeclaration native;
                native.category         = declaration_category(declaration.category);
                native.identity         = declaration.identity;
                native.cpp_symbol       = declaration.cpp_symbol;
                native.result           = value_policy(declaration.result_policy);
                native.exception_policy = exception_policy(declaration.exception_policy);
                native.thread_safety    = thread_safety(declaration.thread_safety);
                for (const Parameter &parameter : declaration.parameters) {
                    native.signature.parameters.push_back(descriptor::Parameter{
                        .name             = parameter.name,
                        .binding_identity = declaration.identity + "::" + parameter.name,
                        .is_const         = parameter.is_const,
                        .type             = schema.at(parameter.type),
                    });
                    native.parameters.push_back(descriptor::NativeParameterPolicy{
                        .name  = parameter.name,
                        .value = value_policy(parameter.policy),
                    });
                }
                if (declaration.result_type) { native.signature.result = schema.at(*declaration.result_type); }
                for (Phase allowed : declaration.phases) { native.phases.push_back(phase(allowed)); }
                for (Effect observable : declaration.effects) { native.effects.push_back(effect(observable)); }
                std::ranges::sort(native.phases);
                std::ranges::sort(native.effects);
                out.native_declarations.push_back(std::move(native));
            }
            descriptor::seal(out);
            return out;
        }
    }  // namespace

    std::string descriptor_json(const Package &package) {
        Descriptor out = describe(package);
        if (const std::optional<descriptor::ReadError> invalid = descriptor::validate(out)) {
            throw std::invalid_argument{invalid->path + ": " + invalid->message};
        }
        return descriptor::to_json(out);
    }

    void write_descriptor(const Package &package, const std::filesystem::path &path) {
        const std::string json = descriptor_json(package);
        std::ofstream     out{path, std::ios::binary | std::ios::trunc};
        if (!out) { throw std::ios_base::failure{"open native descriptor '" + path.string() + "'"}; }
        out.write(json.data(), static_cast<std::streamsize>(json.size()));
        if (!out) { throw std::ios_base::failure{"write native descriptor '" + path.string() + "'"}; }
    }
}  // namespace hgl::native
