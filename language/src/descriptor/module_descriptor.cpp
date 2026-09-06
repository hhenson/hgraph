#include "descriptor/module_descriptor.h"

#include <algorithm>
#include <array>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <utility>

namespace hgl::descriptor
{
    namespace
    {
        template <typename T, typename Projection> void normalize(std::vector<T> &values, Projection projection) {
            std::ranges::sort(values, {}, projection);
            values.erase(std::ranges::unique(values, {}, projection).begin(), values.end());
        }

        void normalize(std::vector<std::string> &values) {
            std::ranges::sort(values);
            values.erase(std::ranges::unique(values).begin(), values.end());
        }

        [[nodiscard]] ExecutionKind execution_kind(hgraph_ir::CallableKind kind) noexcept {
            return kind == hgraph_ir::CallableKind::Composition ? ExecutionKind::Composition : ExecutionKind::RuntimeNode;
        }

        [[nodiscard]] std::string registry_name(std::string_view configured, std::string_view identity) {
            return std::string{configured.empty() ? identity : configured};
        }

        [[nodiscard]] std::string_view category_name(DeclarationCategory category) noexcept {
            switch (category) {
                case DeclarationCategory::Structure: return "structure";
                case DeclarationCategory::Operator: return "operator";
                case DeclarationCategory::Function: return "function";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view execution_name(ExecutionKind execution) noexcept {
            switch (execution) {
                case ExecutionKind::None: return "none";
                case ExecutionKind::Composition: return "composition";
                case ExecutionKind::RuntimeNode: return "runtime-node";
            }
            std::unreachable();
        }

        void quote_json(std::ostream &out, std::string_view value) {
            static constexpr std::array<char, 16> hex{'0', '1', '2', '3', '4', '5', '6', '7',
                                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
            out << '"';
            for (const unsigned char character : value) {
                switch (character) {
                    case '"': out << "\\\""; break;
                    case '\\': out << "\\\\"; break;
                    case '\b': out << "\\b"; break;
                    case '\f': out << "\\f"; break;
                    case '\n': out << "\\n"; break;
                    case '\r': out << "\\r"; break;
                    case '\t': out << "\\t"; break;
                    default:
                        if (character < 0x20U) {
                            out << "\\u00" << hex[character >> 4U] << hex[character & 0x0fU];
                        } else {
                            out << static_cast<char>(character);
                        }
                }
            }
            out << '"';
        }

        void string_array(std::ostream &out, const std::vector<std::string> &values, std::string_view indent) {
            if (values.empty()) {
                out << "[]";
                return;
            }
            out << "[\n";
            for (std::size_t index = 0; index < values.size(); ++index) {
                out << indent << "  ";
                quote_json(out, values[index]);
                out << (index + 1U == values.size() ? "\n" : ",\n");
            }
            out << indent << ']';
        }
    }  // namespace

    ModuleDescriptor describe_module(const hgraph_ir::Module &module, DescribeOptions options) {
        ModuleDescriptor result;
        result.module_identity           = module.path;
        result.language_version          = std::move(options.language_version);
        result.provider_identity         = options.provider_identity.empty() ? module.path : std::move(options.provider_identity);
        result.provider_requirements     = module.provider_requirements;
        result.build.public_headers      = std::move(options.public_headers);
        result.build.cmake_packages      = std::move(options.cmake_packages);
        result.build.imported_targets    = std::move(options.imported_targets);
        result.build.registration_symbol = std::move(options.registration_symbol);

        for (const hgraph_ir::StructContract &structure : module.structures) {
            if (!structure.exported) { continue; }
            result.interface.push_back(InterfaceDeclaration{
                .category = DeclarationCategory::Structure, .identity = structure.identity, .abstract = structure.abstract});
        }
        for (const hgraph_ir::OperatorContract &operation : module.operators) {
            if (operation.imported) { continue; }
            result.interface.push_back(
                InterfaceDeclaration{.category      = DeclarationCategory::Operator,
                                     .identity      = operation.identity,
                                     .registry_name = registry_name(operation.registry_name, operation.identity)});
        }
        for (const hgraph_ir::Callable &callable : module.callables) {
            if (callable.visibility == hgraph_ir::CallableVisibility::Export) {
                result.interface.push_back(InterfaceDeclaration{.category  = DeclarationCategory::Function,
                                                                .identity  = callable.identity,
                                                                .execution = execution_kind(callable.kind)});
            } else if (callable.visibility == hgraph_ir::CallableVisibility::Implementation) {
                result.implementations.push_back(Implementation{
                    .identity               = callable.identity,
                    .operator_identity      = callable.operator_identity,
                    .operator_registry_name = registry_name(callable.operator_registry_name, callable.operator_identity),
                    .execution              = execution_kind(callable.kind)});
            }
        }

        normalize(result.interface, &InterfaceDeclaration::identity);
        normalize(result.implementations, &Implementation::identity);
        normalize(result.provider_requirements);
        normalize(result.build.public_headers);
        normalize(result.build.cmake_packages);
        normalize(result.build.imported_targets);
        return result;
    }

    std::string to_json(const ModuleDescriptor &descriptor) {
        std::ostringstream out;
        out << "{\n"
            << "  \"format\": \"hgl.module\",\n"
            << "  \"format_version\": " << descriptor.format_version << ",\n"
            << "  \"module\": {\n"
            << "    \"identity\": ";
        quote_json(out, descriptor.module_identity);
        out << ",\n    \"language_version\": ";
        quote_json(out, descriptor.language_version);
        out << "\n  },\n  \"interface\": [";
        if (!descriptor.interface.empty()) { out << '\n'; }
        for (std::size_t index = 0; index < descriptor.interface.size(); ++index) {
            const InterfaceDeclaration &declaration = descriptor.interface[index];
            out << "    {\n      \"category\": ";
            quote_json(out, category_name(declaration.category));
            out << ",\n      \"identity\": ";
            quote_json(out, declaration.identity);
            if (!declaration.registry_name.empty()) {
                out << ",\n      \"registry_name\": ";
                quote_json(out, declaration.registry_name);
            }
            if (declaration.execution != ExecutionKind::None) {
                out << ",\n      \"execution\": ";
                quote_json(out, execution_name(declaration.execution));
            }
            if (declaration.category == DeclarationCategory::Structure) {
                out << ",\n      \"abstract\": " << (declaration.abstract ? "true" : "false");
            }
            out << "\n    }" << (index + 1U == descriptor.interface.size() ? "\n" : ",\n");
        }
        if (!descriptor.interface.empty()) { out << "  "; }
        out << "],\n  \"provider\": {\n    \"identity\": ";
        quote_json(out, descriptor.provider_identity);
        out << ",\n    \"implementations\": [";
        if (!descriptor.implementations.empty()) { out << '\n'; }
        for (std::size_t index = 0; index < descriptor.implementations.size(); ++index) {
            const Implementation &implementation = descriptor.implementations[index];
            out << "      {\n        \"identity\": ";
            quote_json(out, implementation.identity);
            out << ",\n        \"operator\": ";
            quote_json(out, implementation.operator_identity);
            out << ",\n        \"registry_name\": ";
            quote_json(out, implementation.operator_registry_name);
            out << ",\n        \"execution\": ";
            quote_json(out, execution_name(implementation.execution));
            out << "\n      }" << (index + 1U == descriptor.implementations.size() ? "\n" : ",\n");
        }
        if (!descriptor.implementations.empty()) { out << "    "; }
        out << "],\n    \"requires\": ";
        string_array(out, descriptor.provider_requirements, "    ");
        out << "\n  },\n  \"build\": {\n    \"public_headers\": ";
        string_array(out, descriptor.build.public_headers, "    ");
        out << ",\n    \"cmake_packages\": ";
        string_array(out, descriptor.build.cmake_packages, "    ");
        out << ",\n    \"imported_targets\": ";
        string_array(out, descriptor.build.imported_targets, "    ");
        out << ",\n    \"registration\": {\n      \"kind\": \"cpp\",\n      \"symbol\": ";
        quote_json(out, descriptor.build.registration_symbol);
        out << "\n    }\n  }\n}\n";
        return out.str();
    }
}  // namespace hgl::descriptor
