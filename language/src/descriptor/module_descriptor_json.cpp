#include "descriptor/module_descriptor.h"

#include "syntax/temporal.h"

#include <array>
#include <cmath>
#include <iomanip>
#include <limits>
#include <locale>
#include <ostream>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgl::descriptor
{
    namespace
    {
        [[nodiscard]] std::string_view declaration_category_name(DeclarationCategory category) noexcept {
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

        [[nodiscard]] std::string_view type_category_name(TypeCategory category) noexcept {
            switch (category) {
                case TypeCategory::Void: return "void";
                case TypeCategory::Scalar: return "scalar";
                case TypeCategory::Symbol: return "symbol";
                case TypeCategory::Tuple: return "tuple";
                case TypeCategory::List: return "list";
                case TypeCategory::Set: return "set";
                case TypeCategory::Map: return "map";
                case TypeCategory::Rolling: return "rolling";
                case TypeCategory::Atomic: return "atomic";
                case TypeCategory::Iterator: return "iterator";
                case TypeCategory::Callable: return "callable";
                case TypeCategory::Capability: return "capability";
                case TypeCategory::HarnessSequence: return "harness-sequence";
                case TypeCategory::Deferred: return "deferred";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view constant_category_name(ConstantExpressionCategory category) noexcept {
            switch (category) {
                case ConstantExpressionCategory::Literal: return "literal";
                case ConstantExpressionCategory::Parameter: return "parameter";
                case ConstantExpressionCategory::Unary: return "unary";
                case ConstantExpressionCategory::Binary: return "binary";
                case ConstantExpressionCategory::Index: return "index";
                case ConstantExpressionCategory::Field: return "field";
                case ConstantExpressionCategory::Sequence: return "sequence";
                case ConstantExpressionCategory::Tuple: return "tuple";
                case ConstantExpressionCategory::Construct: return "construct";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view constraint_category_name(ConstraintCategory category) noexcept {
            switch (category) {
                case ConstraintCategory::Symbol: return "symbol";
                case ConstraintCategory::Type: return "type";
                case ConstraintCategory::Value: return "value";
                case ConstraintCategory::Set: return "set";
                case ConstraintCategory::Call: return "call";
                case ConstraintCategory::Operator: return "operator";
                case ConstraintCategory::Relation: return "relation";
                case ConstraintCategory::Not: return "not";
                case ConstraintCategory::Logic: return "logic";
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

        void schema_reference(std::ostream &out, SchemaId value) {
            if (value == no_schema_id) {
                out << "null";
            } else {
                out << value;
            }
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

        void reference_array(std::ostream &out, const std::vector<SchemaId> &values, std::string_view indent) {
            if (values.empty()) {
                out << "[]";
                return;
            }
            out << "[\n";
            for (std::size_t index = 0; index < values.size(); ++index) {
                out << indent << "  ";
                schema_reference(out, values[index]);
                out << (index + 1U == values.size() ? "\n" : ",\n");
            }
            out << indent << ']';
        }

        [[nodiscard]] std::string floating_spelling(double value) {
            if (std::isnan(value)) { return "nan"; }
            if (std::isinf(value)) { return std::signbit(value) ? "-inf" : "inf"; }
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
            return out.str();
        }

        void literal(std::ostream &out, const ir::hir::Constant &literal) {
            out << "{\n";
            std::visit(
                [&](const auto &value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, ir::hir::NullValue>) {
                        out << "            \"kind\": \"null\"\n";
                    } else if constexpr (std::is_same_v<T, ir::hir::PlaceholderValue>) {
                        out << "            \"kind\": \"placeholder\"\n";
                    } else if constexpr (std::is_same_v<T, bool>) {
                        out << "            \"kind\": \"bool\",\n            \"value\": " << (value ? "true" : "false") << '\n';
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        out << "            \"kind\": \"i64\",\n            \"value\": ";
                        quote_json(out, std::to_string(value));
                        out << '\n';
                    } else if constexpr (std::is_same_v<T, double>) {
                        out << "            \"kind\": \"f64\",\n            \"value\": ";
                        quote_json(out, floating_spelling(value));
                        out << '\n';
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        out << "            \"kind\": \"str\",\n            \"value\": ";
                        quote_json(out, value);
                        out << '\n';
                    } else {
                        out << "            \"kind\": ";
                        quote_json(out, syntax::temporal_kind_name(value.kind));
                        out << ",\n            \"value\": ";
                        quote_json(out, syntax::canonical_spelling(value));
                        out << '\n';
                    }
                },
                literal);
            out << "          }";
        }

        void generic_parameters(std::ostream &out, const std::vector<GenericParameter> &parameters, std::string_view indent) {
            if (parameters.empty()) {
                out << "[]";
                return;
            }
            out << "[\n";
            for (std::size_t index = 0; index < parameters.size(); ++index) {
                const GenericParameter &parameter = parameters[index];
                out << indent << "  {\n" << indent << "    \"name\": ";
                quote_json(out, parameter.name);
                out << ",\n"
                    << indent << "    \"kind\": \"" << (parameter.is_const ? "const" : "type") << "\",\n"
                    << indent << "    \"binding\": ";
                quote_json(out, parameter.binding_identity);
                out << ",\n" << indent << "    \"type\": ";
                schema_reference(out, parameter.type);
                out << "\n" << indent << "  }" << (index + 1U == parameters.size() ? "\n" : ",\n");
            }
            out << indent << ']';
        }

        void parameters(std::ostream &out, const std::vector<Parameter> &parameters, std::string_view indent) {
            if (parameters.empty()) {
                out << "[]";
                return;
            }
            out << "[\n";
            for (std::size_t index = 0; index < parameters.size(); ++index) {
                const Parameter &parameter = parameters[index];
                out << indent << "  {\n" << indent << "    \"name\": ";
                quote_json(out, parameter.name);
                out << ",\n"
                    << indent << "    \"kind\": \"" << (parameter.is_const ? "const" : "signal") << "\",\n"
                    << indent << "    \"binding\": ";
                quote_json(out, parameter.binding_identity);
                out << ",\n" << indent << "    \"type\": ";
                schema_reference(out, parameter.type);
                out << ",\n" << indent << "    \"default\": ";
                schema_reference(out, parameter.default_value);
                out << "\n" << indent << "  }" << (index + 1U == parameters.size() ? "\n" : ",\n");
            }
            out << indent << ']';
        }

        void signature(std::ostream &out, const Signature &signature, std::string_view indent) {
            out << "{\n" << indent << "  \"generic_parameters\": ";
            generic_parameters(out, signature.generics, std::string{indent} + "  ");
            out << ",\n" << indent << "  \"parameters\": ";
            parameters(out, signature.parameters, std::string{indent} + "  ");
            out << ",\n" << indent << "  \"result\": ";
            schema_reference(out, signature.result);
            out << ",\n" << indent << "  \"requires\": ";
            schema_reference(out, signature.requirements);
            out << "\n" << indent << '}';
        }

        void fields(std::ostream &out, const std::vector<StructField> &fields, std::string_view indent) {
            if (fields.empty()) {
                out << "[]";
                return;
            }
            out << "[\n";
            for (std::size_t index = 0; index < fields.size(); ++index) {
                const StructField &field = fields[index];
                out << indent << "  {\n" << indent << "    \"name\": ";
                quote_json(out, field.name);
                out << ",\n" << indent << "    \"type\": ";
                schema_reference(out, field.type);
                out << ",\n"
                    << indent << "    \"optional\": " << (field.optional ? "true" : "false") << ",\n"
                    << indent << "    \"default\": ";
                schema_reference(out, field.default_value);
                out << ",\n" << indent << "    \"origin\": ";
                quote_json(out, field.origin_identity);
                out << "\n" << indent << "  }" << (index + 1U == fields.size() ? "\n" : ",\n");
            }
            out << indent << ']';
        }

        void type_records(std::ostream &out, const std::vector<TypeRecord> &records) {
            out << '[';
            if (!records.empty()) { out << '\n'; }
            for (std::size_t index = 0; index < records.size(); ++index) {
                const TypeRecord &record = records[index];
                out << "      {\n        \"id\": " << index << ",\n        \"kind\": ";
                quote_json(out, type_category_name(record.category));
                if (!record.scalar_name.empty()) {
                    out << ",\n        \"name\": ";
                    quote_json(out, record.scalar_name);
                }
                if (!record.nominal_identity.empty()) {
                    out << ",\n        \"identity\": ";
                    quote_json(out, record.nominal_identity);
                }
                if (!record.binding_identity.empty()) {
                    out << ",\n        \"binding\": ";
                    quote_json(out, record.binding_identity);
                }
                if (!record.children.empty()) {
                    out << ",\n        \"children\": ";
                    reference_array(out, record.children, "        ");
                }
                if (!record.arguments.empty()) {
                    out << ",\n        \"arguments\": [\n";
                    for (std::size_t argument_index = 0; argument_index < record.arguments.size(); ++argument_index) {
                        const TypeArgument &argument = record.arguments[argument_index];
                        out << "          {\n            \"kind\": \""
                            << (argument.category == TypeArgumentCategory::Type ? "type" : "constant")
                            << "\",\n            \"reference\": ";
                        schema_reference(out, argument.reference);
                        out << "\n          }" << (argument_index + 1U == record.arguments.size() ? "\n" : ",\n");
                    }
                    out << "        ]";
                }
                if (record.size != no_schema_id) {
                    out << ",\n        \"size\": ";
                    schema_reference(out, record.size);
                }
                if (record.min_size != no_schema_id) {
                    out << ",\n        \"min_size\": ";
                    schema_reference(out, record.min_size);
                }
                if (record.unbounded) { out << ",\n        \"unbounded\": true"; }
                out << "\n      }" << (index + 1U == records.size() ? "\n" : ",\n");
            }
            if (!records.empty()) { out << "    "; }
            out << ']';
        }

        void constant_records(std::ostream &out, const std::vector<ConstantExpressionRecord> &records) {
            out << '[';
            if (!records.empty()) { out << '\n'; }
            for (std::size_t index = 0; index < records.size(); ++index) {
                const ConstantExpressionRecord &record = records[index];
                out << "      {\n        \"id\": " << index << ",\n        \"kind\": ";
                quote_json(out, constant_category_name(record.category));
                if (record.literal) {
                    out << ",\n        \"literal\": ";
                    literal(out, *record.literal);
                }
                if (!record.parameter_identity.empty()) {
                    out << ",\n        \"parameter\": ";
                    quote_json(out, record.parameter_identity);
                }
                if (!record.operator_spelling.empty() && (record.category == ConstantExpressionCategory::Unary ||
                                                          record.category == ConstantExpressionCategory::Binary)) {
                    out << ",\n        \"operator\": ";
                    quote_json(out, record.operator_spelling);
                }
                if (record.lhs != no_schema_id) {
                    out << ",\n        \"lhs\": ";
                    schema_reference(out, record.lhs);
                }
                if (record.rhs != no_schema_id) {
                    out << ",\n        \"rhs\": ";
                    schema_reference(out, record.rhs);
                }
                if (!record.member.empty()) {
                    out << ",\n        \"member\": ";
                    quote_json(out, record.member);
                }
                if (!record.elements.empty()) {
                    out << ",\n        \"elements\": [\n";
                    for (std::size_t element_index = 0; element_index < record.elements.size(); ++element_index) {
                        const ConstantElement &element = record.elements[element_index];
                        out << "          {";
                        if (element.key != no_schema_id) {
                            out << "\n            \"key\": ";
                            schema_reference(out, element.key);
                            out << ",";
                        }
                        out << "\n            \"value\": ";
                        schema_reference(out, element.value);
                        out << "\n          }" << (element_index + 1U == record.elements.size() ? "\n" : ",\n");
                    }
                    out << "        ]";
                }
                if (!record.items.empty()) {
                    out << ",\n        \"items\": ";
                    reference_array(out, record.items, "        ");
                }
                if (record.constructed_type != no_schema_id) {
                    out << ",\n        \"type\": ";
                    schema_reference(out, record.constructed_type);
                }
                if (!record.arguments.empty()) {
                    out << ",\n        \"arguments\": [\n";
                    for (std::size_t argument_index = 0; argument_index < record.arguments.size(); ++argument_index) {
                        const ConstantArgument &argument = record.arguments[argument_index];
                        out << "          {\n            \"name\": ";
                        quote_json(out, argument.name);
                        out << ",\n            \"value\": ";
                        schema_reference(out, argument.value);
                        out << "\n          }" << (argument_index + 1U == record.arguments.size() ? "\n" : ",\n");
                    }
                    out << "        ]";
                }
                if (record.category == ConstantExpressionCategory::Construct) {
                    out << ",\n        \"delta\": " << (record.delta ? "true" : "false");
                }
                out << "\n      }" << (index + 1U == records.size() ? "\n" : ",\n");
            }
            if (!records.empty()) { out << "    "; }
            out << ']';
        }

        void constraint_records(std::ostream &out, const std::vector<ConstraintRecord> &records) {
            out << '[';
            if (!records.empty()) { out << '\n'; }
            for (std::size_t index = 0; index < records.size(); ++index) {
                const ConstraintRecord &record = records[index];
                out << "      {\n        \"id\": " << index << ",\n        \"kind\": ";
                quote_json(out, constraint_category_name(record.category));
                if (!record.identity.empty()) {
                    out << ",\n        \"identity\": ";
                    quote_json(out, record.identity);
                }
                if (!record.registry_name.empty()) {
                    out << ",\n        \"registry_name\": ";
                    quote_json(out, record.registry_name);
                }
                if (!record.operator_spelling.empty()) {
                    out << ",\n        \"operator\": ";
                    quote_json(out, record.operator_spelling);
                }
                if (!record.relation_category.empty()) {
                    out << ",\n        \"category\": ";
                    quote_json(out, record.relation_category);
                }
                const auto reference = [&](std::string_view name, SchemaId value) {
                    if (value == no_schema_id) { return; }
                    out << ",\n        \"" << name << "\": ";
                    schema_reference(out, value);
                };
                reference("type", record.type);
                reference("value", record.value);
                reference("lhs", record.lhs);
                reference("rhs", record.rhs);
                reference("operand", record.operand);
                reference("result", record.result);
                if (!record.elements.empty()) {
                    out << ",\n        \"elements\": ";
                    reference_array(out, record.elements, "        ");
                }
                if (!record.arguments.empty()) {
                    out << ",\n        \"arguments\": ";
                    reference_array(out, record.arguments, "        ");
                }
                out << "\n      }" << (index + 1U == records.size() ? "\n" : ",\n");
            }
            if (!records.empty()) { out << "    "; }
            out << ']';
        }
    }  // namespace

    std::string to_json(const ModuleDescriptor &descriptor) {
        std::ostringstream out;
        out.imbue(std::locale::classic());
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
            quote_json(out, declaration_category_name(declaration.category));
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
                out << ",\n      \"abstract\": " << (declaration.abstract ? "true" : "false")
                    << ",\n      \"generic_parameters\": ";
                generic_parameters(out, declaration.signature.generics, "      ");
                out << ",\n      \"parents\": ";
                reference_array(out, declaration.parents, "      ");
                out << ",\n      \"requires\": ";
                schema_reference(out, declaration.signature.requirements);
                out << ",\n      \"fields\": ";
                fields(out, declaration.fields, "      ");
            } else {
                out << ",\n      \"signature\": ";
                signature(out, declaration.signature, "      ");
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
            out << ",\n        \"signature\": ";
            signature(out, implementation.signature, "        ");
            out << "\n      }" << (index + 1U == descriptor.implementations.size() ? "\n" : ",\n");
        }
        if (!descriptor.implementations.empty()) { out << "    "; }
        out << "],\n    \"requires\": ";
        string_array(out, descriptor.provider_requirements, "    ");
        out << "\n  },\n  \"schema\": {\n    \"types\": ";
        type_records(out, descriptor.types);
        out << ",\n    \"constant_expressions\": ";
        constant_records(out, descriptor.constant_expressions);
        out << ",\n    \"constraints\": ";
        constraint_records(out, descriptor.constraints);
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
