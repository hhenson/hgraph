#include "descriptor/module_descriptor.h"

#include <algorithm>
#include <string_view>
#include <type_traits>
#include <unordered_map>
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

        [[nodiscard]] TypeCategory type_category(ir::hir::TypeKind kind) noexcept {
            using ir::hir::TypeKind;
            switch (kind) {
                case TypeKind::Void: return TypeCategory::Void;
                case TypeKind::Scalar: return TypeCategory::Scalar;
                case TypeKind::Symbol: return TypeCategory::Symbol;
                case TypeKind::Tuple: return TypeCategory::Tuple;
                case TypeKind::List: return TypeCategory::List;
                case TypeKind::Set: return TypeCategory::Set;
                case TypeKind::Map: return TypeCategory::Map;
                case TypeKind::Rolling: return TypeCategory::Rolling;
                case TypeKind::Atomic: return TypeCategory::Atomic;
                case TypeKind::Reference: return TypeCategory::Reference;
                case TypeKind::Iterator: return TypeCategory::Iterator;
                case TypeKind::Callable: return TypeCategory::Callable;
                case TypeKind::Capability: return TypeCategory::Capability;
                case TypeKind::HarnessSequence: return TypeCategory::HarnessSequence;
                case TypeKind::Deferred: return TypeCategory::Deferred;
            }
            std::unreachable();
        }

        [[nodiscard]] ConstantExpressionCategory constant_category(hgraph_ir::ConstExprKind kind) noexcept {
            using hgraph_ir::ConstExprKind;
            switch (kind) {
                case ConstExprKind::Literal: return ConstantExpressionCategory::Literal;
                case ConstExprKind::Parameter: return ConstantExpressionCategory::Parameter;
                case ConstExprKind::Unary: return ConstantExpressionCategory::Unary;
                case ConstExprKind::Binary: return ConstantExpressionCategory::Binary;
                case ConstExprKind::Index: return ConstantExpressionCategory::Index;
                case ConstExprKind::Field: return ConstantExpressionCategory::Field;
                case ConstExprKind::Sequence: return ConstantExpressionCategory::Sequence;
                case ConstExprKind::Tuple: return ConstantExpressionCategory::Tuple;
                case ConstExprKind::Construct: return ConstantExpressionCategory::Construct;
            }
            std::unreachable();
        }

        [[nodiscard]] std::string unary_spelling(ir::hir::UnaryOp operation) {
            return operation == ir::hir::UnaryOp::Negate ? "-" : "!";
        }

        [[nodiscard]] std::string relation_spelling(hgraph_ir::ConstraintRelationOp operation) {
            using hgraph_ir::ConstraintRelationOp;
            switch (operation) {
                case ConstraintRelationOp::Equal: return "equal";
                case ConstraintRelationOp::In: return "in";
                case ConstraintRelationOp::Is: return "is";
            }
            std::unreachable();
        }

        class SchemaBuilder
        {
          public:
            SchemaBuilder(const hgraph_ir::Module &source, ModuleDescriptor &result) : source_{source}, result_{result} {}

            [[nodiscard]] Signature signature(const std::vector<hgraph_ir::GenericParameter> &generics,
                                              const std::vector<hgraph_ir::Parameter> &parameters, hgraph_ir::TypeId result,
                                              hgraph_ir::ConstraintId requirements) {
                Signature snapshot;
                for (const hgraph_ir::GenericParameter &generic : generics) {
                    snapshot.generics.push_back(GenericParameter{
                        .name             = generic.name,
                        .binding_identity = binding_identity(generic.binding, generic.name),
                        .is_const         = generic.is_const,
                        .type             = type(generic.type),
                    });
                }
                for (const hgraph_ir::Parameter &parameter : parameters) {
                    snapshot.parameters.push_back(Parameter{
                        .name             = parameter.name,
                        .binding_identity = binding_identity(parameter.binding, parameter.name),
                        .is_const         = parameter.is_const,
                        .type             = type(parameter.type),
                        .default_value    = constant(parameter.default_value),
                    });
                }
                snapshot.result       = type(result);
                snapshot.requirements = constraint(requirements);
                return snapshot;
            }

            [[nodiscard]] StructField field(const hgraph_ir::StructField &source) {
                return StructField{
                    .name            = source.name,
                    .type            = type(source.type),
                    .default_value   = constant(source.default_value),
                    .origin_identity = source.origin_identity,
                    .optional        = source.optional,
                };
            }

            [[nodiscard]] SchemaId type_reference(hgraph_ir::TypeId source) { return type(source); }

          private:
            [[nodiscard]] std::string binding_identity(hgraph_ir::BindingId id, std::string_view fallback) const {
                if (!id.valid()) { return std::string{fallback}; }
                const hgraph_ir::Binding &binding = source_.bindings.at(id.value);
                return binding.owner_identity.empty() ? binding.name : binding.owner_identity + "::" + binding.name;
            }

            [[nodiscard]] SchemaId type(hgraph_ir::TypeId source_id) {
                if (!source_id.valid()) { return no_schema_id; }
                if (const auto found = types_.find(source_id.value); found != types_.end()) { return found->second; }

                const SchemaId id{static_cast<SchemaId>(result_.types.size())};
                types_.emplace(source_id.value, id);
                result_.types.emplace_back();

                const hgraph_ir::Type &source = source_.types.at(source_id.value);
                TypeRecord             record;
                record.category         = type_category(source.kind);
                record.scalar_name      = source.kind == ir::hir::TypeKind::Scalar
                                              ? std::string{ir::hir::scalar_type_name(source.scalar)}
                                              : std::string{};
                record.nominal_identity = source.nominal_identity;
                if (source.binding.valid()) { record.binding_identity = binding_identity(source.binding, source.nominal_identity); }
                record.unbounded = source.unbounded;
                for (hgraph_ir::TypeId child : source.children) { record.children.push_back(type(child)); }
                for (const hgraph_ir::TypeArgument &argument : source.arguments) {
                    if (argument.type) {
                        record.arguments.push_back(TypeArgument{TypeArgumentCategory::Type, type(*argument.type)});
                    } else if (argument.value) {
                        record.arguments.push_back(TypeArgument{TypeArgumentCategory::Constant, constant(*argument.value)});
                    }
                }
                record.size       = constant(source.size);
                record.min_size   = constant(source.min_size);
                result_.types[id] = std::move(record);
                return id;
            }

            [[nodiscard]] SchemaId constant(hgraph_ir::ConstExprId source_id) {
                if (!source_id.valid()) { return no_schema_id; }
                if (const auto found = constants_.find(source_id.value); found != constants_.end()) { return found->second; }

                const SchemaId id{static_cast<SchemaId>(result_.constant_expressions.size())};
                constants_.emplace(source_id.value, id);
                result_.constant_expressions.emplace_back();

                const hgraph_ir::ConstExpr &source = source_.const_exprs.at(source_id.value);
                ConstantExpressionRecord    record;
                record.category           = constant_category(source.kind);
                record.literal            = source.literal;
                record.parameter_identity = binding_identity(source.parameter_binding, source.parameter);
                if (source.kind == hgraph_ir::ConstExprKind::Unary) {
                    record.operator_spelling = unary_spelling(source.unary);
                } else if (source.kind == hgraph_ir::ConstExprKind::Binary) {
                    record.operator_spelling = ir::hir::binary_op_spelling(source.binary);
                }
                record.lhs    = constant(source.lhs);
                record.rhs    = constant(source.rhs);
                record.member = source.member;
                for (const hgraph_ir::ConstElement &element : source.elements) {
                    record.elements.push_back(ConstantElement{constant(element.key), constant(element.value)});
                }
                for (hgraph_ir::ConstExprId item : source.items) { record.items.push_back(constant(item)); }
                record.constructed_type = type(source.constructed_type);
                for (const hgraph_ir::ConstArgument &argument : source.arguments) {
                    record.arguments.push_back(ConstantArgument{argument.name, constant(argument.value)});
                }
                record.delta                     = source.delta;
                result_.constant_expressions[id] = std::move(record);
                return id;
            }

            [[nodiscard]] SchemaId constraint(hgraph_ir::ConstraintId source_id) {
                if (!source_id.valid()) { return no_schema_id; }
                if (const auto found = constraints_.find(source_id.value); found != constraints_.end()) { return found->second; }

                const SchemaId id{static_cast<SchemaId>(result_.constraints.size())};
                constraints_.emplace(source_id.value, id);
                result_.constraints.emplace_back();

                const hgraph_ir::Constraint &source = source_.constraints.at(source_id.value);
                ConstraintRecord             record;
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, hgraph_ir::ConstraintSymbol>) {
                            record.category = ConstraintCategory::Symbol;
                            record.identity = node.identity;
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintType>) {
                            record.category = ConstraintCategory::Type;
                            record.type     = type(node.type);
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintValue>) {
                            record.category = ConstraintCategory::Value;
                            record.value    = constant(node.value);
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintSet>) {
                            record.category = ConstraintCategory::Set;
                            for (hgraph_ir::ConstraintId element : node.elements) {
                                record.elements.push_back(constraint(element));
                            }
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintCall>) {
                            record.category = ConstraintCategory::Call;
                            record.identity = node.function_identity;
                            for (hgraph_ir::ConstraintId argument : node.arguments) {
                                record.arguments.push_back(constraint(argument));
                            }
                        } else if constexpr (std::is_same_v<T, hgraph_ir::OperatorRequirement>) {
                            record.category      = ConstraintCategory::Operator;
                            record.identity      = node.operator_identity;
                            record.registry_name = registry_name(node.operator_registry_name, node.operator_identity);
                            for (hgraph_ir::ConstraintId argument : node.arguments) {
                                record.arguments.push_back(constraint(argument));
                            }
                            record.result = type(node.result);
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintRelation>) {
                            record.category          = ConstraintCategory::Relation;
                            record.operator_spelling = relation_spelling(node.op);
                            record.lhs               = constraint(node.lhs);
                            record.rhs               = constraint(node.rhs);
                            record.relation_category = node.category;
                        } else if constexpr (std::is_same_v<T, hgraph_ir::ConstraintNot>) {
                            record.category = ConstraintCategory::Not;
                            record.operand  = constraint(node.operand);
                        } else {
                            record.category          = ConstraintCategory::Logic;
                            record.operator_spelling = node.op == hgraph_ir::ConstraintLogicOp::And ? "and" : "or";
                            record.lhs               = constraint(node.lhs);
                            record.rhs               = constraint(node.rhs);
                        }
                    },
                    source.node);
                result_.constraints[id] = std::move(record);
                return id;
            }

            const hgraph_ir::Module                    &source_;
            ModuleDescriptor                           &result_;
            std::unordered_map<std::uint32_t, SchemaId> types_{};
            std::unordered_map<std::uint32_t, SchemaId> constants_{};
            std::unordered_map<std::uint32_t, SchemaId> constraints_{};
        };

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
        result.build.runtime_images      = std::move(options.runtime_images);
        result.build.registration_symbol = std::move(options.registration_symbol);
        result.build.lifecycle           = std::move(options.lifecycle);

        SchemaBuilder schema{module, result};

        std::vector<const hgraph_ir::StructContract *> structures;
        for (const hgraph_ir::StructContract &structure : module.structures) {
            if (!structure.exported) { continue; }
            structures.push_back(&structure);
        }
        std::ranges::sort(structures, {}, &hgraph_ir::StructContract::identity);
        for (const hgraph_ir::StructContract *structure : structures) {
            InterfaceDeclaration declaration;
            declaration.category  = DeclarationCategory::Structure;
            declaration.identity  = structure->identity;
            declaration.abstract  = structure->abstract;
            declaration.signature = schema.signature(structure->generics, {}, {}, structure->requirements);
            for (hgraph_ir::TypeId parent : structure->parents) { declaration.parents.push_back(schema.type_reference(parent)); }
            for (const hgraph_ir::StructField &field : structure->fields) { declaration.fields.push_back(schema.field(field)); }
            result.interface.push_back(std::move(declaration));
        }

        std::vector<const hgraph_ir::OperatorContract *> operators;
        for (const hgraph_ir::OperatorContract &operation : module.operators) {
            if (operation.imported) { continue; }
            operators.push_back(&operation);
        }
        std::ranges::sort(operators, {}, &hgraph_ir::OperatorContract::identity);
        for (const hgraph_ir::OperatorContract *operation : operators) {
            result.interface.push_back(InterfaceDeclaration{
                .category      = DeclarationCategory::Operator,
                .identity      = operation->identity,
                .registry_name = registry_name(operation->registry_name, operation->identity),
                .signature =
                    schema.signature(operation->generics, operation->parameters, operation->result, operation->requirements),
            });
        }

        std::vector<const hgraph_ir::Callable *> exports;
        std::vector<const hgraph_ir::Callable *> implementations;
        for (const hgraph_ir::Callable &callable : module.callables) {
            if (callable.visibility == hgraph_ir::CallableVisibility::Export) {
                exports.push_back(&callable);
            } else if (callable.visibility == hgraph_ir::CallableVisibility::Implementation) {
                implementations.push_back(&callable);
            }
        }
        std::ranges::sort(exports, {}, &hgraph_ir::Callable::identity);
        for (const hgraph_ir::Callable *callable : exports) {
            result.interface.push_back(InterfaceDeclaration{
                .category  = DeclarationCategory::Function,
                .identity  = callable->identity,
                .execution = execution_kind(callable->kind),
                .signature = schema.signature(callable->generics, callable->parameters, callable->result, callable->requirements),
            });
        }
        std::ranges::sort(implementations, {}, &hgraph_ir::Callable::identity);
        for (const hgraph_ir::Callable *callable : implementations) {
            result.implementations.push_back(Implementation{
                .identity               = callable->identity,
                .operator_identity      = callable->operator_identity,
                .operator_registry_name = registry_name(callable->operator_registry_name, callable->operator_identity),
                .execution              = execution_kind(callable->kind),
                .signature = schema.signature(callable->generics, callable->parameters, callable->result, callable->requirements),
            });
        }

        normalize(result.interface, &InterfaceDeclaration::identity);
        normalize(result.implementations, &Implementation::identity);
        normalize(result.provider_requirements);
        normalize(result.build.public_headers);
        normalize(result.build.cmake_packages);
        normalize(result.build.imported_targets);
        normalize(result.build.runtime_images);
        seal(result);
        return result;
    }

}  // namespace hgl::descriptor
