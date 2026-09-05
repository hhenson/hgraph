#include "hgraph_ir/printer.h"

#include <iomanip>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace hgl::hgraph_ir
{
    namespace
    {
        namespace hir = ir::hir;

        [[nodiscard]] std::string_view type_kind_name(hir::TypeKind kind) noexcept {
            switch (kind) {
                case hir::TypeKind::Void: return "void";
                case hir::TypeKind::Scalar: return "scalar";
                case hir::TypeKind::Symbol: return "symbol";
                case hir::TypeKind::Tuple: return "tuple";
                case hir::TypeKind::List: return "list";
                case hir::TypeKind::Set: return "set";
                case hir::TypeKind::Map: return "map";
                case hir::TypeKind::Rolling: return "rolling";
                case hir::TypeKind::Atomic: return "atomic";
                case hir::TypeKind::Iterator: return "iterator";
                case hir::TypeKind::Callable: return "callable";
                case hir::TypeKind::Capability: return "capability";
                case hir::TypeKind::HarnessSequence: return "harness-sequence";
                case hir::TypeKind::Deferred: return "deferred";
            }
            std::unreachable();
        }

        void print_constant(std::ostream &out, const hir::Constant &constant) {
            std::visit(
                [&](const auto &value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, hir::NullValue>) {
                        out << "null";
                    } else if constexpr (std::is_same_v<T, hir::PlaceholderValue>) {
                        out << '_';
                    } else if constexpr (std::is_same_v<T, bool>) {
                        out << (value ? "true" : "false");
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        out << std::quoted(value);
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        out << syntax::canonical_spelling(value);
                    } else {
                        out << value;
                    }
                },
                constant);
        }

        void print_type_id(std::ostream &out, TypeId id) {
            if (id.valid()) {
                out << 't' << id.value;
            } else {
                out << '_';
            }
        }

        void print_const_expr_id(std::ostream &out, ConstExprId id) {
            if (id.valid()) {
                out << 'c' << id.value;
            } else {
                out << '_';
            }
        }

        void print_constraint_id(std::ostream &out, ConstraintId id) {
            if (id.valid()) {
                out << 'r' << id.value;
            } else {
                out << '_';
            }
        }

        void print_binding_id(std::ostream &out, BindingId id) {
            if (id.valid()) {
                out << 'n' << id.value;
            } else {
                out << '_';
            }
        }

        void print_value_id(std::ostream &out, ValueId id) {
            if (id.valid()) {
                out << 'v' << id.value;
            } else {
                out << '_';
            }
        }

        void print_statement_id(std::ostream &out, StatementId id) {
            if (id.valid()) {
                out << 'q' << id.value;
            } else {
                out << '_';
            }
        }

        void print_block_id(std::ostream &out, BlockId id) {
            if (id.valid()) {
                out << 'b' << id.value;
            } else {
                out << '_';
            }
        }

        void print_callable_id(std::ostream &out, CallableId id) {
            if (id.valid()) {
                out << 'f' << id.value;
            } else {
                out << '_';
            }
        }

        void print_range(std::ostream &out, syntax::SourceRange range) { out << " [" << range.begin << ".." << range.end << ')'; }

        template <typename IdType> void print_ids(std::ostream &out, char prefix, const std::vector<IdType> &ids) {
            out << '[';
            for (std::size_t index = 0; index < ids.size(); ++index) {
                if (index != 0) { out << ", "; }
                out << prefix << ids[index].value;
            }
            out << ']';
        }

        [[nodiscard]] std::string_view unary_name(hir::UnaryOp op) noexcept {
            return op == hir::UnaryOp::Negate ? "negate" : "not";
        }

        [[nodiscard]] std::string_view binary_name(hir::BinaryOp op) noexcept {
            switch (op) {
                case hir::BinaryOp::Mul: return "mul";
                case hir::BinaryOp::Div: return "div";
                case hir::BinaryOp::Rem: return "rem";
                case hir::BinaryOp::Add: return "add";
                case hir::BinaryOp::Sub: return "sub";
                case hir::BinaryOp::Less: return "less";
                case hir::BinaryOp::LessEqual: return "less-equal";
                case hir::BinaryOp::Greater: return "greater";
                case hir::BinaryOp::GreaterEqual: return "greater-equal";
                case hir::BinaryOp::Equal: return "equal";
                case hir::BinaryOp::NotEqual: return "not-equal";
                case hir::BinaryOp::And: return "and";
                case hir::BinaryOp::Or: return "or";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view phase_name(hir::Phase phase) noexcept {
            switch (phase) {
                case hir::Phase::Unknown: return "unknown";
                case hir::Phase::Constant: return "constant";
                case hir::Phase::Wiring: return "wiring";
                case hir::Phase::Runtime: return "runtime";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view value_kind_name(hir::ValueKind kind) noexcept {
            switch (kind) {
                case hir::ValueKind::Unknown: return "unknown";
                case hir::ValueKind::Void: return "void";
                case hir::ValueKind::Constant: return "constant";
                case hir::ValueKind::Signal: return "signal";
                case hir::ValueKind::RuntimeValue: return "runtime-value";
                case hir::ValueKind::Function: return "function";
                case hir::ValueKind::Operator: return "operator";
                case hir::ValueKind::Type: return "type";
                case hir::ValueKind::Iterator: return "iterator";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view operation_kind_name(OperationKind kind) noexcept {
            switch (kind) {
                case OperationKind::None: return "none";
                case OperationKind::ExactFunction: return "exact-function";
                case OperationKind::NominalOperator: return "nominal-operator";
                case OperationKind::Intrinsic: return "intrinsic";
                case OperationKind::Constructor: return "constructor";
                case OperationKind::Capability: return "capability";
                case OperationKind::Index: return "index";
                case OperationKind::Field: return "field";
                case OperationKind::HarnessEval: return "harness-eval";
            }
            std::unreachable();
        }

        [[nodiscard]] std::string_view assign_name(AssignOp op) noexcept {
            switch (op) {
                case AssignOp::Assign: return "assign";
                case AssignOp::Add: return "add-assign";
                case AssignOp::Sub: return "sub-assign";
                case AssignOp::Mul: return "mul-assign";
                case AssignOp::Div: return "div-assign";
            }
            std::unreachable();
        }

        void print_arguments(std::ostream &out, const std::vector<Argument> &arguments) {
            out << '[';
            for (std::size_t index = 0; index < arguments.size(); ++index) {
                if (index != 0) { out << ", "; }
                if (!arguments[index].name.empty()) { out << arguments[index].name << ':'; }
                print_value_id(out, arguments[index].value);
            }
            out << ']';
        }

        void print_operation(std::ostream &out, const Operation &operation) {
            if (operation.kind == OperationKind::None) { return; }
            out << " operation=" << operation_kind_name(operation.kind);
            if (!operation.identity.empty()) { out << " identity=" << operation.identity; }
            if (!operation.registry_name.empty()) { out << " registry=" << operation.registry_name; }
            if (operation.callable.valid()) {
                out << " callable=";
                print_callable_id(out, operation.callable);
            }
            if (operation.candidate.valid()) {
                out << " candidate=";
                print_callable_id(out, operation.candidate);
            }
            if (!operation.candidate_identity.empty()) { out << " candidate-identity=" << operation.candidate_identity; }
            if (!operation.candidate_label.empty()) { out << " candidate-label=" << std::quoted(operation.candidate_label); }
            if (operation.capability.valid()) {
                out << " capability=";
                print_binding_id(out, operation.capability);
            }
            if (!operation.substitutions.empty()) {
                out << " substitutions=[";
                for (std::size_t index = 0; index < operation.substitutions.size(); ++index) {
                    if (index != 0) { out << ", "; }
                    const Substitution &substitution = operation.substitutions[index];
                    if (substitution.parameter.valid()) {
                        print_binding_id(out, substitution.parameter);
                    } else {
                        out << substitution.parameter_identity;
                    }
                    if (substitution.type.valid()) {
                        out << ":";
                        print_type_id(out, substitution.type);
                    }
                    if (substitution.value.valid()) {
                        out << '=';
                        print_const_expr_id(out, substitution.value);
                    }
                }
                out << ']';
            }
            if (operation.deferred) { out << " deferred"; }
        }

        void print_signature(std::ostream &out, const std::vector<GenericParameter> &generics,
                             const std::vector<Parameter> &parameters, TypeId result) {
            if (!generics.empty()) {
                out << '<';
                for (std::size_t index = 0; index < generics.size(); ++index) {
                    if (index != 0) { out << ", "; }
                    const GenericParameter &generic = generics[index];
                    if (generic.is_const) { out << "const "; }
                    out << generic.name;
                    if (generic.type.valid()) {
                        out << ':';
                        print_type_id(out, generic.type);
                    }
                }
                out << '>';
            }
            out << '(';
            for (std::size_t index = 0; index < parameters.size(); ++index) {
                if (index != 0) { out << ", "; }
                const Parameter &parameter = parameters[index];
                if (parameter.is_const) { out << "const "; }
                out << parameter.name << ':';
                print_type_id(out, parameter.type);
                if (parameter.default_value.valid()) {
                    out << '=';
                    print_const_expr_id(out, parameter.default_value);
                }
            }
            out << ") -> ";
            print_type_id(out, result);
        }

        void print_generics(std::ostream &out, const std::vector<GenericParameter> &generics) {
            if (generics.empty()) { return; }
            out << '<';
            for (std::size_t index = 0; index < generics.size(); ++index) {
                if (index != 0) { out << ", "; }
                const GenericParameter &generic = generics[index];
                if (generic.is_const) { out << "const "; }
                out << generic.name;
                if (generic.type.valid()) {
                    out << ':';
                    print_type_id(out, generic.type);
                }
            }
            out << '>';
        }

        void print_effects(std::ostream &out, hir::Effect effects) {
            bool       first = true;
            const auto add   = [&](std::string_view name, hir::Effect flag) {
                if (!hir::has_effect(effects, flag)) { return; }
                if (!first) { out << '|'; }
                out << name;
                first = false;
            };
            add("wire", hir::Effect::WireGraph);
            add("read-input", hir::Effect::ReadRuntimeInput);
            add("read-state", hir::Effect::ReadState);
            add("write-local", hir::Effect::WriteLocal);
            add("write-state", hir::Effect::WriteState);
            add("write-output", hir::Effect::WriteOutput);
            add("capability", hir::Effect::UseCapability);
            add("iterate", hir::Effect::IterateCollection);
            add("harness", hir::Effect::TestHarness);
            if (first) { out << "none"; }
        }
    }  // namespace

    std::string print(const Module &module) {
        std::ostringstream                out;
        static constexpr std::string_view completion_names[]{"interfaces", "bodies", "executable"};
        out << "HGRAPH-IR " << completion_names[static_cast<std::size_t>(module.completion)] << " module " << module.path << '\n';
        out << "constant-expressions\n";
        for (std::size_t index = 0; index < module.const_exprs.size(); ++index) {
            const ConstExpr &expression = module.const_exprs[index];
            out << "  c" << index << ' ';
            switch (expression.kind) {
                case ConstExprKind::Literal:
                    out << "literal ";
                    if (expression.literal) {
                        print_constant(out, *expression.literal);
                    } else {
                        out << '?';
                    }
                    break;
                case ConstExprKind::Parameter: out << "parameter " << expression.parameter; break;
                case ConstExprKind::Unary:
                    out << unary_name(expression.unary) << ' ';
                    print_const_expr_id(out, expression.lhs);
                    break;
                case ConstExprKind::Binary:
                    out << binary_name(expression.binary) << ' ';
                    print_const_expr_id(out, expression.lhs);
                    out << ' ';
                    print_const_expr_id(out, expression.rhs);
                    break;
                case ConstExprKind::Index:
                    out << "index ";
                    print_const_expr_id(out, expression.lhs);
                    out << ' ';
                    print_const_expr_id(out, expression.rhs);
                    break;
                case ConstExprKind::Field:
                    out << "field ";
                    print_const_expr_id(out, expression.lhs);
                    out << ' ' << expression.member;
                    break;
                case ConstExprKind::Sequence:
                    out << "sequence [";
                    for (std::size_t item = 0; item < expression.elements.size(); ++item) {
                        if (item != 0) { out << ", "; }
                        if (expression.elements[item].key.valid()) {
                            print_const_expr_id(out, expression.elements[item].key);
                            out << ':';
                        }
                        print_const_expr_id(out, expression.elements[item].value);
                    }
                    out << ']';
                    break;
                case ConstExprKind::Tuple:
                    out << "tuple [";
                    for (std::size_t item = 0; item < expression.items.size(); ++item) {
                        if (item != 0) { out << ", "; }
                        print_const_expr_id(out, expression.items[item]);
                    }
                    out << ']';
                    break;
                case ConstExprKind::Construct:
                    out << (expression.delta ? "delta " : "construct ");
                    print_type_id(out, expression.constructed_type);
                    out << " [";
                    for (std::size_t argument = 0; argument < expression.arguments.size(); ++argument) {
                        if (argument != 0) { out << ", "; }
                        if (!expression.arguments[argument].name.empty()) { out << expression.arguments[argument].name << '='; }
                        print_const_expr_id(out, expression.arguments[argument].value);
                    }
                    out << ']';
                    break;
            }
            out << '\n';
        }
        out << "types\n";
        for (std::size_t index = 0; index < module.types.size(); ++index) {
            const Type &type = module.types[index];
            out << "  t" << index << ' ' << type_kind_name(type.kind);
            if (type.kind == hir::TypeKind::Scalar) { out << ' ' << hir::scalar_type_name(type.scalar); }
            if (!type.nominal_identity.empty()) { out << " nominal=" << type.nominal_identity; }
            if (!type.children.empty()) {
                out << " children=[";
                for (std::size_t child = 0; child < type.children.size(); ++child) {
                    if (child != 0) { out << ", "; }
                    print_type_id(out, type.children[child]);
                }
                out << ']';
            }
            if (!type.arguments.empty()) {
                out << " arguments=[";
                for (std::size_t argument = 0; argument < type.arguments.size(); ++argument) {
                    if (argument != 0) { out << ", "; }
                    if (type.arguments[argument].type) {
                        print_type_id(out, *type.arguments[argument].type);
                    } else if (type.arguments[argument].value) {
                        print_const_expr_id(out, *type.arguments[argument].value);
                    } else {
                        out << '?';
                    }
                }
                out << ']';
            }
            if (type.size.valid()) {
                out << " size=";
                print_const_expr_id(out, type.size);
            }
            if (type.min_size.valid()) {
                out << " min-size=";
                print_const_expr_id(out, type.min_size);
            }
            if (type.unbounded) { out << " unbounded"; }
            out << '\n';
        }

        out << "constraints\n";
        for (std::size_t index = 0; index < module.constraints.size(); ++index) {
            const Constraint &constraint = module.constraints[index];
            out << "  r" << index << ' ';
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, ConstraintSymbol>) {
                        out << "symbol " << node.identity;
                    } else if constexpr (std::is_same_v<T, ConstraintType>) {
                        out << "type ";
                        print_type_id(out, node.type);
                    } else if constexpr (std::is_same_v<T, ConstraintValue>) {
                        out << "value ";
                        print_const_expr_id(out, node.value);
                    } else if constexpr (std::is_same_v<T, ConstraintSet>) {
                        out << "set ";
                        print_ids(out, 'r', node.elements);
                    } else if constexpr (std::is_same_v<T, ConstraintCall>) {
                        out << "call " << node.function_identity << " arguments=";
                        print_ids(out, 'r', node.arguments);
                    } else if constexpr (std::is_same_v<T, OperatorRequirement>) {
                        out << "operator " << node.operator_identity;
                        if (!node.operator_registry_name.empty()) { out << " registry=" << node.operator_registry_name; }
                        out << " arguments=";
                        print_ids(out, 'r', node.arguments);
                        out << " result=";
                        print_type_id(out, node.result);
                    } else if constexpr (std::is_same_v<T, ConstraintRelation>) {
                        static constexpr std::string_view names[]{"equal", "in", "is"};
                        out << names[static_cast<std::size_t>(node.op)] << ' ';
                        print_constraint_id(out, node.lhs);
                        out << ' ';
                        print_constraint_id(out, node.rhs);
                        if (!node.category.empty()) { out << " category=" << node.category; }
                    } else if constexpr (std::is_same_v<T, ConstraintNot>) {
                        out << "not ";
                        print_constraint_id(out, node.operand);
                    } else {
                        out << (node.op == ConstraintLogicOp::And ? "and " : "or ");
                        print_constraint_id(out, node.lhs);
                        out << ' ';
                        print_constraint_id(out, node.rhs);
                    }
                },
                constraint.node);
            out << '\n';
        }

        out << "structures\n";
        for (const StructContract &structure : module.structures) {
            out << "  ";
            if (structure.exported) { out << "export "; }
            if (structure.abstract) { out << "abstract "; }
            out << "struct " << structure.identity;
            print_generics(out, structure.generics);
            if (!structure.parents.empty()) {
                out << " parents=";
                print_ids(out, 't', structure.parents);
            }
            if (structure.requirements.valid()) {
                out << " requires=";
                print_constraint_id(out, structure.requirements);
            }
            out << " fields=[";
            for (std::size_t index = 0; index < structure.fields.size(); ++index) {
                if (index != 0) { out << ", "; }
                const StructField &field = structure.fields[index];
                out << field.name;
                if (field.optional) { out << '?'; }
                out << ':';
                print_type_id(out, field.type);
                if (field.default_value.valid()) {
                    out << '=';
                    print_const_expr_id(out, field.default_value);
                }
                if (!field.origin_identity.empty()) { out << " origin=" << field.origin_identity; }
            }
            out << "]\n";
        }

        out << "operators\n";
        for (const OperatorContract &op : module.operators) {
            out << "  " << (op.imported ? "import " : "define ") << op.identity;
            if (!op.registry_name.empty()) { out << " registry=" << op.registry_name; }
            if (!op.imported) {
                out << ' ';
                print_signature(out, op.generics, op.parameters, op.result);
            }
            if (op.requirements.valid()) {
                out << " requires=";
                print_constraint_id(out, op.requirements);
            }
            out << '\n';
        }

        out << "callables\n";
        for (const Callable &callable : module.callables) {
            static constexpr std::string_view visibility[]{"internal", "export", "impl"};
            out << "  " << visibility[static_cast<std::size_t>(callable.visibility)] << ' '
                << (callable.kind == CallableKind::Composition ? "composition" : "runtime-node") << ' ' << callable.identity;
            if (!callable.operator_identity.empty()) { out << " operator=" << callable.operator_identity; }
            if (!callable.operator_registry_name.empty()) { out << " registry=" << callable.operator_registry_name; }
            out << ' ';
            print_signature(out, callable.generics, callable.parameters, callable.result);
            if (callable.requirements.valid()) {
                out << " requires=";
                print_constraint_id(out, callable.requirements);
            }
            out << " effects=";
            print_effects(out, callable.effects);
            if (!callable.capabilities.empty()) {
                out << " capabilities=[";
                for (std::size_t index = 0; index < callable.capabilities.size(); ++index) {
                    if (index != 0) { out << ", "; }
                    out << callable.capabilities[index].name << ':';
                    print_type_id(out, callable.capabilities[index].type);
                    out << '@';
                    print_binding_id(out, callable.capabilities[index].binding);
                }
                out << ']';
            }
            if (callable.concise_body.valid()) {
                out << " body=";
                print_value_id(out, callable.concise_body);
            }
            if (callable.block_body.valid()) {
                out << " body=";
                print_block_id(out, callable.block_body);
            }
            out << '\n';
        }

        out << "bindings\n";
        for (std::size_t index = 0; index < module.bindings.size(); ++index) {
            static constexpr std::string_view names[]{
                "type-parameter", "const-parameter", "signal-parameter", "let", "var", "state",
                "capability",     "loop-value",      "lambda-parameter"};
            const Binding &binding = module.bindings[index];
            out << "  n" << index << ' ' << names[static_cast<std::size_t>(binding.kind)] << ' ' << binding.name << ':';
            print_type_id(out, binding.type);
            if (!binding.owner_identity.empty()) { out << " owner=" << binding.owner_identity; }
            out << " index=" << binding.index;
            print_range(out, binding.range);
            out << '\n';
        }

        out << "values\n";
        for (std::size_t index = 0; index < module.values.size(); ++index) {
            const Value &value = module.values[index];
            out << "  v" << index << ' ';
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, Literal>) {
                        out << "literal ";
                        print_constant(out, node.value);
                    } else if constexpr (std::is_same_v<T, Reference>) {
                        static constexpr std::string_view names[]{"binding", "callable", "operator", "struct", "intrinsic"};
                        out << "reference " << names[static_cast<std::size_t>(node.kind)] << ' ';
                        if (node.binding.valid()) {
                            print_binding_id(out, node.binding);
                        } else if (node.callable.valid()) {
                            print_callable_id(out, node.callable);
                        } else {
                            out << node.identity;
                        }
                        if (!node.registry_name.empty()) { out << " registry=" << node.registry_name; }
                    } else if constexpr (std::is_same_v<T, Unary>) {
                        out << unary_name(node.op) << ' ';
                        print_value_id(out, node.operand);
                    } else if constexpr (std::is_same_v<T, Binary>) {
                        out << binary_name(node.op) << ' ';
                        print_value_id(out, node.lhs);
                        out << ' ';
                        print_value_id(out, node.rhs);
                    } else if constexpr (std::is_same_v<T, Call>) {
                        out << "call ";
                        print_value_id(out, node.callee);
                        out << " arguments=";
                        print_arguments(out, node.arguments);
                    } else if constexpr (std::is_same_v<T, Index>) {
                        out << "index ";
                        print_value_id(out, node.target);
                        out << ' ';
                        print_value_id(out, node.index);
                    } else if constexpr (std::is_same_v<T, Field>) {
                        out << "field ";
                        print_value_id(out, node.target);
                        out << ' ' << node.name;
                    } else if constexpr (std::is_same_v<T, Sequence>) {
                        out << "sequence [";
                        for (std::size_t item = 0; item < node.elements.size(); ++item) {
                            if (item != 0) { out << ", "; }
                            if (node.elements[item].key.valid()) {
                                print_value_id(out, node.elements[item].key);
                                out << ':';
                            }
                            print_value_id(out, node.elements[item].value);
                        }
                        out << ']';
                    } else if constexpr (std::is_same_v<T, Tuple>) {
                        out << "tuple ";
                        print_ids(out, 'v', node.elements);
                    } else if constexpr (std::is_same_v<T, Lambda>) {
                        out << "lambda parameters=";
                        print_ids(out, 'n', node.parameters);
                        out << " result=";
                        print_type_id(out, node.result);
                        out << " body=";
                        print_value_id(out, node.body);
                    } else if constexpr (std::is_same_v<T, Conditional>) {
                        out << "if condition=";
                        print_value_id(out, node.condition);
                        out << " then=";
                        print_block_id(out, node.then_block);
                        if (node.otherwise.valid()) {
                            out << " else=";
                            print_value_id(out, node.otherwise);
                        }
                    } else if constexpr (std::is_same_v<T, BlockValue>) {
                        out << "block ";
                        print_block_id(out, node.block);
                    } else if constexpr (std::is_same_v<T, HarnessEval>) {
                        out << "eval ";
                        print_value_id(out, node.callee);
                        out << " arguments=";
                        print_arguments(out, node.arguments);
                    } else {
                        out << (node.delta ? "delta " : "construct ");
                        print_type_id(out, node.type);
                        out << " arguments=";
                        print_arguments(out, node.arguments);
                    }
                },
                value.node);
            out << " type=";
            print_type_id(out, value.type);
            out << " phase=" << phase_name(value.phase) << " kind=" << value_kind_name(value.value_kind) << " effects=";
            print_effects(out, value.effects);
            print_operation(out, value.operation);
            if (value.constant) {
                out << " constant=";
                print_constant(out, *value.constant);
            }
            print_range(out, value.range);
            out << '\n';
        }

        out << "statements\n";
        for (std::size_t index = 0; index < module.statements.size(); ++index) {
            const Statement &statement = module.statements[index];
            out << "  q" << index << ' ';
            std::visit(
                [&](const auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                        out << (std::is_same_v<T, StateBinding> ? "state " : "local ");
                        print_binding_id(out, node.binding);
                        out << " type=";
                        print_type_id(out, node.type);
                        if (node.init.valid()) {
                            out << " init=";
                            print_value_id(out, node.init);
                        }
                    } else if constexpr (std::is_same_v<T, Inject>) {
                        out << "inject ";
                        print_ids(out, 'n', node.bindings);
                    } else if constexpr (std::is_same_v<T, Lifecycle>) {
                        out << (node.kind == LifecycleKind::Start ? "start " : "stop ");
                        print_block_id(out, node.block);
                    } else if constexpr (std::is_same_v<T, Activation>) {
                        out << "when condition=";
                        print_value_id(out, node.condition);
                        out << " body=";
                        print_block_id(out, node.block);
                    } else if constexpr (std::is_same_v<T, Traversal>) {
                        out << "for bindings=";
                        print_ids(out, 'n', node.bindings);
                        out << " iterable=";
                        print_value_id(out, node.iterable);
                        out << " body=";
                        print_block_id(out, node.block);
                    } else if constexpr (std::is_same_v<T, Assignment>) {
                        out << assign_name(node.op) << " place=";
                        print_value_id(out, node.place);
                        out << " value=";
                        print_value_id(out, node.value);
                    } else if constexpr (std::is_same_v<T, Return>) {
                        out << "return ";
                        print_value_id(out, node.value);
                    } else if constexpr (std::is_same_v<T, Assert>) {
                        out << "assert ";
                        print_value_id(out, node.condition);
                    } else {
                        out << "evaluate ";
                        print_value_id(out, node.value);
                    }
                },
                statement.node);
            out << " effects=";
            print_effects(out, statement.effects);
            print_range(out, statement.range);
            out << '\n';
        }

        out << "blocks\n";
        for (std::size_t index = 0; index < module.blocks.size(); ++index) {
            const Block &block = module.blocks[index];
            out << "  b" << index << " statements=[";
            for (std::size_t item = 0; item < block.statements.size(); ++item) {
                if (item != 0) { out << ", "; }
                print_statement_id(out, block.statements[item]);
            }
            out << ']';
            if (block.tail.valid()) {
                out << " tail=";
                print_value_id(out, block.tail);
            }
            out << " effects=";
            print_effects(out, block.effects);
            print_range(out, block.range);
            out << '\n';
        }

        out << "tests\n";
        for (const TestPlan &test : module.tests) {
            out << "  " << test.identity << " body=";
            print_block_id(out, test.body);
            print_range(out, test.range);
            out << '\n';
        }
        return out.str();
    }
}  // namespace hgl::hgraph_ir
