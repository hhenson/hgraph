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
                        out << "temporal(" << static_cast<unsigned>(value.kind) << ',' << value.micros << ')';
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
                if (parameter.default_value) {
                    out << '=';
                    print_constant(out, *parameter.default_value);
                }
            }
            out << ") -> ";
            print_type_id(out, result);
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
        std::ostringstream out;
        out << "HGRAPH-IR " << (module.completion == Completion::Interfaces ? "interfaces" : "executable") << " module "
            << module.path << '\n';
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

        out << "operators\n";
        for (const OperatorContract &op : module.operators) {
            out << "  " << (op.imported ? "import " : "define ") << op.identity;
            if (!op.registry_name.empty()) { out << " registry=" << op.registry_name; }
            if (!op.imported) {
                out << ' ';
                print_signature(out, op.generics, op.parameters, op.result);
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
            out << " effects=";
            print_effects(out, callable.effects);
            if (!callable.capabilities.empty()) {
                out << " capabilities=[";
                for (std::size_t index = 0; index < callable.capabilities.size(); ++index) {
                    if (index != 0) { out << ", "; }
                    out << callable.capabilities[index].name << ':';
                    print_type_id(out, callable.capabilities[index].type);
                }
                out << ']';
            }
            out << '\n';
        }
        return out.str();
    }
}  // namespace hgl::hgraph_ir
