#include "ir/hir_printer.h"

#include "syntax/temporal.h"

#include <iomanip>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgl::ir
{
    namespace
    {
        template <typename Id> std::string ref(char prefix, Id id) {
            return id.valid() ? std::string{prefix} + std::to_string(id.value) : "_";
        }

        template <typename Id> void refs(std::ostream &out, char prefix, const std::vector<Id> &ids) {
            out << '[';
            for (std::size_t index = 0; index < ids.size(); ++index) {
                if (index != 0) { out << ", "; }
                out << ref(prefix, ids[index]);
            }
            out << ']';
        }

        void range(std::ostream &out, syntax::SourceRange value) { out << " [" << value.begin << ".." << value.end << ')'; }

        std::string_view completion_name(hir::Completion completion) noexcept {
            return completion == hir::Completion::Resolved ? "resolved" : "typed";
        }

        std::string_view symbol_kind_name(hir::SymbolKind kind) noexcept {
            using hir::SymbolKind;
            switch (kind) {
                case SymbolKind::Module: return "module";
                case SymbolKind::Struct: return "struct";
                case SymbolKind::Operator: return "operator";
                case SymbolKind::Function: return "function";
                case SymbolKind::Test: return "test";
                case SymbolKind::TypeParameter: return "type-parameter";
                case SymbolKind::ConstParameter: return "const-parameter";
                case SymbolKind::SignalParameter: return "signal-parameter";
                case SymbolKind::LocalLet: return "let";
                case SymbolKind::LocalVar: return "var";
                case SymbolKind::State: return "state";
                case SymbolKind::InjectedCapability: return "inject";
                case SymbolKind::LoopValue: return "loop-value";
                case SymbolKind::LambdaParameter: return "lambda-parameter";
                case SymbolKind::ImportedOperator: return "imported-operator";
                case SymbolKind::Intrinsic: return "intrinsic";
            }
            std::unreachable();
        }

        std::string_view type_kind_name(hir::TypeKind kind) noexcept {
            using hir::TypeKind;
            switch (kind) {
                case TypeKind::Void: return "void";
                case TypeKind::Scalar: return "scalar";
                case TypeKind::Symbol: return "symbol";
                case TypeKind::Tuple: return "tuple";
                case TypeKind::List: return "list";
                case TypeKind::Set: return "set";
                case TypeKind::Map: return "map";
                case TypeKind::Rolling: return "rolling";
                case TypeKind::Atomic: return "atomic";
                case TypeKind::Iterator: return "iterator";
                case TypeKind::Callable: return "callable";
                case TypeKind::Capability: return "capability";
                case TypeKind::HarnessSequence: return "harness-sequence";
                case TypeKind::Deferred: return "deferred";
            }
            std::unreachable();
        }

        std::string_view operation_kind_name(hir::OperationKind kind) noexcept {
            using hir::OperationKind;
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

        void effects(std::ostream &out, hir::Effect value) {
            struct Entry
            {
                hir::Effect      effect;
                std::string_view name;
            };
            static constexpr Entry entries[]{
                {hir::Effect::WireGraph, "wire"},           {hir::Effect::ReadRuntimeInput, "read-runtime"},
                {hir::Effect::ReadState, "read-state"},     {hir::Effect::WriteLocal, "write-local"},
                {hir::Effect::WriteState, "write-state"},   {hir::Effect::WriteOutput, "write-output"},
                {hir::Effect::UseCapability, "capability"}, {hir::Effect::IterateCollection, "iterate"},
                {hir::Effect::TestHarness, "test-harness"},
            };
            out << '[';
            bool first = true;
            for (const Entry &entry : entries) {
                if (!hir::has_effect(value, entry.effect)) { continue; }
                if (!first) { out << ", "; }
                first = false;
                out << entry.name;
            }
            out << ']';
        }

        std::string_view phase_name(hir::Phase phase) noexcept {
            using hir::Phase;
            switch (phase) {
                case Phase::Unknown: return "unknown";
                case Phase::Constant: return "constant";
                case Phase::Wiring: return "wiring";
                case Phase::Runtime: return "runtime";
            }
            std::unreachable();
        }

        std::string_view value_kind_name(hir::ValueKind kind) noexcept {
            using hir::ValueKind;
            switch (kind) {
                case ValueKind::Unknown: return "unknown";
                case ValueKind::Void: return "void";
                case ValueKind::Constant: return "constant";
                case ValueKind::Signal: return "signal";
                case ValueKind::RuntimeValue: return "runtime-value";
                case ValueKind::Function: return "function";
                case ValueKind::Operator: return "operator";
                case ValueKind::Type: return "type";
                case ValueKind::Iterator: return "iterator";
            }
            std::unreachable();
        }

        std::string_view unary_name(hir::UnaryOp op) noexcept { return op == hir::UnaryOp::Negate ? "negate" : "not"; }

        std::string_view binary_name(hir::BinaryOp op) noexcept {
            using hir::BinaryOp;
            switch (op) {
                case BinaryOp::Mul: return "mul";
                case BinaryOp::Div: return "div";
                case BinaryOp::Rem: return "rem";
                case BinaryOp::Add: return "add";
                case BinaryOp::Sub: return "sub";
                case BinaryOp::Less: return "less";
                case BinaryOp::LessEqual: return "less-equal";
                case BinaryOp::Greater: return "greater";
                case BinaryOp::GreaterEqual: return "greater-equal";
                case BinaryOp::Equal: return "equal";
                case BinaryOp::NotEqual: return "not-equal";
                case BinaryOp::And: return "and";
                case BinaryOp::Or: return "or";
            }
            std::unreachable();
        }

        std::string_view assign_name(hir::AssignOp op) noexcept {
            using hir::AssignOp;
            switch (op) {
                case AssignOp::Assign: return "assign";
                case AssignOp::Add: return "add-assign";
                case AssignOp::Sub: return "sub-assign";
                case AssignOp::Mul: return "mul-assign";
                case AssignOp::Div: return "div-assign";
            }
            std::unreachable();
        }

        void string_literal(std::ostream &out, std::string_view value) {
            out << '"';
            for (const char character : value) {
                switch (character) {
                    case '\\': out << "\\\\"; break;
                    case '"': out << "\\\""; break;
                    case '\n': out << "\\n"; break;
                    case '\r': out << "\\r"; break;
                    case '\t': out << "\\t"; break;
                    default: out << character; break;
                }
            }
            out << '"';
        }

        void arguments(std::ostream &out, const std::vector<hir::Argument> &values) {
            out << '[';
            for (std::size_t index = 0; index < values.size(); ++index) {
                if (index != 0) { out << ", "; }
                if (!values[index].name.empty()) { out << values[index].name << ':'; }
                out << ref('e', values[index].value);
            }
            out << ']';
        }

        void constant(std::ostream &out, const hir::Constant &value) {
            std::visit(
                [&](const auto &item) {
                    using T = std::decay_t<decltype(item)>;
                    if constexpr (std::is_same_v<T, hir::NullValue>) {
                        out << "null";
                    } else if constexpr (std::is_same_v<T, hir::PlaceholderValue>) {
                        out << '_';
                    } else if constexpr (std::is_same_v<T, bool>) {
                        out << (item ? "true" : "false");
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        string_literal(out, item);
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        out << syntax::canonical_spelling(item);
                    } else {
                        out << std::setprecision(17) << item;
                    }
                },
                value);
        }

        class Printer
        {
          public:
            explicit Printer(const hir::Module &module) : module_{module} {}

            std::string run() {
                out_ << "HIR " << completion_name(module_.completion) << " module " << module_.path << '\n';
                print_symbols();
                print_types();
                print_expressions();
                print_statements();
                print_blocks();
                print_constraints();
                print_declarations();
                out_ << "source-order ";
                refs(out_, 'd', module_.source_order);
                out_ << '\n';
                return std::move(out_).str();
            }

          private:
            void print_symbols() {
                out_ << "symbols\n";
                for (std::size_t index = 0; index < module_.symbols.size(); ++index) {
                    const hir::Symbol &symbol = module_.symbols[index];
                    out_ << "  s" << index << ' ' << symbol_kind_name(symbol.kind) << ' ' << symbol.name;
                    if (!symbol.external_name.empty()) { out_ << " external=" << symbol.external_name; }
                    if (!symbol.canonical_name.empty()) { out_ << " canonical-name=" << symbol.canonical_name; }
                    out_ << " owner=" << ref('d', symbol.owner) << " type=" << ref('t', symbol.type) << " index=" << symbol.index;
                    range(out_, symbol.range);
                    out_ << '\n';
                }
            }

            void print_types() {
                out_ << "types\n";
                for (std::size_t index = 0; index < module_.types.size(); ++index) {
                    const hir::Type &type = module_.types[index];
                    out_ << "  t" << index << ' ' << type_kind_name(type.kind);
                    if (type.kind == hir::TypeKind::Scalar) { out_ << ' ' << hir::scalar_type_name(type.scalar); }
                    if (type.symbol.valid()) { out_ << " symbol=" << ref('s', type.symbol); }
                    if (type.owner.valid()) { out_ << " owner=" << ref('d', type.owner); }
                    if (!type.children.empty()) {
                        out_ << " children=";
                        refs(out_, 't', type.children);
                    }
                    if (!type.arguments.empty()) {
                        out_ << " arguments=[";
                        for (std::size_t argument = 0; argument < type.arguments.size(); ++argument) {
                            if (argument != 0) { out_ << ", "; }
                            const hir::TypeArgument &value = type.arguments[argument];
                            out_ << (value.kind == hir::TypeArgumentKind::Type ? ref('t', value.type) : ref('e', value.value));
                        }
                        out_ << ']';
                    }
                    if (type.size.valid()) { out_ << " size=" << ref('e', type.size); }
                    if (type.min_size.valid()) { out_ << " min-size=" << ref('e', type.min_size); }
                    if (type.unbounded) { out_ << " unbounded"; }
                    out_ << (type.value_position ? " value" : " signal");
                    if (type.canonical.valid()) { out_ << " canonical=" << ref('t', type.canonical); }
                    range(out_, type.range);
                    out_ << '\n';
                }
            }

            void print_expressions() {
                out_ << "expressions\n";
                for (std::size_t index = 0; index < module_.exprs.size(); ++index) {
                    const hir::Expr &expression = module_.exprs[index];
                    out_ << "  e" << index << " type=" << ref('t', expression.type) << " phase=" << phase_name(expression.phase)
                         << " value=" << value_kind_name(expression.value_kind) << ' ';
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, hir::Literal>) {
                                out_ << "literal ";
                                constant(out_, node.value);
                            } else if constexpr (std::is_same_v<T, hir::SymbolRef>) {
                                out_ << "ref " << ref('s', node.symbol);
                            } else if constexpr (std::is_same_v<T, hir::Unary>) {
                                out_ << unary_name(node.op) << ' ' << ref('e', node.operand);
                            } else if constexpr (std::is_same_v<T, hir::Binary>) {
                                out_ << binary_name(node.op) << ' ' << ref('e', node.lhs) << ' ' << ref('e', node.rhs);
                            } else if constexpr (std::is_same_v<T, hir::Call> || std::is_same_v<T, hir::Eval>) {
                                out_ << (std::is_same_v<T, hir::Call> ? "call " : "eval ") << ref('e', node.callee)
                                     << " arguments=";
                                arguments(out_, node.arguments);
                            } else if constexpr (std::is_same_v<T, hir::Index>) {
                                out_ << "index " << ref('e', node.target) << ' ' << ref('e', node.index);
                            } else if constexpr (std::is_same_v<T, hir::Field>) {
                                out_ << "field " << ref('e', node.target) << '.' << node.name;
                            } else if constexpr (std::is_same_v<T, hir::Sequence>) {
                                out_ << "sequence [";
                                for (std::size_t element = 0; element < node.elements.size(); ++element) {
                                    if (element != 0) { out_ << ", "; }
                                    if (node.elements[element].key.valid()) { out_ << ref('e', node.elements[element].key) << ':'; }
                                    out_ << ref('e', node.elements[element].value);
                                }
                                out_ << ']';
                            } else if constexpr (std::is_same_v<T, hir::Tuple>) {
                                out_ << "tuple ";
                                refs(out_, 'e', node.elements);
                            } else if constexpr (std::is_same_v<T, hir::Lambda>) {
                                out_ << "lambda parameters=";
                                refs(out_, 's', node.parameters);
                                out_ << " result=" << ref('t', node.result) << " body=" << ref('e', node.body);
                            } else if constexpr (std::is_same_v<T, hir::If>) {
                                out_ << "if condition=" << ref('e', node.condition) << " then=" << ref('b', node.then_block)
                                     << " else=" << ref('e', node.otherwise);
                            } else if constexpr (std::is_same_v<T, hir::BlockExpr>) {
                                out_ << "block " << ref('b', node.block);
                            } else if constexpr (std::is_same_v<T, hir::Construct>) {
                                out_ << (node.delta ? "delta " : "construct ") << ref('t', node.type) << " arguments=";
                                arguments(out_, node.arguments);
                            }
                        },
                        expression.node);
                    if (expression.constant && !std::holds_alternative<hir::Literal>(expression.node)) {
                        out_ << " constant=";
                        constant(out_, *expression.constant);
                    }
                    if (expression.operation.kind != hir::OperationKind::None) {
                        const hir::Operation &operation = expression.operation;
                        out_ << " operation=" << operation_kind_name(operation.kind);
                        if (operation.target.valid()) { out_ << ':' << ref('s', operation.target); }
                        if (!operation.identity.empty()) { out_ << ':' << operation.identity; }
                        if (operation.candidate.valid()) { out_ << " candidate=" << ref('s', operation.candidate); }
                        if (!operation.candidate_label.empty()) {
                            out_ << " candidate-label=" << std::quoted(operation.candidate_label);
                        }
                        if (!operation.substitutions.empty()) {
                            out_ << " substitutions=[";
                            for (std::size_t substitution = 0; substitution < operation.substitutions.size(); ++substitution) {
                                if (substitution != 0) { out_ << ", "; }
                                const hir::Substitution &value = operation.substitutions[substitution];
                                out_ << (value.parameter.valid() ? ref('s', value.parameter) : value.name) << '=';
                                if (value.type.valid()) {
                                    out_ << ref('t', value.type);
                                } else if (value.value.valid()) {
                                    out_ << ref('e', value.value);
                                } else if (value.constant) {
                                    constant(out_, *value.constant);
                                } else {
                                    out_ << '_';
                                }
                            }
                            out_ << ']';
                        }
                        if (operation.deferred) { out_ << " deferred"; }
                    }
                    if (module_.completion == hir::Completion::Typed) {
                        out_ << " effects=";
                        effects(out_, expression.effects);
                    }
                    range(out_, expression.range);
                    out_ << '\n';
                }
            }

            void print_statements() {
                out_ << "statements\n";
                for (std::size_t index = 0; index < module_.stmts.size(); ++index) {
                    const hir::Stmt &statement = module_.stmts[index];
                    out_ << "  q" << index << ' ';
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, hir::LocalDecl>) {
                                out_ << "local " << ref('s', node.symbol) << " type=" << ref('t', node.type)
                                     << " init=" << ref('e', node.init);
                            } else if constexpr (std::is_same_v<T, hir::StateDecl>) {
                                out_ << "state " << ref('s', node.symbol) << " type=" << ref('t', node.type)
                                     << " init=" << ref('e', node.init);
                            } else if constexpr (std::is_same_v<T, hir::InjectDecl>) {
                                out_ << "inject ";
                                refs(out_, 's', node.symbols);
                            } else if constexpr (std::is_same_v<T, hir::LifecycleBlock>) {
                                out_ << (node.is_stop ? "stop " : "start ") << ref('b', node.block);
                            } else if constexpr (std::is_same_v<T, hir::WhenStmt>) {
                                out_ << "when condition=" << ref('e', node.condition) << " block=" << ref('b', node.block);
                            } else if constexpr (std::is_same_v<T, hir::ForStmt>) {
                                out_ << "for bindings=";
                                refs(out_, 's', node.bindings);
                                out_ << " iterable=" << ref('e', node.iterable) << " block=" << ref('b', node.block);
                            } else if constexpr (std::is_same_v<T, hir::AssignStmt>) {
                                out_ << assign_name(node.op) << " place=" << ref('e', node.place)
                                     << " value=" << ref('e', node.value);
                            } else if constexpr (std::is_same_v<T, hir::ReturnStmt>) {
                                out_ << "return " << ref('e', node.value);
                            } else if constexpr (std::is_same_v<T, hir::AssertStmt>) {
                                out_ << "assert " << ref('e', node.condition);
                            } else if constexpr (std::is_same_v<T, hir::ExprStmt>) {
                                out_ << "expression " << ref('e', node.expr);
                            }
                        },
                        statement.node);
                    if (module_.completion == hir::Completion::Typed) {
                        out_ << " effects=";
                        effects(out_, statement.effects);
                    }
                    range(out_, statement.range);
                    out_ << '\n';
                }
            }

            void print_blocks() {
                out_ << "blocks\n";
                for (std::size_t index = 0; index < module_.blocks.size(); ++index) {
                    const hir::Block &block = module_.blocks[index];
                    out_ << "  b" << index << " statements=";
                    refs(out_, 'q', block.statements);
                    out_ << " tail=" << ref('e', block.tail);
                    if (module_.completion == hir::Completion::Typed) {
                        out_ << " effects=";
                        effects(out_, block.effects);
                    }
                    range(out_, block.range);
                    out_ << '\n';
                }
            }

            void print_constraints() {
                out_ << "constraints\n";
                for (std::size_t index = 0; index < module_.constraints.size(); ++index) {
                    const hir::Constraint &constraint = module_.constraints[index];
                    out_ << "  c" << index << ' ';
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, hir::ConstraintSymbol>) {
                                out_ << "symbol " << ref('s', node.symbol);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintType>) {
                                out_ << "type " << ref('t', node.type);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintValue>) {
                                out_ << "value " << ref('e', node.value);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintSet>) {
                                out_ << "set ";
                                refs(out_, 'c', node.elements);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintCall>) {
                                out_ << "call " << ref('s', node.function) << " arguments=";
                                refs(out_, 'c', node.arguments);
                            } else if constexpr (std::is_same_v<T, hir::OperatorRequirement>) {
                                out_ << "operator " << ref('s', node.op) << " arguments=";
                                refs(out_, 'c', node.arguments);
                                out_ << " result=" << ref('t', node.result);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintRelation>) {
                                static constexpr std::string_view names[]{"equal", "in", "is"};
                                out_ << names[static_cast<std::size_t>(node.op)] << ' ' << ref('c', node.lhs) << ' '
                                     << ref('c', node.rhs);
                                if (!node.category.empty()) { out_ << " category=" << node.category; }
                            } else if constexpr (std::is_same_v<T, hir::ConstraintNot>) {
                                out_ << "not " << ref('c', node.operand);
                            } else if constexpr (std::is_same_v<T, hir::ConstraintLogic>) {
                                out_ << (node.op == hir::ConstraintLogicOp::And ? "and " : "or ") << ref('c', node.lhs) << ' '
                                     << ref('c', node.rhs);
                            }
                        },
                        constraint.node);
                    range(out_, constraint.range);
                    out_ << '\n';
                }
            }

            void print_signature(const hir::Signature &signature) {
                out_ << " parameters=[";
                for (std::size_t index = 0; index < signature.parameters.size(); ++index) {
                    if (index != 0) { out_ << ", "; }
                    const hir::Parameter &parameter = signature.parameters[index];
                    if (parameter.is_const) { out_ << "const "; }
                    out_ << ref('s', parameter.symbol) << ':' << ref('t', parameter.type);
                    if (parameter.default_value.valid()) { out_ << '=' << ref('e', parameter.default_value); }
                }
                out_ << "] result=" << ref('t', signature.result);
            }

            void print_generics(const std::vector<hir::GenericParameter> &generics) {
                if (generics.empty()) { return; }
                out_ << " generics=[";
                for (std::size_t index = 0; index < generics.size(); ++index) {
                    if (index != 0) { out_ << ", "; }
                    if (generics[index].is_const) { out_ << "const "; }
                    out_ << ref('s', generics[index].symbol) << ':' << ref('t', generics[index].type);
                }
                out_ << ']';
            }

            void print_declarations() {
                out_ << "declarations\n";
                for (std::size_t index = 0; index < module_.declarations.size(); ++index) {
                    const hir::Declaration &declaration = module_.declarations[index];
                    out_ << "  d" << index << " symbol=" << ref('s', declaration.symbol) << ' ';
                    std::visit(
                        [&](const auto &node) {
                            using T = std::decay_t<decltype(node)>;
                            if constexpr (std::is_same_v<T, hir::ModuleDecl>) {
                                out_ << "module";
                            } else if constexpr (std::is_same_v<T, hir::UseDecl>) {
                                out_ << "use " << node.module;
                                if (!node.alias.empty()) { out_ << " as " << node.alias; }
                                if (!node.names.empty()) {
                                    out_ << " names=[";
                                    for (std::size_t name = 0; name < node.names.size(); ++name) {
                                        if (name != 0) { out_ << ", "; }
                                        out_ << node.names[name];
                                    }
                                    out_ << ']';
                                }
                            } else if constexpr (std::is_same_v<T, hir::StructDecl>) {
                                out_ << (node.exported ? "export " : "") << (node.abstract ? "abstract " : "") << "struct";
                                print_generics(node.generics);
                                if (!node.parents.empty()) {
                                    out_ << " parents=";
                                    refs(out_, 't', node.parents);
                                }
                                out_ << " requires=" << ref('c', node.requirements) << " fields=[";
                                for (std::size_t field = 0; field < node.fields.size(); ++field) {
                                    if (field != 0) { out_ << ", "; }
                                    out_ << node.fields[field].name << ':' << ref('t', node.fields[field].type);
                                    if (node.fields[field].optional) { out_ << '?'; }
                                    if (node.fields[field].default_value.valid()) {
                                        out_ << '=' << ref('e', node.fields[field].default_value);
                                    }
                                    out_ << '@' << ref('d', node.fields[field].origin);
                                }
                                out_ << ']';
                            } else if constexpr (std::is_same_v<T, hir::OperatorDecl>) {
                                out_ << "operator";
                                print_generics(node.generics);
                                print_signature(node.signature);
                                out_ << " requires=" << ref('c', node.requirements);
                            } else if constexpr (std::is_same_v<T, hir::FunctionDecl>) {
                                static constexpr std::string_view visibility[]{"internal", "export", "impl"};
                                out_ << visibility[static_cast<std::size_t>(node.visibility)] << ' '
                                     << (node.kind == hir::FunctionKind::Composition ? "composition" : "runtime") << " function";
                                if (node.operator_contract.valid()) { out_ << " operator=" << ref('s', node.operator_contract); }
                                print_generics(node.generics);
                                print_signature(node.signature);
                                out_ << " requires=" << ref('c', node.requirements) << " concise=" << ref('e', node.concise_body)
                                     << " block=" << ref('b', node.block_body);
                                if (module_.completion == hir::Completion::Typed) {
                                    out_ << " effects=";
                                    effects(out_, node.effects);
                                    out_ << " capabilities=";
                                    refs(out_, 's', node.capabilities);
                                }
                            } else if constexpr (std::is_same_v<T, hir::TestDecl>) {
                                out_ << "test block=" << ref('b', node.block);
                            }
                        },
                        declaration.node);
                    range(out_, declaration.range);
                    out_ << '\n';
                }
            }

            const hir::Module &module_;
            std::ostringstream out_{};
        };
    }  // namespace

    std::string print_hir(const hir::Module &module) { return Printer{module}.run(); }
}  // namespace hgl::ir
