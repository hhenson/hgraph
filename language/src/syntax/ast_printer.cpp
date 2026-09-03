#include "syntax/ast_printer.h"

#include "syntax/temporal.h"

#include <format>
#include <string>
#include <string_view>
#include <variant>

// Dump format: one node per line, `Kind [begin..end) details`, children
// indented two spaces per level. A child that fills a named slot of its
// parent (a result type, a default, a size, an `else` arm, ...) carries a
// `slot: ` prefix so the reader can tell the slots apart without counting.
namespace hgl::syntax
{
    namespace
    {
        [[nodiscard]] std::string format_float(double value)
        {
            std::string text = std::format("{}", value);
            const bool  plain = text.find_first_of(".eEn") == std::string::npos;  // n: inf/nan
            if (plain) { text += ".0"; }
            return text;
        }

        [[nodiscard]] std::string escape(std::string_view value)
        {
            std::string out = "\"";
            for (const char c : value)
            {
                switch (c)
                {
                    case '"': out += "\\\""; break;
                    case '\\': out += "\\\\"; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default: out += c; break;
                }
            }
            out += '"';
            return out;
        }

        [[nodiscard]] std::string_view type_kind_name(ast::TypeKind kind) noexcept
        {
            switch (kind)
            {
                case ast::TypeKind::Scalar: return "scalar";
                case ast::TypeKind::Named: return "named";
                case ast::TypeKind::Tuple: return "tuple";
                case ast::TypeKind::List: return "list";
                case ast::TypeKind::Set: return "set";
                case ast::TypeKind::Map: return "map";
                case ast::TypeKind::Rolling: return "rolling";
                case ast::TypeKind::Atomic: return "atomic";
            }
            return "?";
        }

        [[nodiscard]] std::string join_names(const std::vector<ast::Name> &names, std::string_view separator)
        {
            std::string out;
            for (const ast::Name &name : names)
            {
                if (!out.empty()) { out += separator; }
                out += name.text;
            }
            return out;
        }

        class Printer
        {
          public:
            explicit Printer(const ast::Module &module) : module_{module} {}

            std::string run()
            {
                out_ += "Module\n";
                for (const ast::DeclId id : module_.declarations) { decl(1, id); }
                for (const ast::Comment &comment : module_.comments) { line(1, "Comment", comment.range, ""); }
                return std::move(out_);
            }

          private:
            void line(int depth, std::string_view kind, SourceRange range, std::string details,
                      std::string_view slot = {})
            {
                out_.append(static_cast<std::size_t>(depth) * 2, ' ');
                if (!slot.empty())
                {
                    out_ += slot;
                    out_ += ": ";
                }
                out_ += kind;
                out_ += std::format(" [{}..{})", range.begin, range.end);
                if (!details.empty())
                {
                    out_ += ' ';
                    out_ += details;
                }
                out_ += '\n';
            }

            // ---------------------------------------------------------- decls

            void decl(int depth, ast::DeclId id)
            {
                const ast::Decl &node = module_.decl(id);
                std::visit([&](const auto &d) { decl_node(depth, node.range, d); }, node.node);
            }

            void decl_node(int depth, SourceRange range, const ast::ModuleDecl &d)
            {
                line(depth, "ModuleDecl", range, join_names(d.path, "."));
            }

            void decl_node(int depth, SourceRange range, const ast::UseDecl &d)
            {
                std::string details = join_names(d.path, ".");
                if (!d.alias.empty())
                {
                    details += " as ";
                    details += d.alias.text;
                }
                else
                {
                    details += "::{";
                    details += join_names(d.names, ", ");
                    details += "}";
                }
                line(depth, "UseDecl", range, std::move(details));
            }

            void decl_node(int depth, SourceRange range, const ast::OperatorDecl &d)
            {
                line(depth, "OperatorDecl", range, std::string{d.name.text});
                generics(depth + 1, d.generics);
                signature(depth + 1, d.signature);
                if (d.requirements != ast::no_node) { constraint(depth + 1, d.requirements, "requires"); }
            }

            void decl_node(int depth, SourceRange range, const ast::FunctionDecl &d)
            {
                std::string details;
                switch (d.visibility)
                {
                    case ast::FunctionVisibility::Internal: details = "fn "; break;
                    case ast::FunctionVisibility::Export: details = "export fn "; break;
                    case ast::FunctionVisibility::Impl: details = "impl fn "; break;
                }
                details += d.name.text;
                line(depth, "FunctionDecl", range, std::move(details));
                generics(depth + 1, d.generics);
                signature(depth + 1, d.signature);
                if (d.requirements != ast::no_node) { constraint(depth + 1, d.requirements, "requires"); }
                if (d.concise_body != ast::no_node) { expr(depth + 1, d.concise_body, "body"); }
                if (d.block_body != ast::no_node) { block(depth + 1, d.block_body, "body"); }
            }

            void decl_node(int depth, SourceRange range, const ast::StructDecl &d)
            {
                std::string details;
                if (d.exported) { details += "export "; }
                if (d.abstract) { details += "abstract "; }
                details += "struct ";
                details += d.name.text;
                line(depth, "StructDecl", range, std::move(details));
                generics(depth + 1, d.generics);
                for (const ast::TypeId parent : d.parents) { type(depth + 1, parent, "parent"); }
                if (d.requirements != ast::no_node) { constraint(depth + 1, d.requirements, "requires"); }
                for (const ast::StructMember &member : d.members)
                {
                    std::visit(
                        [&](const auto &m) {
                            using T = std::decay_t<decltype(m)>;
                            if constexpr (std::is_same_v<T, ast::StructField>)
                            {
                                SourceRange member_range = m.name.range.join(module_.type(m.type).range);
                                if (m.default_value != ast::no_node)
                                {
                                    member_range = member_range.join(module_.expr(m.default_value).range);
                                }
                                line(depth + 1, "StructField", member_range, std::string{m.name.text});
                                type(depth + 2, m.type, "type");
                                if (m.default_value != ast::no_node) { expr(depth + 2, m.default_value, "default"); }
                            }
                            else
                            {
                                const SourceRange member_range = m.name.range.join(module_.expr(m.value).range);
                                line(depth + 1, "InheritedDefault", member_range, std::string{m.name.text});
                                expr(depth + 2, m.value, "value");
                            }
                        },
                        member);
                }
            }

            void decl_node(int depth, SourceRange range, const ast::TestDecl &d)
            {
                line(depth, "TestDecl", range, std::string{d.name.text});
                block(depth + 1, d.block, "body");
            }

            void generics(int depth, const std::vector<ast::GenericParameter> &generics)
            {
                for (const ast::GenericParameter &generic : generics)
                {
                    SourceRange range = generic.name.range;
                    if (generic.type != ast::no_node) { range = range.join(module_.type(generic.type).range); }
                    line(depth, "GenericParameter", range,
                         (generic.is_const ? "const " : "") + std::string{generic.name.text});
                    if (generic.type != ast::no_node) { type(depth + 1, generic.type, "type"); }
                }
            }

            void signature(int depth, const ast::Signature &signature)
            {
                for (const ast::Parameter &parameter : signature.parameters)
                {
                    SourceRange range = parameter.name.range;
                    if (parameter.type != ast::no_node) { range = range.join(module_.type(parameter.type).range); }
                    if (parameter.default_value != ast::no_node)
                    {
                        range = range.join(module_.expr(parameter.default_value).range);
                    }
                    line(depth, "Parameter", range, (parameter.is_const ? "const " : "") + std::string{parameter.name.text});
                    if (parameter.type != ast::no_node) { type(depth + 1, parameter.type, "type"); }
                    if (parameter.default_value != ast::no_node) { expr(depth + 1, parameter.default_value, "default"); }
                }
                if (signature.result != ast::no_node) { type(depth, signature.result, "result"); }
            }

            // ---------------------------------------------------------- types

            void type(int depth, ast::TypeId id, std::string_view slot = {})
            {
                const ast::Type &node    = module_.type(id);
                std::string      details = std::string{type_kind_name(node.kind)};
                switch (node.kind)
                {
                    case ast::TypeKind::Scalar:
                        details += ' ';
                        details += ast::scalar_type_name(node.scalar);
                        break;
                    case ast::TypeKind::Named:
                        details += ' ';
                        if (!node.qualifier.empty())
                        {
                            details += node.qualifier.text;
                            details += "::";
                        }
                        details += node.name.text;
                        break;
                    case ast::TypeKind::List:
                        if (node.unbounded) { details += " unbounded"; }
                        break;
                    default: break;
                }
                if (node.value_position) { details += " (value)"; }
                line(depth, "Type", node.range, std::move(details), slot);
                for (const ast::GenericArgument &argument : node.arguments)
                {
                    line(depth + 1, "GenericArgument", argument.range, "");
                    if (argument.type != ast::no_node) { type(depth + 2, argument.type); }
                    else if (argument.value != ast::no_node) { expr(depth + 2, argument.value); }
                    else
                    {
                        line(depth + 2, "Name", argument.name.range, std::string{argument.name.text});
                    }
                }
                for (const ast::TypeId child : node.children) { type(depth + 1, child); }
                if (node.size != ast::no_node) { expr(depth + 1, node.size, "size"); }
                if (node.min_size != ast::no_node) { expr(depth + 1, node.min_size, "min"); }
            }

            // ---------------------------------------------------- expressions

            void expr(int depth, ast::ExprId id, std::string_view slot = {})
            {
                const ast::Expr &node = module_.expr(id);
                std::visit([&](const auto &e) { expr_node(depth, node.range, e, slot); }, node.node);
            }

            void expr_node(int depth, SourceRange range, const ast::IntLiteral &e, std::string_view slot)
            { line(depth, "IntLiteral", range, std::format("{}", e.value), slot); }
            void expr_node(int depth, SourceRange range, const ast::FloatLiteral &e, std::string_view slot)
            { line(depth, "FloatLiteral", range, format_float(e.value), slot); }
            void expr_node(int depth, SourceRange range, const ast::StringLiteral &e, std::string_view slot)
            { line(depth, "StringLiteral", range, escape(e.value), slot); }
            void expr_node(int depth, SourceRange range, const ast::BoolLiteral &e, std::string_view slot)
            { line(depth, "BoolLiteral", range, e.value ? "true" : "false", slot); }
            void expr_node(int depth, SourceRange range, const ast::NullLiteral &, std::string_view slot)
            { line(depth, "NullLiteral", range, "", slot); }
            void expr_node(int depth, SourceRange range, const ast::TemporalLiteral &e, std::string_view slot)
            { line(depth, "TemporalLiteral", range, canonical_spelling(e.value), slot); }
            void expr_node(int depth, SourceRange range, const ast::Placeholder &, std::string_view slot)
            { line(depth, "Placeholder", range, "", slot); }
            void expr_node(int depth, SourceRange range, const ast::NameRef &e, std::string_view slot)
            { line(depth, "NameRef", range, std::string{e.name.text}, slot); }
            void expr_node(int depth, SourceRange range, const ast::QualifiedRef &e, std::string_view slot)
            { line(depth, "QualifiedRef", range, std::string{e.qualifier.text} + "::" + std::string{e.name.text}, slot); }
            void expr_node(int depth, SourceRange range, const ast::Unary &e, std::string_view slot)
            {
                line(depth, "Unary", range, std::string{ast::unary_op_spelling(e.op)}, slot);
                expr(depth + 1, e.operand);
            }
            void expr_node(int depth, SourceRange range, const ast::Binary &e, std::string_view slot)
            {
                line(depth, "Binary", range, std::string{ast::binary_op_spelling(e.op)}, slot);
                expr(depth + 1, e.lhs);
                expr(depth + 1, e.rhs);
            }
            void expr_node(int depth, SourceRange range, const ast::Call &e, std::string_view slot)
            {
                line(depth, "Call", range, "", slot);
                expr(depth + 1, e.callee, "callee");
                arguments(depth + 1, e.arguments);
            }
            void expr_node(int depth, SourceRange range, const ast::Index &e, std::string_view slot)
            {
                line(depth, "Index", range, "", slot);
                expr(depth + 1, e.target);
                expr(depth + 1, e.index, "index");
            }
            void expr_node(int depth, SourceRange range, const ast::Field &e, std::string_view slot)
            {
                line(depth, "Field", range, std::string{e.field.text}, slot);
                expr(depth + 1, e.target);
            }
            void expr_node(int depth, SourceRange range, const ast::SequenceLiteral &e, std::string_view slot)
            {
                line(depth, "SequenceLiteral", range, "", slot);
                for (const ast::SequenceElement &element : e.elements)
                {
                    if (element.key == ast::no_node)
                    {
                        expr(depth + 1, element.value);
                        continue;
                    }
                    const SourceRange element_range =
                        module_.expr(element.key).range.join(module_.expr(element.value).range);
                    line(depth + 1, "TimedElement", element_range, "");
                    expr(depth + 2, element.key, "key");
                    expr(depth + 2, element.value);
                }
            }
            void expr_node(int depth, SourceRange range, const ast::TupleLiteral &e, std::string_view slot)
            {
                line(depth, "TupleLiteral", range, "", slot);
                for (const ast::ExprId element : e.elements) { expr(depth + 1, element); }
            }
            void expr_node(int depth, SourceRange range, const ast::AnonymousFn &e, std::string_view slot)
            {
                line(depth, "AnonymousFn", range, "", slot);
                for (const ast::AnonymousParameter &parameter : e.parameters)
                {
                    SourceRange parameter_range = parameter.name.range;
                    if (parameter.type != ast::no_node)
                    {
                        parameter_range = parameter_range.join(module_.type(parameter.type).range);
                    }
                    line(depth + 1, "Parameter", parameter_range, std::string{parameter.name.text});
                    if (parameter.type != ast::no_node) { type(depth + 2, parameter.type, "type"); }
                }
                if (e.result != ast::no_node) { type(depth + 1, e.result, "result"); }
                expr(depth + 1, e.body, "body");
            }
            void expr_node(int depth, SourceRange range, const ast::If &e, std::string_view slot)
            {
                line(depth, "If", range, "", slot);
                expr(depth + 1, e.condition, "condition");
                block(depth + 1, e.then_block, "then");
                if (e.otherwise != ast::no_node) { expr(depth + 1, e.otherwise, "else"); }
            }
            void expr_node(int depth, SourceRange range, const ast::BlockExpr &e, std::string_view slot)
            {
                line(depth, "BlockExpr", range, "", slot);
                block(depth + 1, e.block);
            }
            void expr_node(int depth, SourceRange range, const ast::Eval &e, std::string_view slot)
            {
                line(depth, "Eval", range, "", slot);
                expr(depth + 1, e.callee, "callee");
                arguments(depth + 1, e.arguments);
            }
            void expr_node(int depth, SourceRange range, const ast::Construct &e, std::string_view slot)
            {
                line(depth, e.delta ? "DeltaConstruct" : "StructConstruct", range, "", slot);
                type(depth + 1, e.type, "type");
                arguments(depth + 1, e.arguments);
            }

            // --------------------------------------------------- constraints

            void constraint(int depth, ast::ConstraintId id, std::string_view slot = {})
            {
                const ast::Constraint &node = module_.constraint(id);
                std::visit([&](const auto &c) { constraint_node(depth, node.range, c, slot); }, node.node);
            }

            void constraint_node(int depth, SourceRange range, const ast::ConstraintName &c, std::string_view slot)
            { line(depth, "ConstraintName", range, std::string{c.name.text}, slot); }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintType &c, std::string_view slot)
            {
                line(depth, "ConstraintType", range, "", slot);
                type(depth + 1, c.type);
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintValue &c, std::string_view slot)
            {
                line(depth, "ConstraintValue", range, "", slot);
                expr(depth + 1, c.value);
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintSet &c, std::string_view slot)
            {
                line(depth, "ConstraintSet", range, "", slot);
                for (const ast::ConstraintId element : c.elements) { constraint(depth + 1, element); }
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintCall &c, std::string_view slot)
            {
                std::string name;
                if (!c.qualifier.empty())
                {
                    name += c.qualifier.text;
                    name += "::";
                }
                name += c.name.text;
                line(depth, "ConstraintCall", range, std::move(name), slot);
                for (const ast::ConstraintId argument : c.arguments) { constraint(depth + 1, argument); }
            }
            void constraint_node(int depth, SourceRange range, const ast::OperatorRequirement &c, std::string_view slot)
            {
                std::string name;
                if (!c.qualifier.empty())
                {
                    name += c.qualifier.text;
                    name += "::";
                }
                name += c.name.text;
                line(depth, "OperatorRequirement", range, std::move(name), slot);
                for (const ast::ConstraintId argument : c.arguments) { constraint(depth + 1, argument); }
                if (c.result != ast::no_node) { type(depth + 1, c.result, "result"); }
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintRelation &c, std::string_view slot)
            {
                const std::string_view op = c.op == ast::ConstraintRelationOp::Equal ? "=="
                                            : c.op == ast::ConstraintRelationOp::In  ? "in"
                                                                                     : "is";
                line(depth, "ConstraintRelation", range, std::string{op}, slot);
                constraint(depth + 1, c.lhs);
                if (c.op == ast::ConstraintRelationOp::Is)
                {
                    line(depth + 1, "Category", c.category.range, std::string{c.category.text});
                }
                else
                {
                    constraint(depth + 1, c.rhs);
                }
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintNot &c, std::string_view slot)
            {
                line(depth, "ConstraintNot", range, "", slot);
                constraint(depth + 1, c.operand);
            }
            void constraint_node(int depth, SourceRange range, const ast::ConstraintLogic &c, std::string_view slot)
            {
                line(depth, "ConstraintLogic", range, c.op == ast::ConstraintLogicOp::And ? "&&" : "||", slot);
                constraint(depth + 1, c.lhs);
                constraint(depth + 1, c.rhs);
            }

            void arguments(int depth, const std::vector<ast::Argument> &arguments)
            {
                for (const ast::Argument &argument : arguments)
                {
                    SourceRange range = module_.expr(argument.value).range;
                    if (!argument.name.empty()) { range = argument.name.range.join(range); }
                    line(depth, "Argument", range, std::string{argument.name.text});
                    expr(depth + 1, argument.value);
                }
            }

            // ----------------------------------------------------- statements

            void block(int depth, ast::BlockId id, std::string_view slot = {})
            {
                const ast::Block &node = module_.block(id);
                line(depth, "Block", node.range, "", slot);
                for (const ast::StmtId statement : node.statements) { stmt(depth + 1, statement, node.tail); }
            }

            void stmt(int depth, ast::StmtId id, ast::ExprId tail)
            {
                const ast::Stmt &node = module_.stmt(id);
                std::visit([&](const auto &s) { stmt_node(depth, node.range, s, tail); }, node.node);
            }

            void stmt_node(int depth, SourceRange range, const ast::LocalDecl &s, ast::ExprId)
            {
                line(depth, "LocalDecl", range, (s.mutable_ ? "var " : "let ") + std::string{s.name.text});
                if (s.type != ast::no_node) { type(depth + 1, s.type, "type"); }
                expr(depth + 1, s.init, "init");
            }
            void stmt_node(int depth, SourceRange range, const ast::StateDecl &s, ast::ExprId)
            {
                line(depth, "StateDecl", range, std::string{s.name.text});
                if (s.type != ast::no_node) { type(depth + 1, s.type, "type"); }
                expr(depth + 1, s.init, "init");
            }
            void stmt_node(int depth, SourceRange range, const ast::InjectDecl &s, ast::ExprId)
            {
                line(depth, "InjectDecl", range, join_names(s.names, ", "));
            }
            void stmt_node(int depth, SourceRange range, const ast::LifecycleBlock &s, ast::ExprId)
            {
                line(depth, s.is_stop ? "Stop" : "Start", range, "");
                block(depth + 1, s.block);
            }
            void stmt_node(int depth, SourceRange range, const ast::WhenStmt &s, ast::ExprId)
            {
                line(depth, "When", range, "");
                expr(depth + 1, s.condition, "condition");
                block(depth + 1, s.block);
            }
            void stmt_node(int depth, SourceRange range, const ast::ForStmt &s, ast::ExprId)
            {
                std::string pattern{s.first.text};
                if (!s.second.empty())
                {
                    pattern += ", ";
                    pattern += s.second.text;
                }
                line(depth, "For", range, std::move(pattern));
                expr(depth + 1, s.iterable, "in");
                block(depth + 1, s.block);
            }
            void stmt_node(int depth, SourceRange range, const ast::AssignStmt &s, ast::ExprId)
            {
                line(depth, "Assign", range, std::string{ast::assign_op_spelling(s.op)});
                expr(depth + 1, s.place, "place");
                expr(depth + 1, s.value, "value");
            }
            void stmt_node(int depth, SourceRange range, const ast::ReturnStmt &s, ast::ExprId)
            {
                line(depth, "Return", range, "");
                if (s.value != ast::no_node) { expr(depth + 1, s.value); }
            }
            void stmt_node(int depth, SourceRange range, const ast::AssertStmt &s, ast::ExprId)
            {
                line(depth, "Assert", range, "");
                expr(depth + 1, s.condition);
            }
            void stmt_node(int depth, SourceRange range, const ast::ExprStmt &s, ast::ExprId tail)
            {
                line(depth, "ExprStmt", range, s.expr == tail ? "tail" : "");
                expr(depth + 1, s.expr);
            }

            const ast::Module &module_;
            std::string        out_;
        };
    }  // namespace

    std::string print_ast(const ast::Module &module)
    {
        return Printer{module}.run();
    }
}  // namespace hgl::syntax
