#include "syntax/syntax_tree.h"

#include <array>
#include <utility>

namespace hgl::syntax
{
    namespace
    {
        using KindName = std::pair<SyntaxKind, std::string_view>;

        constexpr std::array kind_names{
            KindName{SyntaxKind::Unknown, "unknown"},
            KindName{SyntaxKind::Newlines, "newlines"},
            KindName{SyntaxKind::LineEnd, "line_end"},
            KindName{SyntaxKind::CommaSeparator, "comma_separator"},
            KindName{SyntaxKind::ModulePath, "module_path"},
            KindName{SyntaxKind::QualifiedName, "qualified_name"},
            KindName{SyntaxKind::GenericArguments, "generic_arguments"},
            KindName{SyntaxKind::GenericArgument, "generic_argument"},
            KindName{SyntaxKind::NamedType, "named_type"},
            KindName{SyntaxKind::TupleType, "tuple_type"},
            KindName{SyntaxKind::ListType, "list_type"},
            KindName{SyntaxKind::SetType, "set_type"},
            KindName{SyntaxKind::MapType, "map_type"},
            KindName{SyntaxKind::RollingType, "rolling_type"},
            KindName{SyntaxKind::AtomicType, "atomic_type"},
            KindName{SyntaxKind::Type, "type"},
            KindName{SyntaxKind::SizeExpression, "size_expression"},
            KindName{SyntaxKind::ContinuedOperator, "continued_operator"},
            KindName{SyntaxKind::ProductExpression, "product_expression"},
            KindName{SyntaxKind::SumExpression, "sum_expression"},
            KindName{SyntaxKind::ComparisonExpression, "comparison_expression"},
            KindName{SyntaxKind::EqualityExpression, "equality_expression"},
            KindName{SyntaxKind::AndExpression, "and_expression"},
            KindName{SyntaxKind::Expression, "expression"},
            KindName{SyntaxKind::Argument, "argument"},
            KindName{SyntaxKind::Arguments, "arguments"},
            KindName{SyntaxKind::IndexPostfix, "index_postfix"},
            KindName{SyntaxKind::CallPostfix, "call_postfix"},
            KindName{SyntaxKind::FieldPostfix, "field_postfix"},
            KindName{SyntaxKind::Postfix, "postfix"},
            KindName{SyntaxKind::ExplicitConstruct, "explicit_construct"},
            KindName{SyntaxKind::PrimaryExpression, "primary_expression"},
            KindName{SyntaxKind::PostfixExpression, "postfix_expression"},
            KindName{SyntaxKind::UnaryExpression, "unary_expression"},
            KindName{SyntaxKind::TupleElement, "tuple_element"},
            KindName{SyntaxKind::TupleOrGroup, "tuple_or_group"},
            KindName{SyntaxKind::SequenceElement, "sequence_element"},
            KindName{SyntaxKind::SequenceLiteral, "sequence_literal"},
            KindName{SyntaxKind::AnonymousParameter, "anonymous_parameter"},
            KindName{SyntaxKind::AnonymousFunction, "anonymous_function"},
            KindName{SyntaxKind::ElseArm, "else_arm"},
            KindName{SyntaxKind::IfExpression, "if_expression"},
            KindName{SyntaxKind::EvalExpression, "eval_expression"},
            KindName{SyntaxKind::LocalDecl, "local_decl"},
            KindName{SyntaxKind::StateDecl, "state_decl"},
            KindName{SyntaxKind::InjectDecl, "inject_decl"},
            KindName{SyntaxKind::LifecycleStmt, "lifecycle_stmt"},
            KindName{SyntaxKind::WhenStmt, "when_stmt"},
            KindName{SyntaxKind::ForStmt, "for_stmt"},
            KindName{SyntaxKind::ReturnStmt, "return_stmt"},
            KindName{SyntaxKind::AssertStmt, "assert_stmt"},
            KindName{SyntaxKind::AssignOrExpressionStmt, "assign_or_expression_stmt"},
            KindName{SyntaxKind::Statement, "statement"},
            KindName{SyntaxKind::BlockItem, "block_item"},
            KindName{SyntaxKind::Block, "block"},
            KindName{SyntaxKind::GenericParameter, "generic_parameter"},
            KindName{SyntaxKind::GenericParameters, "generic_parameters"},
            KindName{SyntaxKind::Parameter, "parameter"},
            KindName{SyntaxKind::Signature, "signature"},
            KindName{SyntaxKind::ConstraintSet, "constraint_set"},
            KindName{SyntaxKind::ConstraintCall, "constraint_call"},
            KindName{SyntaxKind::ConstraintOperand, "constraint_operand"},
            KindName{SyntaxKind::ConstraintTerm, "constraint_term"},
            KindName{SyntaxKind::ConstraintAnd, "constraint_and"},
            KindName{SyntaxKind::Constraint, "constraint"},
            KindName{SyntaxKind::RequiresClause, "requires_clause"},
            KindName{SyntaxKind::OptionalRequiresClause, "optional_requires_clause"},
            KindName{SyntaxKind::FunctionDecl, "function_decl"},
            KindName{SyntaxKind::OperatorDecl, "operator_decl"},
            KindName{SyntaxKind::StructMember, "struct_member"},
            KindName{SyntaxKind::StructBodyItem, "struct_body_item"},
            KindName{SyntaxKind::StructDecl, "struct_decl"},
            KindName{SyntaxKind::UseDecl, "use_decl"},
            KindName{SyntaxKind::TestDecl, "test_decl"},
            KindName{SyntaxKind::Declaration, "declaration"},
            KindName{SyntaxKind::DeclarationLine, "declaration_line"},
            KindName{SyntaxKind::ModuleDecl, "module_decl"},
            KindName{SyntaxKind::Module, "module"},
        };
    }  // namespace

    std::string_view syntax_kind_name(SyntaxKind kind) noexcept {
        for (const auto &[candidate, name] : kind_names) {
            if (candidate == kind) { return name; }
        }
        return "unknown";
    }

    SyntaxKind syntax_kind_from_name(std::string_view name) noexcept {
        for (const auto &[kind, candidate] : kind_names) {
            if (candidate == name) { return kind; }
        }
        return SyntaxKind::Unknown;
    }

    bool SyntaxTree::source_is_complete() const noexcept {
        std::uint32_t next = 0;
        for (const SourceFragment &fragment : fragments) {
            if (fragment.range.empty() || fragment.range.begin != next || fragment.range.end > source_size) { return false; }
            next = fragment.range.end;
        }
        return next == source_size;
    }

    std::string SyntaxTree::reconstruct(const SourceFile &file) const {
        if (file.text().size() != source_size || !source_is_complete()) { return {}; }

        std::string result;
        result.reserve(source_size);
        for (const SourceFragment &fragment : fragments) { result += file.slice(fragment.range); }
        return result;
    }
}  // namespace hgl::syntax
