#include "syntax/ast.h"

namespace hgl::syntax::ast
{
    std::string_view scalar_type_name(ScalarType type) noexcept
    {
        switch (type)
        {
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
        return "?";
    }

    std::string_view unary_op_spelling(UnaryOp op) noexcept
    {
        switch (op)
        {
            case UnaryOp::Negate: return "-";
            case UnaryOp::Not: return "!";
        }
        return "?";
    }

    std::string_view binary_op_spelling(BinaryOp op) noexcept
    {
        switch (op)
        {
            case BinaryOp::Mul: return "*";
            case BinaryOp::Div: return "/";
            case BinaryOp::Rem: return "%";
            case BinaryOp::Add: return "+";
            case BinaryOp::Sub: return "-";
            case BinaryOp::Less: return "<";
            case BinaryOp::LessEqual: return "<=";
            case BinaryOp::Greater: return ">";
            case BinaryOp::GreaterEqual: return ">=";
            case BinaryOp::Equal: return "==";
            case BinaryOp::NotEqual: return "!=";
            case BinaryOp::And: return "&&";
            case BinaryOp::Or: return "||";
        }
        return "?";
    }

    std::string_view assign_op_spelling(AssignOp op) noexcept
    {
        switch (op)
        {
            case AssignOp::Assign: return "=";
            case AssignOp::Add: return "+=";
            case AssignOp::Sub: return "-=";
            case AssignOp::Mul: return "*=";
            case AssignOp::Div: return "/=";
        }
        return "?";
    }
}  // namespace hgl::syntax::ast
