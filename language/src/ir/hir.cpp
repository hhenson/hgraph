#include "ir/hir.h"

#include <array>
#include <utility>

namespace hgl::ir::hir
{
    std::string_view system_operator_name(BinaryOp op) noexcept {
        switch (op) {
            case BinaryOp::Mul: return "mul_";
            case BinaryOp::Div: return "div_";
            case BinaryOp::FloorDiv: return "floordiv_";
            case BinaryOp::Rem: return "mod_";
            case BinaryOp::Add: return "add_";
            case BinaryOp::Sub: return "sub_";
            case BinaryOp::Less: return "lt_";
            case BinaryOp::LessEqual: return "le_";
            case BinaryOp::Greater: return "gt_";
            case BinaryOp::GreaterEqual: return "ge_";
            case BinaryOp::Equal: return "eq_";
            case BinaryOp::NotEqual: return "ne_";
            case BinaryOp::And: return "and_";
            case BinaryOp::Or: return "or_";
        }
        std::unreachable();
    }

    std::string_view system_operator_name(UnaryOp op) noexcept {
        switch (op) {
            case UnaryOp::Negate: return "neg_";
            case UnaryOp::Not: return "not_";
        }
        std::unreachable();
    }

    std::string_view scalar_type_name(ScalarType type) noexcept {
        static constexpr std::array names{
            std::string_view{"bool"},
            std::string_view{"i64"},
            std::string_view{"f64"},
            std::string_view{"str"},
            std::string_view{"date"},
            std::string_view{"time"},
            std::string_view{"datetime"},
            std::string_view{"duration"},
            std::string_view{"civil_datetime"},
            std::string_view{"zoned_datetime"},
            std::string_view{"zoned_time"},
            std::string_view{"timezone"},
        };
        return names[static_cast<std::size_t>(type)];
    }

    std::string_view binary_op_spelling(BinaryOp op) noexcept {
        static constexpr std::array names{
            std::string_view{"*"},  std::string_view{"/"},  std::string_view{"//"}, std::string_view{"%"},  std::string_view{"+"},
            std::string_view{"-"},  std::string_view{"<"},  std::string_view{"<="}, std::string_view{">"},  std::string_view{">="},
            std::string_view{"=="}, std::string_view{"!="}, std::string_view{"&&"}, std::string_view{"||"},
        };
        return names[static_cast<std::size_t>(op)];
    }
}  // namespace hgl::ir::hir
