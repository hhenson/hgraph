#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // Numeric add_ / sub_ / mul_ (plus string concatenation and repetition)
    // and the unary numeric operators. Temporal and container overloads of
    // the same operators live in their own groups.
    void register_arithmetic_numeric_overloads()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_unary_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;
        using tsb_itemwise_impl_detail::tsb_unary_map;

        // add_ — homogeneous and mixed numeric.
        register_overload<add_, lift<scalar_add<Int>>>();                                      // int + int -> int
        register_overload<add_, lift<scalar_add<Float>>>();                                    // float + float -> float
        register_overload<add_, lift<scalar_add<Str>>>();                                      // string concatenation
        register_overload<add_, lift<scalar_add<Int, Float, Float>>>();                        // int + float -> float
        register_overload<add_, lift<scalar_add<Float, Int, Float>>>();                        // float + int -> float
        register_graph_overload<add_, tsl_binary_map<add_>>();
        register_graph_overload<add_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsb_binary_map<add_>>();

        // sub_ — note the result type that differs from the operands.
        register_overload<sub_, lift<scalar_sub<Int>>>();                                      // int - int -> int
        register_overload<sub_, lift<scalar_sub<Float>>>();                                    // float - float -> float
        register_overload<sub_, lift<scalar_sub<Int, Float, Float>>>();                        // int - float -> float
        register_overload<sub_, lift<scalar_sub<Float, Int, Float>>>();                        // float - int -> float
        register_graph_overload<sub_, tsl_binary_map<sub_>>();
        register_graph_overload<sub_, tsl_rhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsl_lhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsb_binary_map<sub_>>();

        // mul_ — numeric products and string repetition.
        register_overload<mul_, lift<scalar_mul<Int>>>();
        register_overload<mul_, lift<scalar_mul<Float>>>();
        register_overload<mul_, lift<scalar_mul<Int, Float, Float>>>();
        register_overload<mul_, lift<scalar_mul<Float, Int, Float>>>();
        register_overload<mul_, repeat_string_right>();
        register_overload<mul_, repeat_string_left>();
        register_graph_overload<mul_, tsl_binary_map<mul_>>();
        register_graph_overload<mul_, tsl_rhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsl_lhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsb_binary_map<mul_>>();

        register_overload<round_, round_float_impl>();

        register_overload<neg_, lift<scalar_neg<Int>>>();
        register_overload<neg_, lift<scalar_neg<Float>>>();
        register_graph_overload<neg_, tsl_unary_map<neg_>>();
        register_graph_overload<neg_, tsb_unary_map<neg_>>();
        register_overload<pos_, lift<scalar_pos<Int>>>();
        register_overload<pos_, lift<scalar_pos<Float>>>();
        register_graph_overload<pos_, tsl_unary_map<pos_>>();
        register_graph_overload<pos_, tsb_unary_map<pos_>>();
        register_overload<abs_, lift<scalar_abs<Int>>>();
        register_overload<abs_, lift<scalar_abs<Float>>>();
        register_graph_overload<abs_, tsl_unary_map<abs_>>();
        register_graph_overload<abs_, tsb_unary_map<abs_>>();
        register_overload<sign, lift<scalar_sign<Int>>>();
        register_overload<sign, lift<scalar_sign<Float>>>();
        register_overload<ln, lift<scalar_ln>>();
    }
}  // namespace hgraph::stdlib
