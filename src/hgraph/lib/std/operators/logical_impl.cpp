#include <hgraph/lib/std/operators/impl/logical_impl.h>

namespace hgraph::stdlib
{
    void register_logical_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_unary_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;
        using tsb_itemwise_impl_detail::tsb_unary_map;

        register_overload<not_, lift<scalar_not<Bool>>>();
        register_overload<not_, lift<scalar_not<Int>>>();
        register_overload<not_, lift<scalar_not<Float>>>();
        register_overload<not_, lift<scalar_not<Str>>>();

        register_truthy_binary_overloads<and_, scalar_and>();
        register_truthy_binary_overloads<or_, scalar_or>();

        register_overload<invert_, lift<scalar_invert<Int>>>();
        register_overload<invert_, lift<scalar_invert_bool>>();
        register_graph_overload<invert_, tsl_unary_map<invert_>>();
        register_graph_overload<invert_, tsb_unary_map<invert_>>();
        register_overload<bit_and, lift<scalar_bit_and<Int>>>();
        register_overload<bit_and, lift<scalar_bit_and<Bool>>>();
        register_graph_overload<bit_and, tsl_binary_map<bit_and>>();
        register_graph_overload<bit_and, tsl_rhs_broadcast_map<bit_and>>();
        register_graph_overload<bit_and, tsl_lhs_broadcast_map<bit_and>>();
        register_graph_overload<bit_and, tsb_binary_map<bit_and>>();
        register_overload<bit_or, lift<scalar_bit_or<Int>>>();
        register_overload<bit_or, lift<scalar_bit_or<Bool>>>();
        register_graph_overload<bit_or, tsl_binary_map<bit_or>>();
        register_graph_overload<bit_or, tsl_rhs_broadcast_map<bit_or>>();
        register_graph_overload<bit_or, tsl_lhs_broadcast_map<bit_or>>();
        register_graph_overload<bit_or, tsb_binary_map<bit_or>>();
        register_overload<bit_xor, lift<scalar_bit_xor<Int>>>();
        register_overload<bit_xor, lift<scalar_bit_xor<Bool>>>();
        register_graph_overload<bit_xor, tsl_binary_map<bit_xor>>();
        register_graph_overload<bit_xor, tsl_rhs_broadcast_map<bit_xor>>();
        register_graph_overload<bit_xor, tsl_lhs_broadcast_map<bit_xor>>();
        register_graph_overload<bit_xor, tsb_binary_map<bit_xor>>();
        register_overload<lshift_, lift<scalar_lshift>>();
        register_graph_overload<lshift_, tsl_binary_map<lshift_>>();
        register_graph_overload<lshift_, tsl_rhs_broadcast_map<lshift_>>();
        register_graph_overload<lshift_, tsl_lhs_broadcast_map<lshift_>>();
        register_graph_overload<lshift_, tsb_binary_map<lshift_>>();
        register_overload<rshift_, lift<scalar_rshift>>();
        register_graph_overload<rshift_, tsl_binary_map<rshift_>>();
        register_graph_overload<rshift_, tsl_rhs_broadcast_map<rshift_>>();
        register_graph_overload<rshift_, tsl_lhs_broadcast_map<rshift_>>();
        register_graph_overload<rshift_, tsb_binary_map<rshift_>>();
    }
}  // namespace hgraph::stdlib
