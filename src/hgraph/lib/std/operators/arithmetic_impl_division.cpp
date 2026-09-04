#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::stdlib
{
    // div_ / floordiv_ / mod_ / divmod_ / pow_ over numeric operands. The
    // TimeDelta quotients are in the temporal group.
    void register_arithmetic_division_overloads()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        // div_ — the two-argument form defaults to DivideByZero::Error; the three-argument
        // form takes an explicit policy. Arity selects between them.
        register_overload<div_, lift<scalar_div<Int>>>();        // int / int -> float
        register_overload<div_, lift<scalar_div<Float>>>();      // float / float -> float
        register_overload<div_, lift<scalar_div<Int, Float>>>();
        register_overload<div_, lift<scalar_div<Float, Int>>>();
        register_overload<div_, div_numbers<Int, Int>>();             // int / int -> float (with policy)
        register_overload<div_, div_numbers<Float, Float>>();         // float / float -> float (with policy)
        register_overload<div_, div_numbers<Int, Float>>();
        register_overload<div_, div_numbers<Float, Int>>();
        register_graph_overload<div_, tsl_binary_map<div_>>();
        register_graph_overload<div_, tsl_rhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsl_lhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsb_binary_map<div_>>();

        // floordiv_ / mod_ — integer outputs for int operands, Float otherwise.
        register_overload<floordiv_, lift<scalar_floordiv<Int>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Int, Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float, Int>>>();
        register_overload<floordiv_, floordiv_ints>();
        register_overload<floordiv_, floordiv_numbers<Float, Float>>();
        register_overload<floordiv_, floordiv_numbers<Int, Float>>();
        register_overload<floordiv_, floordiv_numbers<Float, Int>>();
        register_graph_overload<floordiv_, tsl_binary_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_rhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_lhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsb_binary_map<floordiv_>>();

        register_overload<mod_, lift<scalar_mod<Int>>>();
        register_overload<mod_, lift<scalar_mod<Float>>>();
        register_overload<mod_, lift<scalar_mod<Int, Float>>>();
        register_overload<mod_, lift<scalar_mod<Float, Int>>>();
        register_overload<mod_, mod_ints>();
        register_overload<mod_, mod_numbers<Float, Float>>();
        register_overload<mod_, mod_numbers<Int, Float>>();
        register_overload<mod_, mod_numbers<Float, Int>>();
        register_graph_overload<mod_, tsl_binary_map<mod_>>();
        register_graph_overload<mod_, tsl_rhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsl_lhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsb_binary_map<mod_>>();

        // divmod_ — mirrors floordiv_ / mod_ result typing.
        register_overload<divmod_, divmod_ints>();
        register_overload<divmod_, divmod_numbers<Float, Float>>();
        register_overload<divmod_, divmod_numbers<Int, Float>>();
        register_overload<divmod_, divmod_numbers<Float, Int>>();

        // pow_ — homogeneous integer power remains Int; mixed/float power is Float.
        register_overload<pow_, lift<scalar_pow<Int>>>();
        register_overload<pow_, lift<scalar_pow<Float>>>();
        register_overload<pow_, lift<scalar_pow<Int, Float>>>();
        register_overload<pow_, lift<scalar_pow<Float, Int>>>();
        register_overload<pow_, pow_numbers<Int, Int>>();
        register_overload<pow_, pow_numbers<Float, Float>>();
        register_overload<pow_, pow_numbers<Int, Float>>();
        register_overload<pow_, pow_numbers<Float, Int>>();
        register_graph_overload<pow_, tsl_binary_map<pow_>>();
        register_graph_overload<pow_, tsl_rhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsl_lhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsb_binary_map<pow_>>();
    }
}  // namespace hgraph::stdlib
