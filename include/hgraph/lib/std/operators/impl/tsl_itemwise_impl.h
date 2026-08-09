#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H

#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/operator_dispatch.h>

#include <span>
#include <utility>
#include <vector>

namespace hgraph::stdlib::tsl_itemwise_impl_detail
{
    using namespace hgraph::operator_type_resolution;

    inline void resolve_map_output_with_fn(ResolutionMap &resolution, OperatorCallContext context, WiredFn wired_fn)
    {
        if (output_bound(resolution)) { return; }

        std::vector<WiringArg> args;
        args.reserve(context.args.size() + 1);

        WiringArg func;
        func.kind         = WiringArg::Kind::Scalar;
        func.scalar_value = Value{std::move(wired_fn)};
        func.scalar_meta  = func.scalar_value.schema();
        args.push_back(std::move(func));

        for (const WiringArg &arg : context.args) { args.push_back(arg); }

        const TSValueTypeMetaData *output = fallback_on_exception<const TSValueTypeMetaData *>(nullptr, [&] {
            ResolvedOperatorCall resolved =
                OperatorRegistry::instance().resolve(map_::name,
                                                     std::span<const WiringArg>{args.data(), args.size()},
                                                     true);
            return ts_pattern_resolve(resolved.impl->output, resolved.map);
        });
        bind_output(resolution, output);
    }

    template <typename Op>
    inline void resolve_map_output(ResolutionMap &resolution, OperatorCallContext context)
    {
        resolve_map_output_with_fn(resolution, context, fn<Op>());
    }

    template <typename Op>
    struct tsl_unary_map
    {
        static constexpr auto name = "tsl_unary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 1 && fixed_tsl_arg(context, 0) != nullptr;
        }

        static auto compose(Wiring &w, NamedPort<"ts", TSL<TsVar<"S">>> ts)
        {
            return wire<map_>(w, fn<Op>(), ts);
        }
    };

    template <typename Op>
    struct tsl_binary_map
    {
        static constexpr auto name = "tsl_binary_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && same_fixed_tsl_size(context, 0, 1);
        }

        static auto compose(Wiring &w,
                            NamedPort<"lhs", TSL<TsVar<"L">>> lhs,
                            NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };

    template <typename Lifted>
    struct tsl_binary_lifted_map
    {
        static constexpr auto name = "tsl_binary_lifted_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output_with_fn(resolution, context, Lifted::fn());
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return context.args.size() == 2 && same_fixed_tsl_size(context, 0, 1);
        }

        static auto compose(Wiring &w,
                            NamedPort<"lhs", TSL<TsVar<"L">>> lhs,
                            NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, Lifted::fn(), lhs, rhs);
        }
    };

    template <typename Op>
    struct tsl_rhs_broadcast_map
    {
        static constexpr auto name = "tsl_rhs_broadcast_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return context.args.size() == 2 &&
                       fixed_tsl_arg(context, 0) != nullptr &&
                       !time_series_arg_matches<AnyTSL>(context, 1);
            }

        static auto compose(Wiring &w, NamedPort<"lhs", TSL<TsVar<"L">>> lhs, NamedPort<"rhs", TsVar<"R">> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };

    template <typename Op>
    struct tsl_lhs_broadcast_map
    {
        static constexpr auto name = "tsl_lhs_broadcast_map";

        static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
        {
            resolve_map_output<Op>(resolution, context);
        }

        static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return context.args.size() == 2 &&
                       !time_series_arg_matches<AnyTSL>(context, 0) &&
                       fixed_tsl_arg(context, 1) != nullptr;
            }

        static auto compose(Wiring &w, NamedPort<"lhs", TsVar<"L">> lhs, NamedPort<"rhs", TSL<TsVar<"R">>> rhs)
        {
            return wire<map_>(w, fn<Op>(), lhs, rhs);
        }
    };
}  // namespace hgraph::stdlib::tsl_itemwise_impl_detail

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_TSL_ITEMWISE_IMPL_H
