#ifndef HGRAPH_TYPES_IMPL_DEFAULT_WIRING_H
#define HGRAPH_TYPES_IMPL_DEFAULT_WIRING_H

#include <hgraph/types/graph_wiring.h>

namespace hgraph::stdlib
{
    /** Subscribe to the chosen endpoint by reference so ordinary publications
        pass through without rebinding the selector. */
    struct default_ref_impl
    {
        static constexpr auto name = "default_ref";

        static void eval(In<"ts_ref", REF<TsVar<"S">>, InputValidity::Unchecked> ts_ref,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"default_value", REF<TsVar<"S">>, InputValidity::Unchecked> default_value,
                         Out<REF<TsVar<"S">>> out)
        {
            if (!ts.valid())
            {
                ts.make_active();
                out.set(default_value.value());
            }
            else
            {
                ts.make_passive();
                out.set(ts_ref.value());
            }
        }
    };

    /** Wake the selector when an owned scalar explicitly loses its value.
        A detached forwarding route is a separate reference event. */
    struct default_scalar_withdrawal
    {
        static void eval(In<"ts", SIGNAL, InputValidity::Unchecked> ts, Out<TS<Bool>> withdrawn)
        {
            if (ts.valid()) { return; }
            const auto &owner = ts.base().data_view();
            if (owner.valid() && !owner.ops().is_target_link &&
                owner.tracking().cleared_at(ts.base().evaluation_time()))
            {
                withdrawn.set(true);
            }
        }
    };

    struct default_scalar_ref_impl
    {
        static constexpr auto name = "default_ref";

        static void eval(In<"ts_ref", REF<TsVar<"S">>, InputValidity::Unchecked> ts_ref,
                         In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                         In<"default_value", REF<TsVar<"S">>, InputValidity::Unchecked> default_value,
                         In<"withdrawn", SIGNAL, InputValidity::Unchecked>, Out<REF<TsVar<"S">>> out)
        {
            default_ref_impl::eval(std::move(ts_ref), std::move(ts), std::move(default_value), std::move(out));
        }
    };

    inline WiringPortRef wire_default_sources(Wiring &w, WiringPortRef ts_value, WiringPortRef default_value)
    {
        auto &registry = TypeRegistry::instance();

        WiringPortRef ts_ref = ts_value;
        ts_ref.schema = registry.ref(ts_value.schema);

        WiringPortRef default_ref = std::move(default_value);
        default_ref.schema = registry.ref(default_ref.schema);

        if (ts_value.schema->kind == TSTypeKind::TS)
        {
            auto withdrawal = wire<default_scalar_withdrawal>(w, Port<void>{w, ts_value});
            return wire<default_scalar_ref_impl>(
                       w, Port<void>{w, std::move(ts_ref)}, Port<void>{w, std::move(ts_value)},
                       Port<void>{w, std::move(default_ref)}, withdrawal)
                .erased();
        }
        return wire<default_ref_impl>(
                   w, Port<void>{w, std::move(ts_ref)}, Port<void>{w, std::move(ts_value)},
                   Port<void>{w, std::move(default_ref)})
            .erased();
    }
}
#endif
