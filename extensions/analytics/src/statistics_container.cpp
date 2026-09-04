#include <hgraph/analytics/operators.h>

#include "operator_registration.h"

#include <hgraph/lib/std/operators/impl/arithmetic_impl.h>

namespace hgraph::analytics::detail
{
    namespace
    {
        namespace arithmetic = hgraph::stdlib::arithmetic_impl_detail;

        template <ValueTypeKind Kind, typename Element> void register_container_dispersion() {
            register_overload<std_, arithmetic::container_numeric_agg_plain<arithmetic::ContainerAgg::Std, Kind, Element>>();
            register_overload<std_, arithmetic::container_numeric_agg_default<arithmetic::ContainerAgg::Std, Kind, Element>>();
            register_overload<var_, arithmetic::container_numeric_agg_plain<arithmetic::ContainerAgg::Var, Kind, Element>>();
            register_overload<var_, arithmetic::container_numeric_agg_default<arithmetic::ContainerAgg::Var, Kind, Element>>();
        }

        template <ValueTypeKind Kind> void register_container_dispersion() {
            register_container_dispersion<Kind, Int>();
            register_container_dispersion<Kind, Float>();
        }

    }  // namespace

    // std_ / var_ over TS[map | set | list] values and the running moments;
    // one registration group per translation unit (see "Registration
    // translation units" in the operators developer guide).
    void register_statistics_container_overloads()
    {
        register_container_dispersion<ValueTypeKind::Map>();
        register_container_dispersion<ValueTypeKind::Set>();
        register_container_dispersion<ValueTypeKind::List>();

        register_overload<std_, arithmetic::running_moments_impl<Int, true>>();
        register_overload<std_, arithmetic::running_moments_impl<Float, true>>();
        register_overload<var_, arithmetic::running_moments_impl<Int, false>>();
        register_overload<var_, arithmetic::running_moments_impl<Float, false>>();
    }
}  // namespace hgraph::analytics::detail
