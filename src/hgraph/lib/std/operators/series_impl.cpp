#include <hgraph/lib/std/operators/impl/series_impl.h>

#include <arrow/compute/initialize.h>

namespace hgraph::stdlib
{
    void register_series_operators()
    {
        // Arrow 24 requires the compute module to be initialised before any
        // CallFunction (the built-in kernels register here).
        static const bool initialised = [] {
            const auto status = arrow::compute::Initialize();
            return status.ok();
        }();
        static_cast<void>(initialised);

        register_overload<add_, series_binary_impl<"add", false>>();
        register_overload<sub_, series_binary_impl<"subtract", false>>();
        register_overload<mul_, series_binary_impl<"multiply", false>>();
        register_overload<div_, series_binary_impl<"divide", true>>();
        register_overload<getitem_, series_getitem_impl>();
        register_overload<getitem_, series_getitem_scalar_impl>();
        register_overload<contains_, series_contains_impl>();
    }
}  // namespace hgraph::stdlib
