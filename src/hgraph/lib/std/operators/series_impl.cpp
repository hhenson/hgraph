#include <hgraph/lib/std/operators/impl/series_impl.h>

#include <arrow/compute/initialize.h>

#include <stdexcept>

namespace hgraph::stdlib
{
    void register_series_operators()
    {
        // Arrow requires the compute module to be initialised before any
        // CallFunction (the built-in kernels register here). Fail HERE, at
        // registration, rather than surfacing as confusing per-tick
        // CallFunction errors later.
        static const bool initialised = [] {
            const auto status = arrow::compute::Initialize();
            if (!status.ok())
            {
                throw std::runtime_error("arrow compute initialisation failed: " +
                                         status.ToString());
            }
            return true;
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
