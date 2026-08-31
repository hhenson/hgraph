#include <hgraph/lib/std/operators/impl/series_impl.h>

#include <arrow/compute/initialize.h>

#include <stdexcept>

namespace hgraph::stdlib
{
    void register_series_operators()
    {
        using AnyFrame = FrameOf<ScalarVar<"ROW">>;
        using AnyMetadataFrame = FrameOf<ScalarVar<"ROW">, ScalarVar<"METADATA">>;

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

        register_overload<add_, series_binary_impl<"add", false, "add_series">>();
        register_overload<sub_, series_binary_impl<"subtract", false, "sub_series">>();
        register_overload<mul_, series_binary_impl<"multiply", false, "mul_series">>();
        register_overload<div_, series_binary_impl<"divide", true, "div_series">>();
        register_overload<min_, series_extremum_impl<true>>();
        register_overload<max_, series_extremum_impl<false>>();
        register_overload<getitem_, series_getitem_impl>();
        register_overload<getitem_, series_getitem_scalar_impl>();
        register_overload<getitem_,
                          frame_column_impl<AnyFrame, "key", "getitem_frame_column">>();
        register_overload<getitem_, frame_column_impl<AnyMetadataFrame, "key",
                                                       "getitem_metadata_frame_column">>();
        register_overload<getattr_,
                          frame_column_impl<AnyFrame, "attr", "getattr_frame_column">>();
        register_overload<getattr_, frame_column_impl<AnyMetadataFrame, "attr",
                                                       "getattr_metadata_frame_column">>();
        register_overload<getitem_, frame_row_impl<AnyFrame, Scalar<"key", Int>,
                                                   "getitem_frame_row_scalar">>();
        register_overload<getitem_, frame_row_impl<AnyMetadataFrame, Scalar<"key", Int>,
                                                   "getitem_metadata_frame_row_scalar">>();
        register_overload<getitem_, frame_row_impl<AnyFrame, In<"key", TS<Int>>,
                                                   "getitem_frame_row">>();
        register_overload<getitem_, frame_row_impl<AnyMetadataFrame, In<"key", TS<Int>>,
                                                   "getitem_metadata_frame_row">>();
        register_overload<contains_, series_contains_impl>();
    }
}  // namespace hgraph::stdlib
