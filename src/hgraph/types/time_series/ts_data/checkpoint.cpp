#include <hgraph/types/time_series/ts_data/checkpoint_ops.h>

#include <hgraph/types/time_series/ts_data/base_view.h>

#include <stdexcept>

namespace hgraph::ts_checkpoint_detail
{
    namespace
    {
        [[noreturn]] void refuse(const char *operation)
        {
            throw std::logic_error(std::string{"TSCheckpointOps::"} + operation +
                                   ": this time-series representation does not support "
                                   "checkpointing");
        }

        void unsupported_capture(const void *, const void *, TSCheckpointImage &)
        {
            refuse("capture");
        }

        bool unsupported_validate(const void *, const TSCheckpointImage &,
                                  TSCheckpointDiagnostics &why)
        {
            why.reason = "this time-series representation does not support checkpointing";
            return false;
        }

        void unsupported_import(const void *, void *, const TSCheckpointImage &,
                                const TSCheckpointRestoreGuard &)
        {
            refuse("import");
        }
    }  // namespace

    const TSCheckpointOps &unsupported_checkpoint_ops() noexcept
    {
        static const TSCheckpointOps table{
            .supported = false,
            .capture_impl = &unsupported_capture,
            .validate_impl = &unsupported_validate,
            .import_impl = &unsupported_import,
        };
        return table;
    }
}  // namespace hgraph::ts_checkpoint_detail

namespace hgraph
{
    namespace
    {
        const TSCheckpointOps &checkpoint_ops_for(const TSDataView &data)
        {
            if (!data.valid())
            {
                throw std::logic_error("checkpoint operations require live TSData storage");
            }
            return *data.ops().checkpoint_ops;
        }
    }  // namespace

    bool checkpoint_supported(const TSDataView &data)
    {
        return data.valid() && data.ops().checkpoint_ops->supported;
    }

    TSCheckpointImage capture_checkpoint(const TSDataView &data)
    {
        const auto &ops = checkpoint_ops_for(data);
        TSCheckpointImage image;
        ops.capture_impl(data.ops().context, data.data(), image);
        image.schema = data.storage_type().schema();
        return image;
    }

    bool validate_checkpoint(const TSDataView &data, const TSCheckpointImage &image,
                             TSCheckpointDiagnostics &why)
    {
        const auto &ops = checkpoint_ops_for(data);
        if (image.schema != nullptr && data.storage_type().schema() != nullptr &&
            image.schema != data.storage_type().schema())
        {
            why.reason = "checkpoint image schema does not match this endpoint's schema";
            return false;
        }
        return ops.validate_impl(data.ops().context, image, why);
    }

    void import_checkpoint(const TSDataView &data, const TSCheckpointImage &image,
                           const TSCheckpointRestoreGuard &guard)
    {
        const auto &ops = checkpoint_ops_for(data);
        ops.import_impl(data.ops().context, data.storage_ref().data(), image, guard);
        // Children materialised by the import (dynamic TSL elements, keyed
        // slots) need their parent links attached exactly as fresh
        // construction attaches them; the walk is idempotent for children
        // that already existed.
        detail::attach_owned_ts_data_parents(TSDataView{data.storage_ref()});
    }
}  // namespace hgraph
