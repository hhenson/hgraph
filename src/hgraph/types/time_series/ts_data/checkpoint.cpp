#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_data/impl/checkpoint.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_data/ops.h>

#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace ts_checkpoint_detail
    {
        namespace
        {
            [[noreturn]] void unsupported(const TSDataView &view)
            {
                throw std::invalid_argument("checkpoint is unsupported for time-series representation '" +
                    std::string{view.schema() != nullptr ? view.schema()->name() : "<unbound>"} + "'");
            }

            bool atomic_eligible(const TSDataView &) { return true; }

            TSCheckpointImage atomic_capture(const TSDataView &view)
            {
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                if (view.has_current_value()) { image.payload = Value{view.value()}; }
                return image;
            }

            void atomic_validate(const TSDataView &view, const TSCheckpointImage &image)
            {
                validate_header(view, image);
                if (!image.children.empty() || !image.keys.empty() || !image.slots.empty() ||
                    !image.free_slots.empty() || !image.published.empty() || image.slot_capacity != 0 ||
                    image.key_set_last_modified_time != MIN_DT)
                {
                    throw std::invalid_argument("atomic checkpoint contains structural metadata");
                }
                if (image.payload.has_value() != (image.last_modified_time != MIN_DT))
                {
                    throw std::invalid_argument("atomic checkpoint validity and payload disagree");
                }
                const auto binding = view.layout().value_binding;
                if (image.payload.has_value() && image.payload.binding() != binding &&
                    !binding.ops_ref().accepts_source(binding, image.payload.binding()) &&
                    image.payload.binding().plan() != binding.plan())
                {
                    throw std::invalid_argument("atomic checkpoint payload schema mismatch");
                }
            }

            void atomic_restore(const TSDataView &view, const TSCheckpointImage &image)
            {
                const auto &ops = view.ops();
                void *memory = view.mutable_data();
                if (image.payload.has_value())
                {
                    // This representation's assignment hook updates payload/cache
                    // only. Modification and notification belong to mutation views,
                    // which deliberately do not participate in a quiet import.
                    (void)ops.copy_value_from_impl(ops.context, memory, image.payload.view(),
                                                   image.last_modified_time);
                }
                ops.mutable_tracking_impl(ops.context, memory)->last_modified_time = image.last_modified_time;
            }

            bool fixed_eligible(const TSDataView &view)
            {
                for (std::size_t i = 0; i < view.indexed_child_count(); ++i)
                    if (!ts_checkpoint_eligible(view.indexed_child_at(i))) { return false; }
                return true;
            }

            TSCheckpointImage fixed_capture(const TSDataView &view)
            {
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                image.children.reserve(view.indexed_child_count());
                for (std::size_t i = 0; i < view.indexed_child_count(); ++i)
                    image.children.push_back(capture_ts_checkpoint(view.indexed_child_at(i)));
                return image;
            }

            void fixed_validate(const TSDataView &view, const TSCheckpointImage &image)
            {
                validate_header(view, image);
                if (image.payload.has_value() || !image.keys.empty() || !image.slots.empty() ||
                    !image.free_slots.empty() || !image.published.empty() || image.slot_capacity != 0 ||
                    image.key_set_last_modified_time != MIN_DT ||
                    image.children.size() != view.indexed_child_count())
                {
                    throw std::invalid_argument("fixed structure checkpoint shape mismatch");
                }
                for (std::size_t i = 0; i < image.children.size(); ++i)
                {
                    if (image.children[i].last_modified_time > image.last_modified_time)
                        throw std::invalid_argument("fixed checkpoint child timestamp exceeds its parent");
                    validate_ts_checkpoint(view.indexed_child_at(i), image.children[i]);
                }
            }

            void fixed_restore(const TSDataView &view, const TSCheckpointImage &image)
            {
                const auto &ops = view.ops();
                void *memory = view.mutable_data();
                for (std::size_t i = 0; i < image.children.size(); ++i)
                {
                    restore_validated(view.indexed_child_at(i), image.children[i]);
                    if (image.children[i].last_modified_time != MIN_DT)
                    {
                        // The fixed strategy updates its value-layer field-validity
                        // bit here without touching timestamps or notifying parents.
                        ops.record_child_modified_impl(ops.context, memory, i,
                                                       image.children[i].last_modified_time);
                    }
                }
                ops.mutable_tracking_impl(ops.context, memory)->last_modified_time = image.last_modified_time;
            }
        }

        const TSCheckpointOps &unsupported_checkpoint_ops() noexcept
        {
            static const TSCheckpointOps ops{
                [](const TSDataView &) { return false; },
                [](const TSDataView &view) -> TSCheckpointImage { unsupported(view); },
                [](const TSDataView &view, const TSCheckpointImage &) { unsupported(view); },
                [](const TSDataView &view, const TSCheckpointImage &) { unsupported(view); },
            };
            return ops;
        }

        const TSCheckpointOps &atomic_checkpoint_ops() noexcept
        {
            static const TSCheckpointOps ops{atomic_eligible, atomic_capture, atomic_validate, atomic_restore};
            return ops;
        }

        const TSCheckpointOps &fixed_checkpoint_ops() noexcept
        {
            static const TSCheckpointOps ops{fixed_eligible, fixed_capture, fixed_validate, fixed_restore};
            return ops;
        }

        void validate_header(const TSDataView &view, const TSCheckpointImage &image)
        {
            if (image.version != TSCheckpointImage::current_version || image.schema != view.schema())
                throw std::invalid_argument("checkpoint version or time-series schema mismatch");
            if (!view.ops().allows_mutation || view.last_modified_time() != MIN_DT)
                throw std::invalid_argument("checkpoint restore requires fresh writable endpoint storage");
            (void)view.mutable_data();
        }

        void restore_validated(const TSDataView &view, const TSCheckpointImage &image)
        {
            view.ops().checkpoint_ops->restore_impl(view, image);
        }
    }

    bool ts_checkpoint_eligible(const TSDataView &source)
    {
        return source.data() != nullptr && source.ops().checkpoint_ops->eligible_impl(source);
    }

    TSCheckpointImage capture_ts_checkpoint(const TSDataView &source)
    {
        if (!ts_checkpoint_eligible(source))
            return ts_checkpoint_detail::unsupported_checkpoint_ops().capture_impl(source);
        return source.ops().checkpoint_ops->capture_impl(source);
    }

    void validate_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image)
    {
        if (!ts_checkpoint_eligible(target))
            return ts_checkpoint_detail::unsupported_checkpoint_ops().validate_impl(target, image);
        target.ops().checkpoint_ops->validate_impl(target, image);
    }

    void restore_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image)
    {
        validate_ts_checkpoint(target, image);
        ts_checkpoint_detail::restore_validated(target, image);
    }
}
