#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_data/impl/checkpoint.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series_reference.h>

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

            bool atomic_eligible(const TSDataView &, const TSCheckpointContext *) { return true; }

            TSCheckpointImage atomic_capture(const TSDataView &view, const TSCheckpointContext *)
            {
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                if (view.has_current_value()) { image.payload = Value{view.value()}; }
                return image;
            }

            void atomic_validate(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *)
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

            void atomic_restore(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *)
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

            bool reference_eligible(const TSDataView &, const TSCheckpointContext *context)
            {
                return context != nullptr;
            }

            TSCheckpointImage reference_capture(const TSDataView &view, const TSCheckpointContext *context)
            {
                if (context == nullptr || !context->capture_reference)
                    throw std::invalid_argument("reference checkpoint requires a capture context");
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                if (view.has_current_value())
                {
                    image.reference = context->capture_reference(view.value().checked_as<TimeSeriesReference>());
                    validate_ts_reference_checkpoint(*image.reference);
                }
                return image;
            }

            void reference_validate(const TSDataView &view, const TSCheckpointImage &image,
                                    const TSCheckpointContext *context)
            {
                validate_header(view, image);
                if (context == nullptr || !context->restore_reference)
                    throw std::invalid_argument("reference checkpoint requires a restore context");
                if (image.payload.has_value() || !image.children.empty() || !image.keys.empty() ||
                    !image.slots.empty() || !image.free_slots.empty() || !image.published.empty() ||
                    image.slot_capacity != 0 || image.key_set_last_modified_time != MIN_DT ||
                    image.reference.has_value() != (image.last_modified_time != MIN_DT))
                    throw std::invalid_argument("reference checkpoint shape or validity mismatch");
                if (image.reference) { validate_ts_reference_checkpoint(*image.reference); }
            }

            void reference_restore(const TSDataView &view, const TSCheckpointImage &image,
                                   const TSCheckpointContext *context)
            {
                if (image.reference) { context->restore_reference(view, *image.reference); }
                const auto &ops = view.ops();
                ops.mutable_tracking_impl(ops.context, view.mutable_data())->last_modified_time = image.last_modified_time;
            }

            bool fixed_eligible(const TSDataView &view, const TSCheckpointContext *context)
            {
                for (std::size_t i = 0; i < view.indexed_child_count(); ++i)
                    if (!ts_checkpoint_eligible(view.indexed_child_at(i), context)) { return false; }
                return true;
            }

            TSCheckpointImage fixed_capture(const TSDataView &view, const TSCheckpointContext *context)
            {
                TSCheckpointImage image;
                image.schema = view.schema();
                image.last_modified_time = view.last_modified_time();
                image.children.reserve(view.indexed_child_count());
                for (std::size_t i = 0; i < view.indexed_child_count(); ++i)
                    image.children.push_back(capture_ts_checkpoint(view.indexed_child_at(i), context));
                return image;
            }

            void fixed_validate(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *context)
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
                    validate_ts_checkpoint(view.indexed_child_at(i), image.children[i], context);
                }
            }

            void fixed_restore(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *context)
            {
                const auto &ops = view.ops();
                void *memory = view.mutable_data();
                for (std::size_t i = 0; i < image.children.size(); ++i)
                {
                    restore_validated(view.indexed_child_at(i), image.children[i], context);
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
                [](const TSDataView &, const TSCheckpointContext *) { return false; },
                [](const TSDataView &view, const TSCheckpointContext *) -> TSCheckpointImage { unsupported(view); },
                [](const TSDataView &view, const TSCheckpointImage &, const TSCheckpointContext *) { unsupported(view); },
                [](const TSDataView &view, const TSCheckpointImage &, const TSCheckpointContext *) { unsupported(view); },
            };
            return ops;
        }

        const TSCheckpointOps &atomic_checkpoint_ops() noexcept
        {
            static const TSCheckpointOps ops{atomic_eligible, atomic_capture, atomic_validate, atomic_restore};
            return ops;
        }

        const TSCheckpointOps &reference_checkpoint_ops() noexcept
        {
            static const TSCheckpointOps ops{
                reference_eligible, reference_capture, reference_validate, reference_restore};
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
            if (image.schema->kind != TSTypeKind::REF && image.reference)
                throw std::invalid_argument("non-reference checkpoint contains reference metadata");
            if (image.schema->kind != TSTypeKind::TSW && !image.window_times.empty())
                throw std::invalid_argument("non-window checkpoint contains sample timestamps");
            if (!view.ops().allows_mutation || view.last_modified_time() != MIN_DT)
                throw std::invalid_argument("checkpoint restore requires fresh writable endpoint storage");
            (void)view.mutable_data();
        }

        void restore_validated(const TSDataView &view, const TSCheckpointImage &image, const TSCheckpointContext *context)
        {
            view.ops().checkpoint_ops->restore_impl(view, image, context);
        }
    }

    void validate_ts_reference_checkpoint(const TSReferenceCheckpointImage &image)
    {
        const auto validate = [&](const auto &self, const TSReferenceCheckpointImage &value, std::size_t depth) -> void {
            if (depth > 256) { throw std::invalid_argument("reference checkpoint nesting exceeds supported depth"); }
            switch (value.kind)
            {
                case TSReferenceCheckpointKind::Empty:
                    if (value.target || !value.items.empty())
                        throw std::invalid_argument("empty reference checkpoint contains a target");
                    break;
                case TSReferenceCheckpointKind::Peered:
                    if (value.target_schema == nullptr || !value.target || !value.items.empty())
                        throw std::invalid_argument("peered reference checkpoint has invalid shape");
                    if (value.target->graph_path.size() % 2 != 0 || value.target->endpoint > 4 ||
                        (value.target->endpoint != 4 && value.target->custom_endpoint != 0))
                        throw std::invalid_argument("reference checkpoint locator has invalid shape");
                    for (const auto &binding : value.target->bindings)
                        if (binding.requested_schema == nullptr)
                            throw std::invalid_argument("reference checkpoint binding has no schema");
                    break;
                case TSReferenceCheckpointKind::NonPeered:
                    // Declarations may be explicitly adapted by downcast_ref;
                    // runtime binding owns their compatibility and arity checks.
                    if (value.target || value.target_schema == nullptr)
                        throw std::invalid_argument("non-peered reference checkpoint has invalid shape");
                    for (const auto &item : value.items) { self(self, item, depth + 1); }
                    break;
                default: throw std::invalid_argument("reference checkpoint has invalid kind");
            }
        };
        validate(validate, image, 0);
    }

    void validate_ts_checkpoint_reference_placement(const TSCheckpointImage &image)
    {
        if (!image.reference) { return; }
        if (image.schema == nullptr || image.schema->kind != TSTypeKind::REF || image.payload.has_value())
            throw std::invalid_argument(
                "reference metadata on a non-reference endpoint or beside a value payload");
        validate_ts_reference_checkpoint(*image.reference);
    }

    bool ts_checkpoint_schema_contains_reference(const TSValueTypeMetaData *schema)
    {
        if (schema == nullptr) { return false; }
        if (schema->kind == TSTypeKind::REF) { return true; }
        if (schema->kind == TSTypeKind::TSL || schema->kind == TSTypeKind::TSD)
            return ts_checkpoint_schema_contains_reference(schema->element_ts());
        if (schema->kind == TSTypeKind::TSB)
            for (std::size_t i = 0; i < schema->field_count(); ++i)
                if (ts_checkpoint_schema_contains_reference(schema->fields()[i].type)) { return true; }
        return false;
    }

    bool ts_checkpoint_eligible(const TSDataView &source, const TSCheckpointContext *context)
    {
        return source.data() != nullptr && source.ops().checkpoint_ops->eligible_impl(source, context);
    }

    TSCheckpointImage capture_ts_checkpoint(const TSDataView &source, const TSCheckpointContext *context)
    {
        if (!ts_checkpoint_eligible(source, context))
            return ts_checkpoint_detail::unsupported_checkpoint_ops().capture_impl(source, context);
        return source.ops().checkpoint_ops->capture_impl(source, context);
    }

    void validate_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image, const TSCheckpointContext *context)
    {
        if (!ts_checkpoint_eligible(target, context))
            return ts_checkpoint_detail::unsupported_checkpoint_ops().validate_impl(target, image, context);
        target.ops().checkpoint_ops->validate_impl(target, image, context);
    }

    void restore_ts_checkpoint(const TSDataView &target, const TSCheckpointImage &image, const TSCheckpointContext *context)
    {
        validate_ts_checkpoint(target, image, context);
        ts_checkpoint_detail::restore_validated(target, image, context);
    }
}
