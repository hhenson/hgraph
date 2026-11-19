#ifndef HGRAPH_TS_OUTPUT_FROM_REF_DESCRIPTOR_H
#define HGRAPH_TS_OUTPUT_FROM_REF_DESCRIPTOR_H

#include <hgraph/types/storage_metrics.h>

#include <memory>

namespace hgraph
{
    struct TSValueTypeMetaData;
    class TypeRealizationSnapshot;

    namespace detail
    {
        struct FromRefDescriptor;

        /** Resolve immutable binding metadata for a root REF output adapter. */
        [[nodiscard]] std::shared_ptr<const FromRefDescriptor> get_from_ref_descriptor(
            const TypeRealizationSnapshot &snapshot, const TSValueTypeMetaData &requested,
            const TSValueTypeMetaData *referenced);

        [[nodiscard]] std::shared_ptr<const FromRefDescriptor> build_from_ref_descriptor(
            const TSValueTypeMetaData &requested, const TSValueTypeMetaData *referenced);

        [[nodiscard]] DynamicStorageMetrics from_ref_descriptor_storage_metrics(
            const FromRefDescriptor &descriptor) noexcept;
    }
}

#endif
