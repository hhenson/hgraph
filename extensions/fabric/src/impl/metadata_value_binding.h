#ifndef HGRAPH_FABRIC_IMPL_METADATA_VALUE_BINDING_H
#define HGRAPH_FABRIC_IMPL_METADATA_VALUE_BINDING_H

#include <hgraph/fabric/metadata_codec.h>

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_type_ref.h>

namespace hgraph::fabric::detail
{
    /** Run-local physical plans for Fabric's declared metadata values.

        This is a concrete representation strategy, not part of Fabric's
        public semantic contract. Construct it in node/service start state and
        destroy it with that run; registry reset invalidates its interned plan
        records. */
    class FabricMetadataValueBinding final
    {
      public:
        FabricMetadataValueBinding();

        [[nodiscard]] Value make_data_dependency(
            DataDependencyInput dependency) const;
        [[nodiscard]] Value make_data_revision(
            DataRevisionInput revision) const;
        [[nodiscard]] DataRevisionInput data_revision_input(
            ValueView revision) const;
        [[nodiscard]] Value make_revision_reference(
            MetadataObjectKind kind, RevisionId revision) const;
        [[nodiscard]] RevisionId revision_reference_id(
            ValueView reference, MetadataObjectKind expected_kind) const;

        [[nodiscard]] const ValueTypeMetaData *
        data_revision_schema() const noexcept;
        [[nodiscard]] const ValueTypeMetaData *
        revision_reference_schema() const noexcept;

      private:
        ValueTypeRef int_type_{};
        ValueTypeRef str_type_{};
        ValueTypeRef datetime_type_{};
        ValueTypeRef dependency_type_{};
        ValueTypeRef dependencies_type_{};
        ValueTypeRef revision_type_{};
        ValueTypeRef reference_type_{};
    };
}  // namespace hgraph::fabric::detail

#endif  // HGRAPH_FABRIC_IMPL_METADATA_VALUE_BINDING_H
