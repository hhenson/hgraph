#ifndef HGRAPH_FABRIC_VALUE_BUILDERS_H
#define HGRAPH_FABRIC_VALUE_BUILDERS_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/value/value.h>

namespace hgraph::fabric
{
    /** Build and validate a structural data dependency value. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_data_dependency(DataDependencyInput dependency);

    /** Build a canonical structural revision value. Dependencies are sorted by
        data id; duplicate ids and all invalid identifiers/ordinals fail. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_data_revision(DataRevisionInput revision);

    /** Extract and validate the native revision record represented by a
        structural value. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT DataRevisionInput
    data_revision_input(ValueView revision);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_VALUE_BUILDERS_H
