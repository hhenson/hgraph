#ifndef HGRAPH_FABRIC_VALUE_BUILDERS_H
#define HGRAPH_FABRIC_VALUE_BUILDERS_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/value/value.h>

namespace hgraph::fabric
{
    /** Ad-hoc convenience which resolves Fabric value plans for this call.
        Evaluation code retains the private run-local binding instead. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_data_dependency(DataDependencyInput dependency);

    /** Ad-hoc canonical revision builder. Dependencies are sorted by data id;
        duplicate ids and all invalid identifiers/ordinals fail. Evaluation
        code uses the private run-local binding. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Value
    make_data_revision(DataRevisionInput revision);

    /** Ad-hoc extraction and validation convenience. Evaluation code uses its
        private retained run binding. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT DataRevisionInput
    data_revision_input(ValueView revision);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_VALUE_BUILDERS_H
