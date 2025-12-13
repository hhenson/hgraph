// Python conversion support for the hgraph::value type system.
// This file provides explicit instantiations for the standard hgraph scalar types.

#include <hgraph/types/value/python_conversion.h>

namespace hgraph::value {

// Explicit instantiation of TypeOps for standard types
// These use the generic template with nb::cast for Python conversion
template struct ScalarTypeOpsWithPython<bool>;
template struct ScalarTypeOpsWithPython<int64_t>;
template struct ScalarTypeOpsWithPython<double>;
template struct ScalarTypeOpsWithPython<engine_date_t>;
template struct ScalarTypeOpsWithPython<engine_time_t>;
template struct ScalarTypeOpsWithPython<engine_time_delta_t>;
// Note: nb::object has an explicit specialization in the header

// Explicit instantiation of TypeMeta for standard types
template struct ScalarTypeMetaWithPython<bool>;
template struct ScalarTypeMetaWithPython<int64_t>;
template struct ScalarTypeMetaWithPython<double>;
template struct ScalarTypeMetaWithPython<engine_date_t>;
template struct ScalarTypeMetaWithPython<engine_time_t>;
template struct ScalarTypeMetaWithPython<engine_time_delta_t>;
template struct ScalarTypeMetaWithPython<nb::object>;

} // namespace hgraph::value
