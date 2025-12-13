// Test compilation of python_conversion.h
// This file ensures the header compiles correctly in a nanobind context.

#include <hgraph/types/value/python_conversion.h>

namespace hgraph::value {

// Explicit instantiation to verify template compilation
template struct ScalarTypeOpsWithPython<int>;
template struct ScalarTypeOpsWithPython<int64_t>;
template struct ScalarTypeOpsWithPython<double>;
template struct ScalarTypeOpsWithPython<float>;
template struct ScalarTypeOpsWithPython<bool>;
template struct ScalarTypeOpsWithPython<std::string>;

} // namespace hgraph::value
