#ifndef HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H
#define HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H

#include <hgraph/types/primitive_types.h>

namespace checks::native_dependency
{
    inline hgraph::Float blend(hgraph::Float value, hgraph::Int window) noexcept {
        return value + static_cast<hgraph::Float>(window);
    }
}  // namespace checks::native_dependency

#endif  // HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H
