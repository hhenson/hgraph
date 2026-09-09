#ifndef HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H
#define HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H

#include <hgraph/types/primitive_types.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_input/list_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>

namespace checks::native_dependency
{
    inline hgraph::Float blend(hgraph::Float value, hgraph::Int window) noexcept {
        return value + static_cast<hgraph::Float>(window);
    }

    inline hgraph::Int len(const hgraph::TSLInputView &value) noexcept { return static_cast<hgraph::Int>(value.size()); }

    inline hgraph::Int len(const hgraph::TSSInputView &value) noexcept { return static_cast<hgraph::Int>(value.size()); }

    inline hgraph::Int len(const hgraph::TSDInputView &value) noexcept { return static_cast<hgraph::Int>(value.size()); }
}  // namespace checks::native_dependency

#endif  // HGL_TESTS_CHECKS_NATIVE_DEPENDENCY_H
