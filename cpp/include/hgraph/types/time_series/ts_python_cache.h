//
// PythonCache definition for TSOutput
//
// This header defines the PythonCache struct used by TSOutput for caching
// Python value and delta conversions. It requires nanobind.
//

#ifndef HGRAPH_TS_PYTHON_CACHE_H
#define HGRAPH_TS_PYTHON_CACHE_H

#include <nanobind/nanobind.h>
#include <hgraph/hgraph_base.h>
#include <functional>
#include <memory>

namespace nb = nanobind;

namespace hgraph::ts {

/**
 * Cache for Python value and delta conversions on TSOutput.
 *
 * - cached_value: The Python object for the current value.
 *   Valid when value_cache_time >= output's last_modified_time.
 *
 * - cached_delta: The Python object for the current delta.
 *   Valid only for the current evaluation tick. Cleared by
 *   after-evaluation callback registered via register_delta_reset_callback().
 *
 * - tss_contains_extension: Type-erased storage for TSS contains extension.
 *   Stored on the raw output so both PyTimeSeriesSetOutput::set_value and
 *   set_python_value can access the same extension.
 *
 * - tss_update_contains_for_keys: Callback to update contains outputs for changed keys.
 */
struct PythonCache {
    nb::object cached_value;
    engine_time_t value_cache_time{MIN_DT};
    nb::object cached_delta;
    // Note: delta doesn't need time tracking - it's cleared at tick end

    // TSS contains extension (type-erased) - stored here so both wrapper and
    // set_python_value can access the same extension
    std::shared_ptr<void> tss_contains_extension;
    std::function<void(const nb::handle&)> tss_update_contains_for_keys;
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_PYTHON_CACHE_H
