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
 */
struct PythonCache {
    nb::object cached_value;
    engine_time_t value_cache_time{MIN_DT};
    nb::object cached_delta;
    // Note: delta doesn't need time tracking - it's cleared at tick end
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_PYTHON_CACHE_H
