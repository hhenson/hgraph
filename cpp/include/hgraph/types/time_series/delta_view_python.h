//
// Created by Claude on 19/12/2025.
//
// Python conversion for DeltaView
//
// Converts C++ DeltaView to Python objects matching the expected formats:
// - TS: scalar value
// - TSB: dict[str, Any] of modified fields
// - TSL: dict[int, Any] of modified elements
// - TSS: SetDelta with .added and .removed
// - TSD: frozendict with modified entries + REMOVE sentinel
// - REF: TimeSeriesReference value
//

#ifndef HGRAPH_DELTA_VIEW_PYTHON_H
#define HGRAPH_DELTA_VIEW_PYTHON_H

#include <nanobind/nanobind.h>
#include <hgraph/types/time_series/delta_view.h>

namespace nb = nanobind;

namespace hgraph::ts {

/**
 * Convert a DeltaView to its Python representation.
 *
 * Returns the appropriate Python object based on TimeSeriesKind:
 * - TS: The scalar value
 * - TSB: dict of {field_name: delta_value} for modified fields
 * - TSL: dict of {index: delta_value} for modified elements
 * - TSS: PythonSetDelta with added/removed frozensets
 * - TSD: frozendict with modified entries, removed keys → REMOVE
 * - REF: The reference value
 *
 * @param view The DeltaView to convert
 * @return Python object representing the delta
 */
nb::object delta_to_python(const DeltaView& view);

/**
 * Initialize cached Python type references.
 *
 * Should be called once during module initialization to cache
 * frequently-used Python types (frozendict, PythonSetDelta, REMOVE).
 */
void init_delta_python_types();

/**
 * Check if delta Python types have been initialized.
 */
bool delta_python_types_initialized();

} // namespace hgraph::ts

#endif // HGRAPH_DELTA_VIEW_PYTHON_H
