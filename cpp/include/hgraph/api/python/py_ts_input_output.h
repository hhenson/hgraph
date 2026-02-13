#pragma once

/**
 * @file py_ts_input_output.h
 * @brief Python bindings declarations for TSInput, TSOutput, TSInputView, TSOutputView.
 *
 * This header declares the registration function for exposing the
 * TSInput, TSOutput, TSInputView, and TSOutputView classes to Python via nanobind.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register TSInput, TSOutput, and their view bindings with the Python module.
 *
 * This function registers:
 * - TSOutput class (producer of time-series values)
 * - TSOutputView class (view wrapper for TSOutput)
 * - TSInput class (consumer of time-series values)
 * - TSInputView class (view wrapper for TSInput)
 *
 * @param m The nanobind module to register with
 */
void ts_input_output_register_with_nanobind(nb::module_& m);

} // namespace hgraph
