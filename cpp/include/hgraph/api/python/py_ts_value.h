#pragma once

/**
 * @file py_ts_value.h
 * @brief Python bindings declarations for TSValue and TSView.
 *
 * This header declares the registration function for exposing the
 * TSValue and TSView classes to Python via nanobind.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register TSValue and TSView bindings with the Python module.
 *
 * This function registers:
 * - TSValue class (owning time-series storage)
 * - TSView class (non-owning time-series view)
 * - Kind-specific view accessors
 *
 * @param m The nanobind module to register with
 */
void ts_value_register_with_nanobind(nb::module_& m);

} // namespace hgraph
