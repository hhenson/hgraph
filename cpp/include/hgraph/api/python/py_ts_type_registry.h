#pragma once

/**
 * @file py_ts_type_registry.h
 * @brief Python bindings declarations for the TSTypeRegistry.
 *
 * This header declares the registration function for exposing the
 * TSTypeRegistry to Python via nanobind.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register TSTypeRegistry bindings with the Python module.
 *
 * This function registers:
 * - TSKind enum
 * - TSBFieldInfo class
 * - TSMeta class
 * - TSTypeRegistry class (singleton accessor and factory methods)
 *
 * @param m The nanobind module to register with
 */
void ts_type_registry_register_with_nanobind(nb::module_& m);

} // namespace hgraph
