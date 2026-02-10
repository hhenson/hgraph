#pragma once

/**
 * @file py_ts_type_registry.h
 * @brief Python bindings declarations for the TSTypeRegistry.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register TSTypeRegistry bindings with the Python module.
 *
 * Registers: TSKind enum, TSBFieldInfo, TSMeta, TSTypeRegistry.
 *
 * @param m The nanobind module to register with
 */
void ts_type_registry_register_with_nanobind(nb::module_& m);

} // namespace hgraph
