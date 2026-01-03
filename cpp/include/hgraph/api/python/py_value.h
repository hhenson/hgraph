#pragma once

/**
 * @file py_value.h
 * @brief Python bindings declarations for the Value type system.
 *
 * This header declares the registration function for exposing the
 * Value type system to Python via nanobind.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register Value type system bindings with the Python module.
 *
 * This function registers:
 * - TypeRegistry (singleton accessor)
 * - TypeMeta (read-only schema descriptor)
 * - TypeKind enum
 * - Value classes (PlainValue, CachedValue)
 * - View classes (ConstValueView, ValueView)
 * - Specialized views (TupleView, BundleView, ListView, SetView, MapView)
 *
 * @param m The nanobind module to register with
 */
void value_register_with_nanobind(nb::module_& m);

} // namespace hgraph
