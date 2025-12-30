#pragma once

/**
 * @file type_meta_bindings.h
 * @brief Python bindings for type metadata mapping.
 *
 * This header declares the binding registration function for Python type
 * to C++ TypeMeta* mapping. These bindings allow Python's HgTypeMetaData
 * system to obtain corresponding C++ TypeMeta schemas.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph::value {

/**
 * @brief Register type meta binding functions with Python.
 *
 * This registers the following functions:
 * - get_scalar_type_meta(py_type) - Maps Python type to TypeMeta*
 * - get_dict_type_meta(key_meta, value_meta) - Creates Dict TypeMeta*
 * - get_set_type_meta(element_meta) - Creates Set TypeMeta*
 * - get_dynamic_list_type_meta(element_meta) - Creates List TypeMeta* for tuple[T, ...]
 * - get_bundle_type_meta(fields, type_name) - Creates Bundle TypeMeta* for CompoundScalar
 *
 * @param m The nanobind module to register functions into
 */
void register_type_meta_bindings(nb::module_& m);

} // namespace hgraph::value
