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
#include <hgraph/types/value/type_meta.h>

namespace nb = nanobind;

namespace hgraph::value {

/**
 * Return the registered Python CompoundScalar class for a bundle schema.
 *
 * CompoundScalar schemas are represented in C++ as bundle-compatible value
 * schemas. The value layer uses this registry to reconstruct the original
 * Python class when a bundle schema semantically represents a CompoundScalar
 * rather than a generic dict-like bundle.
 */
nb::object get_compound_scalar_class(const TypeMeta* meta);

/**
 * Associate a bundle-compatible schema with its Python CompoundScalar class.
 *
 * This keeps the public Python behavior stable even though the runtime stores
 * cpp-native compound scalars using the general bundle machinery.
 */
void register_compound_scalar_class(const TypeMeta* meta, nb::object py_class);

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
