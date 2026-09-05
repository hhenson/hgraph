#ifndef HGRAPH_PYTHON_BRIDGE_STATE_H
#define HGRAPH_PYTHON_BRIDGE_STATE_H

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <compare>
#include <hgraph/hgraph_export.h>

#include <nanobind/nanobind.h>

#include <utility>
#include <span>
#include <unordered_map>
#include <vector>

namespace hgraph {
struct ValueTypeMetaData;
struct TSValueTypeMetaData;
class Value;
class ValueView;
class ValueTypeRef;
} // namespace hgraph

/**
 * Shared python-bridge STATE consulted by type-erased conversion impls
 * (the ops tables' to_python / from_python) and by the module itself.
 * The hgraph python package fills these at import time; ops installed on
 * core bindings read them. All slots are IMMORTAL (new-leaked) - the
 * registries-outlive-everything teardown rule.
 */
namespace hgraph::python_bridge {
namespace nb = nanobind;

/**
 * One output-owned retained Python value.  It is deliberately only a holder:
 * schema validation and read-shape normalization remain value-factory policy.
 */
struct HGRAPH_LOCAL PythonValueHolder {
  PyObject *object{nullptr};

  PythonValueHolder() noexcept = default;
  PythonValueHolder(const PythonValueHolder &other);
  PythonValueHolder(PythonValueHolder &&other) noexcept;
  PythonValueHolder &operator=(const PythonValueHolder &other);
  PythonValueHolder &operator=(PythonValueHolder &&other) noexcept;
  ~PythonValueHolder();

  void clear() noexcept;
  void set(nb::handle source);
  [[nodiscard]] nb::object get() const;
  [[nodiscard]] bool has_value() const noexcept { return object != nullptr; }

  void swap(PythonValueHolder &other) noexcept {
    std::swap(object, other.object);
  }
};

[[nodiscard]] HGRAPH_EXPORT nb::object &cmp_result_enum_slot();
[[nodiscard]] HGRAPH_EXPORT nb::object &divide_by_zero_enum_slot();
/** The recording-shape policy enums (RFC 0019). Registered like the enums
    above so a Python member converts to the C++ scalar the ``record``
    overload declares, rather than to a boxed Python object. */
[[nodiscard]] HGRAPH_EXPORT nb::object &to_table_mode_enum_slot();
[[nodiscard]] HGRAPH_EXPORT nb::object &removed_sentinel_slot();

/** hgraph's REMOVE_IF_EXISTS sentinel (lenient TSD key removal; REMOVE
    is strict — absent key raises at delta application). */
[[nodiscard]] HGRAPH_EXPORT nb::object &remove_if_exists_sentinel_slot();

/** hgraph's Removed(item) marker class (TSS delta shaping). */
[[nodiscard]] HGRAPH_EXPORT nb::object &removed_class_slot();

/** hgraph's SetDelta class (a frozenset subclass): TSS delta_value
    results are built as this type so returning them to a TSS output
    applies as a DELTA (a plain frozenset applies as the full value). */
[[nodiscard]] HGRAPH_EXPORT nb::object &set_delta_class_slot();

/** Python callback that maps canonical type-erased delta values onto the
    public compatibility shapes (SetDelta, Removed, and REMOVED). */
[[nodiscard]] HGRAPH_EXPORT nb::object &delta_shaper_slot();

/** The module-installed python->Value INFERENCE hook (schema-free
    conversion is inherently a dispatch on PYTHON types, so it lives in
    the module; core ops that need it - e.g. Any's from_python - call
    through this slot). */
using PyInferValueFn = void *; // set as Value (*)(nb::handle) by the module

[[nodiscard]] HGRAPH_EXPORT PyInferValueFn &py_infer_value_slot();

/** Target-directed Python value conversion installed by the extension.
    TS authoring strategies use this for scalar/value leaves while retaining
    all time-series shape semantics in their own erased operations. */
using PyValueFromSchemaFn = Value (*)(nb::handle, const ValueTypeMetaData *);

[[nodiscard]] HGRAPH_EXPORT PyValueFromSchemaFn &py_value_from_schema_slot();

/** Native JSON's Python boundary. JSON retains Any storage, but unlike a
    general Any its public value must preserve the nominal JSON identity. */
using PyJsonToPythonFn = nb::object (*)(const Value &inner);
using PyJsonFromPythonFn = Value (*)(nb::handle source);

[[nodiscard]] HGRAPH_EXPORT PyJsonToPythonFn &py_json_to_python_slot();
[[nodiscard]] HGRAPH_EXPORT PyJsonFromPythonFn &py_json_from_python_slot();

/**
 * CompoundScalar schema address -> python class (read-back
 * reconstruction). Schema identity is nominal; labels are diagnostic and
 * are not a safe registry key once namespaces and specialisations exist.
 */
[[nodiscard]] HGRAPH_EXPORT nb::dict &bundle_class_registry();

struct HGRAPH_LOCAL NB_EXPORT_SHARED PyBundleClassInfo {
  using Allocator = PyObject *(*)(PyTypeObject *, Py_ssize_t);

  nb::object type{};
  nb::object specialization{};
  Allocator allocator{nullptr};
  std::vector<nb::str> field_names{};
  std::vector<nb::object> field_overrides{};
  std::vector<bool> constructor_fields{};
  std::vector<bool> defaulted_constructor_fields{};
  bool requires_constructor{false};
};

/** Schema-addressed companion to ``bundle_class_registry`` for hot value
    conversion. It retains interned field-name objects so conversion does
    not recreate and hash every field name on every node evaluation. */
[[nodiscard]] HGRAPH_EXPORT
    std::unordered_map<const void *, PyBundleClassInfo> &
    bundle_class_info_registry();

/** Structural ``TSB[CompoundScalar]`` schema -> its scalar Bundle schema.
 * The TS runtime remains structural; this bridge-only association restores the
 * declared Python value class when a Python node reads the complete TSB value.
 */
[[nodiscard]] HGRAPH_EXPORT std::unordered_map<const void *, const void *> &
tsb_compound_value_registry();

/**
 * Restore the public Python value for a structural TSB snapshot.
 *
 * Concrete TSData conversion strategies call this after recursively exporting
 * their children through child TSDataOps. Keeping the class lookup here lets
 * the erased TSData strategy preserve CompoundScalar identity without making
 * Python-facing TimeSeries wrappers understand TSB representation details.
 */
[[nodiscard]] HGRAPH_EXPORT nb::object
materialize_tsb_python_value(const TSValueTypeMetaData *schema, nb::dict value);

/**
 * Install a Python-owned canonical binding for a nominal Bundle schema.
 *
 * The class reconstruction metadata must already be present in
 * ``bundle_class_info_registry``. The resulting binding retains the Python
 * object as-is and projects declared fields lazily through ``BundleView``.
 */
HGRAPH_EXPORT void
register_python_bundle_binding(const ValueTypeMetaData *schema);

/** True when ``schema`` has a Python-owned Bundle storage policy. */
[[nodiscard]] HGRAPH_EXPORT bool
is_python_bundle_schema(const ValueTypeMetaData *schema) noexcept;

/** True when ``binding`` is one of the Python-owned Bundle realizations. */
[[nodiscard]] HGRAPH_EXPORT bool
is_python_bundle_binding(ValueTypeRef binding) noexcept;

/**
 * Return the Python-owned binding for ``schema`` with the supplied realized
 * field bindings. Used by graph snapshots when a declared field contains a
 * closed Bundle hierarchy. Returns an unbound reference for non-Python schemas.
 */
[[nodiscard]] HGRAPH_EXPORT ValueTypeRef
python_bundle_binding_for(const ValueTypeMetaData *schema,
                          std::span<const ValueTypeRef> field_bindings);

/** Test/reset lifecycle hook; retained binding contexts remain immortal. */
HGRAPH_EXPORT void clear_python_bundle_bindings() noexcept;
} // namespace hgraph::python_bridge

#endif // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif // HGRAPH_PYTHON_BRIDGE_STATE_H
