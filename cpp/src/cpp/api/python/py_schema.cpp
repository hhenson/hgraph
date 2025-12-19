//
// Created by Claude on 15/12/2025.
//
// PyTimeSeriesSchema implementation
//

#include <hgraph/api/python/py_schema.h>
#include <hgraph/types/time_series/ts_type_meta.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

// Legacy constructor: keys only
PyTimeSeriesSchema::PyTimeSeriesSchema(std::vector<std::string> keys)
    : PyTimeSeriesSchema(std::move(keys), nb::none()) {}

// Legacy constructor: keys + scalar type
PyTimeSeriesSchema::PyTimeSeriesSchema(std::vector<std::string> keys, nb::object type)
    : _meta(nullptr), _keys(std::move(keys)), _scalar_type(std::move(type)), _keys_cached(true) {}

// Value-based constructor: delegate to TSBTypeMeta
PyTimeSeriesSchema::PyTimeSeriesSchema(const TSBTypeMeta* meta, nb::object scalar_type)
    : _meta(meta), _scalar_type(std::move(scalar_type)), _keys_cached(false) {}

const std::vector<std::string>& PyTimeSeriesSchema::keys() const {
    if (!_keys_cached) {
        if (_meta) {
            // Lazily populate _keys from TSBTypeMeta
            _keys.clear();
            _keys.reserve(_meta->fields.size());
            for (const auto& field : _meta->fields) {
                _keys.push_back(field.name);
            }
        }
        _keys_cached = true;
    }
    return _keys;
}

nb::object PyTimeSeriesSchema::get_value(const std::string& key) const {
    // TimeSeriesSchema doesn't store values, only metadata
    return nb::none();
}

const nb::object& PyTimeSeriesSchema::scalar_type() const {
    return _scalar_type;
}

void PyTimeSeriesSchema::register_with_nanobind(nb::module_& m) {
    nb::class_<PyTimeSeriesSchema, AbstractSchema>(m, "TimeSeriesSchema")
        .def(nb::init<std::vector<std::string>>(), "keys"_a)
        .def(nb::init<std::vector<std::string>, const nb::type_object&>(), "keys"_a, "scalar_type"_a)
        .def_prop_ro("scalar_type", &PyTimeSeriesSchema::scalar_type)
        .def("__str__", [](const PyTimeSeriesSchema& self) {
            if (!self.scalar_type().is_valid() || self.scalar_type().is_none()) {
                return nb::str("unnamed:{}").format(self.keys());
            }
            return nb::str("{}{}").format(self.scalar_type(), self.keys());
        });
}

} // namespace hgraph
