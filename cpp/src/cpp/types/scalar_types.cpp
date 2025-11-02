#include <hgraph/types/scalar_types.h>

namespace hgraph {
    // ============================================================================
    // CompoundScalar (abstract base class)
    // ============================================================================

    std::string CompoundScalar::to_string() const {
        // Default implementation - can be overridden
        nb::dict d = to_dict();
        std::string result = "CompoundScalar(";
        bool first = true;
        for (const auto &key: keys()) {
            if (!first) result += ", ";
            first = false;
            result += key + "=";
            if (d.contains(key.c_str())) {
                nb::object value = d[key.c_str()];
                result += nb::cast<std::string>(nb::str(value));
            } else {
                result += "None";
            }
        }
        result += ")";
        return result;
    }

    bool CompoundScalar::operator==(const CompoundScalar &other) const {
        // Default implementation - compare keys and dict representation
        if (keys() != other.keys()) {
            return false;
        }

        nb::dict this_dict = to_dict();
        nb::dict other_dict = other.to_dict();

        for (const auto &key: keys()) {
            if (this_dict.contains(key.c_str()) != other_dict.contains(key.c_str())) {
                return false;
            }
            if (this_dict.contains(key.c_str())) {
                nb::object this_val = this_dict[key.c_str()];
                nb::object other_val = other_dict[key.c_str()];
                if (!this_val.equal(other_val)) {
                    return false;
                }
            }
        }
        return true;
    }

    size_t CompoundScalar::hash() const {
        // Default implementation - hash based on dict representation
        size_t h = 0;
        nb::dict d = to_dict();

        for (const auto &key: keys()) {
            h ^= std::hash<std::string>{}(key) + 0x9e3779b9 + (h << 6) + (h >> 2);
            if (d.contains(key.c_str())) {
                try {
                    auto py_hash = nb::hash(d[key.c_str()]);
                    h ^= py_hash + 0x9e3779b9 + (h << 6) + (h >> 2);
                } catch (const std::exception &) {
                    // Skip unhashable values
                }
            }
        }
        return h;
    }

    void CompoundScalar::register_with_nanobind(nb::module_ &m) {
        nb::class_<CompoundScalar, AbstractSchema>(m, "CompoundScalar")
                .def("to_dict", &CompoundScalar::to_dict)
                .def("__str__", &CompoundScalar::to_string)
                .def("__repr__", &CompoundScalar::to_string)
                .def("__eq__", &CompoundScalar::operator==)
                .def("__hash__", &CompoundScalar::hash);
    }

    // ============================================================================
    // PythonCompoundScalar (concrete Python-backed implementation)
    // ============================================================================

    PythonCompoundScalar::PythonCompoundScalar(std::vector<std::string> keys, nb::object values)
        : _keys{std::move(keys)}, _values{std::move(values)} {
    }

    const std::vector<std::string> &PythonCompoundScalar::keys() const { return _keys; }

    nb::object PythonCompoundScalar::get_value(const std::string &key) const {
        if (nb::hasattr(_values, key.c_str())) {
            return nb::getattr(_values, key.c_str());
        }
        return nb::none();
    }

    nb::dict PythonCompoundScalar::to_dict() const {
        nb::dict result;

        // If _values is already a dict, return it
        if (nb::isinstance<nb::dict>(_values)) {
            return nb::cast<nb::dict>(_values);
        }

        // Otherwise, try to extract attributes by key name
        for (const auto &key: _keys) {
            try {
                if (nb::hasattr(_values, key.c_str())) {
                    nb::object value = nb::getattr(_values, key.c_str());

                    // If the value is itself a PythonCompoundScalar, recursively convert
                    if (nb::isinstance < PythonCompoundScalar > (value)) {
                        auto compound = nb::cast<PythonCompoundScalar *>(value);
                        result[key.c_str()] = compound->to_dict();
                    } else if (!value.is_none()) {
                        result[key.c_str()] = value;
                    }
                }
            } catch (const std::exception &) {
                // Skip attributes that can't be accessed
            }
        }

        return result;
    }

    PythonCompoundScalar::ptr PythonCompoundScalar::from_dict(const std::vector<std::string> &keys, const nb::dict &d) {
        // Create a simple namespace object to hold the values
        auto types_module = nb::module_::import_("types");
        auto simple_namespace = types_module.attr("SimpleNamespace");
        nb::object values = simple_namespace(**d);

        return new PythonCompoundScalar(keys, values);
    }

    std::string PythonCompoundScalar::to_string() const {
        // Create a string representation like: ClassName(field1=value1, field2=value2, ...)
        std::string result;

        // Try to get the class name from _values if it has one
        if (nb::hasattr(_values, "__class__")) {
            auto cls = nb::getattr(_values, "__class__");
            if (nb::hasattr(cls, "__name__")) {
                result = nb::cast<std::string>(nb::getattr(cls, "__name__"));
            }
        }

        if (result.empty()) {
            result = "CompoundScalar";
        }

        result += "(";
        bool first = true;
        for (const auto &key: _keys) {
            if (!first) result += ", ";
            first = false;

            result += key + "=";
            try {
                if (nb::hasattr(_values, key.c_str())) {
                    nb::object value = nb::getattr(_values, key.c_str());
                    result += nb::cast<std::string>(nb::str(value));
                } else {
                    result += "None";
                }
            } catch (const std::exception &) {
                result += "None";
            }
        }
        result += ")";

        return result;
    }

    bool PythonCompoundScalar::operator==(const CompoundScalar &other) const {
        // Compare keys first
        if (keys() != other.keys()) {
            return false;
        }

        // Try to cast to PythonCompoundScalar to access _values
        const auto *other_python = dynamic_cast<const PythonCompoundScalar *>(&other);
        if (!other_python) {
            // Fall back to dict comparison if not PythonCompoundScalar
            return CompoundScalar::operator==(other);
        }

        // Compare values for each key
        for (const auto &key: keys()) {
            try {
                nb::object this_val = nb::hasattr(_values, key.c_str())
                                          ? nb::getattr(_values, key.c_str())
                                          : nb::none();
                nb::object other_val = nb::hasattr(other_python->_values, key.c_str())
                                           ? nb::getattr(other_python->_values, key.c_str())
                                           : nb::none();

                // Use Python's equality operator
                if (!this_val.equal(other_val)) {
                    return false;
                }
            } catch (const std::exception &) {
                return false;
            }
        }

        return true;
    }

    size_t PythonCompoundScalar::hash() const {
        // Compute hash similar to Python dataclass frozen=True
        size_t h = 0;

        // Hash the keys
        for (const auto &key: _keys) {
            h ^= std::hash<std::string>{}(key) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }

        // Hash the values
        for (const auto &key: _keys) {
            try {
                if (nb::hasattr(_values, key.c_str())) {
                    nb::object value = nb::getattr(_values, key.c_str());
                    // Use Python's hash function
                    auto py_hash = nb::hash(value);
                    h ^= py_hash + 0x9e3779b9 + (h << 6) + (h >> 2);
                }
            } catch (const std::exception &) {
                // If value is unhashable, skip it
            }
        }

        return h;
    }

    void PythonCompoundScalar::register_with_nanobind(nb::module_ &m) {
        nb::class_ < PythonCompoundScalar, CompoundScalar > (m, "PythonCompoundScalar")
                .def(nb::init<std::vector<std::string>, nb::object>(), "keys"_a, "values"_a)
                .def("get_value", &PythonCompoundScalar::get_value, "key"_a)
                .def_static("from_dict", &PythonCompoundScalar::from_dict, "keys"_a, "d"_a)
                .def("__getattr__", [](const PythonCompoundScalar &self, const std::string &key) {
                    return nb::getattr(self._values, key.c_str());
                });
    }
} // namespace hgraph