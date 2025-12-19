#include <hgraph/api/python/py_tsb.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <fmt/format.h>

namespace hgraph
{
    // PyTimeSeriesBundleOutput implementations

    void PyTimeSeriesBundleOutput::set_value(nb::object py_value) {
        if (!_view.valid() || !_meta) return;

        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        auto& ts_value_view = _view.value_view();

        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // Get the TSB metadata to access field information
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        auto* bundle_schema = static_cast<const value::BundleTypeMeta*>(tsb_meta->bundle_value_type);

        if (!bundle_schema) {
            // Fall back to base implementation if no bundle schema
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Iterate over the dict and set each field
        if (!nb::isinstance<nb::dict>(py_value)) {
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        nb::dict d = nb::cast<nb::dict>(py_value);

        // Get the raw data pointer for the bundle storage
        auto bundle_data = ts_value_view.value_view().data();

        // Get the tracker for field modification
        auto tracker = ts_value_view.tracker();

        for (const auto& field : bundle_schema->fields) {
            if (d.contains(field.name.c_str())) {
                // Get the field's storage location
                void* field_ptr = static_cast<char*>(bundle_data) + field.offset;

                // Convert Python value to C++ and store
                if (field.type && field.type->ops && field.type->ops->from_python) {
                    value::value_from_python(field_ptr, d[field.name.c_str()], field.type);
                }

                // Find the field index and mark as modified
                size_t field_index = tsb_meta->field_index(field.name);
                if (field_index != SIZE_MAX) {
                    // Mark this specific field as modified
                    auto field_tracker = tracker.field(field_index);
                    field_tracker.mark_modified(eval_time);
                }
            }
        }

        // Also mark the bundle itself as modified
        _view.mark_modified(eval_time);
    }

    nb::object PyTimeSeriesBundleOutput::get_item(const nb::handle &key) const {
        // TODO: Implement via view field navigation
        return nb::none();
    }

    nb::object PyTimeSeriesBundleOutput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleOutput::contains(const nb::handle &key) const {
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleOutput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleOutput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleOutput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesBundleOutput::len() const {
        return nb::int_(view().field_count());
    }

    nb::bool_ PyTimeSeriesBundleOutput::empty() const {
        return nb::bool_(view().field_count() == 0);
    }

    nb::str PyTimeSeriesBundleOutput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesBundleInput implementations
    nb::object PyTimeSeriesBundleInput::get_item(const nb::handle &key) const {
        return nb::none();
    }

    nb::object PyTimeSeriesBundleInput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleInput::contains(const nb::handle &key) const {
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleInput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleInput::keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::valid_items() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_keys() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_values() const {
        return nb::list();
    }

    nb::object PyTimeSeriesBundleInput::modified_items() const {
        return nb::list();
    }

    nb::int_ PyTimeSeriesBundleInput::len() const {
        return nb::int_(view().field_count());
    }

    nb::bool_ PyTimeSeriesBundleInput::empty() const {
        return nb::bool_(view().field_count() == 0);
    }

    nb::str PyTimeSeriesBundleInput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleInput::py_repr() const {
        return py_str();
    }

    void tsb_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleOutput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            // Override value property to use bundle-aware set_value
            .def_prop_rw("value",
                [](const PyTimeSeriesBundleOutput& self) { return self.PyTimeSeriesOutput::value(); },
                &PyTimeSeriesBundleOutput::set_value,
                nb::arg("value").none())
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("values", &PyTimeSeriesBundleOutput::values)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleOutput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleOutput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleOutput::empty)
            .def("__str__", &PyTimeSeriesBundleOutput::py_str)
            .def("__repr__", &PyTimeSeriesBundleOutput::py_repr);

        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleInput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("values", &PyTimeSeriesBundleInput::values)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleInput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleInput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleInput::empty)
            .def("__str__", &PyTimeSeriesBundleInput::py_str)
            .def("__repr__", &PyTimeSeriesBundleInput::py_repr);
    }

}  // namespace hgraph
