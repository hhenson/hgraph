#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <fmt/format.h>

namespace hgraph
{
    // ============================================================
    // Helper function for delta_value (shared by Input and Output)
    // ============================================================

    static nb::object tsd_compute_delta_value(const TSView& view) {
        // Python: frozendict(chain(
        //     ((k, v.delta_value) for k, v in self.items() if v.modified and v.valid),
        //     ((k, REMOVE) for k in self.removed_keys()),
        // ))
        TSDView dict = view.as_dict();
        Node* n = view.owning_node();
        if (!n || !n->cached_evaluation_time_ptr()) {
            throw std::runtime_error("delta_value requires node context with evaluation time");
        }
        engine_time_t eval_time = *n->cached_evaluation_time_ptr();
        nb::dict result;

        // Add modified items with their delta_values
        for (auto item : dict.items()) {
            TSView value_view = item.second;
            if (value_view.modified_at(eval_time) && value_view.ts_valid()) {
                nb::object wrapped = wrap_input_view(value_view);
                nb::object delta = nb::getattr(wrapped, "delta_value");
                result[item.first.to_python()] = delta;
            }
        }

        // TODO: Add removed keys with REMOVE marker once overlay supports removal tracking
        // For now, the removed keys are not included as the view doesn't track them

        return result;
    }

    // ===== PyTimeSeriesDictOutput Implementation =====

    PyTimeSeriesDictOutput::PyTimeSeriesDictOutput(TSMutableView view)
        : PyTimeSeriesOutput(view) {}

    size_t PyTimeSeriesDictOutput::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::get not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_or_create not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        throw std::runtime_error("PyTimeSeriesDictOutput::create not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::contains not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::key_set not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::items not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::modified_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::was_modified not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::valid_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::valid_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::valid_items not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::added_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::added_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::added_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        return false;
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::was_added not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::removed_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::removed_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        throw std::runtime_error("PyTimeSeriesDictOutput::removed_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        return false;
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictOutput::was_removed not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        return nb::none();
    }

    nb::object PyTimeSeriesDictOutput::delta_value() const {
        return tsd_compute_delta_value(_view);
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        TSDView dict = _view.as_dict();
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void*>(_view.value_view().data()),
                               dict.size(), _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        throw std::runtime_error("PyTimeSeriesDictOutput::set_item not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictOutput::del_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        throw std::runtime_error("PyTimeSeriesDictOutput::pop not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("PyTimeSeriesDictOutput::get_ref not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        throw std::runtime_error("PyTimeSeriesDictOutput::release_ref not yet implemented for view-based wrappers");
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    PyTimeSeriesDictInput::PyTimeSeriesDictInput(TSView view)
        : PyTimeSeriesInput(view) {}

    size_t PyTimeSeriesDictInput::size() const {
        TSDView dict = _view.as_dict();
        return dict.size();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictInput::get_item not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        throw std::runtime_error("PyTimeSeriesDictInput::get not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::get_or_create not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        throw std::runtime_error("PyTimeSeriesDictInput::create not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        throw std::runtime_error("PyTimeSeriesDictInput::contains not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        throw std::runtime_error("PyTimeSeriesDictInput::key_set not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::items not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::modified_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictInput::was_modified not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::valid_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::valid_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::valid_items not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::added_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::added_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::added_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictInput::has_added() const {
        return false;
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictInput::was_added not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        throw std::runtime_error("PyTimeSeriesDictInput::removed_keys not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        throw std::runtime_error("PyTimeSeriesDictInput::removed_values not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        throw std::runtime_error("PyTimeSeriesDictInput::removed_items not yet implemented for view-based wrappers");
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        return false;
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        throw std::runtime_error("PyTimeSeriesDictInput::was_removed not yet implemented for view-based wrappers");
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        return nb::none();
    }

    nb::object PyTimeSeriesDictInput::delta_value() const {
        return tsd_compute_delta_value(_view);
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        TSDView dict = _view.as_dict();
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void*>(_view.value_view().data()),
                               dict.size(), _view.ts_valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::on_key_added not yet implemented for view-based wrappers");
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        throw std::runtime_error("PyTimeSeriesDictInput::on_key_removed not yet implemented for view-based wrappers");
    }

    // ===== Nanobind Registration =====

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("__contains__", &PyTimeSeriesDictOutput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item, "key"_a)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item, "key"_a, "value"_a)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictOutput::size)
            .def("pop", &PyTimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("get", &PyTimeSeriesDictOutput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictOutput::create, "key"_a)
            .def("clear", &PyTimeSeriesDictOutput::clear)
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def("was_added", &PyTimeSeriesDictOutput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictOutput::has_added)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("was_modified", &PyTimeSeriesDictOutput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def("was_removed", &PyTimeSeriesDictOutput::was_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictOutput::key_from_value, "value"_a)
            .def_prop_ro("delta_value", &PyTimeSeriesDictOutput::delta_value)
            .def_prop_ro("has_removed", &PyTimeSeriesDictOutput::has_removed)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref, "key"_a, "requester"_a)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref, "key"_a, "requester"_a)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("__str__", &PyTimeSeriesDictOutput::py_str)
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr);

        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &PyTimeSeriesDictInput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictInput::get_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictInput::size)
            .def("__iter__", &PyTimeSeriesDictInput::iter)
            .def("get", &PyTimeSeriesDictInput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictInput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictInput::create, "key"_a)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def("was_added", &PyTimeSeriesDictInput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictInput::has_added)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("was_modified", &PyTimeSeriesDictInput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def("was_removed", &PyTimeSeriesDictInput::was_removed, "key"_a)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added, "key"_a)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictInput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictInput::has_removed)
            .def_prop_ro("delta_value", &PyTimeSeriesDictInput::delta_value)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }
}  // namespace hgraph
