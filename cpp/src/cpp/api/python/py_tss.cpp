#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>

namespace hgraph
{

    // PyTimeSeriesOutput subclass for wrapping scalar TS[bool] views
    // (is_empty child, contains tracking elements).
    struct FeatureBoolOutput : PyTimeSeriesOutput {
        explicit FeatureBoolOutput(TSOutputView view)
            : PyTimeSeriesOutput(std::move(view)) {}
    };

    // ===== Static helpers =====

    static const value::TypeMeta* get_tss_element_schema(const TSMeta* meta) {
        if (meta->value_type->kind == value::TypeKind::Tuple) {
            return meta->value_type->fields[0].type->element_type;
        }
        return meta->value_type->element_type;
    }

    static value::Value<> elem_from_python(const nb::object &elem, const TSMeta* meta) {
        const auto *elem_schema = get_tss_element_schema(meta);
        value::Value<> elem_val(elem_schema);
        elem_schema->ops->from_python(elem_val.data(), elem, elem_schema);
        return elem_val;
    }

    static nb::object elem_to_python(const value::View& elem, const TSMeta* meta) {
        const auto *elem_schema = get_tss_element_schema(meta);
        return elem_schema->ops->to_python(elem.data(), elem_schema);
    }

    // ===== PyTimeSeriesSetOutput =====

    bool PyTimeSeriesSetOutput::contains(const nb::object &item) const {
        auto set_view = view().as_set();
        auto elem_val = elem_from_python(item, set_view.meta());
        return set_view.contains(elem_val.const_view());
    }

    size_t PyTimeSeriesSetOutput::size() const {
        return view().as_set().size();
    }

    nb::bool_ PyTimeSeriesSetOutput::empty() const {
        return nb::bool_(view().as_set().size() == 0);
    }

    nb::object PyTimeSeriesSetOutput::value() const {
        auto set_view = view().as_set();
        nb::set result;
        for (auto elem : set_view.values()) {
            result.add(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::values() const {
        auto set_view = view().as_set();
        nb::set result;
        for (auto elem : set_view.values()) {
            result.add(elem_to_python(elem, set_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesSetOutput::added() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::set();
        auto added = delta.attr("added");
        if (nb::isinstance<nb::set>(added)) return added;
        return nb::steal(PySet_New(added.ptr()));
    }

    nb::object PyTimeSeriesSetOutput::removed() const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::set();
        auto removed = delta.attr("removed");
        if (nb::isinstance<nb::set>(removed)) return removed;
        return nb::steal(PySet_New(removed.ptr()));
    }

    nb::bool_ PyTimeSeriesSetOutput::was_added(const nb::object &item) const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::bool_(false);
        nb::object added = nb::borrow(delta.attr("added"));
        return nb::bool_(PySequence_Contains(added.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetOutput::was_removed(const nb::object &item) const {
        auto delta = view().delta_to_python();
        if (delta.is_none()) return nb::bool_(false);
        nb::object removed = nb::borrow(delta.attr("removed"));
        return nb::bool_(PySequence_Contains(removed.ptr(), item.ptr()) == 1);
    }

    void PyTimeSeriesSetOutput::add(const nb::object &key) const {
        auto set_view = output_view().ts_view().as_set();
        auto elem_val = elem_from_python(key, set_view.meta());
        set_view.add(elem_val.const_view());
    }

    void PyTimeSeriesSetOutput::remove(const nb::object &key) const {
        auto set_view = output_view().ts_view().as_set();
        auto elem_val = elem_from_python(key, set_view.meta());
        set_view.remove(elem_val.const_view());
    }

    nb::object PyTimeSeriesSetOutput::is_empty_output() const {
        if (is_empty_cache_.is_valid()) {
            return is_empty_cache_;
        }

        auto set_view = view().as_set();
        if (!set_view.has_is_empty_child()) {
            return nb::none();
        }

        TSView is_empty_view = set_view.is_empty_ts();
        TSOutputView out_view(std::move(is_empty_view), nullptr);
        auto* py_out = new FeatureBoolOutput(std::move(out_view));
        is_empty_cache_ = nb::cast(static_cast<PyTimeSeriesOutput*>(py_out), nb::rv_policy::take_ownership);
        return is_empty_cache_;
    }

    // Resolve the TSOutput* owning this view.  The wrapper may have been
    // created with nullptr (e.g. via make_bound_ref for REF resolution).
    // Fall back to the ShortPath stored in the view data.
    static TSOutput* resolve_ts_output(const TSOutputView& ov) {
        auto* out = const_cast<TSOutput*>(ov.output());
        if (out) return out;
        auto* node = ov.view_data().path.node();
        if (node) return node->ts_output();
        return nullptr;
    }

    nb::object PyTimeSeriesSetOutput::get_contains_output(const nb::object& key, const nb::object& requester) const {
        auto set_view = view().as_set();
        auto key_val = elem_from_python(key, set_view.meta());
        auto* output = resolve_ts_output(output_view());
        if (!output) {
            throw std::runtime_error("get_contains_output: cannot resolve TSOutput");
        }
        TSView elem = output->get_contains_view(key_val.const_view(), requester.ptr(), output_view().current_time());
        TSOutputView out_view(std::move(elem), nullptr);
        return nb::cast(static_cast<PyTimeSeriesOutput*>(new FeatureBoolOutput(std::move(out_view))),
                        nb::rv_policy::take_ownership);
    }

    void PyTimeSeriesSetOutput::release_contains_output(const nb::object& key, const nb::object& requester) const {
        auto set_view = view().as_set();
        auto key_val = elem_from_python(key, set_view.meta());
        auto* output = resolve_ts_output(output_view());
        if (output) {
            output->release_contains(key_val.const_view(), requester.ptr());
        }
    }

    nb::str PyTimeSeriesSetOutput::py_str() const {
        auto set_view = view().as_set();
        auto s = fmt::format("TimeSeriesSetOutput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&set_view), set_view.size(), set_view.valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetOutput::py_repr() const { return py_str(); }

    // ===== Collection-specific copy operations =====

    void PyTimeSeriesSetOutput::copy_from_input(const PyTimeSeriesInput &input) {
        // Python logic: compute added/removed, apply if changed
        nb::object input_value_obj = input.value();
        nb::set input_value;
        if (input_value_obj.is_none()) {
            // empty input
        } else if (nb::isinstance<nb::frozenset>(input_value_obj)) {
            input_value = nb::set(input_value_obj);
        } else if (nb::isinstance<nb::set>(input_value_obj)) {
            input_value = nb::cast<nb::set>(input_value_obj);
        }

        nb::object self_value_obj = this->value();
        nb::set self_value;
        if (!self_value_obj.is_none()) {
            if (nb::isinstance<nb::set>(self_value_obj)) {
                self_value = nb::cast<nb::set>(self_value_obj);
            } else if (nb::isinstance<nb::frozenset>(self_value_obj)) {
                self_value = nb::set(self_value_obj);
            }
        }

        // Compute added = input - self
        // Compute removed = self - input
        for (auto elem : input_value) {
            if (!self_value.contains(elem)) {
                this->add(nb::borrow(elem));
            }
        }
        for (auto elem : self_value) {
            if (!input_value.contains(elem)) {
                this->remove(nb::borrow(elem));
            }
        }
    }

    void PyTimeSeriesSetOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        auto output_as_tss = dynamic_cast<const PyTimeSeriesSetOutput*>(&output);
        if (!output_as_tss) {
            PyTimeSeriesOutput::copy_from_output(output);
            return;
        }

        nb::object src_value_obj = output_as_tss->value();
        nb::set src_value;
        if (!src_value_obj.is_none()) {
            if (nb::isinstance<nb::set>(src_value_obj)) {
                src_value = nb::cast<nb::set>(src_value_obj);
            } else if (nb::isinstance<nb::frozenset>(src_value_obj)) {
                src_value = nb::set(src_value_obj);
            }
        }

        nb::object self_value_obj = this->value();
        nb::set self_value;
        if (!self_value_obj.is_none()) {
            if (nb::isinstance<nb::set>(self_value_obj)) {
                self_value = nb::cast<nb::set>(self_value_obj);
            } else if (nb::isinstance<nb::frozenset>(self_value_obj)) {
                self_value = nb::set(self_value_obj);
            }
        }

        for (auto elem : src_value) {
            if (!self_value.contains(elem)) {
                this->add(nb::borrow(elem));
            }
        }
        for (auto elem : self_value) {
            if (!src_value.contains(elem)) {
                this->remove(nb::borrow(elem));
            }
        }
    }

    // ===== PyTimeSeriesSetInput =====

    static ViewData resolve_input_view_data(const PyTimeSeriesSetInput& input) {
        auto ts = input.input_view().ts_view();
        const auto& vd = ts.view_data();

        if (vd.uses_link_target && vd.link_data) {
            auto* lt = static_cast<const LinkTarget*>(vd.link_data);
            if (lt && lt->valid()) {
                ViewData resolved;
                resolved.value_data = lt->value_data;
                resolved.time_data = lt->time_data;
                resolved.observer_data = lt->observer_data;
                resolved.delta_data = lt->delta_data;
                resolved.link_data = lt->link_data;
                resolved.meta = lt->meta ? lt->meta : vd.meta;
                resolved.ops = lt->ops ? lt->ops : vd.ops;
                resolved.path = vd.path;
                resolved.uses_link_target = false;
                resolved.sampled = vd.sampled;
                return resolved;
            }
        }
        return vd;
    }

    bool PyTimeSeriesSetInput::contains(const nb::object &item) const {
        auto resolved_vd = resolve_input_view_data(*this);
        auto set_view = TSSView(resolved_vd, input_view().current_time());
        auto elem_val = elem_from_python(item, set_view.meta());
        return set_view.contains(elem_val.const_view());
    }

    size_t PyTimeSeriesSetInput::size() const {
        auto resolved_vd = resolve_input_view_data(*this);
        return TSSView(resolved_vd, input_view().current_time()).size();
    }

    nb::bool_ PyTimeSeriesSetInput::empty() const {
        auto resolved_vd = resolve_input_view_data(*this);
        return nb::bool_(TSSView(resolved_vd, input_view().current_time()).size() == 0);
    }

    nb::object PyTimeSeriesSetInput::value() const {
        auto ts = input_view().ts_view();
        auto py_val = ts.to_python();
        if (py_val.is_none()) return nb::set();
        if (nb::isinstance<nb::frozenset>(py_val)) {
            return nb::set(py_val);
        }
        return py_val;
    }

    nb::object PyTimeSeriesSetInput::values() const {
        auto ts = input_view().ts_view();
        auto py_val = ts.to_python();
        if (py_val.is_none()) return nb::frozenset(nb::set());
        if (nb::isinstance<nb::set>(py_val)) {
            return nb::frozenset(nb::cast<nb::set>(py_val));
        }
        return py_val;
    }

    static nb::object get_input_delta(const PyTimeSeriesSetInput& input) {
        auto ts = input.input_view().ts_view();
        if (!ts.modified()) return nb::none();
        return ts.delta_to_python();
    }

    nb::object PyTimeSeriesSetInput::added() const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::set();
        auto added = delta.attr("added");
        if (nb::isinstance<nb::set>(added)) return added;
        return nb::steal(PySet_New(added.ptr()));
    }

    nb::object PyTimeSeriesSetInput::removed() const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::set();
        auto removed = delta.attr("removed");
        if (nb::isinstance<nb::set>(removed)) return removed;
        return nb::steal(PySet_New(removed.ptr()));
    }

    nb::bool_ PyTimeSeriesSetInput::was_added(const nb::object &item) const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::bool_(false);
        nb::object added = nb::borrow(delta.attr("added"));
        return nb::bool_(PySequence_Contains(added.ptr(), item.ptr()) == 1);
    }

    nb::bool_ PyTimeSeriesSetInput::was_removed(const nb::object &item) const {
        auto delta = get_input_delta(*this);
        if (delta.is_none()) return nb::bool_(false);
        nb::object removed = nb::borrow(delta.attr("removed"));
        return nb::bool_(PySequence_Contains(removed.ptr(), item.ptr()) == 1);
    }

    nb::str PyTimeSeriesSetInput::py_str() const {
        auto set_view = input_view().ts_view().as_set();
        auto s = fmt::format("TimeSeriesSetInput@{:p}[size={}, valid={}]",
                             static_cast<const void *>(&set_view), set_view.size(), set_view.valid());
        return nb::str(s.c_str());
    }

    nb::str PyTimeSeriesSetInput::py_repr() const { return py_str(); }

    void tss_register_with_nanobind(nb::module_ &m) {
        auto tss_input = nb::class_<PyTimeSeriesSetInput, PyTimeSeriesInput>(m, "TimeSeriesSetInput");
        tss_input.def_prop_ro("value", &PyTimeSeriesSetInput::value)
            .def("__contains__", &PyTimeSeriesSetInput::contains)
            .def("__len__", &PyTimeSeriesSetInput::size)
            .def("empty", &PyTimeSeriesSetInput::empty)
            .def("values", &PyTimeSeriesSetInput::values)
            .def("added", &PyTimeSeriesSetInput::added)
            .def("removed", &PyTimeSeriesSetInput::removed)
            .def("was_added", &PyTimeSeriesSetInput::was_added)
            .def("was_removed", &PyTimeSeriesSetInput::was_removed)
            .def("__str__", &PyTimeSeriesSetInput::py_str)
            .def("__repr__", &PyTimeSeriesSetInput::py_repr);

        auto tss_output = nb::class_<PyTimeSeriesSetOutput, PyTimeSeriesOutput>(m, "TimeSeriesSetOutput");
        tss_output
            .def("copy_from_input", &PyTimeSeriesSetOutput::copy_from_input)
            .def("copy_from_output", &PyTimeSeriesSetOutput::copy_from_output)
            .def_prop_rw("value", &PyTimeSeriesSetOutput::value,
                         [](PyTimeSeriesSetOutput &self, nb::object v) { self.set_value(v); },
                         nb::arg("value").none())
            .def("__contains__", &PyTimeSeriesSetOutput::contains)
            .def("__len__", &PyTimeSeriesSetOutput::size)
            .def("empty", &PyTimeSeriesSetOutput::empty)
            .def("values", &PyTimeSeriesSetOutput::values)
            .def("added", &PyTimeSeriesSetOutput::added)
            .def("removed", &PyTimeSeriesSetOutput::removed)
            .def("was_added", &PyTimeSeriesSetOutput::was_added)
            .def("was_removed", &PyTimeSeriesSetOutput::was_removed)
            .def("add", &PyTimeSeriesSetOutput::add)
            .def("remove", &PyTimeSeriesSetOutput::remove, "key"_a)
            .def("is_empty_output", &PyTimeSeriesSetOutput::is_empty_output)
            .def("get_contains_output", &PyTimeSeriesSetOutput::get_contains_output)
            .def("release_contains_output", &PyTimeSeriesSetOutput::release_contains_output)
            .def("__str__", &PyTimeSeriesSetOutput::py_str)
            .def("__repr__", &PyTimeSeriesSetOutput::py_repr);
    }

} // namespace hgraph
