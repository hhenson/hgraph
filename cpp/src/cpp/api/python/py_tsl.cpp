#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/wrapper_factory.h>

#include <fmt/format.h>
#include <stdexcept>
#include <type_traits>

namespace hgraph
{
namespace
{
    template <typename T_TS>
    nb::object wrap_child(typename PyTimeSeriesList<T_TS>::view_type child) {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return wrap_output_view(std::move(child));
        } else {
            return wrap_input_view(std::move(child));
        }
    }

    template <typename T_TS>
    typename PyTimeSeriesList<T_TS>::view_type child_at(const PyTimeSeriesList<T_TS> &self, size_t index) {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return self.output_view().as_list().at(index);
        } else {
            return self.input_view().as_list().at(index);
        }
    }
}  // namespace

    template <typename T_TS>
    PyTimeSeriesList<T_TS>::PyTimeSeriesList(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::iter() const {
        return nb::iter(values());
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::get_item(const nb::handle &key) const {
        if (!nb::isinstance<nb::int_>(key)) {
            throw std::runtime_error("Invalid key type for TimeSeriesList");
        }

        const size_t index = nb::cast<size_t>(key);
        auto child = child_at(*this, index);
        if (!child) {
            throw nb::index_error();
        }
        return wrap_child<T_TS>(std::move(child));
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::keys() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            out.append(nb::int_(i));
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::values() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (!child) {
                continue;
            }
            out.append(wrap_child<T_TS>(std::move(child)));
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_keys() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(nb::int_(i));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_keys() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(nb::int_(i));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::int_ PyTimeSeriesList<T_TS>::len() const {
        return nb::int_(this->view().as_list().count());
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::items() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (!child) {
                continue;
            }
            out.append(nb::make_tuple(nb::int_(i), wrap_child<T_TS>(std::move(child))));
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_values() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(wrap_child<T_TS>(std::move(child)));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::valid_items() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(nb::make_tuple(nb::int_(i), wrap_child<T_TS>(std::move(child))));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_values() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(wrap_child<T_TS>(std::move(child)));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesList<T_TS>::modified_items() const {
        nb::list out;
        const auto count = this->view().as_list().count();
        for (size_t i = 0; i < count; ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(nb::make_tuple(nb::int_(i), wrap_child<T_TS>(std::move(child))));
            }
        }
        return out;
    }

    template <typename T_TS>
    bool PyTimeSeriesList<T_TS>::empty() const {
        return this->view().as_list().count() == 0;
    }

    template <typename T_TS> constexpr const char *get_list_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesListInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesListOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS>
    nb::str PyTimeSeriesList<T_TS>::py_str() {
        constexpr const char *name = get_list_type_name<T_TS>();
        auto str = fmt::format(fmt::runtime(name), static_cast<const void *>(&this->view()),
                               static_cast<size_t>(len()), static_cast<bool>(this->valid()));
        return nb::str(str.c_str());
    }

    template <typename T_TS>
    nb::str PyTimeSeriesList<T_TS>::py_repr() {
        return py_str();
    }

    // Explicit template instantiations
    template struct PyTimeSeriesList<PyTimeSeriesOutput>;
    template struct PyTimeSeriesList<PyTimeSeriesInput>;

    template <typename T_TS> void _register_tsl_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesList<T_TS>;

        nb::class_<PyTS_Type, T_TS>(m, std::is_same_v<T_TS, PyTimeSeriesInput> ? "TimeSeriesListInput" : "TimeSeriesListOutput")
            .def("__getitem__", &PyTS_Type::get_item)
            .def("__iter__", &PyTS_Type::iter)
            .def("__len__", &PyTS_Type::len)
            .def_prop_ro("empty", &PyTS_Type::empty)
            .def("values", &PyTS_Type::values)
            .def("valid_values", &PyTS_Type::valid_values)
            .def("modified_values", &PyTS_Type::modified_values)
            .def("keys", &PyTS_Type::keys)
            .def("items", &PyTS_Type::items)
            .def("valid_keys", &PyTS_Type::valid_keys)
            .def("valid_items", &PyTS_Type::valid_items)
            .def("modified_keys", &PyTS_Type::modified_keys)
            .def("modified_items", &PyTS_Type::modified_items)
            .def("__str__", &PyTS_Type::py_str)
            .def("__repr__", &PyTS_Type::py_repr);
    }

    void tsl_register_with_nanobind(nb::module_ &m) {
        _register_tsl_with_nanobind<PyTimeSeriesOutput>(m);
        _register_tsl_with_nanobind<PyTimeSeriesInput>(m);
    }
}  // namespace hgraph
