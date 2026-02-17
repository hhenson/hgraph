#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/wrapper_factory.h>

#include <fmt/format.h>
#include <stdexcept>
#include <type_traits>

namespace hgraph
{
namespace
{
    template <typename T_TS>
    nb::object wrap_child(typename PyTimeSeriesBundle<T_TS>::view_type child) {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return wrap_output_view(std::move(child));
        } else {
            return wrap_input_view(std::move(child));
        }
    }

    template <typename T_TS>
    typename PyTimeSeriesBundle<T_TS>::view_type child_at(const PyTimeSeriesBundle<T_TS> &self, size_t index) {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return self.output_view().as_bundle().at(index);
        } else {
            return self.input_view().as_bundle().at(index);
        }
    }

    template <typename T_TS>
    typename PyTimeSeriesBundle<T_TS>::view_type child_by_name(const PyTimeSeriesBundle<T_TS> &self, std::string_view name) {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            return self.output_view().as_bundle().field(name);
        } else {
            return self.input_view().as_bundle().field(name);
        }
    }

    template <typename T_TS>
    nb::list field_names(const PyTimeSeriesBundle<T_TS> &self) {
        nb::list out;
        const auto *meta = self.view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            out.append(nb::str(meta->fields()[i].name));
        }
        return out;
    }
}  // namespace

    template <typename T_TS>
    PyTimeSeriesBundle<T_TS>::PyTimeSeriesBundle(view_type view) : T_TS(std::move(view)) {}

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::iter() const {
        return nb::iter(values());
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::get_item(const nb::handle &key) const {
        if (nb::isinstance<nb::str>(key)) {
            auto child = child_by_name(*this, nb::cast<std::string>(key));
            if (!child) {
                throw nb::key_error();
            }
            return wrap_child<T_TS>(std::move(child));
        }

        if (nb::isinstance<nb::int_>(key)) {
            const size_t index = nb::cast<size_t>(key);
            auto child = child_at(*this, index);
            if (!child) {
                throw nb::index_error();
            }
            return wrap_child<T_TS>(std::move(child));
        }

        throw std::runtime_error("Invalid key type for TimeSeriesBundle");
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    template <typename T_TS>
    nb::bool_ PyTimeSeriesBundle<T_TS>::contains(const nb::handle &key) const {
        if (!nb::isinstance<nb::str>(key)) {
            return nb::bool_(false);
        }

        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return nb::bool_(false);
        }

        const std::string name = nb::cast<std::string>(key);
        for (size_t i = 0; i < meta->field_count(); ++i) {
            if (name == meta->fields()[i].name) {
                return nb::bool_(true);
            }
        }
        return nb::bool_(false);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::schema() const {
        const auto *meta = this->view().ts_meta();
        return meta != nullptr ? meta->python_type() : nb::none();
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::key_from_value(const nb::handle &value) const {
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return nb::none();
        }

        const ShortPath *target_path = nullptr;
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesOutput>) {
            if (auto *wrapped = nb::inst_ptr<PyTimeSeriesOutput>(value)) {
                target_path = &wrapped->output_view().short_path();
            }
        } else {
            if (auto *wrapped = nb::inst_ptr<PyTimeSeriesInput>(value)) {
                target_path = &wrapped->input_view().short_path();
            }
        }

        if (target_path == nullptr) {
            return nb::none();
        }

        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (!child) {
                continue;
            }
            const auto &child_path = child.short_path();
            if (child_path.node == target_path->node && child_path.port_type == target_path->port_type &&
                child_path.indices == target_path->indices) {
                return nb::str(meta->fields()[i].name);
            }
        }
        return nb::none();
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::keys() const {
        return field_names(*this);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::values() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (!child) {
                continue;
            }
            out.append(wrap_child<T_TS>(std::move(child)));
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_keys() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(nb::str(meta->fields()[i].name));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_values() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(wrap_child<T_TS>(std::move(child)));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_keys() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(nb::str(meta->fields()[i].name));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_values() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(wrap_child<T_TS>(std::move(child)));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::int_ PyTimeSeriesBundle<T_TS>::len() const {
        auto bundle = this->view().as_bundle();
        return nb::int_(bundle.count());
    }

    template <typename T_TS>
    nb::bool_ PyTimeSeriesBundle<T_TS>::empty() const {
        auto bundle = this->view().as_bundle();
        return nb::bool_(bundle.count() == 0);
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::items() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (!child) {
                continue;
            }
            out.append(nb::make_tuple(nb::str(meta->fields()[i].name), wrap_child<T_TS>(std::move(child))));
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::valid_items() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.valid()) {
                out.append(nb::make_tuple(nb::str(meta->fields()[i].name), wrap_child<T_TS>(std::move(child))));
            }
        }
        return out;
    }

    template <typename T_TS>
    nb::object PyTimeSeriesBundle<T_TS>::modified_items() const {
        nb::list out;
        const auto *meta = this->view().ts_meta();
        if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
            return out;
        }
        for (size_t i = 0; i < meta->field_count(); ++i) {
            auto child = child_at(*this, i);
            if (child && child.modified()) {
                out.append(nb::make_tuple(nb::str(meta->fields()[i].name), wrap_child<T_TS>(std::move(child))));
            }
        }
        return out;
    }

    template <typename T_TS> constexpr const char *get_bundle_type_name() {
        if constexpr (std::is_same_v<T_TS, PyTimeSeriesInput>) {
            return "TimeSeriesBundleInput@{:p}[keys={}, valid={}]";
        } else {
            return "TimeSeriesBundleOutput@{:p}[keys={}, valid={}]";
        }
    }

    template <typename T_TS>
    nb::str PyTimeSeriesBundle<T_TS>::py_str() {
        constexpr const char *name = get_bundle_type_name<T_TS>();
        const auto count = static_cast<size_t>(len());
        auto str = fmt::format(fmt::runtime(name), static_cast<const void *>(&this->view()), count, static_cast<bool>(this->valid()));
        return nb::str(str.c_str());
    }

    template <typename T_TS>
    nb::str PyTimeSeriesBundle<T_TS>::py_repr() {
        return py_str();
    }

    template struct PyTimeSeriesBundle<PyTimeSeriesOutput>;
    template struct PyTimeSeriesBundle<PyTimeSeriesInput>;

    template <typename T_TS> void _register_tsb_with_nanobind(nb::module_ &m) {
        using PyTS_Type = PyTimeSeriesBundle<T_TS>;
        auto name{std::is_same_v<T_TS, PyTimeSeriesInput> ? "TimeSeriesBundleInput" : "TimeSeriesBundleOutput"};
        nb::class_<PyTS_Type, T_TS>(m, name)
            .def("__getitem__", &PyTS_Type::get_item)
            .def("__getattr__", &PyTS_Type::get_attr)
            .def("__iter__", &PyTS_Type::iter)
            .def("__len__", &PyTS_Type::len)
            .def("__contains__", &PyTS_Type::contains)
            .def("keys", &PyTS_Type::keys)
            .def("items", &PyTS_Type::items)
            .def("values", &PyTS_Type::values)
            .def("valid_keys", &PyTS_Type::valid_keys)
            .def("valid_values", &PyTS_Type::valid_values)
            .def("valid_items", &PyTS_Type::valid_items)
            .def("modified_keys", &PyTS_Type::modified_keys)
            .def("modified_values", &PyTS_Type::modified_values)
            .def("modified_items", &PyTS_Type::modified_items)
            .def_prop_ro("delta_value", [](const PyTS_Type& self) -> nb::object {
                nb::dict out;
                const auto* meta = self.view().ts_meta();
                if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
                    return out;
                }
                for (size_t i = 0; i < meta->field_count(); ++i) {
                    const char* field_name = meta->fields()[i].name;
                    if (field_name == nullptr) {
                        continue;
                    }
                    auto child = child_at(self, i);
                    if (!child) {
                        continue;
                    }
                    nb::object child_delta = child.delta_to_python();
                    if (!child_delta.is_none()) {
                        out[nb::str(field_name)] = child_delta;
                    }
                }
                return out;
            })
            .def("key_from_value", &PyTS_Type::key_from_value)
            .def_prop_ro("empty", &PyTS_Type::empty)
            .def_prop_ro("__schema__", &PyTS_Type::schema)
            .def_prop_ro("as_schema", [](nb::handle self) { return self; })
            .def("__str__", &PyTS_Type::py_str)
            .def("__repr__", &PyTS_Type::py_repr);
    }

    void tsb_register_with_nanobind(nb::module_ &m) {
        _register_tsb_with_nanobind<PyTimeSeriesOutput>(m);
        _register_tsb_with_nanobind<PyTimeSeriesInput>(m);
    }
}  // namespace hgraph
