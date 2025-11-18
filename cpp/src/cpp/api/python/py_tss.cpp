#include <hgraph/api/python/py_tss.h>

namespace hgraph
{
    template<typename K>
    void _register_with_nanobind(nb::module_ &m, std::string name) {
        nb::class_<SetDelta, nb::intrusive_base>(m, "SetDelta_" + name)
            .def_prop_ro("added", &SetDelta::py_added)
            .def_prop_ro("removed", &SetDelta::py_removed)
            .def_prop_ro("tp", &SetDelta::py_type)
            .def(
                "__str__",
                [](SetDelta &self) { return nb::str("SetDelta(added={}, removed={})").format(self.py_added(), self.py_removed()); })
            .def(
                "__repr__",
                [](SetDelta &self) {
                    return nb::str("SetDelta[{}](added={}, removed={})").format(self.py_type(), self.py_added(), self.py_removed());
                })
            .def("__eq__", &SetDelta::operator==)
            .def("__eq__", eq)
            .def("__hash__", &SetDelta::hash);

        using SetDelta_bool = SetDelta_T<bool>;
        nb::class_<SetDelta_bool, SetDelta>(m, "SetDelta_bool")
            .def(nb::init<const std::unordered_set<bool> &, const std::unordered_set<bool> &>(), "added"_a, "removed"_a)
            .def("__add__", &SetDelta_bool::operator+);

        using SetDelta_int = SetDelta_T<int64_t>;
        nb::class_<SetDelta_int, SetDelta>(m, "SetDelta_int")
            .def(nb::init<const std::unordered_set<int64_t> &, const std::unordered_set<int64_t> &>(), "added"_a, "removed"_a)
            .def("__add__", &SetDelta_int::operator+);
        ;
        using SetDelta_float = SetDelta_T<double>;
        nb::class_<SetDelta_float, SetDelta>(m, "SetDelta_float")
            .def(nb::init<const std::unordered_set<double> &, const std::unordered_set<double> &>(), "added"_a, "removed"_a)
            .def("__add__", &SetDelta_float::operator+);

        using SetDelta_date = SetDelta_T<engine_date_t>;
        nb::class_<SetDelta_date, SetDelta>(m, "SetDelta_date")
            .def(nb::init<const std::unordered_set<engine_date_t> &, const std::unordered_set<engine_date_t> &>(), "added"_a,
                 "removed"_a)
            .def("__add__", &SetDelta_date::operator+);

        using SetDelta_date_time = SetDelta_T<engine_time_t>;
        nb::class_<SetDelta_date_time, SetDelta>(m, "SetDelta_date_time")
            .def(nb::init<const std::unordered_set<engine_time_t> &, const std::unordered_set<engine_time_t> &>(), "added"_a,
                 "removed"_a)
            .def("__add__", &SetDelta_date_time::operator+);

        using SetDelta_time_delta = SetDelta_T<engine_time_delta_t>;
        nb::class_<SetDelta_time_delta, SetDelta>(m, "SetDelta_time_delta")
            .def(nb::init<const std::unordered_set<engine_time_delta_t> &, const std::unordered_set<engine_time_delta_t> &>(),
                 "added"_a, "removed"_a)
            .def("__add__", &SetDelta_time_delta::operator+);

        using SetDelta_object = SetDelta_T<nb::object>;
        nb::class_<SetDelta_object, SetDelta>(m, "SetDelta_object")
            .def(nb::init<const std::unordered_set<nb::object> &, const std::unordered_set<nb::object> &, nb::object>(), "added"_a,
                 "removed"_a, "tp"_a)
            .def("__add__", &SetDelta_object::operator+);
    }

    void register_with_nanobind(nb::module_ &m) {
        _register_with_nanobind<bool>(m);
        _register_with_nanobind<int64_t>(m);
        _register_with_nanobind<double>(m);
        _register_with_nanobind<engine_date_t>(m);
        _register_with_nanobind<engine_time_t>(m);
        _register_with_nanobind<engine_time_delta_t>(m);
        _register_with_nanobind<nb::object>(m);
    }
}  // namespace hgraph
