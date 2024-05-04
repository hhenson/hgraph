#include <hgraph/types/scalar_value.h>

namespace hgraph
{

    ScalarValue::ScalarValue(const ScalarValue &value) : m_pimpl{value.m_pimpl->clone()} {}

    ScalarValue::ScalarValue(const ScalarValue *value) : m_pimpl{value->m_pimpl->reference()}{}

    ScalarValue &ScalarValue::operator=(const ScalarValue &value) {
        auto scalar_concept{value.m_pimpl->clone()};
        m_pimpl.swap(scalar_concept);
        return *this;
        // This should copy in the new value and leave the old value to meet with its destructor
    }

    void ScalarValue::py_register(py::module_ &m) {
        py::class_<ScalarValue>(m, "ScalarValue")
            //TODO: For use in python would it be better to use value or shared pointer semantics?
            .def(py::init([](bool value) {return create_scalar_value(value);}))
            .def(py::init([](hg_byte value) {return create_scalar_value(value);}))
            .def(py::init([](hg_int value) {return create_scalar_value(value);}))
            .def(py::init([](hg_float value) {return create_scalar_value(value);}))
            .def(py::init([](hg_string value) {return create_scalar_value(std::move(value));}))
//            .def(py::init([](hg_set value) {return make_shared_scalar_value(value);}))
//            .def(py::init([](hg_dict value) {return make_shared_scalar_value(value);}))
//            .def(py::init([](hg_tuple value) {return make_shared_scalar_value(value);}))
            .def("__lt__", &ScalarValue::operator<)
            .def("__eq__", &ScalarValue::operator==)
            .def("__str__", &ScalarValue::to_string)
            ;
    }
}  // namespace hgraph
