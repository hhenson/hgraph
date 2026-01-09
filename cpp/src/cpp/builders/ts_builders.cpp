//
// ts_builders.cpp - CppTimeSeriesOutputBuilder and CppTimeSeriesInputBuilder implementation
//

#include <hgraph/builders/ts_builders.h>
#include <hgraph/types/node.h>
#include <nanobind/nanobind.h>

namespace hgraph {

void CppTimeSeriesOutputBuilder::register_with_nanobind(nb::module_& m) {
    nb::class_<CppTimeSeriesOutputBuilder, OutputBuilder>(m, "CppTimeSeriesOutputBuilder")
        .def(nb::init<const TSMeta*>(), nb::arg("ts_meta"))
        .def_prop_ro("ts_meta", &CppTimeSeriesOutputBuilder::ts_meta,
                     nb::rv_policy::reference)
        .def("make_ts_value", &CppTimeSeriesOutputBuilder::make_ts_value,
             nb::arg("owner"), nb::arg("output_id") = OUTPUT_MAIN)
        .def("memory_size", &CppTimeSeriesOutputBuilder::memory_size)
        .def("type_alignment", &CppTimeSeriesOutputBuilder::type_alignment);
}

void CppTimeSeriesInputBuilder::register_with_nanobind(nb::module_& m) {
    nb::class_<CppTimeSeriesInputBuilder, InputBuilder>(m, "CppTimeSeriesInputBuilder")
        .def(nb::init<const TSMeta*>(), nb::arg("ts_meta"))
        .def_prop_ro("ts_meta", &CppTimeSeriesInputBuilder::ts_meta,
                     nb::rv_policy::reference)
        .def("make_ts_value", &CppTimeSeriesInputBuilder::make_ts_value,
             nb::arg("owner"))
        .def("memory_size", &CppTimeSeriesInputBuilder::memory_size)
        .def("type_alignment", &CppTimeSeriesInputBuilder::type_alignment);
}

}  // namespace hgraph
