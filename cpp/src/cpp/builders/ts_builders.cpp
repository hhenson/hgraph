//
// ts_builders.cpp - CppTimeSeriesOutputBuilder and CppTimeSeriesInputBuilder implementation
//

#include <hgraph/builders/ts_builders.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/node.h>
#include <nanobind/nanobind.h>

namespace hgraph {

// ========== CppTimeSeriesOutputBuilder ==========

size_t CppTimeSeriesOutputBuilder::memory_size() const {
    return sizeof(TSOutput);
}

size_t CppTimeSeriesOutputBuilder::type_alignment() const {
    return alignof(TSOutput);
}

bool CppTimeSeriesOutputBuilder::is_same_type(const Builder& other) const {
    if (typeid(*this) != typeid(other)) {
        return false;
    }
    auto& other_builder = static_cast<const CppTimeSeriesOutputBuilder&>(other);
    return _ts_meta == other_builder._ts_meta;
}

time_series_output_s_ptr CppTimeSeriesOutputBuilder::make_instance(node_ptr /*owning_node*/) const {
    throw std::runtime_error(
        "CppTimeSeriesOutputBuilder::make_instance(node_ptr) is deprecated. "
        "Use ts_meta() with Node's TSMeta constructor instead."
    );
}

time_series_output_s_ptr CppTimeSeriesOutputBuilder::make_instance(time_series_output_ptr /*owning_output*/) const {
    throw std::runtime_error(
        "CppTimeSeriesOutputBuilder::make_instance(time_series_output_ptr) is deprecated. "
        "Use ts_meta() with Node's TSMeta constructor instead."
    );
}

void CppTimeSeriesOutputBuilder::register_with_nanobind(nb::module_& m) {
    nb::class_<CppTimeSeriesOutputBuilder, OutputBuilder>(m, "CppTimeSeriesOutputBuilder")
        .def(nb::init<const TSMeta*>(), nb::arg("ts_meta"))
        .def_prop_ro("ts_meta", &CppTimeSeriesOutputBuilder::ts_meta,
                     nb::rv_policy::reference)
        .def("memory_size", &CppTimeSeriesOutputBuilder::memory_size)
        .def("type_alignment", &CppTimeSeriesOutputBuilder::type_alignment);
}

// ========== CppTimeSeriesInputBuilder ==========

size_t CppTimeSeriesInputBuilder::memory_size() const {
    return sizeof(TSInput);
}

size_t CppTimeSeriesInputBuilder::type_alignment() const {
    return alignof(TSInput);
}

bool CppTimeSeriesInputBuilder::is_same_type(const Builder& other) const {
    if (typeid(*this) != typeid(other)) {
        return false;
    }
    auto& other_builder = static_cast<const CppTimeSeriesInputBuilder&>(other);
    return _ts_meta == other_builder._ts_meta;
}

time_series_input_s_ptr CppTimeSeriesInputBuilder::make_instance(node_ptr /*owning_node*/) const {
    throw std::runtime_error(
        "CppTimeSeriesInputBuilder::make_instance(node_ptr) is deprecated. "
        "Use ts_meta() with Node's TSMeta constructor instead."
    );
}

time_series_input_s_ptr CppTimeSeriesInputBuilder::make_instance(time_series_input_ptr /*owning_input*/) const {
    throw std::runtime_error(
        "CppTimeSeriesInputBuilder::make_instance(time_series_input_ptr) is deprecated. "
        "Use ts_meta() with Node's TSMeta constructor instead."
    );
}

void CppTimeSeriesInputBuilder::register_with_nanobind(nb::module_& m) {
    nb::class_<CppTimeSeriesInputBuilder, InputBuilder>(m, "CppTimeSeriesInputBuilder")
        .def(nb::init<const TSMeta*>(), nb::arg("ts_meta"))
        .def_prop_ro("ts_meta", &CppTimeSeriesInputBuilder::ts_meta,
                     nb::rv_policy::reference)
        .def("memory_size", &CppTimeSeriesInputBuilder::memory_size)
        .def("type_alignment", &CppTimeSeriesInputBuilder::type_alignment);
}

}  // namespace hgraph
