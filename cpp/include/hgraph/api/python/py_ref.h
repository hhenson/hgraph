#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph
{
    // Register TimeSeriesReference Python bindings
    void ref_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
