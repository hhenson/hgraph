#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * Register private TS runtime scaffolding bindings used by tests.
 */
void ts_runtime_internal_register_with_nanobind(nb::module_& m);

}  // namespace hgraph
