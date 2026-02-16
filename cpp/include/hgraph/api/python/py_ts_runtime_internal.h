#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * Register private TS runtime scaffolding bindings used by tests.
 */
void ts_runtime_internal_register_with_nanobind(nb::module_& m);

/**
 * Reset runtime-scoped TS feature observers.
 */
void reset_ts_runtime_feature_observers();

}  // namespace hgraph
