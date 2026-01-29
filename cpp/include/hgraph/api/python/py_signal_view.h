#pragma once

/**
 * @file py_signal_view.h
 * @brief Python bindings for SignalView.
 */

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Register SignalView Python bindings.
 * @param m The nanobind module
 */
void register_signal_view(nb::module_& m);

} // namespace hgraph
