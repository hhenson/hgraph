#pragma once

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {

class TSOutputView;

/**
 * Register private TS runtime scaffolding bindings used by tests.
 */
void ts_runtime_internal_register_with_nanobind(nb::module_& m);

/**
 * Reset runtime-scoped TS feature observers.
 */
void reset_ts_runtime_feature_observers();

/**
 * Runtime helper: dynamic TSD REF output features.
 */
TSOutputView runtime_tsd_get_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester);
void runtime_tsd_release_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester);

/**
 * Runtime helper: dynamic TSS contains/empty features.
 */
TSOutputView runtime_tss_get_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester);
void runtime_tss_release_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester);
TSOutputView runtime_tss_get_is_empty_output(TSOutputView& self);

}  // namespace hgraph
