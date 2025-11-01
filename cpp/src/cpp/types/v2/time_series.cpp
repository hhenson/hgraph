#include <hgraph/types/v2/time_series.h>

namespace hgraph {

bool operator==(TypeId a, TypeId b) { return a.info == b.info; }

TsEventAny TsEventAny::none(engine_time_t t) { return {t, TsEventKind::None, {}}; }
TsEventAny TsEventAny::invalidate(engine_time_t t) { return {t, TsEventKind::Invalidate, {}}; }

TsValueAny TsValueAny::none() { return {}; }

// Safety check to ensure the configured SBO matches nb::object size as agreed.
static_assert(HGRAPH_TS_VALUE_SBO == sizeof(nanobind::object),
              "HGRAPH_TS_VALUE_SBO must equal sizeof(nanobind::object)");

} // namespace hgraph
