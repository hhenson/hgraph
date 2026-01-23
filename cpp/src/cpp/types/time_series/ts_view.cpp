/**
 * @file ts_view.cpp
 * @brief Implementation of TSView - Non-owning time-series view.
 */

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_meta_schema.h>

namespace hgraph {

// ============================================================================
// TSView Construction
// ============================================================================

TSView::TSView(TSValue& ts_value, engine_time_t current_time)
    : value_view_(ts_value.value_view())
    , time_view_(ts_value.time_view())
    , observer_view_(ts_value.observer_view())
    , delta_value_view_(ts_value.delta_value_view(current_time))
    , meta_(ts_value.meta())
    , current_time_(current_time)
{
}

TSView::TSView(const TSMeta* meta,
               value::View value_view,
               value::View time_view,
               value::View observer_view,
               value::View delta_value_view,
               engine_time_t current_time) noexcept
    : value_view_(value_view)
    , time_view_(time_view)
    , observer_view_(observer_view)
    , delta_value_view_(delta_value_view)
    , meta_(meta)
    , current_time_(current_time)
{
}

// ============================================================================
// Time-Series Semantics
// ============================================================================

engine_time_t TSView::last_modified_time() const {
    if (!meta_ || !time_view_.valid()) {
        return MIN_ST;
    }

    switch (meta_->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Atomic: time_ is directly engine_time_t
            return time_view_.as<engine_time_t>();

        case TSKind::TSD:
        case TSKind::TSL:
        case TSKind::TSB:
            // Composite: time_ is tuple[engine_time_t, ...]
            return time_view_.as_tuple().at(0).as<engine_time_t>();
    }

    return MIN_ST;
}

bool TSView::modified() const {
    // Uses >= comparison per design spec
    return last_modified_time() >= current_time_;
}

bool TSView::valid() const {
    return last_modified_time() != MIN_ST;
}

bool TSView::has_delta() const {
    return hgraph::has_delta(meta_);
}

// ============================================================================
// TSValue::ts_view Implementation
// ============================================================================

TSView TSValue::ts_view(engine_time_t current_time) {
    return TSView(*this, current_time);
}

} // namespace hgraph
