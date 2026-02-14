#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value.h>

namespace hgraph {

class TSView;

/**
 * Owning storage for a time-series value.
 *
 * Phase 1 scope:
 * - Holds parallel value/time/observer/delta/link values.
 * - Exposes ViewData construction for TSView/TSInput/TSOutput.
 */
class HGRAPH_EXPORT TSValue {
public:
    TSValue() = default;
    explicit TSValue(const TSMeta* meta);
    TSValue(const TSMeta* meta, const value::TypeMeta* input_link_schema);

    TSValue(const TSValue&) = delete;
    TSValue& operator=(const TSValue&) = delete;
    TSValue(TSValue&&) noexcept = default;
    TSValue& operator=(TSValue&&) noexcept = default;
    ~TSValue() = default;

    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }
    [[nodiscard]] bool uses_link_target() const noexcept { return uses_link_target_; }

    [[nodiscard]] value::View value_view() const;
    [[nodiscard]] value::ValueView value_view_mut();

    [[nodiscard]] value::View time_view() const;
    [[nodiscard]] value::ValueView time_view_mut();

    [[nodiscard]] value::View observer_view() const;
    [[nodiscard]] value::ValueView observer_view_mut();

    [[nodiscard]] value::View delta_value_view() const;
    [[nodiscard]] value::ValueView delta_value_view_mut();

    [[nodiscard]] value::View link_view() const;
    [[nodiscard]] value::ValueView link_view_mut();

    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool all_valid(engine_time_t current_time) const;
    [[nodiscard]] bool has_delta() const;

    [[nodiscard]] ViewData make_view_data(ShortPath path = {}) const;
    [[nodiscard]] TSView ts_view(engine_time_t current_time, ShortPath path = {}) const;

private:
    void initialize(const TSMeta* meta, const value::TypeMeta* link_schema, bool uses_link_target);

    value::Value value_;
    value::Value time_;
    value::Value observer_;
    value::Value delta_value_;
    value::Value link_;

    const TSMeta* meta_{nullptr};
    bool uses_link_target_{false};
};

}  // namespace hgraph
