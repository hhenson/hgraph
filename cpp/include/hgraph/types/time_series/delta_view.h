#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value_view.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace nb = nanobind;

namespace hgraph {

/**
 * Type-erased delta wrapper.
 *
 * This unifies:
 * - stored delta payloads (`value::View`)
 * - computed TS delta payloads (`ViewData + ts_ops`)
 */
class HGRAPH_EXPORT DeltaView {
public:
    enum class Backing : uint8_t {
        NONE = 0,
        STORED = 1,
        COMPUTED = 2,
    };

    DeltaView() = default;

    static DeltaView from_stored(value::View delta) noexcept {
        DeltaView out;
        out.backing_ = Backing::STORED;
        out.stored_ = std::move(delta);
        return out;
    }

    static DeltaView from_computed(ViewData view_data, engine_time_t current_time) noexcept {
        DeltaView out;
        out.backing_ = Backing::COMPUTED;
        out.computed_ = std::move(view_data);
        out.current_time_ = current_time;
        return out;
    }

    [[nodiscard]] Backing backing() const noexcept {
        return backing_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return valid();
    }

    [[nodiscard]] value::View value() const {
        if (backing_ == Backing::STORED) {
            return stored_;
        }
        if (backing_ != Backing::COMPUTED) {
            return {};
        }
        if (materialized_ != nullptr && materialized_->has_value()) {
            return materialized_->view();
        }
        const ViewData computed = computed_with_time();
        if (computed.ops == nullptr || computed.ops->delta_value == nullptr) {
            return {};
        }
        const value::View delta = computed.ops->delta_value(computed);
        if (!delta.valid()) {
            return {};
        }
        materialized_ = std::make_shared<value::Value>(delta);
        return materialized_->view();
    }

    [[nodiscard]] bool valid() const {
        return value().valid();
    }

    [[nodiscard]] bool empty() const {
        if (!valid()) {
            return true;
        }
        if (backing_ == Backing::COMPUTED &&
            computed_.ops != nullptr &&
            computed_.ops->has_delta != nullptr) {
            const ViewData computed = computed_with_time();
            return !computed.ops->has_delta(computed);
        }
        return change_count() == 0;
    }

    [[nodiscard]] size_t change_count() const {
        const value::View delta = value();
        if (!delta.valid()) {
            return 0;
        }
        if (delta.is_set()) {
            return delta.as_set().size();
        }
        if (delta.is_map()) {
            return delta.as_map().size();
        }
        if (delta.is_list()) {
            return delta.as_list().size();
        }
        if (delta.is_bundle()) {
            return delta.as_bundle().size();
        }
        if (delta.is_tuple()) {
            size_t out = 0;
            auto tuple = delta.as_tuple();
            for (size_t i = 0; i < tuple.size(); ++i) {
                out += DeltaView::from_stored(tuple.at(i)).change_count();
            }
            return out;
        }
        return 1;
    }

    [[nodiscard]] const value::TypeMeta* schema() const {
        return value().schema();
    }

    [[nodiscard]] nb::object to_python() const {
        if (backing_ == Backing::COMPUTED &&
            computed_.ops != nullptr &&
            computed_.ops->delta_to_python != nullptr) {
            const ViewData computed = computed_with_time();
            return computed.ops->delta_to_python(computed, current_time_);
        }

        const value::View delta = value();
        return delta.valid() ? delta.to_python() : nb::none();
    }

private:
    [[nodiscard]] ViewData computed_with_time() const noexcept {
        if (backing_ != Backing::COMPUTED || computed_.engine_time_ptr != nullptr || current_time_ == MIN_DT) {
            return computed_;
        }
        ViewData with_time = computed_;
        with_time.engine_time_ptr = &current_time_;
        return with_time;
    }

    Backing backing_{Backing::NONE};
    value::View stored_{};
    ViewData computed_{};
    engine_time_t current_time_{MIN_DT};
    mutable std::shared_ptr<value::Value> materialized_{};
};

}  // namespace hgraph
