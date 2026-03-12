#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/python_value_cache.h>
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
        if (out.current_time_ == MIN_DT && out.computed_.engine_time_ptr != nullptr) {
            out.current_time_ = *out.computed_.engine_time_ptr;
        }
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
        if (backing_ == Backing::STORED) {
            return stored_.valid();
        }
        if (backing_ != Backing::COMPUTED) {
            return false;
        }
        if (materialized_ != nullptr && materialized_->has_value()) {
            return materialized_->view().valid();
        }
        bool has_delta = false;
        if (try_computed_has_delta(has_delta)) {
            return has_delta;
        }
        return value().valid();
    }

    [[nodiscard]] bool empty() const {
        bool has_delta = false;
        if (try_computed_has_delta(has_delta)) {
            return !has_delta;
        }
        if (!valid()) {
            return true;
        }
        return change_count() == 0;
    }

    [[nodiscard]] size_t change_count() const {
        bool has_delta = false;
        if (try_computed_has_delta(has_delta) && !has_delta) {
            return 0;
        }

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
        if (backing_ == Backing::STORED) {
            return stored_.schema();
        }
        if (backing_ == Backing::COMPUTED) {
            if (materialized_ != nullptr && materialized_->has_value()) {
                return materialized_->view().schema();
            }
            bool has_delta = false;
            if (try_computed_has_delta(has_delta) && !has_delta) {
                return nullptr;
            }
        }
        return value().schema();
    }

    [[nodiscard]] nb::object to_python() const {
        if (materialized_python_.is_valid()) {
            return materialized_python_;
        }

        if (backing_ == Backing::COMPUTED &&
            computed_.ops != nullptr &&
            computed_.ops->delta_to_python != nullptr) {
            const ViewData computed = computed_with_time();
            PythonDeltaCacheEntry* delta_cache_slot = nullptr;
            if (is_delta_to_python_cacheable(computed)) {
                delta_cache_slot = resolve_python_delta_cache_slot_local(computed, true);
                if (delta_cache_slot != nullptr &&
                    delta_cache_slot->is_valid_for(current_time_, computed.delta_semantics)) {
                    materialized_python_ = delta_cache_slot->value;
                    return materialized_python_;
                }
            }

            materialized_python_ = computed.ops->delta_to_python(computed, current_time_);
            if (delta_cache_slot != nullptr) {
                delta_cache_slot->time = current_time_;
                delta_cache_slot->semantics = computed.delta_semantics;
                delta_cache_slot->value = materialized_python_;
            }
            return materialized_python_;
        }

        const value::View delta = value();
        materialized_python_ = delta.valid() ? delta.to_python() : nb::none();
        return materialized_python_;
    }

private:
    [[nodiscard]] bool try_computed_has_delta(bool& out) const {
        if (backing_ != Backing::COMPUTED ||
            computed_.ops == nullptr ||
            computed_.ops->has_delta == nullptr) {
            return false;
        }
        // Allow pre-tick materialization only for explicit policy modes.
        if (allows_pre_tick_delta(computed_.delta_semantics, current_time_)) {
            return false;
        }
        const ViewData computed = computed_with_time();
        out = computed.ops->has_delta(computed);
        return true;
    }

    static PythonDeltaCacheEntry* resolve_python_delta_cache_slot_local(const ViewData& vd, bool create) noexcept {
        auto* root = static_cast<PythonValueCacheNode*>(vd.python_value_cache_data);
        if (root == nullptr) {
            return nullptr;
        }

        const uint16_t d = vd.path_depth();
        if (d == 0) {
            return root->delta_root_value();
        }

        // Collect indices into a stack buffer by walking PathNode leaf→root.
        // Depth is typically 1-4; 16 covers any realistic nesting.
        size_t buf[16];
        size_t i = d;
        for (const PathNode* n = vd.path.get(); n && i > 0; n = n->parent)
            buf[--i] = n->index;

        PythonValueCacheNode* node = root;
        for (size_t depth = 0; depth < d; ++depth) {
            PythonDeltaCacheEntry* slot = node->delta_slot_value(buf[depth], create);
            if (slot == nullptr) {
                return nullptr;
            }
            if (depth + 1 == d) {
                return slot;
            }

            node = node->child_node(buf[depth], create);
            if (node == nullptr) {
                return nullptr;
            }
        }
        return nullptr;
    }

    [[nodiscard]] ViewData computed_with_time() const noexcept {
        if (backing_ != Backing::COMPUTED) {
            return computed_;
        }
        ViewData with_time = computed_;
        // Always route computed access through the captured snapshot time so the
        // view remains stable even if the original engine_time_ptr later mutates.
        with_time.engine_time_ptr = &current_time_;
        return with_time;
    }

    Backing backing_{Backing::NONE};
    value::View stored_{};
    ViewData computed_{};
    engine_time_t current_time_{MIN_DT};
    mutable std::shared_ptr<value::Value> materialized_{};
    mutable nb::object materialized_python_{};
};

}  // namespace hgraph
