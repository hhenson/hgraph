//
// Created by Claude on 19/12/2025.
//
// DeltaView - Lightweight view for accessing time-series delta values
//
// Design principles:
// - Non-owning view (40 bytes, fits in cache line)
// - No allocation unless to_python() is called
// - Kind-specific accessors for each TS type
// - Composable - nested DeltaViews for recursive structures
//

#ifndef HGRAPH_DELTA_VIEW_H
#define HGRAPH_DELTA_VIEW_H

#include <hgraph/types/value/value.h>
#include <hgraph/types/value/modification_tracker.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/util/date_time.h>

namespace hgraph::ts {

// ============================================================================
// DeltaView - Lightweight view for delta value access
// ============================================================================

/**
 * DeltaView - Non-owning view for accessing delta values from time-series
 *
 * Provides type-erased access to delta information based on TimeSeriesKind:
 * - Scalar (TS): Returns the current value
 * - Bundle (TSB): Iterates only modified fields
 * - List (TSL): Iterates only modified elements
 * - Set (TSS): Provides added/removed element access
 * - Dict (TSD): Provides modified entries and removed keys
 * - Ref (REF): Returns the reference value
 *
 * Memory layout: ~48 bytes
 * - ConstValueView: 16 bytes (data ptr + schema ptr)
 * - ModificationTracker: ~16 bytes
 * - TimeSeriesTypeMeta*: 8 bytes
 * - engine_time_t: 8 bytes
 */
class DeltaView {
public:
    DeltaView() = default;

    DeltaView(value::ConstValueView value_view,
              value::ModificationTracker tracker,
              const TimeSeriesTypeMeta* meta,
              engine_time_t time)
        : _value_view(value_view)
        , _tracker(tracker)
        , _meta(meta)
        , _time(time) {}

    // === Validity and type queries ===

    [[nodiscard]] bool valid() const {
        return _value_view.valid() && _meta != nullptr;
    }

    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }

    [[nodiscard]] TimeSeriesKind ts_kind() const {
        return _meta ? _meta->ts_kind : TimeSeriesKind::TS;
    }

    [[nodiscard]] engine_time_t time() const { return _time; }

    [[nodiscard]] const value::ConstValueView& value_view() const { return _value_view; }
    [[nodiscard]] value::ModificationTracker& tracker() { return _tracker; }
    [[nodiscard]] const value::ModificationTracker& tracker() const { return _tracker; }

    // === Scalar delta (TS) ===
    // For scalars, delta_value IS the current value

    [[nodiscard]] value::ConstValueView scalar_delta() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TS) {
            return {};
        }
        return _value_view;
    }

    // === Bundle modified fields (TSB) ===

    [[nodiscard]] size_t bundle_field_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSB) return 0;
        auto* bundle_meta = static_cast<const TSBTypeMeta*>(_meta);
        return bundle_meta->fields.size();
    }

    [[nodiscard]] bool bundle_field_modified(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSB) return false;
        return _tracker.field_modified_at(index, _time);
    }

    [[nodiscard]] std::string_view bundle_field_name(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSB) return {};
        auto* bundle_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (index >= bundle_meta->fields.size()) return {};
        return bundle_meta->fields[index].name;
    }

    [[nodiscard]] DeltaView bundle_field(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSB) {
            return {};
        }

        auto* bundle_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (index >= bundle_meta->fields.size()) {
            return {};
        }

        // Get the field's value view
        auto field_value = _value_view.field(index);
        if (!field_value.valid()) {
            return {};
        }

        // Get the field's tracker (need non-const access)
        auto field_tracker = const_cast<value::ModificationTracker&>(_tracker).field(index);

        // Get the field's TS metadata
        auto* field_meta = bundle_meta->fields[index].type;

        return {field_value, field_tracker, field_meta, _time};
    }

    // Count of modified fields (O(n))
    [[nodiscard]] size_t bundle_modified_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSB) return 0;
        size_t count = 0;
        size_t n = bundle_field_count();
        for (size_t i = 0; i < n; ++i) {
            if (bundle_field_modified(i)) ++count;
        }
        return count;
    }

    // === List modified elements (TSL) ===

    [[nodiscard]] size_t list_element_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSL) return 0;
        auto* list_meta = static_cast<const TSLTypeMeta*>(_meta);
        return list_meta->size;
    }

    [[nodiscard]] bool list_element_modified(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSL) return false;
        return _tracker.element_modified_at(index, _time);
    }

    [[nodiscard]] DeltaView list_element(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSL) {
            return {};
        }

        auto* list_meta = static_cast<const TSLTypeMeta*>(_meta);
        if (index >= list_meta->size) {
            return {};
        }

        // Get the element's value view
        auto elem_value = _value_view.element(index);
        if (!elem_value.valid()) {
            return {};
        }

        // Get the element's tracker (need non-const access)
        auto elem_tracker = const_cast<value::ModificationTracker&>(_tracker).element(index);

        // Get the element's TS metadata
        auto* elem_meta = list_meta->element_ts_type;

        return {elem_value, elem_tracker, elem_meta, _time};
    }

    // Count of modified elements (O(n))
    [[nodiscard]] size_t list_modified_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSL) return 0;
        size_t count = 0;
        size_t n = list_element_count();
        for (size_t i = 0; i < n; ++i) {
            if (list_element_modified(i)) ++count;
        }
        return count;
    }

    // === Set delta (TSS) ===

    [[nodiscard]] size_t set_added_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSS) return 0;
        return _tracker.set_added_count(_time);
    }

    [[nodiscard]] size_t set_removed_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSS) return 0;
        return _tracker.set_removed_count();
    }

    [[nodiscard]] value::ConstTypedPtr set_removed_element(size_t i) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSS) return {};
        return _tracker.set_removed_element(i);
    }

    // Access to the set storage for iterating added elements
    [[nodiscard]] const value::SetStorage* set_storage() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSS) return nullptr;
        return static_cast<const value::SetStorage*>(_value_view.data());
    }

    // === Dict delta (TSD) ===

    [[nodiscard]] size_t dict_entry_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSD) return 0;
        return static_cast<const value::DictStorage*>(_value_view.data())->size();
    }

    [[nodiscard]] bool dict_entry_modified(size_t index) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSD) return false;
        return _tracker.dict_entry_modified_at(index, _time);
    }

    [[nodiscard]] size_t dict_removed_count() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSD) return 0;
        return _tracker.dict_removed_count();
    }

    [[nodiscard]] value::ConstTypedPtr dict_removed_key(size_t i) const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSD) return {};
        return _tracker.dict_removed_key(i);
    }

    // Access to the dict storage for iterating entries
    [[nodiscard]] const value::DictStorage* dict_storage() const {
        if (!valid() || ts_kind() != TimeSeriesKind::TSD) return nullptr;
        return static_cast<const value::DictStorage*>(_value_view.data());
    }

    // === Ref delta (REF) ===
    // For refs, delta_value IS the reference value

    [[nodiscard]] value::ConstValueView ref_delta() const {
        if (!valid() || ts_kind() != TimeSeriesKind::REF) {
            return {};
        }
        return _value_view;
    }

    // === Nested navigation (aliases) ===

    [[nodiscard]] DeltaView field(size_t index) const {
        return bundle_field(index);
    }

    [[nodiscard]] DeltaView element(size_t index) const {
        return list_element(index);
    }

private:
    value::ConstValueView _value_view;
    value::ModificationTracker _tracker;
    const TimeSeriesTypeMeta* _meta{nullptr};
    engine_time_t _time{MIN_DT};
};

} // namespace hgraph::ts

#endif // HGRAPH_DELTA_VIEW_H
