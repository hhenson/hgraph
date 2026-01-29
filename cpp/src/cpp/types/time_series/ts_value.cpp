/**
 * @file ts_value.cpp
 * @brief Implementation of TSValue - Owning time-series value storage.
 */

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/observer_list.h>
#include <hgraph/types/time_series/set_delta.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/delta_nav.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/map_storage.h>

namespace hgraph {

// ============================================================================
// Construction
// ============================================================================

TSValue::TSValue(const TSMeta* meta)
    : meta_(meta)
    , last_delta_clear_time_(MIN_ST)
{
    if (!meta_) {
        return;
    }

    // Get the schema cache instance
    auto& cache = TSMetaSchemaCache::instance();

    // Generate value schema based on TS kind
    const value::TypeMeta* value_schema = nullptr;
    switch (meta_->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::SIGNAL:
            // Scalar-like: use value_type directly
            value_schema = meta_->value_type;
            break;
        case TSKind::TSD:
            // TSD[K,V]: need to construct map type
            if (meta_->key_type && meta_->element_ts && meta_->element_ts->value_type) {
                value_schema = value::TypeRegistry::instance()
                    .map(meta_->key_type, meta_->element_ts->value_type)
                    .build();
            }
            break;
        case TSKind::TSL:
            // TSL[T]: need to construct list type
            if (meta_->element_ts && meta_->element_ts->value_type) {
                if (meta_->fixed_size > 0) {
                    value_schema = value::TypeRegistry::instance()
                        .fixed_list(meta_->element_ts->value_type, meta_->fixed_size)
                        .build();
                } else {
                    value_schema = value::TypeRegistry::instance()
                        .list(meta_->element_ts->value_type)
                        .build();
                }
            }
            break;
        case TSKind::TSB:
            // TSB: need to construct bundle type from fields
            if (meta_->fields && meta_->field_count > 0) {
                auto builder = value::TypeRegistry::instance().bundle(meta_->bundle_name);
                for (size_t i = 0; i < meta_->field_count; ++i) {
                    const TSBFieldInfo& field = meta_->fields[i];
                    if (field.ts_type && field.ts_type->value_type) {
                        builder.field(field.name, field.ts_type->value_type);
                    }
                }
                value_schema = builder.build();
            }
            break;
        case TSKind::REF:
            // REF types are deferred - create empty for now
            break;
    }

    // Allocate value storage
    if (value_schema) {
        value_ = value::Value<>(value_schema);
    }

    // Generate and allocate time storage
    const value::TypeMeta* time_schema = cache.get_time_schema(meta_);
    if (time_schema) {
        time_ = value::Value<>(time_schema);
    }

    // Generate and allocate observer storage
    const value::TypeMeta* observer_schema = cache.get_observer_schema(meta_);
    if (observer_schema) {
        observer_ = value::Value<>(observer_schema);
    }

    // Generate and allocate delta storage (may be nullptr)
    const value::TypeMeta* delta_schema = cache.get_delta_value_schema(meta_);
    if (delta_schema) {
        delta_value_ = value::Value<>(delta_schema);
    }

    // Generate and allocate link storage (may be nullptr for scalars)
    const value::TypeMeta* link_schema = cache.get_link_schema(meta_);
    if (link_schema) {
        link_ = value::Value<>(link_schema);
        // Initialize all link flags to false (not bound)
        // For bool: default constructed is false
        // For fixed_list[bool]: all elements default to false
    }

    // Wire observers for delta tracking
    wire_observers();
}

// ============================================================================
// Move Semantics
// ============================================================================

TSValue::TSValue(TSValue&& other) noexcept
    : value_(std::move(other.value_))
    , time_(std::move(other.time_))
    , observer_(std::move(other.observer_))
    , delta_value_(std::move(other.delta_value_))
    , link_(std::move(other.link_))
    , meta_(other.meta_)
    , last_delta_clear_time_(other.last_delta_clear_time_)
{
    other.meta_ = nullptr;
    other.last_delta_clear_time_ = MIN_ST;
}

TSValue& TSValue::operator=(TSValue&& other) noexcept {
    if (this != &other) {
        value_ = std::move(other.value_);
        time_ = std::move(other.time_);
        observer_ = std::move(other.observer_);
        delta_value_ = std::move(other.delta_value_);
        link_ = std::move(other.link_);
        meta_ = other.meta_;
        last_delta_clear_time_ = other.last_delta_clear_time_;
        other.meta_ = nullptr;
        other.last_delta_clear_time_ = MIN_ST;
    }
    return *this;
}

// ============================================================================
// View Access
// ============================================================================

value::View TSValue::value_view() {
    return value_.view();
}

value::View TSValue::value_view() const {
    return value_.const_view();
}

value::View TSValue::time_view() {
    return time_.view();
}

value::View TSValue::time_view() const {
    return time_.const_view();
}

value::View TSValue::observer_view() {
    return observer_.view();
}

value::View TSValue::observer_view() const {
    return observer_.const_view();
}

value::View TSValue::delta_value_view(engine_time_t current_time) {
    // Lazy delta clearing
    if (current_time > last_delta_clear_time_) {
        clear_delta_value();
        last_delta_clear_time_ = current_time;
    }
    return delta_value_.view();
}

value::View TSValue::delta_value_view() const {
    return delta_value_.const_view();
}

value::View TSValue::link_view() {
    return link_.view();
}

value::View TSValue::link_view() const {
    return link_.const_view();
}

// ============================================================================
// Time-Series Semantics
// ============================================================================

engine_time_t TSValue::last_modified_time() const {
    if (!meta_ || !time_.valid()) {
        return MIN_ST;
    }

    // For atomic types, time_ is just engine_time_t
    // For composite types, time_ is tuple[engine_time_t, ...]
    // In both cases, the first element (or the value itself) is the container time

    auto time_v = time_.const_view();

    switch (meta_->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Atomic: time_ is directly engine_time_t
            return time_v.as<engine_time_t>();

        case TSKind::TSD:
        case TSKind::TSL:
        case TSKind::TSB:
            // Composite: time_ is tuple[engine_time_t, ...]
            // Container time is first element
            return time_v.as_tuple().at(0).as<engine_time_t>();
    }

    return MIN_ST;
}

bool TSValue::modified(engine_time_t current_time) const {
    // Uses >= comparison per design spec
    return last_modified_time() >= current_time;
}

bool TSValue::valid() const {
    return last_modified_time() != MIN_ST;
}

bool TSValue::has_delta() const {
    return hgraph::has_delta(meta_);
}

// ============================================================================
// Internal Methods
// ============================================================================

void TSValue::clear_delta_value() {
    if (!delta_value_.valid()) {
        return;
    }

    auto delta_v = delta_value_.view();

    switch (meta_->kind) {
        case TSKind::TSS: {
            // SetDelta - use direct pointer cast (custom TypeOps)
            auto* set_delta = static_cast<SetDelta*>(delta_v.data());
            set_delta->clear();
            break;
        }
        case TSKind::TSD: {
            // MapDelta - use direct pointer cast (custom TypeOps)
            auto* map_delta = static_cast<MapDelta*>(delta_v.data());
            map_delta->clear();
            break;
        }
        case TSKind::TSB: {
            // BundleDeltaNav - use direct pointer cast (custom TypeOps)
            auto* nav = static_cast<BundleDeltaNav*>(delta_v.data());
            nav->clear();
            break;
        }
        case TSKind::TSL: {
            // ListDeltaNav - use direct pointer cast (custom TypeOps)
            auto* nav = static_cast<ListDeltaNav*>(delta_v.data());
            nav->clear();
            break;
        }
        default:
            // No delta for other types
            break;
    }
}

void TSValue::wire_observers() {
    if (!meta_) {
        return;
    }

    // Wire observers based on TS kind
    switch (meta_->kind) {
        case TSKind::TSS: {
            // TSS: Wire SetDelta to the value's KeySet
            // The delta acts as a SlotObserver to receive on_insert/on_erase notifications
            if (delta_value_.valid() && value_.valid()) {
                auto* set_delta = static_cast<SetDelta*>(delta_value_.view().data());
                auto* set_storage = static_cast<value::SetStorage*>(value_.view().data());
                set_storage->key_set().add_observer(set_delta);
            }
            break;
        }
        case TSKind::TSD: {
            // TSD: Wire MapDelta to the value's KeySet
            // The delta acts as a SlotObserver to receive on_insert/on_erase/on_update notifications
            if (delta_value_.valid() && value_.valid()) {
                auto* map_delta = static_cast<MapDelta*>(delta_value_.view().data());
                auto* map_storage = static_cast<value::MapStorage*>(value_.view().data());
                map_storage->key_set().add_observer(map_delta);
            }
            break;
        }
        case TSKind::TSB: {
            // TSB: Wire child observers recursively
            if (delta_value_.valid()) {
                wire_tsb_observers(value_.view(), delta_value_.view());
            }
            break;
        }
        case TSKind::TSL: {
            // TSL: Wire child observers recursively
            if (delta_value_.valid()) {
                wire_tsl_observers(value_.view(), delta_value_.view());
            }
            break;
        }
        default:
            // No observer wiring for atomic types
            break;
    }
}

void TSValue::wire_tsb_observers(value::View value_v, value::View delta_v) {
    if (!meta_ || !meta_->fields) {
        return;
    }

    // Use direct pointer cast - BundleDeltaNav has custom TypeOps
    auto* nav = static_cast<BundleDeltaNav*>(delta_v.data());

    // Ensure children vector has correct size
    if (nav->children.size() != meta_->field_count) {
        nav->children.resize(meta_->field_count);
    }

    // Iterate through bundle fields
    auto bundle_v = value_v.as_bundle();
    for (size_t i = 0; i < meta_->field_count; ++i) {
        const TSMeta* field_ts = meta_->fields[i].ts_type;
        if (!field_ts) continue;

        // Check if this field has delta
        if (!hgraph::has_delta(field_ts)) {
            nav->children[i] = std::monostate{};
            continue;
        }

        // Note: The actual wiring of child deltas requires the child TSValue
        // instances to be created. In a full implementation, each field would
        // have its own TSValue, and we would wire their delta pointers here.
        // For now, we initialize to monostate and let the parent graph wiring
        // handle the connections.
        nav->children[i] = std::monostate{};
    }
}

void TSValue::wire_tsl_observers(value::View value_v, value::View delta_v) {
    if (!meta_ || !meta_->element_ts) {
        return;
    }

    // Use direct pointer cast - ListDeltaNav has custom TypeOps
    auto* nav = static_cast<ListDeltaNav*>(delta_v.data());

    // Get list size
    auto list_v = value_v.as_list();
    size_t list_size = meta_->fixed_size > 0 ? meta_->fixed_size : list_v.size();

    // Ensure children vector has correct size
    if (nav->children.size() != list_size) {
        nav->children.resize(list_size);
    }

    // Check if element has delta
    if (!hgraph::has_delta(meta_->element_ts)) {
        for (size_t i = 0; i < list_size; ++i) {
            nav->children[i] = std::monostate{};
        }
        return;
    }

    // Note: Similar to TSB, the actual wiring of child deltas requires the
    // child TSValue instances. We initialize to monostate here.
    for (size_t i = 0; i < list_size; ++i) {
        nav->children[i] = std::monostate{};
    }
}

// ============================================================================
// TSView Access (forward declaration, implemented in ts_view.cpp)
// ============================================================================

// TSView TSValue::ts_view(engine_time_t current_time) is implemented
// in ts_view.cpp to avoid circular dependency

} // namespace hgraph
