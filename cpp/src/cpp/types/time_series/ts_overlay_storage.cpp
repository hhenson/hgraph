/**
 * @file ts_overlay_storage.cpp
 * @brief Implementation of TS overlay storage for hierarchical modification tracking.
 */

#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/type_registry.h>
#include <algorithm>
#include <fmt/core.h>

namespace hgraph {

// ============================================================================
// ObserverList Implementation
// ============================================================================

void ObserverList::subscribe(Notifiable* observer) {
    if (observer) {
        _observers.insert(observer);
    }
}

void ObserverList::unsubscribe(Notifiable* observer) {
    if (observer) {
        _observers.erase(observer);
    }
}

void ObserverList::notify(engine_time_t time) {
    // Direct iteration - observers are for scheduling, no side effects
    for (auto* observer : _observers) {
        observer->notify(time);
    }
}

// ============================================================================
// ScalarTSOverlay Implementation
// ============================================================================

ScalarTSOverlay::ScalarTSOverlay(ScalarTSOverlay&& other) noexcept
    : TSOverlayStorage()
    , _last_modified_time(other._last_modified_time)
{
    // Move parent and observers from base class
    _parent = other._parent;
    _observers = std::move(other._observers);

    // Reset source
    other._parent = nullptr;
    other._last_modified_time = MIN_DT;
}

ScalarTSOverlay& ScalarTSOverlay::operator=(ScalarTSOverlay&& other) noexcept {
    if (this != &other) {
        // Move data
        _last_modified_time = other._last_modified_time;
        _parent = other._parent;
        _observers = std::move(other._observers);

        // Reset source
        other._parent = nullptr;
        other._last_modified_time = MIN_DT;
    }
    return *this;
}

void ScalarTSOverlay::mark_modified(engine_time_t time) {
    // Update local timestamp
    _last_modified_time = time;

    // Notify observers at this level
    if (_observers && _observers->has_observers()) {
        _observers->notify(time);
    }

    // Propagate to parent
    propagate_modified_to_parent(time);
}

void ScalarTSOverlay::mark_invalid() {
    // Reset timestamp to invalid state
    _last_modified_time = MIN_DT;

    // Notify observers of invalidation
    if (_observers && _observers->has_observers()) {
        _observers->notify(MIN_DT);
    }

    // Note: Invalidation does NOT propagate to parent
    // (parent validity is independent of child validity)
}

// ============================================================================
// CompositeTSOverlay Implementation
// ============================================================================

CompositeTSOverlay::CompositeTSOverlay(const TSMeta* ts_meta)
    : TSOverlayStorage()
    , _last_modified_time(MIN_DT)
{
    // Determine the number of children and create them
    if (!ts_meta) {
        return;
    }

    // For now, we support TSB (bundle) types
    // P2.T7 will extend this to support other composite types
    if (ts_meta->is_bundle()) {
        auto* bundle_meta = static_cast<const TSBTypeMeta*>(ts_meta);
        _bundle_meta = bundle_meta;

        // Create one child overlay per field
        size_t field_count = bundle_meta->field_count();
        _children.reserve(field_count);

        for (size_t i = 0; i < field_count; ++i) {
            const auto& field_info = bundle_meta->field(i);
            auto child = create_child_overlay(field_info.type);

            // Set parent relationship so child modifications propagate upward
            if (child) {
                child->set_parent(this);
            }

            _children.push_back(std::move(child));
        }
    }
    // Note: Other composite types (TSL with fixed size, etc.) will be
    // added in P2.T7 when the full factory is implemented
}

CompositeTSOverlay::CompositeTSOverlay(CompositeTSOverlay&& other) noexcept
    : TSOverlayStorage()
    , _last_modified_time(other._last_modified_time)
    , _children(std::move(other._children))
    , _bundle_meta(other._bundle_meta)
{
    // Move parent and observers from base class
    _parent = other._parent;
    _observers = std::move(other._observers);

    // Update parent pointers in all children to point to this instance
    for (auto& child : _children) {
        if (child) {
            child->set_parent(this);
        }
    }

    // Reset source
    other._parent = nullptr;
    other._last_modified_time = MIN_DT;
    other._bundle_meta = nullptr;
}

CompositeTSOverlay& CompositeTSOverlay::operator=(CompositeTSOverlay&& other) noexcept {
    if (this != &other) {
        // Move data
        _last_modified_time = other._last_modified_time;
        _children = std::move(other._children);
        _bundle_meta = other._bundle_meta;
        _parent = other._parent;
        _observers = std::move(other._observers);

        // Update parent pointers in all children to point to this instance
        for (auto& child : _children) {
            if (child) {
                child->set_parent(this);
            }
        }

        // Reset source
        other._parent = nullptr;
        other._last_modified_time = MIN_DT;
        other._bundle_meta = nullptr;
    }
    return *this;
}

void CompositeTSOverlay::mark_modified(engine_time_t time) {
    // Update local timestamp
    _last_modified_time = time;

    // Notify observers at this level
    if (_observers && _observers->has_observers()) {
        _observers->notify(time);
    }

    // Propagate to parent
    propagate_modified_to_parent(time);
}

void CompositeTSOverlay::mark_invalid() {
    // Reset timestamp to invalid state
    _last_modified_time = MIN_DT;

    // Notify observers of invalidation
    if (_observers && _observers->has_observers()) {
        _observers->notify(MIN_DT);
    }

    // Note: Invalidation does NOT propagate to parent or children
    // (each level's validity is independent)
}

TSOverlayStorage* CompositeTSOverlay::child(std::string_view name) noexcept {
    if (!_bundle_meta) {
        return nullptr;
    }

    // Look up field by name
    auto* field_info = _bundle_meta->field(std::string(name));
    if (!field_info) {
        return nullptr;
    }

    // Return the child at the corresponding index
    return child(field_info->index);
}

const TSOverlayStorage* CompositeTSOverlay::child(std::string_view name) const noexcept {
    if (!_bundle_meta) {
        return nullptr;
    }

    // Look up field by name
    auto* field_info = _bundle_meta->field(std::string(name));
    if (!field_info) {
        return nullptr;
    }

    // Return the child at the corresponding index
    return child(field_info->index);
}

std::unique_ptr<TSOverlayStorage> CompositeTSOverlay::create_child_overlay(const TSMeta* child_ts_meta) {
    // Use the unified factory function
    return make_ts_overlay(child_ts_meta);
}

std::vector<size_t> CompositeTSOverlay::modified_indices(engine_time_t time) const {
    std::vector<size_t> result;
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->last_modified_time() == time) {
            result.push_back(i);
        }
    }
    return result;
}

bool CompositeTSOverlay::has_modified(engine_time_t time) const {
    for (const auto& child : _children) {
        if (child && child->last_modified_time() == time) {
            return true;
        }
    }
    return false;
}

// ============================================================================
// ListTSOverlay Implementation
// ============================================================================

ListTSOverlay::ListTSOverlay(const TSMeta* ts_meta)
    : TSOverlayStorage()
    , _last_modified_time(MIN_DT)
{
    // Extract element type from TSLTypeMeta or TSWTypeMeta
    if (!ts_meta) {
        return;
    }

    if (ts_meta->kind() == TSTypeKind::TSL) {
        auto* list_meta = static_cast<const TSLTypeMeta*>(ts_meta);
        _element_type = list_meta->element_type();

        // For fixed-size TSL, pre-create child overlays
        // This ensures element(i).ts_valid() works correctly by checking
        // child overlay's last_modified_time instead of falling back to data validity
        if (list_meta->is_fixed_size()) {
            size_t fixed_size = list_meta->fixed_size();
            _children.reserve(fixed_size);
            for (size_t i = 0; i < fixed_size; ++i) {
                auto child = create_child_overlay();
                if (child) {
                    child->set_parent(this);
                }
                _children.push_back(std::move(child));
            }
        }
    } else if (ts_meta->kind() == TSTypeKind::TSW) {
        // Windows (TSW) behave like lists with cyclic buffer semantics
        // Each window slot tracks when a value was added (scalar tracking)
        // We use nullptr to signal that elements should be scalar overlays
        _element_type = nullptr;  // Signals: create ScalarTSOverlay for each element
    }
    // Note: For dynamic TSL, _children starts empty; elements are added via push_back() or resize()
}

ListTSOverlay::ListTSOverlay(ListTSOverlay&& other) noexcept
    : TSOverlayStorage()
    , _last_modified_time(other._last_modified_time)
    , _children(std::move(other._children))
    , _element_type(other._element_type)
{
    // Move parent and observers from base class
    _parent = other._parent;
    _observers = std::move(other._observers);

    // Update parent pointers in all children to point to this instance
    for (auto& child : _children) {
        if (child) {
            child->set_parent(this);
        }
    }

    // Reset source
    other._parent = nullptr;
    other._last_modified_time = MIN_DT;
    other._element_type = nullptr;
}

ListTSOverlay& ListTSOverlay::operator=(ListTSOverlay&& other) noexcept {
    if (this != &other) {
        // Move data
        _last_modified_time = other._last_modified_time;
        _children = std::move(other._children);
        _element_type = other._element_type;
        _parent = other._parent;
        _observers = std::move(other._observers);

        // Update parent pointers in all children to point to this instance
        for (auto& child : _children) {
            if (child) {
                child->set_parent(this);
            }
        }

        // Reset source
        other._parent = nullptr;
        other._last_modified_time = MIN_DT;
        other._element_type = nullptr;
    }
    return *this;
}

void ListTSOverlay::mark_modified(engine_time_t time) {
    // Update local timestamp
    _last_modified_time = time;

    // Notify observers at this level
    if (_observers && _observers->has_observers()) {
        _observers->notify(time);
    }

    // Propagate to parent
    propagate_modified_to_parent(time);
}

void ListTSOverlay::mark_invalid() {
    // Reset timestamp to invalid state
    _last_modified_time = MIN_DT;

    // Notify observers of invalidation
    if (_observers && _observers->has_observers()) {
        _observers->notify(MIN_DT);
    }

    // Note: Invalidation does NOT propagate to parent or children
    // (each level's validity is independent)
}

void ListTSOverlay::resize(size_t new_size) {
    size_t current_size = _children.size();

    if (new_size > current_size) {
        // Add new children
        _children.reserve(new_size);
        for (size_t i = current_size; i < new_size; ++i) {
            auto child = create_child_overlay();
            if (child) {
                child->set_parent(this);
            }
            _children.push_back(std::move(child));
        }
    } else if (new_size < current_size) {
        // Remove children from the end
        _children.resize(new_size);
    }
    // If new_size == current_size, no-op
}

TSOverlayStorage* ListTSOverlay::push_back() {
    auto child = create_child_overlay();
    TSOverlayStorage* result = child.get();

    if (child) {
        child->set_parent(this);
    }

    _children.push_back(std::move(child));
    return result;
}

void ListTSOverlay::pop_back() {
    if (!_children.empty()) {
        _children.pop_back();
    }
}

void ListTSOverlay::clear() {
    _children.clear();
}

std::vector<size_t> ListTSOverlay::modified_indices(engine_time_t time) const {
    std::vector<size_t> result;
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->last_modified_time() == time) {
            result.push_back(i);
        }
    }
    return result;
}

bool ListTSOverlay::has_modified(engine_time_t time) const {
    for (const auto& child : _children) {
        if (child && child->last_modified_time() == time) {
            return true;
        }
    }
    return false;
}

std::unique_ptr<TSOverlayStorage> ListTSOverlay::create_child_overlay() {
    // For TSW (window) types, _element_type is nullptr, so we create scalar overlays
    if (!_element_type) {
        return std::make_unique<ScalarTSOverlay>();
    }

    // For TSL types, use the unified factory function
    return make_ts_overlay(_element_type);
}

// ============================================================================
// SetTSOverlay Implementation
// ============================================================================

SetTSOverlay::SetTSOverlay(const TSMeta* ts_meta)
    : TSOverlayStorage()
    , _last_modified_time(MIN_DT)
{
    // Extract element type from TSSTypeMeta
    if (!ts_meta) {
        return;
    }

    // Store the ts_meta for future use
    _element_type = ts_meta;

    // Initialize hash sets for O(1) lookup if we have element type info
    if (ts_meta->kind() == TSTypeKind::TSS) {
        auto* tss_meta = static_cast<const TSSTypeMeta*>(ts_meta);
        const value::TypeMeta* element_type = tss_meta->element_type();
        if (element_type) {
            // Create set schema for this element type and initialize the lookup sets
            const value::TypeMeta* set_schema = value::TypeRegistry::instance().set(element_type).build();
            if (set_schema) {
                _added_values_set = value::PlainValue(set_schema);
                _removed_values_set = value::PlainValue(set_schema);
            }
        }
    }

    // Note: _added_indices and _removed_indices start empty
}

SetTSOverlay::SetTSOverlay(SetTSOverlay&& other) noexcept
    : TSOverlayStorage()
    , _last_modified_time(other._last_modified_time)
    , _added_indices(std::move(other._added_indices))
    , _removed_indices(std::move(other._removed_indices))
    , _removed_values(std::move(other._removed_values))
    , _added_values(std::move(other._added_values))
    , _added_values_set(std::move(other._added_values_set))
    , _removed_values_set(std::move(other._removed_values_set))
    , _element_type(other._element_type)
{
    // Move parent and observers from base class
    _parent = other._parent;
    _observers = std::move(other._observers);

    // Reset source
    other._parent = nullptr;
    other._last_modified_time = MIN_DT;
    other._element_type = nullptr;
}

SetTSOverlay& SetTSOverlay::operator=(SetTSOverlay&& other) noexcept {
    if (this != &other) {
        // Move data
        _last_modified_time = other._last_modified_time;
        _added_indices = std::move(other._added_indices);
        _removed_indices = std::move(other._removed_indices);
        _removed_values = std::move(other._removed_values);
        _added_values = std::move(other._added_values);
        _added_values_set = std::move(other._added_values_set);
        _removed_values_set = std::move(other._removed_values_set);
        _element_type = other._element_type;
        _parent = other._parent;
        _observers = std::move(other._observers);

        // Reset source
        other._parent = nullptr;
        other._last_modified_time = MIN_DT;
        other._element_type = nullptr;
    }
    return *this;
}

void SetTSOverlay::mark_modified(engine_time_t time) {
    // Update local timestamp
    _last_modified_time = time;

    // Notify observers at this level
    if (_observers && _observers->has_observers()) {
        _observers->notify(time);
    }

    // Propagate to parent
    propagate_modified_to_parent(time);
}

void SetTSOverlay::mark_invalid() {
    // Reset timestamp to invalid state
    _last_modified_time = MIN_DT;

    // Notify observers of invalidation
    if (_observers && _observers->has_observers()) {
        _observers->notify(MIN_DT);
    }

    // Note: Invalidation does NOT propagate to parent
    // (parent validity is independent of child validity)
}

void SetTSOverlay::hook_on_insert(void* ctx, size_t index) {
    // No-op: The caller is responsible for calling record_added() with the time
    (void)ctx;
    (void)index;
}

void SetTSOverlay::hook_on_swap(void* ctx, size_t index_a, size_t index_b) {
    auto* self = static_cast<SetTSOverlay*>(ctx);
    if (!self) return;

    // Update any indices in added/removed buffers that match the swapped positions
    auto update_index = [index_a, index_b](size_t& idx) {
        if (idx == index_a) {
            idx = index_b;
        } else if (idx == index_b) {
            idx = index_a;
        }
    };

    for (auto& idx : self->_added_indices) {
        update_index(idx);
    }
    for (auto& idx : self->_removed_indices) {
        update_index(idx);
    }
}

void SetTSOverlay::hook_on_erase(void* ctx, size_t index) {
    // No-op: The caller is responsible for calling record_removed() with the time
    (void)ctx;
    (void)index;
}

void SetTSOverlay::record_added(size_t index, engine_time_t time, value::PlainValue added_value) {
    // Lazy cleanup: reset buffers if time changed since last modification
    maybe_reset_delta(time);

    // Record the addition
    _added_indices.push_back(index);

    // Insert into hash set for O(1) lookup
    if (_added_values_set.valid()) {
        _added_values_set.view().as_set().insert(added_value.const_view());
    }

    // Buffer the added value
    _added_values.push_back(std::move(added_value));

    // Update container-level modification time
    mark_modified(time);
}

void SetTSOverlay::record_removed(size_t index, engine_time_t time, value::PlainValue removed_value) {
    // Lazy cleanup: reset buffers if time changed since last modification
    maybe_reset_delta(time);

    // Record the removal
    _removed_indices.push_back(index);

    // Insert into hash set for O(1) lookup
    if (_removed_values_set.valid()) {
        _removed_values_set.view().as_set().insert(removed_value.const_view());
    }

    // Buffer the removed value so it can be accessed until delta is cleared
    _removed_values.push_back(std::move(removed_value));

    // Update container-level modification time
    mark_modified(time);
}

bool SetTSOverlay::was_added_element(const value::ConstValueView& element) const {
    // Use hash set for O(1) lookup
    if (_added_values_set.valid()) {
        return _added_values_set.const_view().as_set().contains(element);
    }
    // Fallback to linear scan if set not initialized
    for (const auto& val : _added_values) {
        if (element == val.const_view()) {
            return true;
        }
    }
    return false;
}

bool SetTSOverlay::was_removed_element(const value::ConstValueView& element) const {
    // Use hash set for O(1) lookup
    if (_removed_values_set.valid()) {
        return _removed_values_set.const_view().as_set().contains(element);
    }
    // Fallback to linear scan if set not initialized
    for (const auto& val : _removed_values) {
        if (element == val.const_view()) {
            return true;
        }
    }
    return false;
}

// ============================================================================
// MapTSOverlay Implementation
// ============================================================================

MapTSOverlay::MapTSOverlay(const TSMeta* ts_meta)
    : TSOverlayStorage()
    , _last_modified_time(MIN_DT)
{
    // Extract value type from TSDTypeMeta
    if (ts_meta && ts_meta->kind() == TSTypeKind::TSD) {
        auto* tsd_meta = static_cast<const TSDTypeMeta*>(ts_meta);
        _value_type = tsd_meta->value_ts_type();
    }

    // Note: _value_overlays and buffers start empty; allocated on demand via hooks
}

MapTSOverlay::MapTSOverlay(MapTSOverlay&& other) noexcept
    : TSOverlayStorage()
    , _last_modified_time(other._last_modified_time)
    , _last_delta_time(other._last_delta_time)
    , _added_key_indices(std::move(other._added_key_indices))
    , _removed_key_indices(std::move(other._removed_key_indices))
    , _removed_key_values(std::move(other._removed_key_values))
    , _value_overlays(std::move(other._value_overlays))
    , _removed_value_overlays(std::move(other._removed_value_overlays))
    , _value_type(other._value_type)
{
    // Move parent and observers from base class
    _parent = other._parent;
    _observers = std::move(other._observers);

    // Update parent pointers in child overlays
    for (auto& overlay : _value_overlays) {
        if (overlay) {
            overlay->set_parent(this);
        }
    }

    // Reset source
    other._parent = nullptr;
    other._last_modified_time = MIN_DT;
    other._last_delta_time = MIN_DT;
    other._value_type = nullptr;
}

MapTSOverlay& MapTSOverlay::operator=(MapTSOverlay&& other) noexcept {
    if (this != &other) {
        // Move data
        _last_modified_time = other._last_modified_time;
        _last_delta_time = other._last_delta_time;
        _added_key_indices = std::move(other._added_key_indices);
        _removed_key_indices = std::move(other._removed_key_indices);
        _removed_key_values = std::move(other._removed_key_values);
        _value_overlays = std::move(other._value_overlays);
        _removed_value_overlays = std::move(other._removed_value_overlays);
        _value_type = other._value_type;
        _parent = other._parent;
        _observers = std::move(other._observers);

        // Update parent pointers in child overlays
        for (auto& overlay : _value_overlays) {
            if (overlay) {
                overlay->set_parent(this);
            }
        }

        // Reset source
        other._parent = nullptr;
        other._last_modified_time = MIN_DT;
        other._last_delta_time = MIN_DT;
        other._value_type = nullptr;
    }
    return *this;
}

void MapTSOverlay::mark_modified(engine_time_t time) {
    // Update local timestamp
    _last_modified_time = time;

    // Notify observers at this level
    if (_observers && _observers->has_observers()) {
        _observers->notify(time);
    }

    // Propagate to parent
    propagate_modified_to_parent(time);
}

void MapTSOverlay::mark_invalid() {
    // Reset timestamp to invalid state
    _last_modified_time = MIN_DT;

    // Notify observers of invalidation
    if (_observers && _observers->has_observers()) {
        _observers->notify(MIN_DT);
    }

    // Note: Invalidation does NOT propagate to parent
    // (parent validity is independent of child validity)
}

void MapTSOverlay::record_key_added(size_t index, engine_time_t time) {
    // Lazy cleanup: reset buffers if time changed since last modification
    maybe_reset_delta(time);

    // Record in added buffer
    _added_key_indices.push_back(index);

    // Ensure value overlay exists
    ensure_value_overlay(index);

    // Update container-level modification time
    mark_modified(time);
}

void MapTSOverlay::record_key_removed(size_t index, engine_time_t time, value::PlainValue removed_key) {
    // Lazy cleanup: reset buffers if time changed since last modification
    maybe_reset_delta(time);

    // Record in removed buffer
    _removed_key_indices.push_back(index);

    // Buffer the removed key value
    _removed_key_values.push_back(std::move(removed_key));

    // Buffer the value overlay so it can still be accessed until delta is cleared
    if (index < _value_overlays.size() && _value_overlays[index]) {
        _removed_value_overlays.push_back(std::move(_value_overlays[index]));
    }

    // Update container-level modification time
    mark_modified(time);
}

TSOverlayStorage* MapTSOverlay::ensure_value_overlay(size_t index) {
    // Ensure capacity
    if (index >= _value_overlays.size()) {
        _value_overlays.resize(index + 1);
    }

    // Create child overlay if it doesn't exist
    if (!_value_overlays[index]) {
        _value_overlays[index] = create_value_overlay();
        if (_value_overlays[index]) {
            _value_overlays[index]->set_parent(this);
        }
    }

    return _value_overlays[index].get();
}

std::vector<size_t> MapTSOverlay::modified_key_indices(engine_time_t time) const {
    std::vector<size_t> result;

    // Check each value overlay for modification at this time
    for (size_t i = 0; i < _value_overlays.size(); ++i) {
        if (!_value_overlays[i]) continue;

        // Check if this value was modified at the current time
        if (_value_overlays[i]->last_modified_time() == time) {
            // Exclude if this key was added this tick (not a modification of existing key)
            bool is_added = std::find(_added_key_indices.begin(), _added_key_indices.end(), i)
                            != _added_key_indices.end();
            if (!is_added) {
                result.push_back(i);
            }
        }
    }

    return result;
}

bool MapTSOverlay::has_modified_keys(engine_time_t time) const {
    // Check each value overlay for modification at this time
    for (size_t i = 0; i < _value_overlays.size(); ++i) {
        if (!_value_overlays[i]) continue;

        // Check if this value was modified at the current time
        if (_value_overlays[i]->last_modified_time() == time) {
            // Exclude if this key was added this tick
            bool is_added = std::find(_added_key_indices.begin(), _added_key_indices.end(), i)
                            != _added_key_indices.end();
            if (!is_added) {
                return true;
            }
        }
    }
    return false;
}

std::unique_ptr<TSOverlayStorage> MapTSOverlay::create_value_overlay() {
    if (_value_type) {
        return make_ts_overlay(_value_type);
    }
    // Fallback to scalar overlay if no value type info
    return std::make_unique<ScalarTSOverlay>();
}

void MapTSOverlay::hook_on_insert(void* ctx, size_t index) {
    // No-op: The caller is responsible for calling record_key_added() with the time
    (void)ctx;
    (void)index;
}

void MapTSOverlay::hook_on_swap(void* ctx, size_t index_a, size_t index_b) {
    auto* self = static_cast<MapTSOverlay*>(ctx);
    if (!self) return;

    // Update any indices in added/removed buffers that match the swapped positions
    auto update_index = [index_a, index_b](size_t& idx) {
        if (idx == index_a) {
            idx = index_b;
        } else if (idx == index_b) {
            idx = index_a;
        }
    };

    for (auto& idx : self->_added_key_indices) {
        update_index(idx);
    }
    for (auto& idx : self->_removed_key_indices) {
        update_index(idx);
    }

    // Swap the value overlays
    size_t max_idx = std::max(index_a, index_b);
    if (max_idx >= self->_value_overlays.size()) {
        self->_value_overlays.resize(max_idx + 1);
    }
    std::swap(self->_value_overlays[index_a], self->_value_overlays[index_b]);
}

void MapTSOverlay::hook_on_erase(void* ctx, size_t index) {
    // No-op: The caller is responsible for calling record_key_removed() with the time
    // before the erase. That function handles moving the value overlay to the removed buffer.
    (void)ctx;
    (void)index;
}

KeySetOverlayView MapTSOverlay::key_set_view() noexcept {
    return KeySetOverlayView(this);
}

void MapTSOverlay::update_is_empty_state(engine_time_t time, size_t current_size) {
    bool new_is_empty = (current_size == 0);

    // Check if the is_empty state actually changed
    if (new_is_empty != _is_empty_value) {
        _is_empty_value = new_is_empty;
        // Mark the is_empty overlay as modified - this will notify any observers
        _is_empty_overlay.mark_modified(time);
    }
}

// ============================================================================
// Factory Function Implementation
// ============================================================================

std::unique_ptr<TSOverlayStorage> make_ts_overlay(const TSMeta* ts_meta) {
    // Handle nullptr gracefully
    if (!ts_meta) {
        return nullptr;
    }

    // Create the appropriate overlay type based on TSMeta kind
    switch (ts_meta->kind()) {
        case TSTypeKind::TS:
            // Scalar time-series: TS[T]
            return std::make_unique<ScalarTSOverlay>();

        case TSTypeKind::TSB:
            // Bundle: TSB[fields...]
            // CompositeTSOverlay constructor handles recursive child creation
            return std::make_unique<CompositeTSOverlay>(ts_meta);

        case TSTypeKind::TSL:
            // List: TSL[TS[T], Size]
            // ListTSOverlay starts empty; elements added via push_back/resize
            return std::make_unique<ListTSOverlay>(ts_meta);

        case TSTypeKind::TSS:
            // Set: TSS[T]
            // Per-element timestamp tracking with backing store alignment
            return std::make_unique<SetTSOverlay>(ts_meta);

        case TSTypeKind::TSD:
            // Map/Dict: TSD[K, TS[V]]
            // Per-entry timestamp tracking (key_added + value_modified)
            return std::make_unique<MapTSOverlay>(ts_meta);

        case TSTypeKind::TSW:
            // Window: TSW[T, Size, MinSize]
            // Windows behave like lists with cyclic buffer semantics
            return std::make_unique<ListTSOverlay>(ts_meta);

        case TSTypeKind::REF:
            // Reference: REF[TS[T]]
            // References behave like scalars at the overlay level
            return std::make_unique<ScalarTSOverlay>();

        case TSTypeKind::SIGNAL:
            // Signal (tick with no value)
            // Signals track modification time like scalars
            return std::make_unique<ScalarTSOverlay>();

        default:
            // Unknown type - return nullptr as safe fallback
            return nullptr;
    }
}

}  // namespace hgraph
