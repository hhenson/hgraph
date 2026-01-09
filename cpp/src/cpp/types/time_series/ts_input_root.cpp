//
// ts_input_root.cpp - TSInputRoot implementation
//

#include <hgraph/types/time_series/ts_input_root.h>
#include <stdexcept>

namespace hgraph {

// ============================================================================
// Construction
// ============================================================================

TSInputRoot::TSInputRoot(const TSBTypeMeta* meta, Node* node)
    : _value(meta, node)
    , _node(node)
    , _active(false)
{
    _value.enable_link_support();
}

TSInputRoot::TSInputRoot(const TSMeta* meta, Node* node)
    : _node(node)
    , _active(false)
{
    if (!meta || meta->kind() != TSTypeKind::TSB) {
        throw std::runtime_error("TSInputRoot: meta must be a bundle type (TSB)");
    }
    _value = TSValue(meta, node);
    _value.enable_link_support();
}

// ============================================================================
// Validity
// ============================================================================

bool TSInputRoot::valid() const noexcept {
    return _value.valid() && _value.ts_meta() &&
           _value.ts_meta()->kind() == TSTypeKind::TSB;
}

// ============================================================================
// Navigation
// ============================================================================

TSView TSInputRoot::field(size_t index) const {
    return bundle_view().field(index);
}

TSView TSInputRoot::field(const std::string& name) const {
    return bundle_view().field(name);
}

size_t TSInputRoot::size() const noexcept {
    if (!valid()) return 0;
    return bundle_meta()->field_count();
}

const TSBTypeMeta* TSInputRoot::bundle_meta() const noexcept {
    if (!valid()) return nullptr;
    return static_cast<const TSBTypeMeta*>(_value.ts_meta());
}

TSBView TSInputRoot::bundle_view() const {
    if (!valid()) {
        throw std::runtime_error("TSInputRoot::bundle_view() called on invalid input");
    }

    // Get the base view
    TSView base_view = _value.view();

    // Set the link source for transparent navigation
    base_view.set_link_source(&_value);

    // Convert to bundle view (link source is propagated)
    return base_view.as_bundle();
}

// ============================================================================
// Binding
// ============================================================================

void TSInputRoot::bind_field(size_t index, const TSValue* output) {
    if (!valid()) {
        throw std::runtime_error("TSInputRoot::bind_field() called on invalid input");
    }

    if (index >= size()) {
        throw std::out_of_range("TSInputRoot::bind_field: index out of bounds");
    }

    _value.create_link(index, output);

    // If active, the link should auto-subscribe (handled in create_link via TSLink)
    if (_active) {
        TSLink* link = _value.link_at(index);
        if (link) {
            link->make_active();
        }
    }
}

void TSInputRoot::bind_field(const std::string& name, const TSValue* output) {
    bind_field(field_index(name), output);
}

void TSInputRoot::unbind_field(size_t index) {
    if (!valid()) {
        return;  // Silently ignore
    }

    _value.remove_link(index);
}

void TSInputRoot::unbind_field(const std::string& name) {
    if (!valid()) {
        return;
    }

    auto* meta = bundle_meta();
    const TSBFieldInfo* info = meta->field(name);
    if (info) {
        unbind_field(info->index);
    }
}

bool TSInputRoot::is_field_bound(size_t index) const noexcept {
    if (!valid() || index >= size()) {
        return false;
    }
    return _value.is_linked(index);
}

bool TSInputRoot::is_field_bound(const std::string& name) const noexcept {
    if (!valid()) {
        return false;
    }

    auto* meta = bundle_meta();
    const TSBFieldInfo* info = meta->field(name);
    if (!info) {
        return false;
    }
    return is_field_bound(info->index);
}

// ============================================================================
// Active Control
// ============================================================================

void TSInputRoot::make_active() {
    if (!_active) {
        _active = true;
        _value.make_links_active();
    }
}

void TSInputRoot::make_passive() {
    if (_active) {
        _active = false;
        _value.make_links_passive();
    }
}

// ============================================================================
// State Queries
// ============================================================================

bool TSInputRoot::modified_at(engine_time_t time) const {
    if (!valid()) {
        return false;
    }

    // Check all linked fields
    for (size_t i = 0; i < _value.child_count(); ++i) {
        if (auto* link = _value.link_at(i)) {
            if (link->modified_at(time)) {
                return true;
            }
        }
        // TODO: Check non-linked children with nested links recursively
    }

    return false;
}

bool TSInputRoot::all_valid() const {
    if (!valid()) {
        return false;
    }

    // Check all linked fields
    for (size_t i = 0; i < _value.child_count(); ++i) {
        if (auto* link = _value.link_at(i)) {
            if (!link->valid()) {
                return false;
            }
        }
        // TODO: Check non-linked children with nested links recursively
    }

    return true;
}

engine_time_t TSInputRoot::last_modified_time() const {
    if (!valid()) {
        return MIN_DT;
    }

    engine_time_t latest = MIN_DT;

    // Find the most recent modification time
    for (size_t i = 0; i < _value.child_count(); ++i) {
        if (auto* link = _value.link_at(i)) {
            engine_time_t link_time = link->last_modified_time();
            if (link_time > latest) {
                latest = link_time;
            }
        }
        // TODO: Check non-linked children with nested links recursively
    }

    return latest;
}

// ============================================================================
// Private Helpers
// ============================================================================

size_t TSInputRoot::field_index(const std::string& name) const {
    if (!valid()) {
        throw std::runtime_error("TSInputRoot::field_index() called on invalid input");
    }

    auto* meta = bundle_meta();
    const TSBFieldInfo* info = meta->field(name);
    if (!info) {
        throw std::runtime_error("TSInputRoot: field '" + name + "' not found");
    }
    return info->index;
}

}  // namespace hgraph
