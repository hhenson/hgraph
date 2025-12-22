//
// Created by Claude on 16/12/2025.
//
// TSInput implementation - Value-based time-series input with hierarchical access strategies
//

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/node.h>
#include <fmt/format.h>

namespace hgraph::ts {

// ============================================================================
// TSInput implementation
// ============================================================================

void TSInput::bind_output(value::TSView output_view) {
    if (!output_view.valid()) {
        unbind_output();
        return;
    }

    // First, unbind any existing binding
    if (bound()) {
        bool was_active = _active;
        unbind_output();
        _active = was_active;  // Preserve activation state
    }

    // Get output metadata from the view
    auto* output_meta = output_view.ts_meta();
    _strategy = build_access_strategy(_meta, output_meta, this);

    // Bind the strategy to the output view
    if (_strategy) {
        _strategy->bind(output_view);

        // If active, activate the strategy
        if (_active) {
            _strategy->make_active();
        }
    }
}

void TSInput::unbind_output() {
    bool was_active = _active;

    if (_strategy) {
        _strategy->unbind();
        _strategy.reset();
    }

    _active = was_active;  // Preserve the activation request (will apply on next bind)
}

void TSInput::make_active() {
    if (_active) return;
    _active = true;

    if (_strategy) {
        _strategy->make_active();
    }
}

void TSInput::make_passive() {
    if (!_active) return;

    if (_strategy) {
        _strategy->make_passive();
    }
    _active = false;
}

void TSInput::notify(engine_time_t time) {

    // Let strategy handle the notification first (e.g., detect reference changes)
    if (_strategy) {
        _strategy->on_notify(time);
    }

    // Propagate notification to owning node if active
    if (_active && _owning_node) {
        _owning_node->notify(time);
    }
}

TSInputView TSInput::view() const {
    if (!bound()) {
        return {};
    }

    // Return a view pointing to the strategy - never materialized
    // The view will fetch fresh data from the strategy on each access
    return TSInputView(_strategy.get(), _meta);
}

// ============================================================================
// TSInputView navigation implementation
// ============================================================================

TSInputView TSInputView::element(size_t index) const {
    if (!valid()) {
        return {};
    }

    // Check if source is a CollectionAccessStrategy with a child at this index
    if (auto* collection = dynamic_cast<CollectionAccessStrategy*>(_source)) {
        if (auto* child = collection->child(index)) {
            // Get appropriate metadata based on type
            const TSMeta* child_meta = nullptr;
            if (_meta) {
                if (_meta->ts_kind == TSKind::TSB) {
                    // For bundles, use field_meta at index
                    child_meta = _meta->field_meta(index);
                } else {
                    // For lists and other collections, use element_meta
                    child_meta = _meta->element_meta();
                }
            }
            // Child strategy is at root of its own context - path should be empty
            // The child strategy already knows how to access its bound output
            return TSInputView(child, child_meta);
        }
    }

    // Peered mode: For TSB/TSL inputs bound directly to an output, we can still
    // navigate by storing the field index in the path and using the parent strategy
    // to access the bound output's fields when fetching values
    if (_meta && (_meta->ts_kind == TSKind::TSB || _meta->ts_kind == TSKind::TSL)) {
        const TSMeta* child_meta = nullptr;
        if (_meta->ts_kind == TSKind::TSB) {
            child_meta = _meta->field_meta(index);
        } else {
            child_meta = _meta->element_meta();
        }

        // Create a view with the same source but with updated path
        // The source strategy's value()/has_value() methods will navigate based on the path
        return TSInputView(_source, child_meta, _path.with(index));
    }

    // No child strategy - return invalid view
    return {};
}

value::ConstValueView TSInputView::value_view() const {
    if (!_source) {
        return {};
    }

    // If no path, return the source's value directly
    if (_path.empty()) {
        return _source->value();
    }

    // Navigate through the bound output's view using the path
    auto* bound = _source->bound_output();
    if (!bound) {
        return {};
    }

    // Get the metadata from the bound output to determine navigation type at each level
    auto output_view = bound->view();
    auto current_ts_kind = output_view.ts_kind();

    for (size_t i = 0; i < _path.depth(); ++i) {
        // Determine if this is a list element or bundle field
        if (current_ts_kind == TSKind::TSL) {
            output_view = output_view.element(_path[i]);
        } else {
            output_view = output_view.field(_path[i]);
        }
        if (!output_view.valid()) return {};
        current_ts_kind = output_view.ts_kind();
    }

    return output_view.value_view();  // Returns ConstValueView
}

bool TSInputView::has_value() const {
    if (!_source) return false;

    // If no path, check the source's has_value directly
    if (_path.empty()) {
        return _source->has_value();
    }

    // Navigate through the bound output's view using the path
    auto* bound = _source->bound_output();
    if (!bound) return false;

    auto output_view = bound->view();

    // First check if the root output has a value
    // If the root doesn't have a value, no elements can have values
    if (!output_view.has_value()) {
        return false;
    }

    auto current_ts_kind = output_view.ts_kind();

    for (size_t i = 0; i < _path.depth(); ++i) {
        // Determine if this is a list element or bundle field
        if (current_ts_kind == TSKind::TSL) {
            output_view = output_view.element(_path[i]);
        } else {
            output_view = output_view.field(_path[i]);
        }
        if (!output_view.valid()) return false;
        current_ts_kind = output_view.ts_kind();
    }

    // After navigating to the element, check its validity
    // For simple TS elements within a list/bundle, the element may not have its own
    // tracker set up. In this case, if we successfully navigated here and the parent
    // had a value, we consider the element valid (it will at least have a default value).
    // For scalar elements, check if tracker is set up - if not, trust parent validity
    auto elem_has_value = output_view.has_value();
    if (!elem_has_value && current_ts_kind == TSKind::TS) {
        // For TS elements without their own tracker, if we got here the element is valid
        // because the parent list/bundle was valid
        return true;
    }

    return elem_has_value;
}

TSInputView TSInputView::field(const std::string& name) const {
    if (!valid() || !_meta || _meta->ts_kind != TSKind::TSB) {
        return {};
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field index by name and use element() for navigation
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            return element(i);
        }
    }

    return {};
}

size_t TSInput::field_count_from_meta() const {
    if (!_meta || _meta->ts_kind != TSKind::TSB) {
        return 0;
    }
    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
    return tsb_meta->fields.size();
}

TSInputBindableView TSInput::element(size_t index) const {
    if (!_meta) {
        return {};  // Invalid view
    }

    // Get child metadata based on type
    const TSMeta* child_meta = nullptr;

    if (_meta->ts_kind == TSKind::TSB) {
        // For bundles, get field metadata at index
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (index >= tsb_meta->fields.size()) {
            return {};  // Index out of range
        }
        child_meta = tsb_meta->fields[index].type;
    } else if (_meta->ts_kind == TSKind::TSL) {
        // For lists, get element metadata
        child_meta = _meta->element_meta();
    } else {
        return {};  // Not a navigable type
    }

    return TSInputBindableView(const_cast<TSInput*>(this), {index}, child_meta);
}

TSInputBindableView TSInput::field(const std::string& name) const {
    if (!is_bundle()) {
        return {};  // Invalid view - only bundles support name-based lookup
    }
    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field by name and use element() for navigation
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            return element(i);
        }
    }

    return {};  // Field not found
}

CollectionAccessStrategy* TSInput::ensure_collection_strategy() {
    if (!_meta) {
        throw std::runtime_error("ensure_collection_strategy called on input with no metadata");
    }

    // Determine capacity based on type
    size_t capacity = 0;
    if (_meta->ts_kind == TSKind::TSB) {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        capacity = tsb_meta->fields.size();
    } else if (_meta->ts_kind == TSKind::TSL) {
        // TSL has dynamic size - start with reasonable capacity
        // The strategy will grow as needed
        capacity = 8;  // Initial capacity, can be expanded
    } else {
        throw std::runtime_error(
            fmt::format("ensure_collection_strategy called on non-collection input (kind={})",
                        static_cast<int>(_meta->ts_kind)));
    }

    if (!_strategy) {
        // Create a new CollectionAccessStrategy at root
        auto new_strategy = std::make_unique<CollectionAccessStrategy>(this, capacity);
        auto* result = new_strategy.get();
        _strategy = std::move(new_strategy);
        return result;
    }

    // Check if existing strategy is a CollectionAccessStrategy
    auto* collection = dynamic_cast<CollectionAccessStrategy*>(_strategy.get());
    if (!collection) {
        throw std::runtime_error("Cannot get collection strategy: input already has non-collection strategy");
    }
    return collection;
}

CollectionAccessStrategy* TSInput::ensure_collection_strategy_at_path(const std::vector<size_t>& path) {
    if (path.empty()) {
        // Empty path means root level - ensure root is a collection
        return ensure_collection_strategy();
    }

    // Start from root collection strategy
    CollectionAccessStrategy* current = ensure_collection_strategy();
    const TSMeta* current_meta = _meta;

    // Walk the path, creating CollectionAccessStrategies as needed
    // We return the CollectionAccessStrategy AT the end of the path (not the parent)
    for (size_t depth = 0; depth < path.size(); ++depth) {
        size_t index = path[depth];

        // Get child metadata based on current type
        const TSMeta* child_meta = nullptr;

        if (current_meta->ts_kind == TSKind::TSB) {
            // For bundles, get field metadata at index
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(current_meta);
            if (index >= tsb_meta->fields.size()) {
                throw std::runtime_error(
                    fmt::format("Path navigation error at depth {}: index {} out of range (max {})",
                                depth, index, tsb_meta->fields.size() - 1));
            }
            child_meta = tsb_meta->fields[index].type;
        } else if (current_meta->ts_kind == TSKind::TSL) {
            // For lists, all elements have the same type
            child_meta = current_meta->element_meta();
        } else {
            throw std::runtime_error(
                fmt::format("Path navigation error at depth {}: expected bundle or list type, got kind {}",
                            depth, static_cast<int>(current_meta->ts_kind)));
        }

        // Get or create child strategy at this index
        AccessStrategy* child = current->child(index);

        if (!child) {
            // Create a CollectionAccessStrategy for this child (if it's a bundle or list)
            if (!child_meta ||
                (child_meta->ts_kind != TSKind::TSB && child_meta->ts_kind != TSKind::TSL)) {
                throw std::runtime_error(
                    fmt::format("Path navigation error at depth {}: expected bundle or list type, got kind {}",
                                depth, child_meta ? static_cast<int>(child_meta->ts_kind) : -1));
            }

            size_t child_capacity = 0;
            if (child_meta->ts_kind == TSKind::TSB) {
                auto* child_tsb = static_cast<const TSBTypeMeta*>(child_meta);
                child_capacity = child_tsb->fields.size();
            } else {
                child_capacity = 8;  // Initial capacity for list
            }

            auto new_child = std::make_unique<CollectionAccessStrategy>(this, child_capacity);
            child = new_child.get();
            current->set_child(index, std::move(new_child));
        }

        // Move to next level (including for the last element)
        auto* child_collection = dynamic_cast<CollectionAccessStrategy*>(child);
        if (!child_collection) {
            throw std::runtime_error(
                fmt::format("Path navigation error at depth {}: child is not a collection strategy", depth));
        }
        current = child_collection;
        current_meta = child_meta;
    }

    return current;
}

// ============================================================================
// TSInputBindableView implementation
// ============================================================================

TSInputBindableView::TSInputBindableView(TSInput* root)
    : _root(root)
    , _meta(root ? root->meta() : nullptr) {}

TSInputBindableView::TSInputBindableView(TSInput* root, std::vector<size_t> path, const TSMeta* meta)
    : _root(root)
    , _path(std::move(path))
    , _meta(meta) {}

TSInputBindableView TSInputBindableView::element(size_t index) const {
    if (!valid() || !_meta) {
        return {};
    }

    // Get child metadata based on type
    const TSMeta* child_meta = nullptr;

    if (_meta->ts_kind == TSKind::TSB) {
        // For bundles, get field metadata at index
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (index >= tsb_meta->fields.size()) {
            return {};  // Index out of range
        }
        child_meta = tsb_meta->fields[index].type;
    } else if (_meta->ts_kind == TSKind::TSL) {
        // For lists, get element metadata
        child_meta = _meta->element_meta();
    } else {
        return {};  // Not a navigable type
    }

    // Build new path
    std::vector<size_t> new_path = _path;
    new_path.push_back(index);

    return TSInputBindableView(_root, std::move(new_path), child_meta);
}

TSInputBindableView TSInputBindableView::field(const std::string& name) const {
    if (!valid()) {
        return {};
    }

    if (!_meta || _meta->ts_kind != TSKind::TSB) {
        return {};  // Not a bundle type - only bundles support name lookup
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field by name and use element() for navigation
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            return element(i);
        }
    }

    return {};  // Field not found
}

void TSInputBindableView::bind(value::TSView output_view) {
    if (!valid()) {
        throw std::runtime_error("TSInputBindableView::bind() called on invalid view");
    }

    if (_path.empty()) {
        // Binding at root - just use normal bind_output
        _root->bind_output(output_view);
        return;
    }

    // Get the parent's path (all but last element)
    std::vector<size_t> parent_path(_path.begin(), _path.end() - 1);
    size_t final_index = _path.back();

    // Ensure we have the collection strategy at the parent path
    CollectionAccessStrategy* parent_strategy = _root->ensure_collection_strategy_at_path(parent_path);

    // Build a child strategy for binding to the output view
    const TSMeta* output_meta = output_view.valid() ? output_view.ts_meta() : nullptr;
    auto child_strategy = build_access_strategy(_meta, output_meta, _root);

    if (output_view.valid()) {
        child_strategy->bind(output_view);
    }

    // If input is active, activate the new child
    if (_root->active() && child_strategy) {
        child_strategy->make_active();
    }

    // Set the child strategy in the parent
    parent_strategy->set_child(final_index, std::move(child_strategy));
}

} // namespace hgraph::ts
