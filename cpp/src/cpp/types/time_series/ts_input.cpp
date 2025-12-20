//
// Created by Claude on 16/12/2025.
//
// TSInput implementation - Value-based time-series input with hierarchical access strategies
//

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/node.h>
#include <fmt/format.h>

namespace hgraph::ts {

// ============================================================================
// TSInput implementation
// ============================================================================

void TSInput::bind_output(TSOutput* output) {
    if (!output) {
        unbind_output();
        return;
    }

    // First, unbind any existing binding
    if (bound()) {
        bool was_active = _active;
        unbind_output();
        _active = was_active;  // Preserve activation state
    }

    // Build strategy tree based on schema comparison
    auto* output_meta = output->meta();
    _strategy = build_access_strategy(_meta, output_meta, this);

    // Bind the strategy to the output
    if (_strategy) {
        _strategy->bind(output);

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

TSInputView TSInputView::field(size_t index) const {
    if (!valid()) {
        return {};
    }

    // Check if source is a CollectionAccessStrategy with a child at this index
    if (auto* collection = dynamic_cast<CollectionAccessStrategy*>(_source)) {
        if (auto* child = collection->child(index)) {
            auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
            return TSInputView(child, field_meta, _path.with(index));
        }
    }

    // No child strategy - return invalid view
    return {};
}

TSInputView TSInputView::field(const std::string& name) const {
    if (!valid() || !_meta || _meta->ts_kind != TSKind::TSB) {
        return {};
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field index by name
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            return field(i);
        }
    }

    return {};
}

TSInputView TSInputView::element(size_t index) const {
    if (!valid()) {
        return {};
    }

    // Check if source is a CollectionAccessStrategy with a child at this index
    if (auto* collection = dynamic_cast<CollectionAccessStrategy*>(_source)) {
        if (auto* child = collection->child(index)) {
            auto elem_meta = _meta ? _meta->element_meta() : nullptr;
            return TSInputView(child, elem_meta, _path.with(index));
        }
    }

    // No child strategy - return invalid view
    return {};
}

size_t TSInput::field_count_from_meta() const {
    if (!_meta || _meta->ts_kind != TSKind::TSB) {
        return 0;
    }
    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
    return tsb_meta->fields.size();
}

TSInputBindableView TSInput::field(size_t index) const {
    if (!is_bundle()) {
        return {};  // Invalid view
    }
    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
    if (index >= tsb_meta->fields.size()) {
        return {};  // Invalid view
    }

    const TSMeta* field_meta = tsb_meta->fields[index].type;
    return TSInputBindableView(const_cast<TSInput*>(this), {index}, field_meta);
}

TSInputBindableView TSInput::field(const std::string& name) const {
    if (!is_bundle()) {
        return {};  // Invalid view
    }
    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field by name
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            const TSMeta* field_meta = tsb_meta->fields[i].type;
            return TSInputBindableView(const_cast<TSInput*>(this), {i}, field_meta);
        }
    }

    return {};  // Field not found
}

CollectionAccessStrategy* TSInput::ensure_collection_strategy() {
    if (!is_bundle()) {
        throw std::runtime_error("ensure_collection_strategy called on non-bundle input");
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    if (!_strategy) {
        // Create a new CollectionAccessStrategy at root
        auto new_strategy = std::make_unique<CollectionAccessStrategy>(this, tsb_meta->fields.size());
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
    for (size_t depth = 0; depth < path.size(); ++depth) {
        size_t index = path[depth];

        // Get metadata for this level
        if (!current_meta || current_meta->ts_kind != TSKind::TSB) {
            throw std::runtime_error(
                fmt::format("Path navigation error at depth {}: expected bundle type", depth));
        }
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(current_meta);
        if (index >= tsb_meta->fields.size()) {
            throw std::runtime_error(
                fmt::format("Path navigation error at depth {}: index {} out of range (max {})",
                            depth, index, tsb_meta->fields.size() - 1));
        }

        const TSMeta* field_meta = tsb_meta->fields[index].type;

        // Get or create child strategy at this index
        AccessStrategy* child = current->child(index);

        if (!child) {
            // Create a CollectionAccessStrategy for this field (if it's a bundle)
            if (depth < path.size() - 1) {
                // Not the last element - need a collection for further navigation
                if (!field_meta || field_meta->ts_kind != TSKind::TSB) {
                    throw std::runtime_error(
                        fmt::format("Path navigation error at depth {}: expected bundle type for nested navigation",
                                    depth));
                }
                auto* field_tsb = static_cast<const TSBTypeMeta*>(field_meta);
                auto new_child = std::make_unique<CollectionAccessStrategy>(this, field_tsb->fields.size());
                child = new_child.get();
                current->set_child(index, std::move(new_child));
            } else {
                // Last element - caller will set the final child strategy
                // Return current and let caller handle the final step
            }
        }

        if (depth < path.size() - 1) {
            // Move to next level
            auto* child_collection = dynamic_cast<CollectionAccessStrategy*>(child);
            if (!child_collection) {
                throw std::runtime_error(
                    fmt::format("Path navigation error at depth {}: child is not a collection strategy", depth));
            }
            current = child_collection;
            current_meta = field_meta;
        }
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

TSInputBindableView TSInputBindableView::field(size_t index) const {
    if (!valid()) {
        return {};
    }

    if (!_meta || _meta->ts_kind != TSKind::TSB) {
        return {};  // Not a bundle type
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
    if (index >= tsb_meta->fields.size()) {
        return {};  // Index out of range
    }

    // Build new path
    std::vector<size_t> new_path = _path;
    new_path.push_back(index);

    return TSInputBindableView(_root, std::move(new_path), tsb_meta->fields[index].type);
}

TSInputBindableView TSInputBindableView::field(const std::string& name) const {
    if (!valid()) {
        return {};
    }

    if (!_meta || _meta->ts_kind != TSKind::TSB) {
        return {};  // Not a bundle type
    }

    auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);

    // Find field by name
    for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
        if (tsb_meta->fields[i].name == name) {
            std::vector<size_t> new_path = _path;
            new_path.push_back(i);
            return TSInputBindableView(_root, std::move(new_path), tsb_meta->fields[i].type);
        }
    }

    return {};  // Field not found
}

void TSInputBindableView::bind(TSOutput* output) {
    if (!valid()) {
        throw std::runtime_error("TSInputBindableView::bind() called on invalid view");
    }

    if (_path.empty()) {
        // Binding at root - just use normal bind_output
        _root->bind_output(output);
        return;
    }

    // Get the parent's path (all but last element)
    std::vector<size_t> parent_path(_path.begin(), _path.end() - 1);
    size_t final_index = _path.back();

    // Ensure we have the collection strategy at the parent path
    CollectionAccessStrategy* parent_strategy = _root->ensure_collection_strategy_at_path(parent_path);

    // Build a child strategy for binding to the output
    const TSMeta* output_meta = output ? output->meta() : nullptr;
    auto child_strategy = build_access_strategy(_meta, output_meta, _root);

    if (output) {
        child_strategy->bind(output);
    }

    // If input is active, activate the new child
    if (_root->active() && child_strategy) {
        child_strategy->make_active();
    }

    // Set the child strategy in the parent
    parent_strategy->set_child(final_index, std::move(child_strategy));
}

} // namespace hgraph::ts
