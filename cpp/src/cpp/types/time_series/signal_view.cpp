/**
 * @file signal_view.cpp
 * @brief SignalView implementation.
 */

#include <hgraph/types/time_series/signal_view.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/observer_list.h>

#include <algorithm>
#include <stdexcept>

namespace hgraph {

// Static invalid signal for returning from const accessors
const SignalView SignalView::invalid_signal_{};

// ============================================================================
// Construction
// ============================================================================

SignalView::SignalView(ViewData view_data, engine_time_t current_time) noexcept
    : view_data_(std::move(view_data))
    , source_meta_(view_data_.meta)
    , current_time_(current_time)
{}

SignalView::SignalView(const TSView& source, engine_time_t current_time)
    : current_time_(current_time)
{
    bind(source);
}

// ============================================================================
// Core Signal Methods
// ============================================================================

bool SignalView::modified() const {
    // If we have children, aggregate from all children
    if (has_children()) {
        for (const auto& child : children_) {
            if (child->modified()) {
                return true;
            }
        }
        return false;
    }

    // No children: delegate to bound source
    if (!view_data_.valid()) {
        return false;
    }

    return view_data_.ops->modified(view_data_, current_time_);
}

bool SignalView::valid() const {
    // If we have children, aggregate from all children
    if (has_children()) {
        for (const auto& child : children_) {
            if (child->valid()) {
                return true;
            }
        }
        return false;
    }

    // No children: delegate to bound source
    if (!view_data_.valid()) {
        return false;
    }

    return view_data_.ops->valid(view_data_);
}

engine_time_t SignalView::last_modified_time() const {
    // If we have children, return MAX of all children
    if (has_children()) {
        engine_time_t max_time = MIN_DT;
        for (const auto& child : children_) {
            max_time = std::max(max_time, child->last_modified_time());
        }
        return max_time;
    }

    // No children: delegate to bound source
    if (!view_data_.valid()) {
        return MIN_DT;
    }

    return view_data_.ops->last_modified_time(view_data_);
}

// ============================================================================
// Child Signal Access
// ============================================================================

SignalView& SignalView::get_or_create_child(size_t index) {
    // Extend vector if needed
    while (index >= children_.size()) {
        auto child = std::make_unique<SignalView>();
        child->current_time_ = current_time_;

        // New children inherit active state
        if (active_) {
            child->active_ = true;
        }

        children_.push_back(std::move(child));
    }

    return *children_[index];
}

SignalView& SignalView::operator[](size_t index) {
    SignalView& child = get_or_create_child(index);

    // If we have a bound source, bind the child to the corresponding source child
    if (view_data_.valid() && source_meta_) {
        bind_child(child, index);
    }

    return child;
}

const SignalView& SignalView::at(size_t index) const {
    if (index >= children_.size()) {
        return invalid_signal_;
    }
    return *children_[index];
}

SignalView& SignalView::field(const std::string& name) {
    // source_meta_ must be a TSB to use field access
    if (!source_meta_ || source_meta_->kind != TSKind::TSB) {
        throw std::invalid_argument("field() requires binding to a TSB source");
    }

    // Look up the field by name
    for (size_t i = 0; i < source_meta_->field_count; ++i) {
        if (name == source_meta_->fields[i].name) {
            // Found the field - use index-based access
            return (*this)[source_meta_->fields[i].index];
        }
    }

    throw std::invalid_argument("field not found: " + name);
}

void SignalView::bind_child(SignalView& child, size_t index) {
    // Use source_view_ for child navigation (TSView handles this correctly)
    if (!source_view_.has_value()) {
        return;
    }

    // Navigate to the source child using TSView operator[]
    TSView source_child = (*source_view_)[index];
    if (source_child) {
        child.bind(source_child);
    }
}

// ============================================================================
// Binding
// ============================================================================

bool SignalView::bound() const noexcept {
    // Bound if we have a valid view_data OR if we have children
    return view_data_.valid() || has_children();
}

void SignalView::bind(const TSView& source) {
    if (!source) {
        return;
    }

    const TSMeta* source_ts_meta = source.ts_meta();
    if (!source_ts_meta) {
        return;
    }

    // Dereference the source schema (removes REF wrappers)
    auto& registry = TSTypeRegistry::instance();
    const TSMeta* deref_meta = registry.dereference(source_ts_meta);

    // If the schema changed (had REFs), we need to navigate through the REFs
    // For now, we bind directly to the source's view_data
    // The dereferencing happens when we navigate to children

    view_data_ = source.view_data();
    source_meta_ = deref_meta;
    current_time_ = source.current_time();

    // Store source TSView for child navigation (TSView handles composite navigation correctly)
    source_view_ = source;

    // Clear existing children - they need to rebind
    children_.clear();
}

void SignalView::unbind() {
    view_data_ = ViewData{};
    source_meta_ = nullptr;
    source_view_.reset();

    // Clear all children
    children_.clear();
}

// ============================================================================
// Active/Passive State
// ============================================================================

void SignalView::make_active() {
    active_ = true;

    // Propagate to all children
    for (auto& child : children_) {
        child->make_active();
    }
}

void SignalView::make_passive() {
    active_ = false;

    // Propagate to all children
    for (auto& child : children_) {
        child->make_passive();
    }
}

// ============================================================================
// Output Operations
// ============================================================================

void SignalView::tick() {
    if (!view_data_.valid()) {
        throw std::runtime_error("tick() called on unbound SignalView");
    }

    // Update the modification time
    if (view_data_.time_data) {
        *static_cast<engine_time_t*>(view_data_.time_data) = current_time_;
    }

    // Notify observers
    if (view_data_.observer_data) {
        auto* observers = static_cast<ObserverList*>(view_data_.observer_data);
        observers->notify_modified(current_time_);
    }
}

// ============================================================================
// Metadata
// ============================================================================

const TSMeta* SignalView::ts_meta() const noexcept {
    // Return the SIGNAL singleton
    return TSTypeRegistry::instance().signal();
}

} // namespace hgraph
