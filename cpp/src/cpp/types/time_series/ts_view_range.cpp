/**
 * @file ts_view_range.cpp
 * @brief Implementation of TSViewRange and TSFieldRange iterators.
 */

#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_ops.h>

namespace hgraph {

// ============================================================================
// TSViewIterator
// ============================================================================

TSView TSViewIterator::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

// ============================================================================
// TSFieldIterator
// ============================================================================

TSView TSFieldIterator::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

// ============================================================================
// TSDictIterator
// ============================================================================

TSView TSDictIterator::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

// ============================================================================
// TSDictSlotIterator
// ============================================================================

TSView TSDictSlotIterator::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops || current_ == end_) {
        return TSView{};
    }
    // Use child_at with the actual slot index from the set iterator
    size_t slot = *current_;
    return nav_data_->ops->child_at(*nav_data_, slot, current_time_);
}

// ============================================================================
// FilteredTSFieldIterator<Filter> - Template implementations
// ============================================================================

template<TSFilter Filter>
TSView FilteredTSFieldIterator<Filter>::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

template<TSFilter Filter>
void FilteredTSFieldIterator<Filter>::advance_to_match() {
    while (current_index_ < end_index_ && !matches_filter()) {
        ++current_index_;
    }
}

template<TSFilter Filter>
bool FilteredTSFieldIterator<Filter>::matches_filter() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return false;
    }
    TSView view = nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
    // Compile-time dispatch - no runtime switch
    if constexpr (Filter == TSFilter::VALID) {
        return view.valid();
    } else {
        return view.modified();
    }
}

// Explicit instantiations
template class FilteredTSFieldIterator<TSFilter::VALID>;
template class FilteredTSFieldIterator<TSFilter::MODIFIED>;

// ============================================================================
// FilteredTSViewIterator<Filter> - Template implementations
// ============================================================================

template<TSFilter Filter>
TSView FilteredTSViewIterator<Filter>::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

template<TSFilter Filter>
void FilteredTSViewIterator<Filter>::advance_to_match() {
    while (current_index_ < end_index_ && !matches_filter()) {
        ++current_index_;
    }
}

template<TSFilter Filter>
bool FilteredTSViewIterator<Filter>::matches_filter() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return false;
    }
    TSView view = nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
    // Compile-time dispatch - no runtime switch
    if constexpr (Filter == TSFilter::VALID) {
        return view.valid();
    } else {
        return view.modified();
    }
}

// Explicit instantiations
template class FilteredTSViewIterator<TSFilter::VALID>;
template class FilteredTSViewIterator<TSFilter::MODIFIED>;

// ============================================================================
// FilteredTSDictIterator<Filter> - Template implementations
// ============================================================================

template<TSFilter Filter>
TSView FilteredTSDictIterator<Filter>::operator*() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return TSView{};
    }
    return nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
}

template<TSFilter Filter>
void FilteredTSDictIterator<Filter>::advance_to_match() {
    while (current_index_ < end_index_ && !matches_filter()) {
        ++current_index_;
    }
}

template<TSFilter Filter>
bool FilteredTSDictIterator<Filter>::matches_filter() const {
    if (!nav_data_ || !nav_data_->valid() || !nav_data_->ops) {
        return false;
    }
    TSView view = nav_data_->ops->child_at(*nav_data_, current_index_, current_time_);
    // Compile-time dispatch - no runtime switch
    if constexpr (Filter == TSFilter::VALID) {
        return view.valid();
    } else {
        return view.modified();
    }
}

// Explicit instantiations
template class FilteredTSDictIterator<TSFilter::VALID>;
template class FilteredTSDictIterator<TSFilter::MODIFIED>;

} // namespace hgraph
