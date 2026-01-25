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

} // namespace hgraph
