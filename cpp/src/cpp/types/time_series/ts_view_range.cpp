/**
 * @file ts_view_range.cpp
 * @brief Implementation of TSViewRange and TSFieldRange iterators.
 */

#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/map_delta.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/value/map_storage.h>

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
    if (!nav_data_.valid() || !nav_data_.ops) {
        return TSView{};
    }
    return nav_data_.ops->child_at(nav_data_, current_index_, current_time_);
}

// ============================================================================
// TSDictSlotIterator
// ============================================================================

TSView TSDictSlotIterator::operator*() const {
    if (!nav_data_.valid() || !nav_data_.ops || current_ == end_) {
        return TSView{};
    }
    // Use child_at with the actual slot index from the set iterator
    size_t slot = *current_;
    TSView view = nav_data_.ops->child_at(nav_data_, slot, current_time_);
    if (view) {
        return view;
    }

    // Removed-slot fallback: child_at() intentionally hides dead slots.
    // Delta iterators still need slot access for removed_items() semantics.
    if (!nav_data_.meta || nav_data_.meta->kind != TSKind::TSD ||
        !nav_data_.meta->element_ts || !nav_data_.value_data ||
        !nav_data_.time_data || !nav_data_.observer_data) {
        return TSView{};
    }

    auto* storage = static_cast<value::MapStorage*>(nav_data_.value_data);
    if (!storage) {
        return TSView{};
    }

    auto& cache = TSMetaSchemaCache::instance();
    const auto* time_schema = cache.get_time_schema(nav_data_.meta);
    const auto* observer_schema = cache.get_observer_schema(nav_data_.meta);
    if (!time_schema || !observer_schema) {
        return TSView{};
    }

    value::View time_view(nav_data_.time_data, time_schema);
    value::View observer_view(nav_data_.observer_data, observer_schema);
    auto time_list = time_view.as_tuple().at(1).as_list();
    auto observer_list = observer_view.as_tuple().at(1).as_list();
    if (slot >= time_list.size() || slot >= observer_list.size()) {
        return TSView{};
    }

    ViewData elem_vd;
    elem_vd.path = nav_data_.path.child(slot);
    elem_vd.value_data = storage->value_at_slot(slot);
    elem_vd.time_data = time_list.at(slot).data();
    elem_vd.observer_data = observer_list.at(slot).data();
    if (nav_data_.meta->element_ts->kind == TSKind::TSD && nav_data_.delta_data) {
        auto* parent_delta = static_cast<MapDelta*>(nav_data_.delta_data);
        auto* inner_storage = static_cast<value::MapStorage*>(elem_vd.value_data);
        elem_vd.delta_data = parent_delta->get_or_create_child_map_delta(slot, inner_storage);
    } else {
        elem_vd.delta_data = nullptr;
    }
    elem_vd.uses_link_target = nav_data_.uses_link_target;
    elem_vd.ops = get_ts_ops(nav_data_.meta->element_ts);
    elem_vd.meta = nav_data_.meta->element_ts;

    // Preserve per-element link storage when present.
    if (nav_data_.link_data) {
        const value::TypeMeta* link_schema = nav_data_.uses_link_target
            ? cache.get_input_link_schema(nav_data_.meta)
            : cache.get_link_schema(nav_data_.meta);
        if (link_schema) {
            value::View link_view(nav_data_.link_data, link_schema);
            auto link_list = link_view.as_tuple().at(1).as_list();
            if (slot < link_list.size()) {
                elem_vd.link_data = link_list.at(slot).data();
            }
        }
    }

    return TSView(elem_vd, current_time_);
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
    if (!nav_data_.valid() || !nav_data_.ops) {
        return TSView{};
    }
    return nav_data_.ops->child_at(nav_data_, current_index_, current_time_);
}

template<TSFilter Filter>
void FilteredTSDictIterator<Filter>::advance_to_match() {
    while (current_index_ < end_index_ && !matches_filter()) {
        ++current_index_;
    }
}

template<TSFilter Filter>
bool FilteredTSDictIterator<Filter>::matches_filter() const {
    if (!nav_data_.valid() || !nav_data_.ops) {
        return false;
    }
    TSView view = nav_data_.ops->child_at(nav_data_, current_index_, current_time_);
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
