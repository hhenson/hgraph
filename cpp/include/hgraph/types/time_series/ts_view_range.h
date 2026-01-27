#pragma once

/**
 * @file ts_view_range.h
 * @brief TSViewRange - Iterator for time-series views.
 *
 * This class provides range-based iteration over sequences of TSViews.
 */

#include <hgraph/types/time_series/slot_set.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/map_storage.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>

#include <iterator>
#include <string>
#include <vector>

namespace hgraph {

// Forward declarations
class TSView;
struct ts_ops;

/**
 * @brief Iterator for a sequence of TSViews.
 *
 * Used by TSLView::values(), TSBView::fields(), etc.
 */
class TSViewIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TSView;
    using difference_type = std::ptrdiff_t;
    using pointer = TSView*;
    using reference = TSView;

    TSViewIterator() noexcept
        : nav_data_(nullptr)
        , current_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSViewIterator(const ViewData* nav_data, size_t index, size_t end, engine_time_t current_time) noexcept
        : nav_data_(nav_data)
        , current_index_(index)
        , end_index_(end)
        , current_time_(current_time)
    {}

    /**
     * @brief Dereference to get TSView.
     * @note Implementation in ts_view_range.cpp
     */
    reference operator*() const;

    /**
     * @brief Get the current index.
     */
    [[nodiscard]] size_t index() const noexcept { return current_index_; }

    TSViewIterator& operator++() {
        ++current_index_;
        return *this;
    }

    TSViewIterator operator++(int) {
        TSViewIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const TSViewIterator& other) const noexcept {
        return current_index_ == other.current_index_;
    }

    bool operator!=(const TSViewIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const ViewData* nav_data_;
    size_t current_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Range for iterating over TSViews.
 *
 * Provides begin()/end() for range-based for loops.
 *
 * Usage:
 * @code
 * for (auto view : list_view.values()) {
 *     // Process each element
 * }
 *
 * // Or with index:
 * for (auto it = range.begin(); it != range.end(); ++it) {
 *     size_t idx = it.index();
 *     TSView view = *it;
 * }
 * @endcode
 */
class TSViewRange {
public:
    TSViewRange() noexcept
        : nav_data_()
        , begin_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSViewRange(ViewData nav_data, size_t begin_idx, size_t end_idx, engine_time_t current_time) noexcept
        : nav_data_(std::move(nav_data))
        , begin_index_(begin_idx)
        , end_index_(end_idx)
        , current_time_(current_time)
    {}

    [[nodiscard]] TSViewIterator begin() const {
        return TSViewIterator(&nav_data_, begin_index_, end_index_, current_time_);
    }

    [[nodiscard]] TSViewIterator end() const {
        return TSViewIterator(&nav_data_, end_index_, end_index_, current_time_);
    }

    [[nodiscard]] size_t size() const { return end_index_ - begin_index_; }
    [[nodiscard]] bool empty() const { return begin_index_ == end_index_; }

private:
    ViewData nav_data_;
    size_t begin_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Named iterator for bundle fields.
 *
 * Like TSViewIterator but also provides field names.
 */
class TSFieldIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TSView;
    using difference_type = std::ptrdiff_t;
    using pointer = TSView*;
    using reference = TSView;

    TSFieldIterator() noexcept
        : nav_data_(nullptr)
        , meta_(nullptr)
        , current_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSFieldIterator(const ViewData* nav_data, const TSMeta* meta, size_t index, size_t end, engine_time_t current_time) noexcept
        : nav_data_(nav_data)
        , meta_(meta)
        , current_index_(index)
        , end_index_(end)
        , current_time_(current_time)
    {}

    /**
     * @brief Dereference to get TSView.
     * @note Implementation in ts_view_range.cpp
     */
    reference operator*() const;

    /**
     * @brief Get the current field index.
     */
    [[nodiscard]] size_t index() const noexcept { return current_index_; }

    /**
     * @brief Get the current field name.
     */
    [[nodiscard]] const char* name() const noexcept {
        if (meta_ && current_index_ < meta_->field_count) {
            return meta_->fields[current_index_].name;
        }
        return "";
    }

    TSFieldIterator& operator++() {
        ++current_index_;
        return *this;
    }

    TSFieldIterator operator++(int) {
        TSFieldIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const TSFieldIterator& other) const noexcept {
        return current_index_ == other.current_index_;
    }

    bool operator!=(const TSFieldIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const ViewData* nav_data_;
    const TSMeta* meta_;
    size_t current_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Range for iterating over bundle fields with names.
 */
class TSFieldRange {
public:
    TSFieldRange() noexcept
        : nav_data_()
        , meta_(nullptr)
        , begin_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSFieldRange(ViewData nav_data, const TSMeta* meta, size_t begin_idx, size_t end_idx, engine_time_t current_time) noexcept
        : nav_data_(std::move(nav_data))
        , meta_(meta)
        , begin_index_(begin_idx)
        , end_index_(end_idx)
        , current_time_(current_time)
    {}

    [[nodiscard]] TSFieldIterator begin() const {
        return TSFieldIterator(&nav_data_, meta_, begin_index_, end_index_, current_time_);
    }

    [[nodiscard]] TSFieldIterator end() const {
        return TSFieldIterator(&nav_data_, meta_, end_index_, end_index_, current_time_);
    }

    [[nodiscard]] size_t size() const { return end_index_ - begin_index_; }
    [[nodiscard]] bool empty() const { return begin_index_ == end_index_; }

private:
    ViewData nav_data_;
    const TSMeta* meta_;
    size_t begin_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Iterator for TSD (dict) entries with key access.
 *
 * Like TSViewIterator but also provides key access.
 */
class TSDictIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TSView;
    using difference_type = std::ptrdiff_t;
    using pointer = TSView*;
    using reference = TSView;

    TSDictIterator() noexcept
        : nav_data_(nullptr)
        , meta_(nullptr)
        , current_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSDictIterator(const ViewData* nav_data, const TSMeta* meta, size_t index, size_t end, engine_time_t current_time) noexcept
        : nav_data_(nav_data)
        , meta_(meta)
        , current_index_(index)
        , end_index_(end)
        , current_time_(current_time)
    {}

    /**
     * @brief Dereference to get TSView.
     * @note Implementation in ts_view_range.cpp
     */
    reference operator*() const;

    /**
     * @brief Get the current slot index.
     */
    [[nodiscard]] size_t index() const noexcept { return current_index_; }

    /**
     * @brief Get the key at the current slot as a value::View.
     *
     * This provides access to the key for the current entry.
     */
    [[nodiscard]] value::View key() const {
        if (!nav_data_ || !meta_ || !meta_->key_type) {
            return value::View{};
        }
        // ViewData::value_data points to the MapStorage for TSD types
        auto* map_storage = static_cast<const value::MapStorage*>(nav_data_->value_data);
        const void* key_ptr = map_storage->key_at_slot(current_index_);
        return value::View(const_cast<void*>(key_ptr), meta_->key_type);
    }

    TSDictIterator& operator++() {
        ++current_index_;
        return *this;
    }

    TSDictIterator operator++(int) {
        TSDictIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const TSDictIterator& other) const noexcept {
        return current_index_ == other.current_index_;
    }

    bool operator!=(const TSDictIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const ViewData* nav_data_;
    const TSMeta* meta_;
    size_t current_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Range for iterating over TSD entries with key access.
 *
 * Provides begin()/end() for range-based for loops.
 * Each iterator provides index(), key(), and dereference to TSView.
 *
 * Usage:
 * @code
 * for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
 *     size_t slot = it.index();
 *     value::View key = it.key();
 *     TSView val = *it;
 *     // Process entry
 * }
 * @endcode
 */
class TSDictRange {
public:
    TSDictRange() noexcept
        : nav_data_()
        , meta_(nullptr)
        , begin_index_(0)
        , end_index_(0)
        , current_time_(MIN_ST)
    {}

    TSDictRange(ViewData nav_data, const TSMeta* meta, size_t begin_idx, size_t end_idx, engine_time_t current_time) noexcept
        : nav_data_(std::move(nav_data))
        , meta_(meta)
        , begin_index_(begin_idx)
        , end_index_(end_idx)
        , current_time_(current_time)
    {}

    [[nodiscard]] TSDictIterator begin() const {
        return TSDictIterator(&nav_data_, meta_, begin_index_, end_index_, current_time_);
    }

    [[nodiscard]] TSDictIterator end() const {
        return TSDictIterator(&nav_data_, meta_, end_index_, end_index_, current_time_);
    }

    [[nodiscard]] size_t size() const { return end_index_ - begin_index_; }
    [[nodiscard]] bool empty() const { return begin_index_ == end_index_; }

private:
    ViewData nav_data_;
    const TSMeta* meta_;
    size_t begin_index_;
    size_t end_index_;
    engine_time_t current_time_;
};

/**
 * @brief Iterator for TSD entries filtered by a set of slots.
 *
 * Iterates over entries at specific slot indices from a SlotSet.
 */
class TSDictSlotIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TSView;
    using difference_type = std::ptrdiff_t;
    using pointer = TSView*;
    using reference = TSView;
    using set_iterator = SlotSet::const_iterator;

    TSDictSlotIterator() noexcept
        : nav_data_(nullptr)
        , meta_(nullptr)
        , current_()
        , end_()
        , current_time_(MIN_ST)
    {}

    TSDictSlotIterator(const ViewData* nav_data, const TSMeta* meta,
                       set_iterator current, set_iterator end,
                       engine_time_t current_time) noexcept
        : nav_data_(nav_data)
        , meta_(meta)
        , current_(current)
        , end_(end)
        , current_time_(current_time)
    {}

    /**
     * @brief Dereference to get TSView.
     * @note Implementation in ts_view_range.cpp
     */
    reference operator*() const;

    /**
     * @brief Get the current slot index in the underlying storage.
     */
    [[nodiscard]] size_t slot() const noexcept {
        return current_ != end_ ? *current_ : 0;
    }

    /**
     * @brief Get the key at the current slot as a value::View.
     */
    [[nodiscard]] value::View key() const {
        if (!nav_data_ || !meta_ || !meta_->key_type || current_ == end_) {
            return value::View{};
        }
        auto* map_storage = static_cast<const value::MapStorage*>(nav_data_->value_data);
        const void* key_ptr = map_storage->key_at_slot(*current_);
        return value::View(const_cast<void*>(key_ptr), meta_->key_type);
    }

    TSDictSlotIterator& operator++() {
        ++current_;
        return *this;
    }

    TSDictSlotIterator operator++(int) {
        TSDictSlotIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const TSDictSlotIterator& other) const noexcept {
        return current_ == other.current_;
    }

    bool operator!=(const TSDictSlotIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const ViewData* nav_data_;
    const TSMeta* meta_;
    set_iterator current_;
    set_iterator end_;
    engine_time_t current_time_;
};

/**
 * @brief Range for iterating over TSD entries at specific slots.
 *
 * Used for filtered iteration (added_items, modified_items, etc.)
 *
 * Usage:
 * @code
 * for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
 *     size_t slot = it.slot();
 *     value::View key = it.key();
 *     TSView val = *it;
 * }
 * @endcode
 */
class TSDictSlotRange {
public:
    TSDictSlotRange() noexcept
        : nav_data_()
        , meta_(nullptr)
        , slots_(nullptr)
        , current_time_(MIN_ST)
    {}

    TSDictSlotRange(ViewData nav_data, const TSMeta* meta,
                    const SlotSet* slots, engine_time_t current_time) noexcept
        : nav_data_(std::move(nav_data))
        , meta_(meta)
        , slots_(slots)
        , current_time_(current_time)
    {}

    [[nodiscard]] TSDictSlotIterator begin() const {
        if (!slots_) {
            return TSDictSlotIterator{};
        }
        return TSDictSlotIterator(&nav_data_, meta_, slots_->begin(), slots_->end(), current_time_);
    }

    [[nodiscard]] TSDictSlotIterator end() const {
        if (!slots_) {
            return TSDictSlotIterator{};
        }
        return TSDictSlotIterator(&nav_data_, meta_, slots_->end(), slots_->end(), current_time_);
    }

    [[nodiscard]] size_t size() const { return slots_ ? slots_->size() : 0; }
    [[nodiscard]] bool empty() const { return !slots_ || slots_->empty(); }

private:
    ViewData nav_data_;
    const TSMeta* meta_;
    const SlotSet* slots_;
    engine_time_t current_time_;
};

/**
 * @brief Iterator yielding key Views at specific slots.
 *
 * Used for iterating over keys filtered by a SlotSet (added_keys, modified_keys, etc.)
 */
class SlotKeyIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = value::View;
    using difference_type = std::ptrdiff_t;
    using pointer = value::View*;
    using reference = value::View;
    using set_iterator = SlotSet::const_iterator;

    SlotKeyIterator() noexcept = default;

    SlotKeyIterator(const value::MapStorage* storage, const value::TypeMeta* key_type,
                    set_iterator current, set_iterator end) noexcept
        : storage_(storage)
        , key_type_(key_type)
        , current_(current)
        , end_(end)
    {}

    /**
     * @brief Dereference to get key View at current slot.
     */
    reference operator*() const {
        if (!storage_ || current_ == end_) {
            return value::View{};
        }
        const void* key_ptr = storage_->key_at_slot(*current_);
        return value::View(const_cast<void*>(key_ptr), key_type_);
    }

    /**
     * @brief Get the current slot index.
     */
    [[nodiscard]] size_t slot() const noexcept {
        return current_ != end_ ? *current_ : 0;
    }

    SlotKeyIterator& operator++() {
        ++current_;
        return *this;
    }

    SlotKeyIterator operator++(int) {
        SlotKeyIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const SlotKeyIterator& other) const noexcept {
        return current_ == other.current_;
    }

    bool operator!=(const SlotKeyIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const value::MapStorage* storage_{nullptr};
    const value::TypeMeta* key_type_{nullptr};
    set_iterator current_;
    set_iterator end_;
};

/**
 * @brief Range yielding key Views at specific slots.
 *
 * Used for TSD key iteration methods like added_keys(), modified_keys(),
 * updated_keys(), and removed_keys(). Each iteration yields a value::View
 * for the key at that slot.
 *
 * Note: Removed keys remain accessible during the current tick because their
 * slots are placed on a free list that is only used in the next engine cycle.
 *
 * Usage:
 * @code
 * for (auto key : dict_view.added_keys()) {
 *     std::cout << key.as<std::string>() << " was added\n";
 * }
 * @endcode
 */
class SlotKeyRange {
public:
    SlotKeyRange() noexcept = default;

    SlotKeyRange(const value::MapStorage* storage, const value::TypeMeta* key_type,
                 const SlotSet* slots) noexcept
        : storage_(storage)
        , key_type_(key_type)
        , slots_(slots)
    {}

    [[nodiscard]] SlotKeyIterator begin() const {
        if (!slots_ || !storage_) {
            return SlotKeyIterator{};
        }
        return SlotKeyIterator(storage_, key_type_, slots_->begin(), slots_->end());
    }

    [[nodiscard]] SlotKeyIterator end() const {
        if (!slots_ || !storage_) {
            return SlotKeyIterator{};
        }
        return SlotKeyIterator(storage_, key_type_, slots_->end(), slots_->end());
    }

    [[nodiscard]] size_t size() const { return slots_ ? slots_->size() : 0; }
    [[nodiscard]] bool empty() const { return !slots_ || slots_->empty(); }

private:
    const value::MapStorage* storage_{nullptr};
    const value::TypeMeta* key_type_{nullptr};
    const SlotSet* slots_{nullptr};
};

/**
 * @brief Iterator yielding element Views at specific slots.
 *
 * Used for iterating over set elements filtered by a SlotSet (added elements, etc.)
 */
class SlotElementIterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = value::View;
    using difference_type = std::ptrdiff_t;
    using pointer = value::View*;
    using reference = value::View;
    using set_iterator = SlotSet::const_iterator;

    SlotElementIterator() noexcept = default;

    SlotElementIterator(const value::SetStorage* storage, const value::TypeMeta* element_type,
                        set_iterator current, set_iterator end) noexcept
        : storage_(storage)
        , element_type_(element_type)
        , current_(current)
        , end_(end)
    {}

    /**
     * @brief Dereference to get element View at current slot.
     */
    reference operator*() const {
        if (!storage_ || current_ == end_) {
            return value::View{};
        }
        const void* elem_ptr = storage_->key_set().key_at_slot(*current_);
        return value::View(const_cast<void*>(elem_ptr), element_type_);
    }

    /**
     * @brief Get the current slot index.
     */
    [[nodiscard]] size_t slot() const noexcept {
        return current_ != end_ ? *current_ : 0;
    }

    SlotElementIterator& operator++() {
        ++current_;
        return *this;
    }

    SlotElementIterator operator++(int) {
        SlotElementIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const SlotElementIterator& other) const noexcept {
        return current_ == other.current_;
    }

    bool operator!=(const SlotElementIterator& other) const noexcept {
        return !(*this == other);
    }

private:
    const value::SetStorage* storage_{nullptr};
    const value::TypeMeta* element_type_{nullptr};
    set_iterator current_;
    set_iterator end_;
};

/**
 * @brief Range yielding element Views at specific slots.
 *
 * Used for TSS element iteration methods like added() and removed().
 * Each iteration yields a value::View for the element at that slot.
 *
 * Note: Removed elements remain accessible during the current tick because their
 * slots are placed on a free list that is only used in the next engine cycle.
 *
 * Usage:
 * @code
 * for (auto elem : set_view.added()) {
 *     std::cout << elem.as<int64_t>() << " was added\n";
 * }
 * @endcode
 */
class SlotElementRange {
public:
    SlotElementRange() noexcept = default;

    SlotElementRange(const value::SetStorage* storage, const value::TypeMeta* element_type,
                     const SlotSet* slots) noexcept
        : storage_(storage)
        , element_type_(element_type)
        , slots_(slots)
    {}

    [[nodiscard]] SlotElementIterator begin() const {
        if (!slots_ || !storage_) {
            return SlotElementIterator{};
        }
        return SlotElementIterator(storage_, element_type_, slots_->begin(), slots_->end());
    }

    [[nodiscard]] SlotElementIterator end() const {
        if (!slots_ || !storage_) {
            return SlotElementIterator{};
        }
        return SlotElementIterator(storage_, element_type_, slots_->end(), slots_->end());
    }

    [[nodiscard]] size_t size() const { return slots_ ? slots_->size() : 0; }
    [[nodiscard]] bool empty() const { return !slots_ || slots_->empty(); }

private:
    const value::SetStorage* storage_{nullptr};
    const value::TypeMeta* element_type_{nullptr};
    const SlotSet* slots_{nullptr};
};

} // namespace hgraph
