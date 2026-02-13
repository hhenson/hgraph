#pragma once

/**
 * @file indexed_view.h
 * @brief Indexed view classes for positional access.
 *
 * This header defines views for types that support positional (index-based) access:
 * - IndexedView: Base class for indexed access
 * - TupleView: Heterogeneous indexed collections
 * - BundleView: Struct-like types with named + indexed access
 * - ListView: Homogeneous indexed collections
 * - SetView: Unique element collections (supports read-only mode)
 * - MapView: Key-value collections (supports read-only mode)
 *
 * All views provide:
 * - at(index) / operator[](index) for element access
 * - size() for element count
 * - Iteration support via begin()/end()
 */

#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <hgraph/types/value/queue_ops.h>

#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string_view>

namespace hgraph::value {

inline void require_typed_view(const View& view, const TypeMeta* expected_schema,
                               const char* name, bool allow_null = false) {
    if (!allow_null && !view.valid()) {
        throw std::runtime_error(std::string(name) + " must be non-null");
    }
    if (view.valid() && expected_schema && view.schema() != expected_schema) {
        throw std::runtime_error(std::string(name) + " schema mismatch");
    }
}

// Forward declarations
template<typename Policy>
class Value;

// ============================================================================
// IndexedView - Positional Access (Const + Mutable)
// ============================================================================

/**
 * @brief Base class for types supporting index-based access.
 */
class IndexedView : public ValueView {
public:
    // ========== Construction ==========

    using ValueView::ValueView;

    /// Construct a read-only indexed view from a base view
    explicit IndexedView(const View& view) noexcept
        : ValueView(view) {}

    /// Construct from base view
    explicit IndexedView(const ValueView& view)
        : ValueView(view) {}

    // ========== Size ==========

    /**
     * @brief Get the number of elements.
     * @return Element count
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        if (_schema->ops().has_size()) {
            return _schema->ops().size(_data, _schema);
        }
        return _schema->field_count;
    }

    /**
     * @brief Check if empty.
     * @return true if size() == 0
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    // ========== Element Access (Const) ==========

    /**
     * @brief Get element at index (const).
     */
    [[nodiscard]] View at(size_t index) const {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        const void* elem_data = _schema->ops().at(_data, index, _schema);
        const TypeMeta* elem_schema = get_element_schema(index);
        return View(elem_data, elem_schema);
    }

    /**
     * @brief Get element at index (mutable).
     */
    [[nodiscard]] ValueView at(size_t index) {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        // Preserve mutability for nested element views.
        void* elem_data = const_cast<void*>(_schema->ops().at(data(), index, _schema));
        const TypeMeta* elem_schema = get_element_schema(index);
        if (!is_mutable()) {
            return ValueView(View(elem_data, elem_schema));
        }
        return ValueView(elem_data, elem_schema);
    }

    /**
     * @brief Get element at index (const, operator[]).
     */
    [[nodiscard]] View operator[](size_t index) const {
        return at(index);
    }

    /**
     * @brief Get element at index (mutable, operator[]).
     */
    [[nodiscard]] ValueView operator[](size_t index) {
        return at(index);
    }

    // ========== Iteration ==========

    /**
     * @brief Const iterator for indexed views.
     */
    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = View;
        using difference_type = std::ptrdiff_t;
        using pointer = const View*;
        using reference = View;

        const_iterator() = default;
        const_iterator(const IndexedView* view, size_t index)
            : _view(view), _index(index) {}

        reference operator*() const {
            return _view->at(_index);
        }

        const_iterator& operator++() {
            ++_index;
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const const_iterator& other) const {
            return _view == other._view && _index == other._index;
        }

        bool operator!=(const const_iterator& other) const {
            return !(*this == other);
        }

    private:
        const IndexedView* _view{nullptr};
        size_t _index{0};
    };

    [[nodiscard]] const_iterator begin() const {
        return const_iterator(this, 0);
    }

    [[nodiscard]] const_iterator end() const {
        return const_iterator(this, size());
    }

    // ========== Mutation ==========

    /**
     * @brief Set element at index from a view.
     *
     * @param index The element index
     * @param value The value to set
     */
    void set(size_t index, const View& value) {
        require_mutable("set");
        assert(valid() && "set() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        _schema->ops().set_at(data(), index, value.data(), _schema);
    }

    /**
     * @brief Set element at index from a typed value.
     *
     * @tparam T The value type
     * @param index The element index
     * @param value The value to set
     */
    template<typename T>
    void set(size_t index, const T& value);  // Implemented after Value is defined

private:
    [[nodiscard]] const TypeMeta* get_element_schema(size_t index) const {
        if (_schema->kind == TypeKind::List || _schema->kind == TypeKind::Set ||
            _schema->kind == TypeKind::CyclicBuffer || _schema->kind == TypeKind::Queue) {
            return _schema->element_type;
        } else if (_schema->kind == TypeKind::Bundle || _schema->kind == TypeKind::Tuple) {
            return _schema->fields[index].type;
        }
        return nullptr;
    }
};

// ============================================================================
// TupleView - Heterogeneous Indexed Access
// ============================================================================

/**
 * @brief View for tuple types.
 */
class TupleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the type of element at index.
     */
    [[nodiscard]] const TypeMeta* element_type(size_t index) const {
        assert(valid() && index < size() && "Invalid index");
        return _schema->fields[index].type;
    }
};

// ============================================================================
// BundleView - Struct-like Access
// ============================================================================

/**
 * @brief View for bundle types.
 */
class BundleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // ========== Named Field Access (Const) ==========

    /**
     * @brief Get field by name (const).
     */
    [[nodiscard]] View at(std::string_view name) const {
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        return IndexedView::at(idx);
    }

    /**
     * @brief Get field by name (mutable).
     */
    [[nodiscard]] ValueView at(std::string_view name) {
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        return IndexedView::at(idx);
    }

    /**
     * @brief Get field by name (const, operator[]).
     */
    [[nodiscard]] View operator[](std::string_view name) const {
        return at(name);
    }

    /**
     * @brief Get field by name (mutable, operator[]).
     */
    [[nodiscard]] ValueView operator[](std::string_view name) {
        return at(name);
    }

    // Bring base class operators into scope
    using IndexedView::operator[];
    using IndexedView::at;

    // ========== Named Field Mutation ==========

    /**
     * @brief Set field by name from a view.
     */
    void set(std::string_view name, const View& value) {
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        IndexedView::set(idx, value);
    }

    /**
     * @brief Set field by name from a typed value.
     */
    template<typename T>
    void set(std::string_view name, const T& value);  // Implemented after Value

    // Bring base class set into scope
    using IndexedView::set;

    // ========== Field Metadata ==========

    [[nodiscard]] size_t field_count() const { return size(); }

    [[nodiscard]] const BundleFieldInfo* field_info(size_t index) const {
        assert(valid() && index < size() && "Invalid field index");
        return &_schema->fields[index];
    }

    [[nodiscard]] const BundleFieldInfo* field_info(std::string_view name) const {
        size_t idx = field_index(name);
        return (idx < size()) ? &_schema->fields[idx] : nullptr;
    }

    [[nodiscard]] bool has_field(std::string_view name) const {
        return field_index(name) < size();
    }

    [[nodiscard]] size_t field_index(std::string_view name) const {
        assert(valid() && "field_index() on invalid view");
        for (size_t i = 0; i < _schema->field_count; ++i) {
            if (name == _schema->fields[i].name) {
                return i;
            }
        }
        return size();
    }

    // ========== Items Iteration ==========

    struct field_pair {
        std::string_view name;
        ValueView value;
    };

    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = field_pair;
        using difference_type = std::ptrdiff_t;
        using pointer = const field_pair*;
        using reference = field_pair;

        items_iterator() = default;
        items_iterator(void* data, const TypeMeta* schema, size_t index)
            : data_(data), schema_(schema), index_(index) {}

        reference operator*() const {
            return {
                std::string_view{schema_->fields[index_].name},
                ValueView(const_cast<void*>(schema_->ops().at(data_, index_, schema_)),
                          schema_->fields[index_].type)
            };
        }

        items_iterator& operator++() { ++index_; return *this; }
        items_iterator operator++(int) { auto tmp = *this; ++index_; return tmp; }
        bool operator==(const items_iterator& o) const { return index_ == o.index_; }
        bool operator!=(const items_iterator& o) const { return index_ != o.index_; }

    private:
        void* data_{nullptr};
        const TypeMeta* schema_{nullptr};
        size_t index_{0};
    };

    struct items_range {
        items_iterator begin_, end_;
        items_iterator begin() const { return begin_; }
        items_iterator end() const { return end_; }
    };

    [[nodiscard]] items_range items() {
        require_mutable("items");
        return {
            items_iterator(data(), _schema, 0),
            items_iterator(data(), _schema, size())
        };
    }
};

// ============================================================================
// ListView - Indexed Collection Access
// ============================================================================

/**
 * @brief View for list types.
 */
class ListView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the first element (mutable).
     */
    [[nodiscard]] ValueView front() {
        return at(0);
    }

    /**
     * @brief Get the first element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the last element (mutable).
     */
    [[nodiscard]] ValueView back() {
        return at(size() - 1);
    }

    /**
     * @brief Get the last element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->element_type;
    }

    /**
     * @brief Check if this is a fixed-size list.
     */
    [[nodiscard]] bool is_fixed() const {
        return _schema->is_fixed_size();
    }

    // ========== Items Iteration ==========

    struct indexed_pair {
        size_t index;
        ValueView value;
    };

    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = indexed_pair;
        using difference_type = std::ptrdiff_t;
        using pointer = const indexed_pair*;
        using reference = indexed_pair;

        items_iterator() = default;
        items_iterator(void* data, const TypeMeta* schema, size_t index)
            : data_(data), schema_(schema), index_(index) {}

        reference operator*() const {
            return {
                index_,
                ValueView(const_cast<void*>(schema_->ops().at(data_, index_, schema_)),
                          schema_->element_type)
            };
        }

        items_iterator& operator++() { ++index_; return *this; }
        items_iterator operator++(int) { auto tmp = *this; ++index_; return tmp; }
        bool operator==(const items_iterator& o) const { return index_ == o.index_; }
        bool operator!=(const items_iterator& o) const { return index_ != o.index_; }

    private:
        void* data_{nullptr};
        const TypeMeta* schema_{nullptr};
        size_t index_{0};
    };

    struct items_range {
        items_iterator begin_, end_;
        items_iterator begin() const { return begin_; }
        items_iterator end() const { return end_; }
    };

    [[nodiscard]] items_range items() {
        require_mutable("items");
        return {
            items_iterator(data(), _schema, 0),
            items_iterator(data(), _schema, size())
        };
    }

    // ========== Dynamic List Operations ==========

    /**
     * @brief Append an element.
     *
     * @throws std::runtime_error if the list is fixed-size or resize not supported
     */
    void push_back(const View& value) {
        require_mutable("push_back");
        if (is_fixed()) {
            throw std::runtime_error("Cannot push_back on fixed-size list");
        }
        if (!_schema->ops().has_resize()) {
            throw std::runtime_error("List type does not support resize operation");
        }

        // IMPORTANT: Copy the source value BEFORE resize, in case the source
        // is a temporary that may be destroyed by allocations during resize.
        // The resize may trigger memory allocations that could reuse the
        // memory where the source value was stored.
        const TypeMeta* elem_type = _schema->element_type;
        alignas(16) std::byte local_buffer[64];  // Stack buffer for small values
        void* temp_storage = nullptr;
        bool using_heap = false;

        if (elem_type && elem_type->size <= sizeof(local_buffer)) {
            temp_storage = local_buffer;
        } else if (elem_type) {
            temp_storage = ::operator new(elem_type->size, std::align_val_t{elem_type->alignment});
            using_heap = true;
        }

        // Copy-construct the value into temp storage
        if (temp_storage && elem_type) {
            elem_type->ops().construct(temp_storage, elem_type);
            elem_type->ops().copy(temp_storage, value.data(), elem_type);
        }

        // Now resize - this may reallocate and potentially reuse freed memory
        size_t current_size = size();
        _schema->ops().resize(data(), current_size + 1, _schema);

        // Copy from our temp storage to the new element
        if (temp_storage && elem_type) {
            void* elem_ptr = ListOps::get_element_ptr(data(), current_size, _schema);
            elem_type->ops().copy(elem_ptr, temp_storage, elem_type);
            elem_type->ops().destroy(temp_storage, elem_type);
        }

        if (using_heap && temp_storage) {
            ::operator delete(temp_storage, std::align_val_t{elem_type->alignment});
        }
    }

    /**
     * @brief Remove the last element.
     *
     * @throws std::runtime_error if the list is fixed-size, empty, or resize not supported
     */
    void pop_back() {
        require_mutable("pop_back");
        if (is_fixed()) {
            throw std::runtime_error("Cannot pop_back on fixed-size list");
        }
        if (empty()) {
            throw std::runtime_error("Cannot pop_back on empty list");
        }
        if (!_schema->ops().has_resize()) {
            throw std::runtime_error("List type does not support resize operation");
        }
        // Resize to remove the last element
        _schema->ops().resize(data(), size() - 1, _schema);
    }

    /**
     * @brief Clear all elements.
     *
     * @throws std::runtime_error if the list is fixed-size
     */
    void clear() {
        require_mutable("clear");
        if (is_fixed()) {
            throw std::runtime_error("Cannot clear fixed-size list");
        }
        if (_schema->ops().has_clear()) {
            _schema->ops().clear(data(), _schema);
        }
    }

    /**
     * @brief Resize the list.
     *
     * @throws std::runtime_error if the list is fixed-size
     */
    void resize(size_t new_size) {
        require_mutable("resize");
        if (is_fixed()) {
            throw std::runtime_error("Cannot resize fixed-size list");
        }
        if (_schema->ops().has_resize()) {
            _schema->ops().resize(data(), new_size, _schema);
        }
    }

    /**
     * @brief Reset all elements to a sentinel value.
     *
     * Works on both fixed and dynamic lists.
     */
    void reset(const View& sentinel) {
        require_mutable("reset");
        for (size_t i = 0; i < size(); ++i) {
            set(i, sentinel);
        }
    }

    /**
     * @brief Append a typed value.
     */
    template<typename T>
    void push_back(const T& value);  // Implemented after Value

    /**
     * @brief Reset with a typed sentinel.
     */
    template<typename T>
    void reset(const T& sentinel);  // Implemented after Value
};

// ============================================================================
// CyclicBufferView - Fixed-Size Circular Buffer Access
// ============================================================================

/**
 * @brief View for cyclic buffer types.
 */
class CyclicBufferView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the oldest element (mutable).
     */
    [[nodiscard]] ValueView front() {
        return at(0);
    }

    /**
     * @brief Get the oldest element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the newest element (mutable).
     */
    [[nodiscard]] ValueView back() {
        return at(size() - 1);
    }

    /**
     * @brief Get the newest element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->element_type;
    }

    /**
     * @brief Get the fixed capacity.
     */
    [[nodiscard]] size_t capacity() const {
        return _schema->fixed_size;
    }

    /**
     * @brief Check if the buffer is full.
     */
    [[nodiscard]] bool full() const {
        return size() == capacity();
    }

    /**
     * @brief Push a value to the back of the cyclic buffer.
     *
     * If the buffer is not full, adds at the end.
     * If the buffer is full, overwrites the oldest element.
     */
    void push(const View& value);

    /**
     * @brief Clear all elements from the buffer.
     */
    void clear() {
        require_mutable("clear");
        if (_schema->ops().has_clear()) {
            _schema->ops().clear(data(), _schema);
        }
    }

    /**
     * @brief Push a typed value.
     */
    template<typename T>
    void push(const T& value);  // Implemented after Value
};

// ============================================================================
// QueueView - FIFO Queue Access
// ============================================================================

/**
 * @brief View for queue types.
 */
class QueueView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the front element (mutable).
     */
    [[nodiscard]] ValueView front() {
        return at(0);
    }

    /**
     * @brief Get the front element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the back element (mutable).
     */
    [[nodiscard]] ValueView back() {
        return at(size() - 1);
    }

    /**
     * @brief Get the back element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->element_type;
    }

    /**
     * @brief Get the max capacity (0 = unbounded).
     */
    [[nodiscard]] size_t max_capacity() const {
        return _schema->fixed_size;
    }

    /**
     * @brief Check if the queue has a max capacity.
     */
    [[nodiscard]] bool has_max_capacity() const {
        return max_capacity() > 0;
    }

    /**
     * @brief Push a value to the back of the queue.
     */
    void push(const View& value);

    /**
     * @brief Remove the front element.
     */
    void pop();

    /**
     * @brief Clear all elements from the queue.
     */
    void clear() {
        require_mutable("clear");
        if (_schema->ops().has_clear()) {
            _schema->ops().clear(data(), _schema);
        }
    }

    /**
     * @brief Push a typed value.
     */
    template<typename T>
    void push(const T& value);  // Implemented after Value
};

// ============================================================================
// SetView - Set Operations (Mutable + Read-Only Mode)
// ============================================================================

/**
 * @brief Mutable view for set types.
 */
class SetView : public ValueView {
public:
    using ValueView::ValueView;

    SetView() noexcept = default;

    /**
     * @brief Construct a read-only SetView from a read-only view.
     *
     * Mutation methods will throw on this instance.
     */
    explicit SetView(const View& view) noexcept
        : ValueView(const_cast<void*>(view.data()), view.schema()), _mutable_access(false) {}

    /**
     * @brief Get the number of elements.
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        return _schema->ops().size(_data, _schema);
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    /**
     * @brief Check if an element is in the set.
     */
    [[nodiscard]] bool contains(const View& value) const {
        assert(valid() && "contains() on invalid view");
        require_typed_view(value, element_type(), "Set element");
        return _schema->ops().contains(_data, value.data(), _schema);
    }

    /**
     * @brief Insert an element.
     *
     * @return true if the element was inserted (not already present)
     */
    bool add(const View& value) {
        assert(valid() && "add() on invalid view");
        require_mutable("add");
        require_typed_view(value, element_type(), "Set element");
        if (contains(value)) return false;
        _schema->ops().add(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Remove an element.
     *
     * @return true if the element was removed (was present)
     */
    bool remove(const View& value) {
        assert(valid() && "remove() on invalid view");
        require_mutable("remove");
        require_typed_view(value, element_type(), "Set element");
        if (!contains(value)) return false;
        _schema->ops().remove(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Clear all elements.
     */
    void clear() {
        assert(valid() && "clear() on invalid view");
        require_mutable("clear");
        if (_schema->ops().has_clear()) {
            _schema->ops().clear(data(), _schema);
        }
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->element_type;
    }

    // ========== Iteration ==========

    /**
     * @brief Const iterator for set views.
     *
     * Iterates over set elements in O(n) total time using index-based access.
     *
     * IMPORTANT: Stores data pointer and schema directly (NOT a view pointer)
     * to avoid dangling pointer issues when iterating over temporary views.
     */
    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = View;
        using difference_type = std::ptrdiff_t;
        using pointer = const View*;
        using reference = View;

        const_iterator() = default;
        const_iterator(const void* data, const TypeMeta* schema, size_t index, size_t /*size*/)
            : _data(data), _schema(schema), _index(index) {}

        reference operator*() const;

        const_iterator& operator++() {
            ++_index;
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const const_iterator& other) const {
            return _data == other._data && _index == other._index;
        }

        bool operator!=(const const_iterator& other) const {
            return !(*this == other);
        }

    private:
        const void* _data{nullptr};
        const TypeMeta* _schema{nullptr};
        size_t _index{0};
    };

    [[nodiscard]] const_iterator begin() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0);
        return const_iterator(_data, _schema, 0, size());
    }

    [[nodiscard]] const_iterator end() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0);
        size_t sz = size();
        return const_iterator(_data, _schema, sz, sz);
    }

    // Templated operations - implemented after Value
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const;

    template<typename T>
    bool add(const T& value);

    template<typename T>
    bool remove(const T& value);

private:
    void require_mutable(const char* method) const {
        if (!_mutable_access) {
            throw std::runtime_error(std::string("SetView::") + method +
                                     " requires mutable storage");
        }
    }

    bool _mutable_access{true};
};

// ============================================================================
// KeySetView - Read-only Set View Over Map Keys
// ============================================================================

/**
 * @brief Read-only set view over map keys.
 *
 * Provides the same interface as SetView's read-only operations for accessing map keys.
 * This allows unified set-like access to both actual sets and map key sets.
 *
 * @note This is a read-only view. Map keys cannot be modified through this view.
 */
class KeySetView : public View {
public:
    // ========== Construction ==========

    using View::View;

    /// Construct from a map view
    explicit KeySetView(const View& map_view)
        : View(map_view) {
        // Verify this is actually a map
        assert(map_view.is_map() && "KeySetView requires a map type");
    }

    // ========== Size ==========

    /**
     * @brief Get the number of keys (same as map size).
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        return _schema->ops().size(_data, _schema);
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    // ========== Contains ==========

    /**
     * @brief Check if a key exists in the map.
     */
    [[nodiscard]] bool contains(const View& key) const {
        assert(valid() && "contains() on invalid view");
        require_typed_view(key, element_type(), "Map key");
        return _schema->ops().contains(_data, key.data(), _schema);
    }

    /**
     * @brief Check if a typed key exists in the map.
     */
    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;  // Implemented after Value

    // ========== Type Info ==========

    /**
     * @brief Get the key type (element type for the key set).
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->key_type;
    }

    // ========== Iteration ==========

    /**
     * @brief Const iterator for key set view.
     *
     * Iterates over the keys stored in the underlying map.
     */
    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = View;
        using difference_type = std::ptrdiff_t;
        using pointer = const View*;
        using reference = View;

        const_iterator() = default;
        const_iterator(const KeySetView* view, size_t index)
            : _view(view), _index(index) {}

        reference operator*() const;

        const_iterator& operator++() {
            ++_index;
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const const_iterator& other) const {
            return _view == other._view && _index == other._index;
        }

        bool operator!=(const const_iterator& other) const {
            return !(*this == other);
        }

    private:
        const KeySetView* _view{nullptr};
        size_t _index{0};
    };

    [[nodiscard]] const_iterator begin() const {
        return const_iterator(this, 0);
    }

    [[nodiscard]] const_iterator end() const {
        return const_iterator(this, size());
    }
};

// ============================================================================
// MapView - Key-Value Operations (Mutable + Read-Only Mode)
// ============================================================================

/**
 * @brief Mutable view for map types.
 */
class MapView : public ValueView {
public:
    using ValueView::ValueView;

    MapView() noexcept = default;

    /**
     * @brief Construct a read-only MapView from a read-only view.
     *
     * Mutation methods will throw on this instance.
     */
    explicit MapView(const View& view) noexcept
        : ValueView(const_cast<void*>(view.data()), view.schema()), _mutable_access(false) {}

    /**
     * @brief Get the number of entries.
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        return _schema->ops().size(_data, _schema);
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    /**
     * @brief Get value by key (const).
     */
    [[nodiscard]] View at(const View& key) const {
        assert(valid() && "at() on invalid view");
        require_typed_view(key, key_type(), "Map key");
        const void* value_data = _schema->ops().map_at(_data, key.data(), _schema);
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        return View(value_data, _schema->element_type);
    }

    /**
     * @brief Get value by key (mutable).
     */
    [[nodiscard]] ValueView at(const View& key) {
        assert(valid() && "at() on invalid view");
        require_mutable("at");
        require_typed_view(key, key_type(), "Map key");
        void* value_data = const_cast<void*>(_schema->ops().map_at(data(), key.data(), _schema));
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        return ValueView(value_data, _schema->element_type);
    }

    /**
     * @brief Get value by key (const, operator[]).
     */
    [[nodiscard]] View operator[](const View& key) const {
        return at(key);
    }

    /**
     * @brief Get value by key (mutable, operator[]).
     */
    [[nodiscard]] ValueView operator[](const View& key) {
        return at(key);
    }

    /**
     * @brief Check if a key exists.
     */
    [[nodiscard]] bool contains(const View& key) const {
        assert(valid() && "contains() on invalid view");
        require_typed_view(key, key_type(), "Map key");
        return _schema->ops().contains(_data, key.data(), _schema);
    }

    /**
     * @brief Set value for key.
     */
    void set(const View& key, const View& value) {
        assert(valid() && "set() on invalid view");
        require_mutable("set");
        require_typed_view(key, key_type(), "Map key");
        require_typed_view(value, value_type(), "Map value");
        _schema->ops().set_item(data(), key.data(), value.data(), _schema);
    }

    /**
     * @brief Insert key-value pair.
     *
     * @return true if inserted (key was new)
     */
    bool add(const View& key, const View& value) {
        require_mutable("add");
        require_typed_view(key, key_type(), "Map key");
        require_typed_view(value, value_type(), "Map value");
        if (contains(key)) return false;
        set(key, value);
        return true;
    }

    /**
     * @brief Remove entry by key.
     *
     * @return true if removed (key existed)
     */
    bool remove(const View& key) {
        assert(valid() && "remove() on invalid view");
        require_mutable("remove");
        require_typed_view(key, key_type(), "Map key");
        if (!contains(key)) return false;
        _schema->ops().remove(data(), key.data(), _schema);
        return true;
    }

    /**
     * @brief Clear all entries.
     */
    void clear() {
        assert(valid() && "clear() on invalid view");
        require_mutable("clear");
        if (_schema->ops().has_clear()) {
            _schema->ops().clear(data(), _schema);
        }
    }

    /**
     * @brief Get the key type.
     */
    [[nodiscard]] const TypeMeta* key_type() const {
        return _schema->key_type;
    }

    /**
     * @brief Get the value type.
     */
    [[nodiscard]] const TypeMeta* value_type() const {
        return _schema->element_type;
    }

    // ========== Key Set View ==========

    /**
     * @brief Get a read-only set view over the map's keys.
     *
     * @return KeySetView over map keys
     */
    [[nodiscard]] KeySetView keys() const {
        return KeySetView(View(_data, _schema));
    }

    /**
     * @brief Get a SetView over the map's keys.
     *
     * Returns a SetView wrapping the underlying SetStorage, allowing
     * set operations (contains, iteration) on the key set.
     */
    [[nodiscard]] SetView key_set() const {
        auto* storage = static_cast<const MapStorage*>(_data);
        const TypeMeta* set_schema = TypeRegistry::instance().set(_schema->key_type).build();
        return SetView(View(static_cast<const void*>(&storage->as_set()), set_schema));
    }

    // ========== Items Iteration ==========

    struct kv_pair {
        View key;
        ValueView value;
    };

    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = kv_pair;
        using difference_type = std::ptrdiff_t;
        using pointer = const kv_pair*;
        using reference = kv_pair;

        items_iterator() = default;
        items_iterator(MapStorage* storage, KeySet::iterator it,
                       const TypeMeta* key_type, const TypeMeta* val_type)
            : storage_(storage), it_(it), key_type_(key_type), val_type_(val_type) {}

        reference operator*() const {
            size_t slot = *it_;
            return {
                View(storage_->key_at_slot(slot), key_type_),
                ValueView(storage_->value_at_slot(slot), val_type_)
            };
        }

        items_iterator& operator++() { ++it_; return *this; }
        items_iterator operator++(int) { auto tmp = *this; ++it_; return tmp; }
        bool operator==(const items_iterator& o) const { return it_ == o.it_; }
        bool operator!=(const items_iterator& o) const { return it_ != o.it_; }

    private:
        MapStorage* storage_{nullptr};
        KeySet::iterator it_;
        const TypeMeta* key_type_{nullptr};
        const TypeMeta* val_type_{nullptr};
    };

    struct items_range {
        items_iterator begin_, end_;
        items_iterator begin() const { return begin_; }
        items_iterator end() const { return end_; }
    };

    [[nodiscard]] items_range items() {
        require_mutable("items");
        auto* storage = static_cast<MapStorage*>(data());
        return {
            items_iterator(storage, storage->key_set().begin(),
                           _schema->key_type, _schema->element_type),
            items_iterator(storage, storage->key_set().end(),
                           _schema->key_type, _schema->element_type)
        };
    }

    // Templated operations - implemented after Value
    template<typename K>
    [[nodiscard]] View at(const K& key) const;

    template<typename K>
    [[nodiscard]] ValueView at(const K& key);

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;

    template<typename K, typename V>
    void set(const K& key, const V& value);

    template<typename K, typename V>
    bool add(const K& key, const V& value);

    template<typename K>
    bool remove(const K& key);

private:
    void require_mutable(const char* method) const {
        if (!_mutable_access) {
            throw std::runtime_error(std::string("MapView::") + method +
                                     " requires mutable storage");
        }
    }

    bool _mutable_access{true};
};

// ============================================================================
// View Conversion Implementations
// ============================================================================

// View conversions (safe versions)

inline std::optional<TupleView> View::try_as_tuple() const {
    if (!is_tuple()) return std::nullopt;
    return TupleView(*this);
}

inline std::optional<BundleView> View::try_as_bundle() const {
    if (!is_bundle()) return std::nullopt;
    return BundleView(*this);
}

inline std::optional<ListView> View::try_as_list() const {
    if (!is_list()) return std::nullopt;
    return ListView(*this);
}

inline std::optional<SetView> View::try_as_set() const {
    if (!is_set()) return std::nullopt;
    return SetView(*this);
}

inline std::optional<MapView> View::try_as_map() const {
    if (!is_map()) return std::nullopt;
    return MapView(*this);
}

inline std::optional<CyclicBufferView> View::try_as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) return std::nullopt;
    return CyclicBufferView(*this);
}

inline std::optional<QueueView> View::try_as_queue() const {
    if (!is_queue()) return std::nullopt;
    return QueueView(*this);
}

// View conversions (throwing versions)

inline TupleView View::as_tuple() const {
    if (!is_tuple()) {
        throw std::runtime_error("Not a tuple type");
    }
    return TupleView(*this);
}

inline BundleView View::as_bundle() const {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    return BundleView(*this);
}

inline ListView View::as_list() const {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    return ListView(*this);
}

inline SetView View::as_set() const {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    return SetView(*this);
}

inline MapView View::as_map() const {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    return MapView(*this);
}

inline CyclicBufferView View::as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    return CyclicBufferView(*this);
}

inline QueueView View::as_queue() const {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    return QueueView(*this);
}

// ValueView conversions (safe versions)

inline std::optional<TupleView> ValueView::try_as_tuple() {
    if (!is_tuple()) return std::nullopt;
    return is_mutable() ? TupleView(_mutable_data, _schema) : TupleView(View(_data, _schema));
}

inline std::optional<BundleView> ValueView::try_as_bundle() {
    if (!is_bundle()) return std::nullopt;
    return is_mutable() ? BundleView(_mutable_data, _schema) : BundleView(View(_data, _schema));
}

inline std::optional<ListView> ValueView::try_as_list() {
    if (!is_list()) return std::nullopt;
    return is_mutable() ? ListView(_mutable_data, _schema) : ListView(View(_data, _schema));
}

inline std::optional<SetView> ValueView::try_as_set() {
    if (!is_set()) return std::nullopt;
    return is_mutable() ? SetView(_mutable_data, _schema) : SetView(View(_data, _schema));
}

inline std::optional<MapView> ValueView::try_as_map() {
    if (!is_map()) return std::nullopt;
    return is_mutable() ? MapView(_mutable_data, _schema) : MapView(View(_data, _schema));
}

inline std::optional<CyclicBufferView> ValueView::try_as_cyclic_buffer() {
    if (!is_cyclic_buffer()) return std::nullopt;
    return is_mutable()
        ? CyclicBufferView(_mutable_data, _schema)
        : CyclicBufferView(View(_data, _schema));
}

inline std::optional<QueueView> ValueView::try_as_queue() {
    if (!is_queue()) return std::nullopt;
    return is_mutable() ? QueueView(_mutable_data, _schema) : QueueView(View(_data, _schema));
}

// ValueView conversions (throwing versions)

inline TupleView ValueView::as_tuple() {
    if (!is_tuple()) {
        throw std::runtime_error("Not a tuple type");
    }
    return is_mutable() ? TupleView(_mutable_data, _schema) : TupleView(View(_data, _schema));
}

inline BundleView ValueView::as_bundle() {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    return is_mutable() ? BundleView(_mutable_data, _schema) : BundleView(View(_data, _schema));
}

inline ListView ValueView::as_list() {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    return is_mutable() ? ListView(_mutable_data, _schema) : ListView(View(_data, _schema));
}

inline SetView ValueView::as_set() {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    return is_mutable() ? SetView(_mutable_data, _schema) : SetView(View(_data, _schema));
}

inline MapView ValueView::as_map() {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    return is_mutable() ? MapView(_mutable_data, _schema) : MapView(View(_data, _schema));
}

inline CyclicBufferView ValueView::as_cyclic_buffer() {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    return is_mutable()
        ? CyclicBufferView(_mutable_data, _schema)
        : CyclicBufferView(View(_data, _schema));
}

inline QueueView ValueView::as_queue() {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    return is_mutable() ? QueueView(_mutable_data, _schema) : QueueView(View(_data, _schema));
}

// ============================================================================
// CyclicBufferView Operations Implementation
// ============================================================================

inline void CyclicBufferView::push(const View& value) {
    require_mutable("push");
    CyclicBufferOps::push(data(), value.data(), _schema);
}

// ============================================================================
// QueueView Operations Implementation
// ============================================================================

inline void QueueView::push(const View& value) {
    require_mutable("push");
    QueueOps::push(data(), value.data(), _schema);
}

inline void QueueView::pop() {
    require_mutable("pop");
    QueueOps::pop(data(), _schema);
}

// ============================================================================
// SetView Iterator Implementation
// ============================================================================

inline View SetView::const_iterator::operator*() const {
    // Delegate to the ops layer's at() which iterates KeySet alive slots
    const void* elem = _schema->ops().at(_data, _index, _schema);
    return View(elem, _schema->element_type);
}

// ============================================================================
// KeySetView Iterator Implementation
// ============================================================================

inline View KeySetView::const_iterator::operator*() const {
    // Access the MapStorage to get the key at the current iteration position
    auto* storage = static_cast<const MapStorage*>(_view->data());

    if (_index >= storage->size()) {
        throw std::out_of_range("Key set iterator out of range");
    }

    // Iterate KeySet alive slots to find the n-th key
    auto it = storage->key_set().begin();
    std::advance(it, _index);
    size_t slot = *it;

    return View(storage->key_at_slot(slot), _view->element_type());
}

} // namespace hgraph::value
