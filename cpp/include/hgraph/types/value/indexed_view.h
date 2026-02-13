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
 * - SetView: Unique element collections (also used for map key views via key_set())
 * - MapView: Key-value collections
 * - CyclicBufferView: Fixed-size circular buffer
 * - QueueView: FIFO queue
 *
 * All views provide both const and non-const access through method overloads:
 * - at(index) / operator[](index) for element access
 * - size() for element count
 * - Iteration support via begin()/end()
 */

#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/composite_ops.h>
#include <hgraph/types/value/cyclic_buffer_ops.h>
#include <hgraph/types/value/queue_ops.h>
#ifndef NDEBUG
#include <execinfo.h>
#endif

#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string_view>

namespace hgraph::value {

// Forward declarations
template<typename Policy>
class Value;

// ============================================================================
// IndexedView - Base for Positional Access (merged const/mutable)
// ============================================================================

/**
 * @brief Base class for types supporting index-based access.
 *
 * Provides unified at()/operator[] interface for accessing elements by index.
 * Used as a base for tuples, bundles, lists, cyclic buffers, and queues.
 * Supports both const and non-const access through method overloads.
 */
class IndexedView : public View {
public:
    // ========== Construction ==========

    using View::View;

    /// Construct from base view
    explicit IndexedView(const View& view)
        : View(view) {}

    // ========== Size ==========

    /**
     * @brief Get the number of elements.
     * @return Element count
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        if (_schema->ops->size) {
            return _schema->ops->size(_data, _schema);
        }
        // For static structures like Bundle/Tuple, use field_count
        return _schema->field_count;
    }

    /**
     * @brief Check if empty.
     * @return true if size() == 0
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    // ========== Element Access ==========

    /**
     * @brief Get element at index (const).
     *
     * @param index The element index
     * @return View of the element
     * @throws std::out_of_range if index >= size()
     */
    [[nodiscard]] View at(size_t index) const {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        const void* elem_data = _schema->ops->get_at(_data, index, _schema);
        const TypeMeta* elem_schema = get_element_schema(index);
        View result(elem_data, elem_schema);
        propagate_path_with_index(result, index);
        return result;
    }

    /**
     * @brief Get element at index (mutable).
     *
     * @param index The element index
     * @return View of the element
     * @throws std::out_of_range if index >= size()
     */
    [[nodiscard]] View at(size_t index) {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        // Use const get_at and cast - we know we have mutable access
        void* elem_data = const_cast<void*>(_schema->ops->get_at(data(), index, _schema));
        const TypeMeta* elem_schema = get_element_schema(index);
        View result(elem_data, elem_schema);
        propagate_path_with_index(result, index);
        return result;
    }

    /**
     * @brief Get element at index (const, operator[]).
     *
     * Same as at() but uses operator syntax.
     *
     * @param index The element index
     * @return View of the element
     */
    [[nodiscard]] View operator[](size_t index) const {
        return at(index);
    }

    /**
     * @brief Get element at index (mutable, operator[]).
     */
    [[nodiscard]] View operator[](size_t index) {
        return at(index);
    }

    // ========== Mutation ==========

    /**
     * @brief Set element at index from a view.
     *
     * @param index The element index
     * @param value The value to set
     */
    void set(size_t index, const View& value) {
        assert(valid() && "set() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        _schema->ops->set_at(data(), index, value.data(), _schema);
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

protected:
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
// TupleView - Heterogeneous Index-Only Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for tuple types (merged const/mutable).
 *
 * Tuples are heterogeneous collections with index-only access.
 * Each element can have a different type.
 */
class TupleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the type of element at index.
     *
     * @param index The element index
     * @return The element's type schema
     */
    [[nodiscard]] const TypeMeta* element_type(size_t index) const {
        assert(valid() && index < size() && "Invalid index");
        return _schema->fields[index].type;
    }
};

// ============================================================================
// BundleView - Struct-like Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for bundle types (merged const/mutable).
 *
 * Bundles support both index-based and name-based field access.
 * Field order is significant.
 */
class BundleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // ========== Named Field Access ==========

    /**
     * @brief Get field by name (const).
     *
     * @param name The field name
     * @return View of the field
     * @throws std::runtime_error if field not found
     */
    [[nodiscard]] View at(std::string_view name) const {
        assert(valid() && "at(name) on invalid view");
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        const void* elem_data = _schema->ops->get_at(_data, idx, _schema);
        const TypeMeta* elem_schema = get_element_schema(idx);
        View result(elem_data, elem_schema);
        propagate_path_with_field(result, std::string(name));
        return result;
    }

    /**
     * @brief Get field by name (mutable).
     *
     * @param name The field name
     * @return View of the field
     * @throws std::runtime_error if field not found
     */
    [[nodiscard]] View at(std::string_view name) {
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        void* elem_data = const_cast<void*>(_schema->ops->get_at(data(), idx, _schema));
        const TypeMeta* elem_schema = get_element_schema(idx);
        View result(elem_data, elem_schema);
        propagate_path_with_field(result, std::string(name));
        return result;
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
    [[nodiscard]] View operator[](std::string_view name) {
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

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t field_count() const { return size(); }

    /**
     * @brief Get field info by index.
     *
     * @param index The field index
     * @return Pointer to field info
     */
    [[nodiscard]] const BundleFieldInfo* field_info(size_t index) const {
        assert(valid() && index < size() && "Invalid field index");
        return &_schema->fields[index];
    }

    /**
     * @brief Get field info by name.
     *
     * @param name The field name
     * @return Pointer to field info, or nullptr if not found
     */
    [[nodiscard]] const BundleFieldInfo* field_info(std::string_view name) const {
        size_t idx = field_index(name);
        return (idx < size()) ? &_schema->fields[idx] : nullptr;
    }

    /**
     * @brief Check if a field exists.
     *
     * @param name The field name
     * @return true if the field exists
     */
    [[nodiscard]] bool has_field(std::string_view name) const {
        return field_index(name) < size();
    }

    /**
     * @brief Get field index by name.
     *
     * @param name The field name
     * @return The field index, or size() if not found
     */
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

    /**
     * @brief Iterator yielding (name, view) pairs for bundle fields.
     */
    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<std::string_view, View>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = value_type;

        items_iterator() = default;
        items_iterator(const BundleView* view, size_t index)
            : _view(view), _index(index) {}

        reference operator*() const {
            return {_view->_schema->fields[_index].name, _view->at(_index)};
        }

        items_iterator& operator++() {
            ++_index;
            return *this;
        }

        items_iterator operator++(int) {
            items_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const items_iterator& other) const {
            return _view == other._view && _index == other._index;
        }

        bool operator!=(const items_iterator& other) const {
            return !(*this == other);
        }

    private:
        const BundleView* _view{nullptr};
        size_t _index{0};
    };

    /**
     * @brief Range for items iteration.
     */
    class items_range {
    public:
        items_range(const BundleView* view) : _view(view) {}

        [[nodiscard]] items_iterator begin() const {
            return items_iterator(_view, 0);
        }

        [[nodiscard]] items_iterator end() const {
            return items_iterator(_view, _view->size());
        }

    private:
        const BundleView* _view;
    };

    /**
     * @brief Iterate over (name, view) pairs.
     *
     * @code
     * for (auto [name, field] : bv.items()) {
     *     std::cout << name << ": " << field.to_string() << "\n";
     * }
     * @endcode
     */
    [[nodiscard]] items_range items() const {
        return items_range(this);
    }
};

// ============================================================================
// ListView - Indexed Collection Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for list types (merged const/mutable).
 *
 * Lists are homogeneous indexed collections. They can be fixed-size or dynamic.
 */
class ListView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the first element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the first element (mutable).
     */
    [[nodiscard]] View front() {
        return at(0);
    }

    /**
     * @brief Get the last element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the last element (mutable).
     */
    [[nodiscard]] View back() {
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

    // ========== Dynamic List Operations ==========

    /**
     * @brief Append an element.
     *
     * @throws std::runtime_error if the list is fixed-size or resize not supported
     */
    void append(const View& value) {
        if (is_fixed()) {
            throw std::runtime_error("Cannot append to fixed-size list");
        }
        if (!_schema->ops->resize) {
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
        if (temp_storage && elem_type && elem_type->ops) {
            elem_type->ops->construct(temp_storage, elem_type);
            elem_type->ops->copy_assign(temp_storage, value.data(), elem_type);
        }

        // Now resize - this may reallocate and potentially reuse freed memory
        size_t current_size = size();
        _schema->ops->resize(data(), current_size + 1, _schema);

        // Copy from our temp storage to the new element
        if (temp_storage && elem_type && elem_type->ops) {
            void* elem_ptr = ListOps::get_element_ptr(data(), current_size, _schema);
            elem_type->ops->copy_assign(elem_ptr, temp_storage, elem_type);
            elem_type->ops->destruct(temp_storage, elem_type);
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
        if (is_fixed()) {
            throw std::runtime_error("Cannot pop_back on fixed-size list");
        }
        if (empty()) {
            throw std::runtime_error("Cannot pop_back on empty list");
        }
        if (!_schema->ops->resize) {
            throw std::runtime_error("List type does not support resize operation");
        }
        // Resize to remove the last element
        _schema->ops->resize(data(), size() - 1, _schema);
    }

    /**
     * @brief Clear all elements.
     *
     * @throws std::runtime_error if the list is fixed-size
     */
    void clear() {
        if (is_fixed()) {
            throw std::runtime_error("Cannot clear fixed-size list");
        }
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Resize the list.
     *
     * @throws std::runtime_error if the list is fixed-size
     */
    void resize(size_t new_size) {
        if (is_fixed()) {
            throw std::runtime_error("Cannot resize fixed-size list");
        }
        if (_schema->ops->resize) {
            _schema->ops->resize(data(), new_size, _schema);
        }
    }

    /**
     * @brief Reset all elements to a sentinel value.
     *
     * Works on both fixed and dynamic lists.
     */
    void reset(const View& sentinel) {
        for (size_t i = 0; i < size(); ++i) {
            set(i, sentinel);
        }
    }

    /**
     * @brief Append a typed value.
     */
    template<typename T>
    void append(const T& value);  // Implemented after Value

    /**
     * @brief Reset with a typed sentinel.
     */
    template<typename T>
    void reset(const T& sentinel);  // Implemented after Value

    // ========== Items Iteration ==========

    /**
     * @brief Iterator yielding (index, view) pairs for list elements.
     */
    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<size_t, View>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = value_type;

        items_iterator() = default;
        items_iterator(const ListView* view, size_t index)
            : _view(view), _index(index) {}

        reference operator*() const {
            return {_index, _view->at(_index)};
        }

        items_iterator& operator++() {
            ++_index;
            return *this;
        }

        items_iterator operator++(int) {
            items_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const items_iterator& other) const {
            return _view == other._view && _index == other._index;
        }

        bool operator!=(const items_iterator& other) const {
            return !(*this == other);
        }

    private:
        const ListView* _view{nullptr};
        size_t _index{0};
    };

    /**
     * @brief Range for items iteration.
     */
    class items_range {
    public:
        items_range(const ListView* view) : _view(view) {}

        [[nodiscard]] items_iterator begin() const {
            return items_iterator(_view, 0);
        }

        [[nodiscard]] items_iterator end() const {
            return items_iterator(_view, _view->size());
        }

    private:
        const ListView* _view;
    };

    /**
     * @brief Iterate over (index, view) pairs.
     *
     * @code
     * for (auto [idx, elem] : lv.items()) {
     *     std::cout << idx << ": " << elem.to_string() << "\n";
     * }
     * @endcode
     */
    [[nodiscard]] items_range items() const {
        return items_range(this);
    }
};

// ============================================================================
// CyclicBufferView - Fixed-Size Circular Buffer Access (merged const/mutable)
// ============================================================================

// Forward declaration
struct CyclicBufferStorage;

/**
 * @brief View for cyclic buffer types (merged const/mutable).
 *
 * CyclicBuffer is a fixed-size circular buffer that re-centers on read.
 * Logical index 0 always refers to the oldest element.
 */
class CyclicBufferView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the oldest element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the oldest element (mutable).
     */
    [[nodiscard]] View front() {
        return at(0);
    }

    /**
     * @brief Get the newest element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the newest element (mutable).
     */
    [[nodiscard]] View back() {
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
     * @brief Append a value to the cyclic buffer.
     *
     * If the buffer is not full, adds at the end.
     * If the buffer is full, overwrites the oldest element.
     */
    void append(const View& value);

    /**
     * @brief Clear all elements from the buffer.
     */
    void clear() {
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Append a typed value.
     */
    template<typename T>
    void append(const T& value);  // Implemented after Value
};

// ============================================================================
// QueueView - FIFO Queue Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for queue types (merged const/mutable).
 *
 * Queue is a FIFO data structure with optional max capacity.
 * Elements are accessed in insertion order.
 */
class QueueView : public IndexedView {
public:
    using IndexedView::IndexedView;

    /**
     * @brief Get the front element (const).
     */
    [[nodiscard]] View front() const {
        return at(0);
    }

    /**
     * @brief Get the front element (mutable).
     */
    [[nodiscard]] View front() {
        return at(0);
    }

    /**
     * @brief Get the back element (const).
     */
    [[nodiscard]] View back() const {
        return at(size() - 1);
    }

    /**
     * @brief Get the back element (mutable).
     */
    [[nodiscard]] View back() {
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
     * @brief Append a value to the back of the queue.
     */
    void append(const View& value);

    /**
     * @brief Remove the front element.
     */
    void pop_front();

    /**
     * @brief Clear all elements from the queue.
     */
    void clear() {
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Append a typed value.
     */
    template<typename T>
    void append(const T& value);  // Implemented after Value
};

// ============================================================================
// SetView - Unique Element Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for set types (merged const/mutable).
 *
 * Sets are unordered collections of unique elements.
 * Also used to view map keys via key_set().
 */
class SetView : public View {
public:
    using View::View;

    /**
     * @brief Create a SetView over a map's keys.
     *
     * This creates a read-only SetView that iterates over the keys
     * of a MapStorage. The view uses the map's key_type as its element type.
     *
     * @param map_view A view pointing to MapStorage with map schema
     * @return SetView configured to iterate over map keys
     */
    static SetView from_map_keys(const View& map_view) {
        assert(map_view.is_map() && "from_map_keys requires a map type");
        SetView sv;
        // Use const_cast since this is a read-only view (add/remove/clear are disabled)
        sv._data = const_cast<void*>(static_cast<const void*>(map_view.data()));
        sv._schema = map_view.schema();
        sv._is_map_key_view = true;
        return sv;
    }

    /**
     * @brief Check if this is a read-only view over map keys.
     */
    [[nodiscard]] bool is_map_key_view() const {
        return _is_map_key_view;
    }

    /**
     * @brief Get the number of elements.
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        return _schema->ops->size(_data, _schema);
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
        return _schema->ops->contains(_data, value.data(), _schema);
    }

    /**
     * @brief Check if a typed value is in the set.
     */
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const;  // Implemented after Value

    /**
     * @brief Add an element.
     *
     * @return true if the element was added (not already present)
     * @note Not available for map key views (they are read-only)
     */
    bool add(const View& value) {
        assert(valid() && "add() on invalid view");
        assert(!_is_map_key_view && "Cannot modify map keys through SetView");
        if (contains(value)) return false;
        _schema->ops->insert(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Remove an element.
     *
     * @return true if the element was removed (was present)
     * @note Not available for map key views (they are read-only)
     */
    bool remove(const View& value) {
        assert(valid() && "remove() on invalid view");
        assert(!_is_map_key_view && "Cannot modify map keys through SetView");
        if (!contains(value)) return false;
        _schema->ops->erase(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Clear all elements.
     * @note Not available for map key views (they are read-only)
     */
    void clear() {
        assert(valid() && "clear() on invalid view");
        assert(!_is_map_key_view && "Cannot modify map keys through SetView");
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Get the element type.
     *
     * For regular sets, returns schema->element_type.
     * For map key views, returns schema->key_type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        if (_is_map_key_view) {
            return _schema->key_type;
        }
        return _schema->element_type;
    }

    // Templated operations - implemented after Value
    template<typename T>
    bool add(const T& value);

    template<typename T>
    bool remove(const T& value);

    // ========== Iteration ==========

    /**
     * @brief Const iterator for set views.
     *
     * Iterates over set elements in O(n) total time using index-based access.
     * Handles both regular SetStorage and MapStorage (for map key views).
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
        const_iterator(const void* data, const TypeMeta* schema, size_t index, size_t /*size*/, bool is_map_key_view = false)
            : _data(data), _schema(schema), _index(index), _is_map_key_view(is_map_key_view) {}

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
        bool _is_map_key_view{false};
    };

    [[nodiscard]] const_iterator begin() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0, false);
        return const_iterator(_data, _schema, 0, size(), _is_map_key_view);
    }

    [[nodiscard]] const_iterator end() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0, false);
        size_t sz = size();
        return const_iterator(_data, _schema, sz, sz, _is_map_key_view);
    }

private:
    bool _is_map_key_view{false};
};

// ============================================================================
// MapView - Key-Value Access (merged const/mutable)
// ============================================================================

/**
 * @brief View for map types (merged const/mutable).
 *
 * Maps are key-value pair collections.
 */
class MapView : public View {
public:
    using View::View;

    /**
     * @brief Get the number of entries.
     */
    [[nodiscard]] size_t size() const {
        assert(valid() && "size() on invalid view");
        return _schema->ops->size(_data, _schema);
    }

    /**
     * @brief Check if empty.
     */
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    /**
     * @brief Get value by key (const).
     *
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] View at(const View& key) const {
        assert(valid() && "at() on invalid view");
        const void* value_data = _schema->ops->map_get(_data, key.data(), _schema);
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        View result(value_data, _schema->element_type);
        // Use key's string representation in path
        propagate_path_with_field(result, "[" + key.to_string() + "]");
        return result;
    }

    /**
     * @brief Get value by key (mutable).
     *
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] View at(const View& key) {
        assert(valid() && "at() on invalid view");
        void* value_data = const_cast<void*>(_schema->ops->map_get(data(), key.data(), _schema));
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        View result(value_data, _schema->element_type);
        // Use key's string representation in path
        propagate_path_with_field(result, "[" + key.to_string() + "]");
        return result;
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
    [[nodiscard]] View operator[](const View& key) {
        return at(key);
    }

    /**
     * @brief Check if a key exists.
     */
    [[nodiscard]] bool contains(const View& key) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, key.data(), _schema);
    }

    /**
     * @brief Set value for key.
     */
    void set_item(const View& key, const View& value) {
        assert(valid() && "set_item() on invalid view");
        _schema->ops->map_set(data(), key.data(), value.data(), _schema);
    }

    /**
     * @brief Insert key-value pair.
     *
     * @return true if inserted (key was new)
     */
    bool insert(const View& key, const View& value) {
        if (contains(key)) return false;
        set_item(key, value);
        return true;
    }

    /**
     * @brief Remove entry by key.
     *
     * @return true if removed (key existed)
     */
    bool remove(const View& key) {
        assert(valid() && "remove() on invalid view");
        if (!contains(key)) return false;
        _schema->ops->erase(data(), key.data(), _schema);
        return true;
    }

    /**
     * @brief Clear all entries.
     */
    void clear() {
        assert(valid() && "clear() on invalid view");
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
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
     * @return SetView configured to iterate over map keys
     */
    [[nodiscard]] SetView key_set() const {
        return SetView::from_map_keys(View(_data, _schema));
    }

    /**
     * @brief Get a read-only set view over the map's keys.
     *
     * @return SetView configured to iterate over map keys
     * @note Alias for key_set() for convenience
     */
    [[nodiscard]] SetView keys() const {
        return key_set();
    }

    // Templated operations - implemented after Value
    template<typename K>
    [[nodiscard]] View at(const K& key) const;

    template<typename K>
    [[nodiscard]] View at(const K& key);

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;

    template<typename K, typename V>
    void set_item(const K& key, const V& value);

    template<typename K, typename V>
    bool insert(const K& key, const V& value);

    template<typename K>
    bool remove(const K& key);

    // ========== Items Iteration ==========

    /**
     * @brief Iterator yielding (key, value) pairs for map entries.
     */
    class items_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<View, View>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = value_type;

        items_iterator() = default;
        items_iterator(const void* data, const TypeMeta* schema, size_t index)
            : _data(data), _schema(schema), _index(index) {}

        reference operator*() const;  // Implemented after MapStorage is available

        items_iterator& operator++() {
            ++_index;
            return *this;
        }

        items_iterator operator++(int) {
            items_iterator tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const items_iterator& other) const {
            return _data == other._data && _index == other._index;
        }

        bool operator!=(const items_iterator& other) const {
            return !(*this == other);
        }

    private:
        const void* _data{nullptr};
        const TypeMeta* _schema{nullptr};
        size_t _index{0};
    };

    /**
     * @brief Range for items iteration.
     */
    class items_range {
    public:
        items_range(const MapView* view) : _view(view) {}

        [[nodiscard]] items_iterator begin() const {
            return items_iterator(_view->data(), _view->schema(), 0);
        }

        [[nodiscard]] items_iterator end() const {
            return items_iterator(_view->data(), _view->schema(), _view->size());
        }

    private:
        const MapView* _view;
    };

    /**
     * @brief Iterate over (key, value) pairs.
     *
     * @code
     * for (auto [key, value] : mv.items()) {
     *     std::cout << key.as<std::string>() << ": " << value.as<int>() << "\n";
     * }
     * @endcode
     */
    [[nodiscard]] items_range items() const {
        return items_range(this);
    }
};

// ============================================================================
// View Conversion Implementations
// ============================================================================

// View conversions (safe versions - const)

inline std::optional<TupleView> View::try_as_tuple() const {
    if (!is_tuple()) return std::nullopt;
    TupleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<BundleView> View::try_as_bundle() const {
    if (!is_bundle()) return std::nullopt;
    BundleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<ListView> View::try_as_list() const {
    if (!is_list()) return std::nullopt;
    ListView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<SetView> View::try_as_set() const {
    if (!is_set()) return std::nullopt;
    SetView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<MapView> View::try_as_map() const {
    if (!is_map()) return std::nullopt;
    MapView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<CyclicBufferView> View::try_as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) return std::nullopt;
    CyclicBufferView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<QueueView> View::try_as_queue() const {
    if (!is_queue()) return std::nullopt;
    QueueView result(_data, _schema);
    copy_path_to(result);
    return result;
}

// View conversions (throwing versions - const)

inline TupleView View::as_tuple() const {
    if (!is_tuple()) {
        #ifndef NDEBUG
        fprintf(stderr, "[AS_TUPLE] FAIL: data=%p schema=%p schema_kind=%d schema_name=%s\n",
                _data, (void*)_schema, _schema ? static_cast<int>(_schema->kind) : -1,
                _schema && _schema->name ? _schema->name : "<null>");
        // Print stack trace for debugging
        void* callstack[20];
        int frames = backtrace(callstack, 20);
        char** strs = backtrace_symbols(callstack, frames);
        for (int i = 0; i < frames && i < 8; ++i) {
            fprintf(stderr, "  [%d] %s\n", i, strs[i]);
        }
        free(strs);
        #endif
        throw std::runtime_error("Not a tuple type");
    }
    TupleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline BundleView View::as_bundle() const {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    BundleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline ListView View::as_list() const {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    ListView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline SetView View::as_set() const {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    SetView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline MapView View::as_map() const {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    MapView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline CyclicBufferView View::as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    CyclicBufferView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline QueueView View::as_queue() const {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    QueueView result(_data, _schema);
    copy_path_to(result);
    return result;
}

// View conversions (safe versions - mutable)

inline std::optional<TupleView> View::try_as_tuple() {
    if (!is_tuple()) return std::nullopt;
    TupleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<BundleView> View::try_as_bundle() {
    if (!is_bundle()) return std::nullopt;
    BundleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<ListView> View::try_as_list() {
    if (!is_list()) return std::nullopt;
    ListView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<SetView> View::try_as_set() {
    if (!is_set()) return std::nullopt;
    SetView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<MapView> View::try_as_map() {
    if (!is_map()) return std::nullopt;
    MapView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<CyclicBufferView> View::try_as_cyclic_buffer() {
    if (!is_cyclic_buffer()) return std::nullopt;
    CyclicBufferView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline std::optional<QueueView> View::try_as_queue() {
    if (!is_queue()) return std::nullopt;
    QueueView result(_data, _schema);
    copy_path_to(result);
    return result;
}

// View conversions (throwing versions - mutable)

inline TupleView View::as_tuple() {
    if (!is_tuple()) {
        throw std::runtime_error("Not a tuple type");
    }
    TupleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline BundleView View::as_bundle() {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    BundleView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline ListView View::as_list() {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    ListView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline SetView View::as_set() {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    SetView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline MapView View::as_map() {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    MapView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline CyclicBufferView View::as_cyclic_buffer() {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    CyclicBufferView result(_data, _schema);
    copy_path_to(result);
    return result;
}

inline QueueView View::as_queue() {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    QueueView result(_data, _schema);
    copy_path_to(result);
    return result;
}

// ============================================================================
// CyclicBufferView Operations Implementation
// ============================================================================

inline void CyclicBufferView::append(const View& value) {
    CyclicBufferOps::push_back(data(), value.data(), _schema);
}

// ============================================================================
// QueueView Operations Implementation
// ============================================================================

inline void QueueView::append(const View& value) {
    QueueOps::push_back(data(), value.data(), _schema);
}

inline void QueueView::pop_front() {
    QueueOps::pop_front(data(), _schema);
}

// ============================================================================
// SetView Iterator Implementation
// ============================================================================

inline View SetView::const_iterator::operator*() const {
    // Use get_at which works uniformly for both sets and map key views:
    // - For sets: returns element at index
    // - For maps: returns key at index
    const void* elem = _schema->ops->get_at(_data, _index, _schema);
    if (!elem) {
        throw std::out_of_range("Set iterator out of range");
    }
    // For regular sets: element_type is the set element type
    // For map key views: key_type is the key type (but we use element_type here,
    // and SetView::element_type() handles returning the correct one)
    const TypeMeta* elem_type = _is_map_key_view ? _schema->key_type : _schema->element_type;
    return View(elem, elem_type);
}

// ============================================================================
// MapView Items Iterator Implementation
// ============================================================================

inline MapView::items_iterator::reference MapView::items_iterator::operator*() const {
    // Access the MapStorage to get key and value at the current iteration position
    auto* storage = static_cast<const MapStorage*>(_data);

    auto* index_set = storage->key_set().index_set();
    if (!index_set || _index >= index_set->size()) {
        throw std::out_of_range("Map items iterator out of range");
    }

    // Get the storage index at this iteration position
    auto it = index_set->begin();
    std::advance(it, _index);
    size_t storage_idx = *it;

    // Get key and value pointers
    const void* key_ptr = storage->key_at_slot(storage_idx);
    const void* val_ptr = storage->value_at_slot(storage_idx);

    // Return pair of views
    return {View(key_ptr, _schema->key_type), View(val_ptr, _schema->element_type)};
}

} // namespace hgraph::value
