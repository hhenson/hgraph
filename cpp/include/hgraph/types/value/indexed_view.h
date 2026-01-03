#pragma once

/**
 * @file indexed_view.h
 * @brief Indexed view classes for positional access.
 *
 * This header defines views for types that support positional (index-based) access:
 * - ConstIndexedView / IndexedView: Base classes for indexed access
 * - ConstTupleView / TupleView: Heterogeneous indexed collections
 * - ConstBundleView / BundleView: Struct-like types with named + indexed access
 * - ConstListView / ListView: Homogeneous indexed collections
 * - ConstSetView / SetView: Unique element collections
 * - ConstMapView / MapView: Key-value collections
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

// Forward declarations
template<typename Policy>
class Value;

// ============================================================================
// ConstIndexedView - Base for Positional Access
// ============================================================================

/**
 * @brief Base class for types supporting const index-based access.
 *
 * Provides unified at()/operator[] interface for accessing elements by index.
 * Used as a base for tuples, bundles, and lists.
 */
class ConstIndexedView : public ConstValueView {
public:
    // ========== Construction ==========

    using ConstValueView::ConstValueView;

    /// Construct from base view
    explicit ConstIndexedView(const ConstValueView& view)
        : ConstValueView(view) {}

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
     * @return Const view of the element
     * @throws std::out_of_range if index >= size()
     */
    [[nodiscard]] ConstValueView at(size_t index) const {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        const void* elem_data = _schema->ops->get_at(_data, index, _schema);
        // Determine element type
        const TypeMeta* elem_schema = nullptr;
        if (_schema->kind == TypeKind::List || _schema->kind == TypeKind::Set ||
            _schema->kind == TypeKind::CyclicBuffer || _schema->kind == TypeKind::Queue) {
            elem_schema = _schema->element_type;
        } else if (_schema->kind == TypeKind::Bundle || _schema->kind == TypeKind::Tuple) {
            elem_schema = _schema->fields[index].type;
        }
        return ConstValueView(elem_data, elem_schema);
    }

    /**
     * @brief Get element at index (const, operator[]).
     *
     * Same as at() but uses operator syntax.
     *
     * @param index The element index
     * @return Const view of the element
     */
    [[nodiscard]] ConstValueView operator[](size_t index) const {
        return at(index);
    }

    // ========== Iteration ==========

    /**
     * @brief Const iterator for indexed views.
     */
    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = ConstValueView;
        using difference_type = std::ptrdiff_t;
        using pointer = const ConstValueView*;
        using reference = ConstValueView;

        const_iterator() = default;
        const_iterator(const ConstIndexedView* view, size_t index)
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
        const ConstIndexedView* _view{nullptr};
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
// IndexedView - Mutable Positional Access
// ============================================================================

/**
 * @brief Base class for types supporting mutable index-based access.
 */
class IndexedView : public ValueView {
public:
    // ========== Construction ==========

    using ValueView::ValueView;

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
        if (_schema->ops->size) {
            return _schema->ops->size(_data, _schema);
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
    [[nodiscard]] ConstValueView at(size_t index) const {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        const void* elem_data = _schema->ops->get_at(_data, index, _schema);
        const TypeMeta* elem_schema = get_element_schema(index);
        return ConstValueView(elem_data, elem_schema);
    }

    /**
     * @brief Get element at index (mutable).
     */
    [[nodiscard]] ValueView at(size_t index) {
        assert(valid() && "at() on invalid view");
        if (index >= size()) {
            throw std::out_of_range("Index out of range");
        }
        // Use const get_at and cast - we know we have mutable access
        void* elem_data = const_cast<void*>(_schema->ops->get_at(data(), index, _schema));
        const TypeMeta* elem_schema = get_element_schema(index);
        return ValueView(elem_data, elem_schema);
    }

    /**
     * @brief Get element at index (const, operator[]).
     */
    [[nodiscard]] ConstValueView operator[](size_t index) const {
        return at(index);
    }

    /**
     * @brief Get element at index (mutable, operator[]).
     */
    [[nodiscard]] ValueView operator[](size_t index) {
        return at(index);
    }

    // ========== Mutation ==========

    /**
     * @brief Set element at index from a view.
     *
     * @param index The element index
     * @param value The value to set
     */
    void set(size_t index, const ConstValueView& value) {
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
// ConstTupleView - Heterogeneous Index-Only Access
// ============================================================================

/**
 * @brief Const view for tuple types.
 *
 * Tuples are heterogeneous collections with index-only access.
 * Each element can have a different type.
 */
class ConstTupleView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

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
// TupleView - Mutable Heterogeneous Access
// ============================================================================

/**
 * @brief Mutable view for tuple types.
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
// ConstBundleView - Struct-like Access
// ============================================================================

/**
 * @brief Const view for bundle types.
 *
 * Bundles support both index-based and name-based field access.
 * Field order is significant.
 */
class ConstBundleView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    // ========== Named Field Access ==========

    /**
     * @brief Get field by name.
     *
     * @param name The field name
     * @return Const view of the field
     * @throws std::runtime_error if field not found
     */
    [[nodiscard]] ConstValueView at(std::string_view name) const {
        assert(valid() && "at(name) on invalid view");
        size_t idx = field_index(name);
        if (idx >= size()) {
            throw std::runtime_error("Field not found: " + std::string(name));
        }
        return ConstIndexedView::at(idx);
    }

    /**
     * @brief Get field by name (operator[]).
     */
    [[nodiscard]] ConstValueView operator[](std::string_view name) const {
        return at(name);
    }

    // Bring base class operator[] into scope
    using ConstIndexedView::operator[];

    // ========== Field Metadata ==========

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t field_count() const {
        return size();
    }

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
};

// ============================================================================
// BundleView - Mutable Struct-like Access
// ============================================================================

/**
 * @brief Mutable view for bundle types.
 */
class BundleView : public IndexedView {
public:
    using IndexedView::IndexedView;

    // ========== Named Field Access (Const) ==========

    /**
     * @brief Get field by name (const).
     */
    [[nodiscard]] ConstValueView at(std::string_view name) const {
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
    [[nodiscard]] ConstValueView operator[](std::string_view name) const {
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
    void set(std::string_view name, const ConstValueView& value) {
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
};

// ============================================================================
// ConstListView - Indexed Collection Access
// ============================================================================

/**
 * @brief Const view for list types.
 *
 * Lists are homogeneous indexed collections. They can be fixed-size or dynamic.
 */
class ConstListView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    /**
     * @brief Get the first element.
     */
    [[nodiscard]] ConstValueView front() const {
        return at(0);
    }

    /**
     * @brief Get the last element.
     */
    [[nodiscard]] ConstValueView back() const {
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
};

// ============================================================================
// ListView - Mutable Indexed Collection
// ============================================================================

/**
 * @brief Mutable view for list types.
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
     * @brief Get the last element (mutable).
     */
    [[nodiscard]] ValueView back() {
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
    void push_back(const ConstValueView& value) {
        if (is_fixed()) {
            throw std::runtime_error("Cannot push_back on fixed-size list");
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
    void reset(const ConstValueView& sentinel) {
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
// ConstCyclicBufferView - Fixed-Size Circular Buffer Access
// ============================================================================

// Forward declaration
struct CyclicBufferStorage;

/**
 * @brief Const view for cyclic buffer types.
 *
 * CyclicBuffer is a fixed-size circular buffer that re-centers on read.
 * Logical index 0 always refers to the oldest element.
 */
class ConstCyclicBufferView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    /**
     * @brief Get the oldest element.
     */
    [[nodiscard]] ConstValueView front() const {
        return at(0);
    }

    /**
     * @brief Get the newest element.
     */
    [[nodiscard]] ConstValueView back() const {
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
};

// ============================================================================
// CyclicBufferView - Mutable Fixed-Size Circular Buffer
// ============================================================================

/**
 * @brief Mutable view for cyclic buffer types.
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
     * @brief Get the newest element (mutable).
     */
    [[nodiscard]] ValueView back() {
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
    void push_back(const ConstValueView& value);

    /**
     * @brief Clear all elements from the buffer.
     */
    void clear() {
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Push a typed value.
     */
    template<typename T>
    void push_back(const T& value);  // Implemented after Value
};

// ============================================================================
// ConstQueueView - FIFO Queue Access
// ============================================================================

/**
 * @brief Const view for queue types.
 *
 * Queue is a FIFO data structure with optional max capacity.
 * Elements are accessed in insertion order.
 */
class ConstQueueView : public ConstIndexedView {
public:
    using ConstIndexedView::ConstIndexedView;

    /**
     * @brief Get the front element (first in queue).
     */
    [[nodiscard]] ConstValueView front() const {
        return at(0);
    }

    /**
     * @brief Get the back element (last in queue).
     */
    [[nodiscard]] ConstValueView back() const {
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
};

// ============================================================================
// QueueView - Mutable FIFO Queue
// ============================================================================

/**
 * @brief Mutable view for queue types.
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
     * @brief Get the back element (mutable).
     */
    [[nodiscard]] ValueView back() {
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
    void push_back(const ConstValueView& value);

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
     * @brief Push a typed value.
     */
    template<typename T>
    void push_back(const T& value);  // Implemented after Value
};

// ============================================================================
// ConstSetView - Unique Element Access
// ============================================================================

/**
 * @brief Const view for set types.
 */
class ConstSetView : public ConstValueView {
public:
    using ConstValueView::ConstValueView;

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
    [[nodiscard]] bool contains(const ConstValueView& value) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, value.data(), _schema);
    }

    /**
     * @brief Check if a typed value is in the set.
     */
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const;  // Implemented after Value

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
        using value_type = ConstValueView;
        using difference_type = std::ptrdiff_t;
        using pointer = const ConstValueView*;
        using reference = ConstValueView;

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
};

// ============================================================================
// SetView - Mutable Set Operations
// ============================================================================

/**
 * @brief Mutable view for set types.
 */
class SetView : public ValueView {
public:
    using ValueView::ValueView;

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
    [[nodiscard]] bool contains(const ConstValueView& value) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, value.data(), _schema);
    }

    /**
     * @brief Insert an element.
     *
     * @return true if the element was inserted (not already present)
     */
    bool insert(const ConstValueView& value) {
        assert(valid() && "insert() on invalid view");
        if (contains(value)) return false;
        _schema->ops->insert(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Remove an element.
     *
     * @return true if the element was removed (was present)
     */
    bool erase(const ConstValueView& value) {
        assert(valid() && "erase() on invalid view");
        if (!contains(value)) return false;
        _schema->ops->erase(data(), value.data(), _schema);
        return true;
    }

    /**
     * @brief Clear all elements.
     */
    void clear() {
        assert(valid() && "clear() on invalid view");
        if (_schema->ops->clear) {
            _schema->ops->clear(data(), _schema);
        }
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const {
        return _schema->element_type;
    }

    // Templated operations - implemented after Value
    template<typename T>
    [[nodiscard]] bool contains(const T& value) const;

    template<typename T>
    bool insert(const T& value);

    template<typename T>
    bool erase(const T& value);

    // ========== Iteration ==========

    // Reuse ConstSetView's const_iterator for iteration
    using const_iterator = ConstSetView::const_iterator;

    [[nodiscard]] const_iterator begin() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0);
        return const_iterator(_data, _schema, 0, size());
    }

    [[nodiscard]] const_iterator end() const {
        if (!valid()) return const_iterator(nullptr, nullptr, 0, 0);
        size_t sz = size();
        return const_iterator(_data, _schema, sz, sz);
    }
};

// ============================================================================
// ConstKeySetView - Read-only Set View Over Map Keys
// ============================================================================

/**
 * @brief Read-only set view over map keys.
 *
 * Provides the same interface as ConstSetView for accessing map keys.
 * This allows unified set-like access to both actual sets and map key sets.
 *
 * @note This is a read-only view. Map keys cannot be modified through this view.
 */
class ConstKeySetView : public ConstValueView {
public:
    // ========== Construction ==========

    using ConstValueView::ConstValueView;

    /// Construct from a ConstMapView
    explicit ConstKeySetView(const ConstValueView& map_view)
        : ConstValueView(map_view) {
        // Verify this is actually a map
        assert(map_view.is_map() && "ConstKeySetView requires a map type");
    }

    // ========== Size ==========

    /**
     * @brief Get the number of keys (same as map size).
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

    // ========== Contains ==========

    /**
     * @brief Check if a key exists in the map.
     */
    [[nodiscard]] bool contains(const ConstValueView& key) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, key.data(), _schema);
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
        using value_type = ConstValueView;
        using difference_type = std::ptrdiff_t;
        using pointer = const ConstValueView*;
        using reference = ConstValueView;

        const_iterator() = default;
        const_iterator(const ConstKeySetView* view, size_t index)
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
        const ConstKeySetView* _view{nullptr};
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
// ConstMapView - Key-Value Access
// ============================================================================

/**
 * @brief Const view for map types.
 */
class ConstMapView : public ConstValueView {
public:
    using ConstValueView::ConstValueView;

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
     * @brief Get value by key.
     *
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] ConstValueView at(const ConstValueView& key) const {
        assert(valid() && "at() on invalid view");
        const void* value_data = _schema->ops->map_get(_data, key.data(), _schema);
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        return ConstValueView(value_data, _schema->element_type);
    }

    /**
     * @brief Get value by key (operator[]).
     */
    [[nodiscard]] ConstValueView operator[](const ConstValueView& key) const {
        return at(key);
    }

    /**
     * @brief Check if a key exists.
     */
    [[nodiscard]] bool contains(const ConstValueView& key) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, key.data(), _schema);
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
     * @return ConstKeySetView with same interface as ConstSetView
     */
    [[nodiscard]] ConstKeySetView keys() const {
        return ConstKeySetView(*this);
    }

    // Templated operations - implemented after Value
    template<typename K>
    [[nodiscard]] ConstValueView at(const K& key) const;

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;
};

// ============================================================================
// MapView - Mutable Key-Value Operations
// ============================================================================

/**
 * @brief Mutable view for map types.
 */
class MapView : public ValueView {
public:
    using ValueView::ValueView;

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
     */
    [[nodiscard]] ConstValueView at(const ConstValueView& key) const {
        assert(valid() && "at() on invalid view");
        const void* value_data = _schema->ops->map_get(_data, key.data(), _schema);
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        return ConstValueView(value_data, _schema->element_type);
    }

    /**
     * @brief Get value by key (mutable).
     */
    [[nodiscard]] ValueView at(const ConstValueView& key) {
        assert(valid() && "at() on invalid view");
        void* value_data = const_cast<void*>(_schema->ops->map_get(data(), key.data(), _schema));
        if (!value_data) {
            throw std::runtime_error("Key not found");
        }
        return ValueView(value_data, _schema->element_type);
    }

    /**
     * @brief Get value by key (const, operator[]).
     */
    [[nodiscard]] ConstValueView operator[](const ConstValueView& key) const {
        return at(key);
    }

    /**
     * @brief Get value by key (mutable, operator[]).
     */
    [[nodiscard]] ValueView operator[](const ConstValueView& key) {
        return at(key);
    }

    /**
     * @brief Check if a key exists.
     */
    [[nodiscard]] bool contains(const ConstValueView& key) const {
        assert(valid() && "contains() on invalid view");
        return _schema->ops->contains(_data, key.data(), _schema);
    }

    /**
     * @brief Set value for key.
     */
    void set(const ConstValueView& key, const ConstValueView& value) {
        assert(valid() && "set() on invalid view");
        _schema->ops->map_set(data(), key.data(), value.data(), _schema);
    }

    /**
     * @brief Insert key-value pair.
     *
     * @return true if inserted (key was new)
     */
    bool insert(const ConstValueView& key, const ConstValueView& value) {
        if (contains(key)) return false;
        set(key, value);
        return true;
    }

    /**
     * @brief Remove entry by key.
     *
     * @return true if removed (key existed)
     */
    bool erase(const ConstValueView& key) {
        assert(valid() && "erase() on invalid view");
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
     * @return ConstKeySetView with same interface as ConstSetView
     */
    [[nodiscard]] ConstKeySetView keys() const {
        return ConstKeySetView(ConstValueView(_data, _schema));
    }

    // Templated operations - implemented after Value
    template<typename K>
    [[nodiscard]] ConstValueView at(const K& key) const;

    template<typename K>
    [[nodiscard]] ValueView at(const K& key);

    template<typename K>
    [[nodiscard]] bool contains(const K& key) const;

    template<typename K, typename V>
    void set(const K& key, const V& value);

    template<typename K, typename V>
    bool insert(const K& key, const V& value);

    template<typename K>
    bool erase(const K& key);
};

// ============================================================================
// View Conversion Implementations
// ============================================================================

// ConstValueView conversions (safe versions)

inline std::optional<ConstTupleView> ConstValueView::try_as_tuple() const {
    if (!is_tuple()) return std::nullopt;
    return ConstTupleView(_data, _schema);
}

inline std::optional<ConstBundleView> ConstValueView::try_as_bundle() const {
    if (!is_bundle()) return std::nullopt;
    return ConstBundleView(_data, _schema);
}

inline std::optional<ConstListView> ConstValueView::try_as_list() const {
    if (!is_list()) return std::nullopt;
    return ConstListView(_data, _schema);
}

inline std::optional<ConstSetView> ConstValueView::try_as_set() const {
    if (!is_set()) return std::nullopt;
    return ConstSetView(_data, _schema);
}

inline std::optional<ConstMapView> ConstValueView::try_as_map() const {
    if (!is_map()) return std::nullopt;
    return ConstMapView(_data, _schema);
}

inline std::optional<ConstCyclicBufferView> ConstValueView::try_as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) return std::nullopt;
    return ConstCyclicBufferView(_data, _schema);
}

inline std::optional<ConstQueueView> ConstValueView::try_as_queue() const {
    if (!is_queue()) return std::nullopt;
    return ConstQueueView(_data, _schema);
}

// ConstValueView conversions (throwing versions)

inline ConstTupleView ConstValueView::as_tuple() const {
    if (!is_tuple()) {
        throw std::runtime_error("Not a tuple type");
    }
    return ConstTupleView(_data, _schema);
}

inline ConstBundleView ConstValueView::as_bundle() const {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    return ConstBundleView(_data, _schema);
}

inline ConstListView ConstValueView::as_list() const {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    return ConstListView(_data, _schema);
}

inline ConstSetView ConstValueView::as_set() const {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    return ConstSetView(_data, _schema);
}

inline ConstMapView ConstValueView::as_map() const {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    return ConstMapView(_data, _schema);
}

inline ConstCyclicBufferView ConstValueView::as_cyclic_buffer() const {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    return ConstCyclicBufferView(_data, _schema);
}

inline ConstQueueView ConstValueView::as_queue() const {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    return ConstQueueView(_data, _schema);
}

// ValueView conversions (safe versions)

inline std::optional<TupleView> ValueView::try_as_tuple() {
    if (!is_tuple()) return std::nullopt;
    return TupleView(_mutable_data, _schema);
}

inline std::optional<BundleView> ValueView::try_as_bundle() {
    if (!is_bundle()) return std::nullopt;
    return BundleView(_mutable_data, _schema);
}

inline std::optional<ListView> ValueView::try_as_list() {
    if (!is_list()) return std::nullopt;
    return ListView(_mutable_data, _schema);
}

inline std::optional<SetView> ValueView::try_as_set() {
    if (!is_set()) return std::nullopt;
    return SetView(_mutable_data, _schema);
}

inline std::optional<MapView> ValueView::try_as_map() {
    if (!is_map()) return std::nullopt;
    return MapView(_mutable_data, _schema);
}

inline std::optional<CyclicBufferView> ValueView::try_as_cyclic_buffer() {
    if (!is_cyclic_buffer()) return std::nullopt;
    return CyclicBufferView(_mutable_data, _schema);
}

inline std::optional<QueueView> ValueView::try_as_queue() {
    if (!is_queue()) return std::nullopt;
    return QueueView(_mutable_data, _schema);
}

// ValueView conversions (throwing versions)

inline TupleView ValueView::as_tuple() {
    if (!is_tuple()) {
        throw std::runtime_error("Not a tuple type");
    }
    return TupleView(_mutable_data, _schema);
}

inline BundleView ValueView::as_bundle() {
    if (!is_bundle()) {
        throw std::runtime_error("Not a bundle type");
    }
    return BundleView(_mutable_data, _schema);
}

inline ListView ValueView::as_list() {
    if (!is_list()) {
        throw std::runtime_error("Not a list type");
    }
    return ListView(_mutable_data, _schema);
}

inline SetView ValueView::as_set() {
    if (!is_set()) {
        throw std::runtime_error("Not a set type");
    }
    return SetView(_mutable_data, _schema);
}

inline MapView ValueView::as_map() {
    if (!is_map()) {
        throw std::runtime_error("Not a map type");
    }
    return MapView(_mutable_data, _schema);
}

inline CyclicBufferView ValueView::as_cyclic_buffer() {
    if (!is_cyclic_buffer()) {
        throw std::runtime_error("Not a cyclic buffer type");
    }
    return CyclicBufferView(_mutable_data, _schema);
}

inline QueueView ValueView::as_queue() {
    if (!is_queue()) {
        throw std::runtime_error("Not a queue type");
    }
    return QueueView(_mutable_data, _schema);
}

// ============================================================================
// CyclicBufferView Operations Implementation
// ============================================================================

inline void CyclicBufferView::push_back(const ConstValueView& value) {
    CyclicBufferOps::push_back(data(), value.data(), _schema);
}

// ============================================================================
// QueueView Operations Implementation
// ============================================================================

inline void QueueView::push_back(const ConstValueView& value) {
    QueueOps::push_back(data(), value.data(), _schema);
}

inline void QueueView::pop_front() {
    QueueOps::pop_front(data(), _schema);
}

// ============================================================================
// ConstSetView Iterator Implementation
// ============================================================================

inline ConstValueView ConstSetView::const_iterator::operator*() const {
    // Access the SetStorage to get the element at the current iteration position
    auto* storage = static_cast<const SetStorage*>(_data);

    if (!storage->index_set || _index >= storage->index_set->size()) {
        throw std::out_of_range("Set iterator out of range");
    }

    // ankerl::unordered_dense::set supports random access via its vector backend
    auto it = storage->index_set->begin();
    std::advance(it, _index);
    size_t storage_idx = *it;

    // Return a view of the element at this storage index
    return ConstValueView(storage->get_element_ptr(storage_idx), _schema->element_type);
}

// ============================================================================
// ConstKeySetView Iterator Implementation
// ============================================================================

inline ConstValueView ConstKeySetView::const_iterator::operator*() const {
    // Access the MapStorage to get the key at the current iteration position
    auto* storage = static_cast<const MapStorage*>(_view->data());

    if (!storage->index_set() || _index >= storage->index_set()->size()) {
        throw std::out_of_range("Key set iterator out of range");
    }

    // Get the storage index at this iteration position
    auto it = storage->index_set()->begin();
    std::advance(it, _index);
    size_t storage_idx = *it;

    // Return a view of the key at this storage index
    const void* key_ptr = storage->get_key_ptr(storage_idx);
    return ConstValueView(key_ptr, _view->element_type());
}

} // namespace hgraph::value
