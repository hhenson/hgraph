#pragma once

/**
 * @file view_range.h
 * @brief Range types for unified iteration over Value containers.
 *
 * Provides ViewRange (single values) and ViewPairRange (key-value pairs)
 * for consistent iteration patterns across Set, Map, List, Bundle, and Delta types.
 */

#include <hgraph/types/value/value_view.h>

#include <iterator>
#include <cstddef>
#include <utility>

namespace hgraph::value {

/**
 * @brief Range yielding single Views per element.
 *
 * Used for iteration over:
 * - Set elements
 * - List elements
 * - Map keys() or values()
 * - Delta added() or removed()
 *
 * Supports both contiguous and non-contiguous storage through stride.
 */
class ViewRange {
public:
    /**
     * @brief Forward iterator for ViewRange.
     */
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = View;
        using difference_type = std::ptrdiff_t;
        using pointer = const View*;
        using reference = View;

        iterator() = default;

        iterator(const void* data, const TypeMeta* element_type, size_t stride, size_t index)
            : _data(static_cast<const std::byte*>(data))
            , _element_type(element_type)
            , _stride(stride)
            , _index(index) {}

        View operator*() const {
            return View(_data + _index * _stride, _element_type);
        }

        iterator& operator++() {
            ++_index;
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const iterator& other) const { return _index == other._index; }
        bool operator!=(const iterator& other) const { return _index != other._index; }

    private:
        const std::byte* _data{nullptr};
        const TypeMeta* _element_type{nullptr};
        size_t _stride{0};
        size_t _index{0};
    };

    ViewRange() = default;

    /**
     * @brief Construct a ViewRange over contiguous or strided data.
     *
     * @param data Pointer to the first element
     * @param element_type TypeMeta of each element
     * @param stride Byte distance between elements (typically element_type->size)
     * @param count Number of elements
     */
    ViewRange(const void* data, const TypeMeta* element_type, size_t stride, size_t count)
        : _data(data), _element_type(element_type), _stride(stride), _count(count) {}

    /**
     * @brief Construct from contiguous storage (stride = element size).
     */
    ViewRange(const void* data, const TypeMeta* element_type, size_t count)
        : _data(data)
        , _element_type(element_type)
        , _stride(element_type ? element_type->size : 0)
        , _count(count) {}

    iterator begin() const { return iterator(_data, _element_type, _stride, 0); }
    iterator end() const { return iterator(_data, _element_type, _stride, _count); }

    [[nodiscard]] size_t size() const { return _count; }
    [[nodiscard]] bool empty() const { return _count == 0; }

    /**
     * @brief Random access to elements.
     */
    View operator[](size_t idx) const {
        return View(static_cast<const std::byte*>(_data) + idx * _stride, _element_type);
    }

    /**
     * @brief Get the element type.
     */
    [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }

private:
    const void* _data{nullptr};
    const TypeMeta* _element_type{nullptr};
    size_t _stride{0};
    size_t _count{0};
};

/**
 * @brief Range yielding pairs of Views (key/index + value).
 *
 * Used for iteration over:
 * - Map items() (key, value pairs)
 * - Bundle entries() (name, value pairs)
 * - Delta added_items(), updated_items()
 *
 * Keys and values may have different types and strides.
 */
class ViewPairRange {
public:
    /**
     * @brief Forward iterator yielding key-value pairs.
     */
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<View, View>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = value_type;

        iterator() = default;

        iterator(const void* keys, const void* values,
                 const TypeMeta* key_type, const TypeMeta* value_type,
                 size_t key_stride, size_t value_stride, size_t index)
            : _keys(static_cast<const std::byte*>(keys))
            , _values(static_cast<const std::byte*>(values))
            , _key_type(key_type)
            , _value_type(value_type)
            , _key_stride(key_stride)
            , _value_stride(value_stride)
            , _index(index) {}

        std::pair<View, View> operator*() const {
            return {
                View(_keys + _index * _key_stride, _key_type),
                View(_values + _index * _value_stride, _value_type)
            };
        }

        iterator& operator++() {
            ++_index;
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            ++_index;
            return tmp;
        }

        bool operator==(const iterator& other) const { return _index == other._index; }
        bool operator!=(const iterator& other) const { return _index != other._index; }

    private:
        const std::byte* _keys{nullptr};
        const std::byte* _values{nullptr};
        const TypeMeta* _key_type{nullptr};
        const TypeMeta* _value_type{nullptr};
        size_t _key_stride{0};
        size_t _value_stride{0};
        size_t _index{0};
    };

    ViewPairRange() = default;

    /**
     * @brief Construct a ViewPairRange over parallel key/value arrays.
     *
     * @param keys Pointer to the first key
     * @param values Pointer to the first value
     * @param key_type TypeMeta of each key
     * @param value_type TypeMeta of each value
     * @param key_stride Byte distance between keys
     * @param value_stride Byte distance between values
     * @param count Number of pairs
     */
    ViewPairRange(const void* keys, const void* values,
                  const TypeMeta* key_type, const TypeMeta* value_type,
                  size_t key_stride, size_t value_stride, size_t count)
        : _keys(keys), _values(values)
        , _key_type(key_type), _value_type(value_type)
        , _key_stride(key_stride), _value_stride(value_stride)
        , _count(count) {}

    iterator begin() const {
        return iterator(_keys, _values, _key_type, _value_type, _key_stride, _value_stride, 0);
    }

    iterator end() const {
        return iterator(_keys, _values, _key_type, _value_type, _key_stride, _value_stride, _count);
    }

    [[nodiscard]] size_t size() const { return _count; }
    [[nodiscard]] bool empty() const { return _count == 0; }

    /**
     * @brief Random access to key-value pairs.
     */
    std::pair<View, View> operator[](size_t idx) const {
        return {
            View(static_cast<const std::byte*>(_keys) + idx * _key_stride, _key_type),
            View(static_cast<const std::byte*>(_values) + idx * _value_stride, _value_type)
        };
    }

    /**
     * @brief Get the key type.
     */
    [[nodiscard]] const TypeMeta* key_type() const { return _key_type; }

    /**
     * @brief Get the value type.
     */
    [[nodiscard]] const TypeMeta* value_type() const { return _value_type; }

private:
    const void* _keys{nullptr};
    const void* _values{nullptr};
    const TypeMeta* _key_type{nullptr};
    const TypeMeta* _value_type{nullptr};
    size_t _key_stride{0};
    size_t _value_stride{0};
    size_t _count{0};
};

} // namespace hgraph::value
