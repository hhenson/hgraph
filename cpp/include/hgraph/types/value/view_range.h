#pragma once

/**
 * @file view_range.h
 * @brief Type-erased iteration ranges for the Value type system.
 *
 * ViewRange yields a View per element (used for values, keys, elements).
 * ViewPairRange yields a pair of Views per element (used for items, field name+value).
 *
 * Both are lightweight, stack-allocated ranges built on top of type_ops
 * indexed access (size + at). They satisfy the C++ range concept for use
 * in range-based for loops.
 *
 * Usage:
 * @code
 * // Iterate values of a list
 * for (View elem : ViewRange::values(list_view)) {
 *     process(elem.as<int64_t>());
 * }
 *
 * // Iterate key-value pairs of a map
 * for (auto [key, val] : ViewPairRange::items(map_view)) {
 *     process(key, val);
 * }
 *
 * // Iterate named fields of a bundle
 * for (auto [name, val] : ViewPairRange::fields(bundle_view)) {
 *     process(name, val);
 * }
 * @endcode
 */

#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <iterator>
#include <string_view>
#include <utility>

namespace hgraph::value {

// ============================================================================
// ViewRange — Single-Value Iteration
// ============================================================================

/**
 * @brief Type-erased range yielding a View per element.
 *
 * Iterates over indexed elements using type_ops::size() and type_ops::at().
 * Suitable for list elements, set elements, map keys, and positional access.
 */
class ViewRange {
public:
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = View;
        using difference_type = std::ptrdiff_t;
        using pointer = const View*;
        using reference = View;

        iterator() = default;
        iterator(const void* data, const TypeMeta* schema,
                 const TypeMeta* element_schema, size_t index) noexcept
            : data_(data), schema_(schema),
              element_schema_(element_schema), index_(index) {}

        [[nodiscard]] reference operator*() const {
            const void* elem = schema_->ops().at(data_, index_, schema_);
            return View(elem, element_schema_);
        }

        iterator& operator++() noexcept { ++index_; return *this; }
        iterator operator++(int) noexcept { auto tmp = *this; ++index_; return tmp; }

        [[nodiscard]] bool operator==(const iterator& o) const noexcept {
            return index_ == o.index_;
        }
        [[nodiscard]] bool operator!=(const iterator& o) const noexcept {
            return index_ != o.index_;
        }

    private:
        const void* data_{nullptr};
        const TypeMeta* schema_{nullptr};
        const TypeMeta* element_schema_{nullptr};
        size_t index_{0};
    };

    ViewRange() = default;
    ViewRange(const void* data, const TypeMeta* schema,
              const TypeMeta* element_schema, size_t count) noexcept
        : data_(data), schema_(schema),
          element_schema_(element_schema), count_(count) {}

    [[nodiscard]] iterator begin() const noexcept {
        return {data_, schema_, element_schema_, 0};
    }
    [[nodiscard]] iterator end() const noexcept {
        return {data_, schema_, element_schema_, count_};
    }
    [[nodiscard]] size_t size() const noexcept { return count_; }
    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }

    // ========== Factory Methods ==========

    /**
     * @brief Create a range over list/set/cyclic-buffer/queue elements.
     *
     * Uses ops.size() for count and ops.at() for element access.
     * Element schema is derived from TypeMeta::element_type.
     */
    [[nodiscard]] static ViewRange elements(const View& view) {
        if (!view.valid()) return {};
        const auto* schema = view.schema();
        size_t n = schema->ops().size(view.data(), schema);
        return {view.data(), schema, schema->element_type, n};
    }

    /**
     * @brief Create a range over bundle/tuple fields by index.
     *
     * Each element is the field value. The element_schema changes per index
     * for heterogeneous types, so this uses the parent schema for dispatch
     * and derives per-element schemas in the iterator.
     */
    [[nodiscard]] static ViewRange fields(const View& view) {
        if (!view.valid()) return {};
        const auto* schema = view.schema();
        size_t n = (schema->kind == TypeKind::Bundle || schema->kind == TypeKind::Tuple)
                       ? schema->field_count
                       : schema->ops().size(view.data(), schema);
        // For heterogeneous types, element_schema is nullptr;
        // the iterator will derive per-element schema from fields[index].type.
        return {view.data(), schema, nullptr, n};
    }

private:
    const void* data_{nullptr};
    const TypeMeta* schema_{nullptr};
    const TypeMeta* element_schema_{nullptr};
    size_t count_{0};
};

// ============================================================================
// ViewPairRange — Key-Value Pair Iteration
// ============================================================================

/**
 * @brief Type-erased range yielding a pair of Views per element.
 *
 * Used for:
 * - Bundle fields: (field_name_view, field_value_view)
 * - Map items: (key_view, value_view)
 * - Tuple elements: (index_view, element_view)
 *
 * The "key" interpretation depends on context:
 * - For bundles: a View over the field name (string)
 * - For maps: a View over the map key
 * - For tuples/lists: a View over the index (size_t)
 */
class ViewPairRange {
public:
    struct Pair {
        View first;   ///< Key, field name, or index
        View second;  ///< Value
    };

    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Pair;
        using difference_type = std::ptrdiff_t;
        using pointer = const Pair*;
        using reference = Pair;

        /// Function pointer type for producing key-value pair at index
        using pair_fn = Pair (*)(const void* data, const TypeMeta* schema, size_t index);

        iterator() = default;
        iterator(const void* data, const TypeMeta* schema,
                 size_t index, pair_fn fn) noexcept
            : data_(data), schema_(schema), index_(index), fn_(fn) {}

        [[nodiscard]] reference operator*() const {
            return fn_(data_, schema_, index_);
        }

        iterator& operator++() noexcept { ++index_; return *this; }
        iterator operator++(int) noexcept { auto tmp = *this; ++index_; return tmp; }

        [[nodiscard]] bool operator==(const iterator& o) const noexcept {
            return index_ == o.index_;
        }
        [[nodiscard]] bool operator!=(const iterator& o) const noexcept {
            return index_ != o.index_;
        }

    private:
        const void* data_{nullptr};
        const TypeMeta* schema_{nullptr};
        size_t index_{0};
        pair_fn fn_{nullptr};
    };

    ViewPairRange() = default;
    ViewPairRange(const void* data, const TypeMeta* schema,
                  size_t count, iterator::pair_fn fn) noexcept
        : data_(data), schema_(schema), count_(count), fn_(fn) {}

    [[nodiscard]] iterator begin() const noexcept {
        return {data_, schema_, 0, fn_};
    }
    [[nodiscard]] iterator end() const noexcept {
        return {data_, schema_, count_, fn_};
    }
    [[nodiscard]] size_t size() const noexcept { return count_; }
    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }

    // ========== Factory Methods ==========

    /**
     * @brief Create a range over bundle fields as (name_view, value_view) pairs.
     *
     * The name is exposed as a View over a string_view-compatible representation.
     * Since field names are string literals (const char*), the View wraps
     * the BundleFieldInfo name pointer.
     */
    [[nodiscard]] static ViewPairRange bundle_items(const View& view);

    /**
     * @brief Create a range over tuple elements as (index, value_view) pairs.
     */
    [[nodiscard]] static ViewPairRange tuple_items(const View& view);

    /**
     * @brief Create a range over list elements as (index, value_view) pairs.
     */
    [[nodiscard]] static ViewPairRange list_items(const View& view);

private:
    const void* data_{nullptr};
    const TypeMeta* schema_{nullptr};
    size_t count_{0};
    iterator::pair_fn fn_{nullptr};
};

} // namespace hgraph::value
