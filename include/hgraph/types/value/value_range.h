#ifndef HGRAPH_CPP_ROOT_VALUE_RANGE_H
#define HGRAPH_CPP_ROOT_VALUE_RANGE_H

#include <cstddef>
#include <iterator>
#include <utility>

namespace hgraph
{
    /**
     * Type-erased lazy range over storage-backed value-layer results.
     *
     * Carries four function-pointer-friendly fields:
     *
     * - ``context`` — opaque pointer the predicate / projector both
     *   receive (typically ops state).
     * - ``memory``  — value storage pointer the predicate /
     *   projector both receive.
     * - ``limit``   — bound of the ordinal index space the range
     *   walks.
     * - ``predicate`` — optional filter; when null the range yields
     *   every index in ``[0, limit)``. When non-null, indices for
     *   which the predicate returns ``false`` are skipped (this is
     *   how slot-store-backed layouts skip dead slots, while compact
     *   layouts pass ``nullptr`` and walk every ordinal).
     * - ``projector`` — given ``(context, memory, index)`` returns
     *   the yielded ``T`` (typically a ``ValueView`` built from the
     *   storage's element memory at that index).
     *
     * The range is allocation-free, copy-cheap, and decoupled from
     * any concrete storage layout. The same ``Range<ValueView>`` is
     * produced by compact storage and (in future) slot-store-backed
     * storage — only the predicate / projector pointers differ.
     */
    template <typename T>
    struct Range
    {
        using predicate_fn = bool (*)(const void *context, const void *memory, std::size_t index);
        using projector_fn = T (*)(const void *context, const void *memory, std::size_t index);

        const void  *context{nullptr};
        const void  *memory{nullptr};
        std::size_t  limit{0};
        predicate_fn predicate{nullptr};
        projector_fn projector{nullptr};

        // Self-contained iterator: copies the range's fields so
        // it remains valid even after the range it was built from goes
        // out of scope. This lets a view expose ``begin()``/``end()``
        // directly without forcing callers to materialise the range
        // into a stable storage location.
        struct iterator
        {
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = T;
            using reference         = T;
            using pointer           = void;

            const void  *context{nullptr};
            const void  *memory{nullptr};
            std::size_t  limit{0};
            predicate_fn predicate{nullptr};
            projector_fn projector{nullptr};
            std::size_t  index{0};

            [[nodiscard]] T operator*() const { return projector(context, memory, index); }

            iterator &operator++() noexcept
            {
                ++index;
                advance_to_match();
                return *this;
            }
            iterator operator++(int) noexcept
            {
                auto cur = *this;
                ++(*this);
                return cur;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept
            {
                return context == other.context && memory == other.memory && index == other.index;
            }

            void advance_to_match() noexcept
            {
                if (predicate == nullptr) { return; }
                while (index < limit && !predicate(context, memory, index)) { ++index; }
            }
        };

        [[nodiscard]] iterator begin() const noexcept
        {
            iterator it{context, memory, limit, predicate, projector, 0};
            it.advance_to_match();
            return it;
        }

        [[nodiscard]] iterator end() const noexcept
        {
            return iterator{context, memory, limit, predicate, projector, limit};
        }
    };

    /**
     * Type-erased lazy range over key/value pairs. Same shape as
     * ``Range<T>`` but the projector returns ``std::pair<K, V>``.
     */
    template <typename K, typename V>
    struct KeyValueRange
    {
        using value_type   = std::pair<K, V>;
        using predicate_fn = bool (*)(const void *context, const void *memory, std::size_t index);
        using projector_fn = value_type (*)(const void *context, const void *memory, std::size_t index);

        const void  *context{nullptr};
        const void  *memory{nullptr};
        std::size_t  limit{0};
        predicate_fn predicate{nullptr};
        projector_fn projector{nullptr};

        struct iterator
        {
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using reference         = value_type;
            using pointer           = void;

            const void  *context{nullptr};
            const void  *memory{nullptr};
            std::size_t  limit{0};
            predicate_fn predicate{nullptr};
            projector_fn projector{nullptr};
            std::size_t  index{0};

            [[nodiscard]] value_type operator*() const { return projector(context, memory, index); }

            iterator &operator++() noexcept
            {
                ++index;
                advance_to_match();
                return *this;
            }
            iterator operator++(int) noexcept
            {
                auto cur = *this;
                ++(*this);
                return cur;
            }

            [[nodiscard]] bool operator==(const iterator &other) const noexcept
            {
                return context == other.context && memory == other.memory && index == other.index;
            }

            void advance_to_match() noexcept
            {
                if (predicate == nullptr) { return; }
                while (index < limit && !predicate(context, memory, index)) { ++index; }
            }
        };

        [[nodiscard]] iterator begin() const noexcept
        {
            iterator it{context, memory, limit, predicate, projector, 0};
            it.advance_to_match();
            return it;
        }

        [[nodiscard]] iterator end() const noexcept
        {
            return iterator{context, memory, limit, predicate, projector, limit};
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_RANGE_H
