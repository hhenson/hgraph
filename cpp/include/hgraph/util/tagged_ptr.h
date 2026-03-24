#pragma once

#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>

namespace hgraph
{
    namespace detail
    {
        template <typename T, typename... Ts>
        inline constexpr bool one_of_v = (std::same_as<T, Ts> || ...);

        template <typename... Ts>
        inline constexpr bool all_distinct_v = true;

        template <typename T, typename... Ts>
        inline constexpr bool all_distinct_v<T, Ts...> = (!(std::same_as<T, Ts>) && ...) && all_distinct_v<Ts...>;

        template <size_t Count>
        inline constexpr size_t tag_bits_for_count_v =
            Count <= 1 ? 0 : std::bit_width(static_cast<std::size_t>(Count - 1));

        template <typename T, typename... Ts>
        struct type_index;

        template <typename T, typename... Ts>
        struct type_index<T, T, Ts...> : std::integral_constant<size_t, 0>
        {};

        template <typename T, typename U, typename... Ts>
        struct type_index<T, U, Ts...> : std::integral_constant<size_t, 1 + type_index<T, Ts...>::value>
        {};

        template <typename T0, typename... Ts>
        inline constexpr size_t min_alignment_v = [] {
            size_t result = alignof(T0);
            ((result = result < alignof(Ts) ? result : alignof(Ts)), ...);
            return result;
        }();

        /**
         * Low-level pointer-plus-bits storage for erased pointer families.
         *
         * This utility packs a raw pointer and a small tag into one machine
         * word by using low pointer bits that are guaranteed clear by the
         * supplied `Alignment` contract.
         *
         * This is intentionally the low-level escape hatch. Call sites should
         * normally prefer `tagged_ptr<T, TagBits>`, which derives the
         * alignment contract from `T`, or `tagged_void_ptr<TagBits>`, which is
         * appropriate for ordinarily heap-allocated erased objects.
         *
         * `Alignment` is not discovered here. It is a promise from the caller
         * that every stored pointer value will have at least that alignment.
         * If that promise is wrong, the packed pointer can be corrupted.
         *
         * Example:
         * @code
         * using SlotPtr = hgraph::detail::erased_tagged_ptr<alignof(void *), 1>;
         *
         * SlotPtr slot;
         * slot.set(some_heap_object, 1);  // store pointer and one ownership bit
         *
         * if (slot.has_tag(1)) {
         *     auto *typed = slot.as<MyType>();
         * }
         * @endcode
         */
        template <size_t Alignment, size_t TagBits = 1>
        class erased_tagged_ptr
        {
          public:
            using storage_type = std::uintptr_t;

            static_assert(Alignment != 0, "Alignment must be non-zero");
            static_assert(std::has_single_bit(static_cast<storage_type>(Alignment)), "Alignment must be a power of two");

            static constexpr size_t available_tag_bits = std::countr_zero(static_cast<storage_type>(Alignment));
            static_assert(TagBits <= available_tag_bits,
                          "TagBits exceeds the low bits guaranteed clear by the declared alignment");

            static constexpr storage_type tag_mask = TagBits == 0 ? storage_type{0} : (storage_type{1} << TagBits) - 1;
            static constexpr storage_type ptr_mask = ~tag_mask;

            constexpr erased_tagged_ptr() noexcept = default;
            constexpr erased_tagged_ptr(std::nullptr_t) noexcept {}

            constexpr erased_tagged_ptr(void *ptr, storage_type tag = 0) noexcept
            {
                set(ptr, tag);
            }

            template <typename T>
            constexpr erased_tagged_ptr(T *ptr, storage_type tag = 0) noexcept
            {
                set(static_cast<void *>(ptr), tag);
            }

            [[nodiscard]] constexpr void *ptr() const noexcept
            {
                return reinterpret_cast<void *>(m_bits & ptr_mask);
            }

            template <typename T>
            [[nodiscard]] constexpr T *as() const noexcept
            {
                return static_cast<T *>(ptr());
            }

            [[nodiscard]] constexpr storage_type tag() const noexcept
            {
                return m_bits & tag_mask;
            }

            [[nodiscard]] constexpr bool has_tag(storage_type tag_value) const noexcept
            {
                return tag() == tag_value;
            }

            constexpr void set(void *ptr, storage_type tag_value = 0) noexcept
            {
                assert((reinterpret_cast<storage_type>(ptr) & tag_mask) == 0);
                assert((tag_value & ~tag_mask) == 0);
                m_bits = reinterpret_cast<storage_type>(ptr) | tag_value;
            }

            template <typename T>
            constexpr void set(T *ptr, storage_type tag_value = 0) noexcept
            {
                set(static_cast<void *>(ptr), tag_value);
            }

            constexpr void set_ptr(void *ptr) noexcept
            {
                set(ptr, tag());
            }

            template <typename T>
            constexpr void set_ptr(T *ptr) noexcept
            {
                set(static_cast<void *>(ptr), tag());
            }

            constexpr void set_tag(storage_type tag_value) noexcept
            {
                set(ptr(), tag_value);
            }

            constexpr void clear() noexcept
            {
                m_bits = 0;
            }

            [[nodiscard]] constexpr explicit operator bool() const noexcept
            {
                return ptr() != nullptr;
            }

            [[nodiscard]] constexpr storage_type raw_bits() const noexcept
            {
                return m_bits;
            }

          private:
            storage_type m_bits{0};
        };

        /**
         * Low-level discriminated pointer for erased or incomplete type
         * families.
         *
         * This stores "pointer to one of `Ts...`" in a single word. The active
         * alternative is encoded in the low bits and the pointer payload lives
         * in the remaining high bits.
         *
         * Like `erased_tagged_ptr`, the `Alignment` template argument is an
         * explicit contract. This type exists for internal cases where the
         * alternatives are forward-declared or intentionally erased at the
         * declaration site. Most callers should prefer `discriminated_ptr<Ts...>`.
         *
         * Null is represented by a null pointer payload, not by a reserved tag
         * value. That means:
         * - `operator bool()` and `empty()` answer whether a pointer is present
         * - `index()` returns `npos` for null
         * - `tag()` is only meaningful when the pointer is non-null
         *
         * Example:
         * @code
         * using ParentPtr =
         *     hgraph::detail::erased_discriminated_ptr<alignof(void *),
         *                                              TSLState,
         *                                              TSDState,
         *                                              TSBState,
         *                                              TSInput,
         *                                              TSOutput>;
         *
         * ParentPtr parent = some_output;
         * if (auto *output = parent.get<TSOutput>(); output != nullptr) {
         *     // ...
         * }
         * @endcode
         */
        template <size_t Alignment, typename... Ts>
        class erased_discriminated_ptr
            : private erased_tagged_ptr<Alignment, tag_bits_for_count_v<sizeof...(Ts)>>
        {
          public:
            static_assert(sizeof...(Ts) > 0, "erased_discriminated_ptr<Ts...> requires at least one alternative");
            static_assert((std::is_object_v<Ts> && ...), "erased_discriminated_ptr<Ts...> requires object types");
            static_assert(all_distinct_v<Ts...>, "erased_discriminated_ptr<Ts...> requires distinct alternative types");

            using base_type = erased_tagged_ptr<Alignment, tag_bits_for_count_v<sizeof...(Ts)>>;
            using storage_type = typename base_type::storage_type;

            static constexpr size_t alternative_count = sizeof...(Ts);
            static constexpr size_t tag_bits = tag_bits_for_count_v<alternative_count>;
            static constexpr size_t alignment = Alignment;
            static constexpr size_t npos = static_cast<size_t>(-1);

            constexpr erased_discriminated_ptr() noexcept = default;
            constexpr erased_discriminated_ptr(std::nullptr_t) noexcept : base_type(nullptr) {}

            template <typename T>
                requires(one_of_v<T, Ts...>)
            constexpr erased_discriminated_ptr(T *ptr) noexcept
            {
                set(ptr);
            }

            constexpr erased_discriminated_ptr &operator=(std::nullptr_t) noexcept
            {
                clear();
                return *this;
            }

            template <typename T>
                requires(one_of_v<T, Ts...>)
            constexpr erased_discriminated_ptr &operator=(T *ptr) noexcept
            {
                set(ptr);
                return *this;
            }

            [[nodiscard]] constexpr storage_type tag() const noexcept
            {
                return base_type::tag();
            }

            [[nodiscard]] constexpr size_t index() const noexcept
            {
                return *this ? static_cast<size_t>(tag()) : npos;
            }

            [[nodiscard]] constexpr bool empty() const noexcept
            {
                return !static_cast<bool>(*this);
            }

            [[nodiscard]] constexpr explicit operator bool() const noexcept
            {
                return static_cast<bool>(static_cast<const base_type &>(*this));
            }

            [[nodiscard]] constexpr void *ptr() const noexcept
            {
                return base_type::ptr();
            }

            [[nodiscard]] constexpr storage_type raw_bits() const noexcept
            {
                return base_type::raw_bits();
            }

            constexpr void clear() noexcept
            {
                base_type::clear();
            }

            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] constexpr bool is() const noexcept
            {
                return *this && tag() == tag_for<T>();
            }

            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] constexpr T *get() const noexcept
            {
                return is<T>() ? base_type::template as<T>() : nullptr;
            }

            template <typename T>
                requires(one_of_v<T, Ts...>)
            constexpr void set(T *ptr) noexcept
            {
                if (ptr == nullptr) {
                    clear();
                    return;
                }

                base_type::set(ptr, tag_for<T>());
            }

            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] static consteval storage_type tag_for() noexcept
            {
                return static_cast<storage_type>(type_index<T, Ts...>::value);
            }
        };
    }  // namespace detail

    /**
     * Erased tagged pointer for ordinarily heap-allocated objects.
     *
     * This is the convenient erased form of `erased_tagged_ptr` for the common
     * case where the pointed object is allocated with ordinary global `new`
     * and therefore satisfies the standard `std::max_align_t` alignment
     * guarantee.
     *
     * Prefer this over `detail::erased_tagged_ptr<...>` when:
     * - the pointee type is incomplete or intentionally erased
     * - the pointer really is a normal heap pointer
     * - you only need a few low-bit flags
     *
     * Example:
     * @code
     * hgraph::tagged_void_ptr<1> slot;
     * slot.set(new TimeSeriesStateV{}, 1);  // ownership bit in the low bit
     *
     * if (slot.has_tag(1)) {
     *     delete slot.as<TimeSeriesStateV>();
     * }
     * @endcode
     */
    template <size_t TagBits = 1>
    using tagged_void_ptr = detail::erased_tagged_ptr<alignof(std::max_align_t), TagBits>;

    /**
     * Type-safe tagged pointer with low-bit flag storage.
     *
     * This is the normal public entry point for "pointer plus a few flag bits".
     * The usable tag width is checked against `alignof(T)`, so callers do not
     * need to reason about pointer alignment directly.
     *
     * The pointer payload and the tag are orthogonal:
     * - `ptr()` returns the stored pointer
     * - `tag()` returns the stored flag bits
     * - `operator bool()` reports whether the pointer is non-null
     *
     * This is useful when one pointer naturally carries a small amount of
     * metadata such as ownership, inline/external storage, or a tiny state
     * enum.
     *
     * Example:
     * @code
     * struct ChildState {};
     *
     * hgraph::tagged_ptr<ChildState, 1> child{&state, 1};
     *
     * if (child && child.has_tag(1)) {
     *     ChildState *ptr = child.ptr();
     * }
     * @endcode
     *
     * For erased or forward-declared pointee types, prefer `tagged_void_ptr`
     * or `detail::erased_tagged_ptr` instead.
     */
    template <typename T, size_t TagBits = 1>
    class tagged_ptr : public detail::erased_tagged_ptr<alignof(T), TagBits>
    {
      public:
        static_assert(std::is_object_v<T>, "tagged_ptr<T, TagBits> requires an object type");

        using base_type = detail::erased_tagged_ptr<alignof(T), TagBits>;
        using typename base_type::storage_type;

        constexpr tagged_ptr() noexcept = default;
        constexpr tagged_ptr(std::nullptr_t) noexcept : base_type(nullptr) {}
        constexpr tagged_ptr(T *ptr, storage_type tag = 0) noexcept : base_type(ptr, tag) {}

        [[nodiscard]] constexpr T *ptr() const noexcept
        {
            return this->template as<T>();
        }

        constexpr void set(T *ptr, storage_type tag_value = 0) noexcept
        {
            base_type::set(ptr, tag_value);
        }

        constexpr void set_ptr(T *ptr) noexcept
        {
            base_type::set_ptr(ptr);
        }
    };

    /**
     * Type-safe discriminated pointer over a fixed family of pointee types.
     *
     * This is the normal public entry point for "pointer to one of these
     * concrete types". The active alternative index is stored in the low bits
     * of the pointer, and the required tag width is derived from the minimum
     * alignment across `Ts...`.
     *
     * Null is represented by a null pointer payload, not by reserving an extra
     * tag value. That means the first alternative legitimately has tag/index
     * `0`. In normal code:
     * - use `operator bool()` or `empty()` to test for presence
     * - use `is<T>()` or `get<T>()` to branch on the active type
     * - use `index()` if you explicitly want the active alternative number
     *
     * This is a good fit for pointer-only sum types such as:
     * - parent pointers that can point at one of a few container/root types
     * - link targets that can point at one of a few state node types
     * - other runtime unions where ownership is not part of the variant
     *
     * Example:
     * @code
     * using ParentPtr = hgraph::discriminated_ptr<TSLState, TSDState, TSBState, TSInput, TSOutput>;
     *
     * ParentPtr parent = &bundle_state;
     *
     * if (auto *bundle = parent.get<TSBState>(); bundle != nullptr) {
     *     // bundle parent
     * } else if (auto *input = parent.get<TSInput>(); input != nullptr) {
     *     // rooted at an input endpoint
     * }
     * @endcode
     *
     * If the alternative family is only forward-declared at the declaration
     * site, prefer `detail::erased_discriminated_ptr` internally and keep this
     * typed wrapper for complete-type call sites.
     */
    template <typename... Ts>
    class discriminated_ptr : public detail::erased_discriminated_ptr<detail::min_alignment_v<Ts...>, Ts...>
    {
      public:
        using base_type = detail::erased_discriminated_ptr<detail::min_alignment_v<Ts...>, Ts...>;
        using base_type::base_type;
        using typename base_type::storage_type;
        using base_type::alignment;
        using base_type::alternative_count;
        using base_type::clear;
        using base_type::empty;
        using base_type::get;
        using base_type::index;
        using base_type::is;
        using base_type::npos;
        using base_type::operator bool;
        using base_type::operator=;
        using base_type::ptr;
        using base_type::raw_bits;
        using base_type::set;
        using base_type::tag;
        using base_type::tag_bits;
        using base_type::tag_for;
    };
}  // namespace hgraph
