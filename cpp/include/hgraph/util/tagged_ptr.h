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
         * Low-level erased tagged-pointer storage.
         *
         * This is the escape hatch for cases where the stored pointer type is
         * intentionally erased or incomplete at the declaration site. Most
         * call sites should use `tagged_ptr<T, TagBits>` instead.
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
     * This uses the standard global `new` alignment guarantee and is intended
     * for erased object pointers where the concrete pointee type is not
     * available at the declaration site.
     */
    template <size_t TagBits = 1>
    using tagged_void_ptr = detail::erased_tagged_ptr<alignof(std::max_align_t), TagBits>;

    /**
     * Type-safe tagged pointer.
     *
     * The tag width is derived against `alignof(T)`, which is the real safety
     * constraint for storing data in the low bits of a pointer to `T`.
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
     * Discriminated pointer over a fixed family of pointee types.
     *
     * The active alternative is stored in the low pointer bits, with tag width
     * derived from the minimum alignment across the alternative types.
     * Empty/null pointers are represented by tag value 0.
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
