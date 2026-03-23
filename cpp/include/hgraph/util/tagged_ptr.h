#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace hgraph
{
    namespace detail
    {
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
}  // namespace hgraph
