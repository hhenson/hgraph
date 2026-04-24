#pragma once

#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

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

        template <typename... Fs>
        struct overloaded_fn : Fs...
        {
            using Fs::operator()...;
        };

        template <typename... Fs>
        overloaded_fn(Fs...) -> overloaded_fn<Fs...>;

        template <typename Visitor>
        constexpr void invoke_empty_visitor(Visitor &&visitor)
        {
            if constexpr (std::invocable<Visitor>) {
                std::forward<Visitor>(visitor)();
            } else if constexpr (std::invocable<Visitor, std::nullptr_t>) {
                std::forward<Visitor>(visitor)(nullptr);
            }
        }

        template <size_t Index, typename Tuple, typename Visitor>
        constexpr void visit_non_empty_discriminated_ptr(size_t active_index, void *ptr, Visitor &&visitor)
        {
            if constexpr (Index + 1 == std::tuple_size_v<Tuple>) {
                using T = std::tuple_element_t<Index, Tuple>;
                assert(active_index == Index);
                if constexpr (std::invocable<Visitor, T *>) {
                    std::forward<Visitor>(visitor)(static_cast<T *>(ptr));
                }
            } else {
                if (active_index == Index) {
                    using T = std::tuple_element_t<Index, Tuple>;
                    if constexpr (std::invocable<Visitor, T *>) {
                        std::forward<Visitor>(visitor)(static_cast<T *>(ptr));
                    }
                    return;
                }

                visit_non_empty_discriminated_ptr<Index + 1, Tuple>(active_index, ptr, std::forward<Visitor>(visitor));
            }
        }

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
                set(ptr, tag);
            }

            [[nodiscard]] constexpr void *ptr() const noexcept
            {
                return reinterpret_cast<void *>(m_bits & ptr_mask);
            }

            template <typename T>
            [[nodiscard]] constexpr T *as() const noexcept
            {
                return reinterpret_cast<T *>(m_bits & ptr_mask);
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
                const storage_type ptr_bits = reinterpret_cast<storage_type>(ptr);
                assert((ptr_bits & tag_mask) == 0);
                assert((tag_value & ~tag_mask) == 0);
                m_bits = ptr_bits | tag_value;
            }

            constexpr void set_ptr(void *ptr) noexcept
            {
                set(ptr, tag());
            }

            template <typename T>
            constexpr void set_ptr(T *ptr) noexcept
            {
                set(ptr, tag());
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

            template <typename Visitor>
            constexpr void visit(Visitor &&visitor) const
            {
                auto &&bound_visitor = visitor;

                if (!*this) {
                    detail::invoke_empty_visitor(bound_visitor);
                    return;
                }

                using tuple_type = std::tuple<Ts...>;
                detail::visit_non_empty_discriminated_ptr<0, tuple_type>(tag(), ptr(), bound_visitor);
            }

            template <typename Visitor, typename EmptyVisitor>
            constexpr void visit(Visitor &&visitor, EmptyVisitor &&empty_visitor) const
            {
                auto &&bound_visitor = visitor;
                auto &&bound_empty_visitor = empty_visitor;

                if (!*this) {
                    detail::invoke_empty_visitor(bound_empty_visitor);
                    return;
                }

                using tuple_type = std::tuple<Ts...>;
                detail::visit_non_empty_discriminated_ptr<0, tuple_type>(tag(), ptr(), bound_visitor);
            }
        };
    }  // namespace detail

    template <size_t Alignment, size_t TagBits = 1>
    using erased_tagged_ptr = detail::erased_tagged_ptr<Alignment, TagBits>;

    template <size_t TagBits = 1>
    using tagged_void_ptr = erased_tagged_ptr<alignof(std::max_align_t), TagBits>;

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

    template <size_t Alignment, typename... Ts>
    using erased_discriminated_ptr = detail::erased_discriminated_ptr<Alignment, Ts...>;

    template <typename... Ts>
    using pointer_aligned_discriminated_ptr = erased_discriminated_ptr<alignof(void *), Ts...>;

    template <typename... Ts>
    class discriminated_ptr : public erased_discriminated_ptr<detail::min_alignment_v<Ts...>, Ts...>
    {
      public:
        using base_type = erased_discriminated_ptr<detail::min_alignment_v<Ts...>, Ts...>;
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
        using base_type::visit;
    };

    template <size_t Alignment, typename... Ts, typename... Handlers>
    constexpr void visit(const erased_discriminated_ptr<Alignment, Ts...> &ptr, Handlers &&...handlers)
    {
        auto visitor = detail::overloaded_fn{std::forward<Handlers>(handlers)...};
        ptr.visit(visitor);
    }
}  // namespace hgraph
