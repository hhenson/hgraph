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

        /**
         * Type-erased tagged pointer that packs a small tag into the low
         * alignment bits of a pointer.
         *
         * Template parameters:
         * - ``Alignment``  the guaranteed alignment of the pointed-to object;
         *                   the number of available tag bits is derived from
         *                   the trailing zero bits of the alignment.
         * - ``TagBits``    how many low bits to use for the tag (0 disables).
         * - ``TagEnum``    optional enum type whose values are stored in the
         *                   tag and recovered through ``enum_value()``.
         *
         * Storage is one ``uintptr_t``. The class is exposed publicly through
         * the ``hgraph::erased_tagged_ptr`` alias.
         */
        template <size_t Alignment, size_t TagBits = 1, typename TagEnum = void>
        class erased_tagged_ptr
        {
          public:
            using storage_type = std::uintptr_t;
            using tag_enum_type = TagEnum;

            static_assert(Alignment != 0, "Alignment must be non-zero");
            static_assert(std::has_single_bit(static_cast<storage_type>(Alignment)), "Alignment must be a power of two");
            static_assert(std::is_void_v<tag_enum_type> || std::is_enum_v<tag_enum_type>,
                          "TagEnum must be void or an enum type");

            static constexpr size_t available_tag_bits = std::countr_zero(static_cast<storage_type>(Alignment));
            static_assert(TagBits <= available_tag_bits,
                          "TagBits exceeds the low bits guaranteed clear by the declared alignment");

            /** Bit mask covering the tag bits in the encoded storage. */
            static constexpr storage_type tag_mask = TagBits == 0 ? storage_type{0} : (storage_type{1} << TagBits) - 1;
            /** Bit mask covering the pointer bits in the encoded storage. */
            static constexpr storage_type ptr_mask = ~tag_mask;

            /** Construct an empty (null pointer, zero tag) handle. */
            constexpr erased_tagged_ptr() noexcept = default;
            /** Construct an empty handle from ``nullptr``. */
            constexpr erased_tagged_ptr(std::nullptr_t) noexcept {}

            /** Construct from a raw pointer and an integer tag value. */
            constexpr erased_tagged_ptr(void *ptr, storage_type tag = 0) noexcept
            {
                set(ptr, tag);
            }

            /** Construct from a raw pointer and an enum tag value. */
            template <typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            constexpr erased_tagged_ptr(void *ptr, Enum tag) noexcept
            {
                set(ptr, tag);
            }

            /** Construct from a typed pointer and an integer tag value. */
            template <typename T>
            constexpr erased_tagged_ptr(T *ptr, storage_type tag = 0) noexcept
            {
                set(ptr, tag);
            }

            /** Construct from a typed pointer and an enum tag value. */
            template <typename T, typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            constexpr erased_tagged_ptr(T *ptr, Enum tag) noexcept
            {
                set(ptr, tag);
            }

            /** Recover the embedded pointer with the tag bits cleared. */
            [[nodiscard]] constexpr void *ptr() const noexcept
            {
                return reinterpret_cast<void *>(m_bits & ptr_mask);
            }

            /** Recover the embedded pointer cast to ``T *``. */
            template <typename T>
            [[nodiscard]] constexpr T *as() const noexcept
            {
                return reinterpret_cast<T *>(m_bits & ptr_mask);
            }

            /** Read the integer tag value. */
            [[nodiscard]] constexpr storage_type tag() const noexcept
            {
                return m_bits & tag_mask;
            }

            /** Test whether the integer tag equals ``tag_value``. */
            [[nodiscard]] constexpr bool has_tag(storage_type tag_value) const noexcept
            {
                return tag() == tag_value;
            }

            /** Recover the tag as an enum value when ``TagEnum`` is set. */
            template <typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            [[nodiscard]] constexpr Enum enum_value() const noexcept
            {
                return static_cast<Enum>(tag());
            }

            /** Test whether the enum tag equals ``tag_value``. */
            template <typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            [[nodiscard]] constexpr bool has_enum(Enum tag_value) const noexcept
            {
                return tag() == enum_to_storage(tag_value);
            }

            /** Replace both pointer and integer tag. */
            constexpr void set(void *ptr, storage_type tag_value = 0) noexcept
            {
                assert((reinterpret_cast<storage_type>(ptr) & tag_mask) == 0);
                assert((tag_value & ~tag_mask) == 0);
                m_bits = reinterpret_cast<storage_type>(ptr) | tag_value;
            }

            /** Replace both pointer and enum tag. */
            template <typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            constexpr void set(void *ptr, Enum tag_value) noexcept
            {
                set(ptr, enum_to_storage(tag_value));
            }

            /** Replace both pointer and integer tag from a typed pointer. */
            template <typename T>
            constexpr void set(T *ptr, storage_type tag_value = 0) noexcept
            {
                const storage_type ptr_bits = reinterpret_cast<storage_type>(ptr);
                assert((ptr_bits & tag_mask) == 0);
                assert((tag_value & ~tag_mask) == 0);
                m_bits = ptr_bits | tag_value;
            }

            /** Replace both pointer and enum tag from a typed pointer. */
            template <typename T, typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            constexpr void set(T *ptr, Enum tag_value) noexcept
            {
                set(ptr, enum_to_storage(tag_value));
            }

            /** Replace the pointer while preserving the current tag. */
            constexpr void set_ptr(void *ptr) noexcept
            {
                set(ptr, tag());
            }

            /** Replace the pointer (typed) while preserving the current tag. */
            template <typename T>
            constexpr void set_ptr(T *ptr) noexcept
            {
                set(ptr, tag());
            }

            /** Replace the integer tag while preserving the current pointer. */
            constexpr void set_tag(storage_type tag_value) noexcept
            {
                set(ptr(), tag_value);
            }

            /** Replace the enum tag while preserving the current pointer. */
            template <typename Enum = tag_enum_type>
                requires(std::is_enum_v<Enum>)
            constexpr void set_tag(Enum tag_value) noexcept
            {
                set(ptr(), tag_value);
            }

            /** Reset to empty (null pointer, zero tag). */
            constexpr void clear() noexcept
            {
                m_bits = 0;
            }

            /** True when the embedded pointer is non-null. */
            [[nodiscard]] constexpr explicit operator bool() const noexcept
            {
                return ptr() != nullptr;
            }

            /** Access the underlying encoded ``uintptr_t``. */
            [[nodiscard]] constexpr storage_type raw_bits() const noexcept
            {
                return m_bits;
            }

          private:
            template <typename Enum>
                requires(std::is_enum_v<Enum>)
            [[nodiscard]] static constexpr storage_type enum_to_storage(Enum value) noexcept
            {
                return static_cast<storage_type>(std::to_underlying(value));
            }

            storage_type m_bits{0};
        };

        /**
         * Type-erased discriminated pointer over a closed set of alternative
         * object types.
         *
         * Like ``erased_tagged_ptr``, the active alternative is encoded in the
         * low bits of the pointer; the number of bits is derived from the
         * number of alternatives. Templates require that the listed types be
         * distinct object types and share the declared ``Alignment``.
         *
         * Exposed through the ``hgraph::erased_discriminated_ptr`` alias.
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

            /** Number of alternatives in the discriminated union. */
            static constexpr size_t alternative_count = sizeof...(Ts);
            /** Number of low bits used to encode the active alternative. */
            static constexpr size_t tag_bits = tag_bits_for_count_v<alternative_count>;
            /** Required pointer alignment (matches the template parameter). */
            static constexpr size_t alignment = Alignment;
            /** Sentinel returned by ``index()`` when the handle is empty. */
            static constexpr size_t npos = static_cast<size_t>(-1);

            /** Construct an empty handle. */
            constexpr erased_discriminated_ptr() noexcept = default;
            /** Construct an empty handle from ``nullptr``. */
            constexpr erased_discriminated_ptr(std::nullptr_t) noexcept : base_type(nullptr) {}

            /** Construct holding a pointer of one of the alternative types. */
            template <typename T>
                requires(one_of_v<T, Ts...>)
            constexpr erased_discriminated_ptr(T *ptr) noexcept
            {
                set(ptr);
            }

            /** Reset to empty when assigned ``nullptr``. */
            constexpr erased_discriminated_ptr &operator=(std::nullptr_t) noexcept
            {
                clear();
                return *this;
            }

            /** Replace the active alternative with a pointer of type ``T``. */
            template <typename T>
                requires(one_of_v<T, Ts...>)
            constexpr erased_discriminated_ptr &operator=(T *ptr) noexcept
            {
                set(ptr);
                return *this;
            }

            /** Read the integer discriminator (raw tag bits). */
            [[nodiscard]] constexpr storage_type tag() const noexcept
            {
                return base_type::tag();
            }

            /** Index of the active alternative, or ``npos`` if empty. */
            [[nodiscard]] constexpr size_t index() const noexcept
            {
                return *this ? static_cast<size_t>(tag()) : npos;
            }

            /** True when the handle does not hold any alternative. */
            [[nodiscard]] constexpr bool empty() const noexcept
            {
                return !static_cast<bool>(*this);
            }

            /** True when the handle holds an alternative (non-null pointer). */
            [[nodiscard]] constexpr explicit operator bool() const noexcept
            {
                return static_cast<bool>(static_cast<const base_type &>(*this));
            }

            /** Recover the embedded pointer as ``void *``. */
            [[nodiscard]] constexpr void *ptr() const noexcept
            {
                return base_type::ptr();
            }

            /** Access the underlying encoded ``uintptr_t``. */
            [[nodiscard]] constexpr storage_type raw_bits() const noexcept
            {
                return base_type::raw_bits();
            }

            /** Reset to the empty state. */
            constexpr void clear() noexcept
            {
                base_type::clear();
            }

            /** True when the active alternative is ``T``. */
            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] constexpr bool is() const noexcept
            {
                return *this && tag() == tag_for<T>();
            }

            /** Pointer to the active alternative if it is ``T``; otherwise ``nullptr``. */
            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] constexpr T *get() const noexcept
            {
                return is<T>() ? base_type::template as<T>() : nullptr;
            }

            /** Replace the held alternative with a pointer of type ``T``. */
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

            /** Compile-time tag value for alternative ``T``. */
            template <typename T>
                requires(one_of_v<T, Ts...>)
            [[nodiscard]] static consteval storage_type tag_for() noexcept
            {
                return static_cast<storage_type>(type_index<T, Ts...>::value);
            }

            /**
             * Visit the active alternative.
             *
             * If the handle is empty, ``visitor`` is invoked with no arguments
             * (or with ``nullptr`` if it accepts ``std::nullptr_t``). If the
             * handle holds alternative ``T``, ``visitor`` is invoked with a
             * ``T *`` only when it can accept that type.
             */
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

            /**
             * Visit using a separate callable for the empty case.
             *
             * ``empty_visitor`` is invoked when the handle is empty;
             * otherwise ``visitor`` is dispatched to the active alternative as
             * in the single-argument overload.
             */
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

    /**
     * Public alias for the type-erased tagged pointer; see
     * ``detail::erased_tagged_ptr``.
     */
    template <size_t Alignment, size_t TagBits = 1, typename TagEnum = void>
    using erased_tagged_ptr = detail::erased_tagged_ptr<Alignment, TagBits, TagEnum>;

    /**
     * Tagged pointer over ``void *`` with the alignment of
     * ``std::max_align_t``.
     */
    template <size_t TagBits = 1, typename TagEnum = void>
    using tagged_void_ptr = erased_tagged_ptr<alignof(std::max_align_t), TagBits, TagEnum>;

    /**
     * Strongly-typed tagged pointer over ``T``.
     *
     * Inherits the encoded layout from ``erased_tagged_ptr`` and adds typed
     * ``ptr()`` and ``set()`` overloads so callers do not have to ``as<T>()``
     * at every use site.
     */
    template <typename T, size_t TagBits = 1, typename TagEnum = void>
    class tagged_ptr : public detail::erased_tagged_ptr<alignof(T), TagBits, TagEnum>
    {
      public:
        static_assert(std::is_object_v<T>, "tagged_ptr<T, TagBits> requires an object type");

        using base_type = detail::erased_tagged_ptr<alignof(T), TagBits, TagEnum>;
        using typename base_type::storage_type;
        using typename base_type::tag_enum_type;

        /** Construct an empty handle. */
        constexpr tagged_ptr() noexcept = default;
        /** Construct an empty handle from ``nullptr``. */
        constexpr tagged_ptr(std::nullptr_t) noexcept : base_type(nullptr) {}
        /** Construct from a typed pointer and integer tag. */
        constexpr tagged_ptr(T *ptr, storage_type tag = 0) noexcept : base_type(ptr, tag) {}

        /** Construct from a typed pointer and enum tag. */
        template <typename Enum = tag_enum_type>
            requires(std::is_enum_v<Enum>)
        constexpr tagged_ptr(T *ptr, Enum tag) noexcept : base_type(ptr, tag) {}

        /** Recover the embedded pointer typed as ``T *``. */
        [[nodiscard]] constexpr T *ptr() const noexcept
        {
            return this->template as<T>();
        }

        /** Replace both pointer (typed) and integer tag. */
        constexpr void set(T *ptr, storage_type tag_value = 0) noexcept
        {
            base_type::set(ptr, tag_value);
        }

        /** Replace both pointer (typed) and enum tag. */
        template <typename Enum = tag_enum_type>
            requires(std::is_enum_v<Enum>)
        constexpr void set(T *ptr, Enum tag_value) noexcept
        {
            base_type::set(ptr, tag_value);
        }

        /** Replace the pointer (typed) while preserving the current tag. */
        constexpr void set_ptr(T *ptr) noexcept
        {
            base_type::set_ptr(ptr);
        }
    };

    /**
     * Public alias for the type-erased discriminated pointer; see
     * ``detail::erased_discriminated_ptr``.
     */
    template <size_t Alignment, typename... Ts>
    using erased_discriminated_ptr = detail::erased_discriminated_ptr<Alignment, Ts...>;

    /**
     * Discriminated pointer using ``alignof(void *)`` as the alignment
     * guarantee. Useful when the alternatives are heap-allocated by the
     * default new-expression.
     */
    template <typename... Ts>
    using pointer_aligned_discriminated_ptr = erased_discriminated_ptr<alignof(void *), Ts...>;

    /**
     * Discriminated pointer using the smallest alignment shared by all
     * alternatives. Re-exports the public surface of
     * ``detail::erased_discriminated_ptr`` for ergonomics.
     */
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

    /**
     * Free-function visitor for ``erased_discriminated_ptr``.
     *
     * Bundles overloaded handlers via ``detail::overloaded_fn`` and dispatches
     * to the matching one for the active alternative. Pass one handler per
     * alternative type (and optionally one for the empty case).
     */
    template <size_t Alignment, typename... Ts, typename... Handlers>
    constexpr void visit(const erased_discriminated_ptr<Alignment, Ts...> &ptr, Handlers &&...handlers)
    {
        auto visitor = detail::overloaded_fn{std::forward<Handlers>(handlers)...};
        ptr.visit(visitor);
    }
}  // namespace hgraph
