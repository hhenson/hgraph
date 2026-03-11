#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/validity_bitmap.h>

#include <compare>
#include <cstddef>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{

    struct ValueBuilder;

    namespace detail
    {
        struct FixedListStateOps;
        struct DynamicListStateOps;

        /**
         * List-specific dispatch surface layered on top of the generic erased
         * `ViewDispatch`.
         *
         * The generic view wrapper continues to own schema validation. Once a
         * `ListView` has been created, this dispatch surface exposes indexed
         * traversal and mutation against the represented list state.
         *
         * Element validity is tracked separately from element storage. This
         * allows the list to return invalid child views without losing access to
         * the underlying element storage needed to mutate the slot later.
         */
        struct ListViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t                 size() const noexcept                     = 0;
            [[nodiscard]] virtual bool                   is_fixed() const noexcept                 = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept          = 0;
            [[nodiscard]] virtual bool                   element_valid(size_t index) const         = 0;
            [[nodiscard]] virtual ViewDispatch          *element_dispatch(size_t index) = 0;
            [[nodiscard]] virtual const ViewDispatch    *element_dispatch(size_t index) const = 0;
            virtual void                                 set_element_valid(size_t index, bool valid) = 0;
            virtual void                                 resize(size_t new_size, const ValueBuilder &element_builder) = 0;
            virtual void                                 clear(const ValueBuilder &element_builder) = 0;
        };

    }  // namespace detail

    /**
     * Shared list behavior for schema-resolved list state.
     *
     * `ListStateBase` owns the resolved element schema and implements the
     * generic erased operations that are common to both fixed-size and dynamic
     * lists. Concrete derived states only need to provide the storage-specific
     * indexed access and resizing behavior.
     */
    struct HGRAPH_EXPORT ListStateBase : detail::ListViewDispatch
    {
        explicit ListStateBase(const value::TypeMeta &element_schema) noexcept;
        ListStateBase(const ListStateBase &other) noexcept;
        ListStateBase(ListStateBase &&other) noexcept;
        ListStateBase &operator=(const ListStateBase &other) noexcept;
        ListStateBase &operator=(ListStateBase &&other) noexcept;
        ~ListStateBase() override = default;

        [[nodiscard]] const value::TypeMeta &element_schema() const noexcept override;
        [[nodiscard]] size_t                 hash() const override;
        [[nodiscard]] std::string            to_string() const override;
        [[nodiscard]] std::partial_ordering  operator<=>(const detail::ViewDispatch &other) const override;
        [[nodiscard]] nb::object             to_python(const value::TypeMeta *schema) const override;
        void                                 from_python(const nb::object &src, const value::TypeMeta *schema) override;
        void                                 assign_from(const detail::ViewDispatch &other) override;
        void                                 set_from_cpp(const void *src, const value::TypeMeta *src_schema) override;
        void                                 move_from_cpp(void *src, const value::TypeMeta *src_schema) override;

      protected:
        [[nodiscard]] View                element_view(size_t index) noexcept;
        [[nodiscard]] View                element_view(size_t index) const noexcept;
        void                              assign_element(size_t index, const View &value);

      private:
        std::reference_wrapper<const value::TypeMeta> m_element_schema;
    };

    /**
     * Fixed-size list state with inline element storage.
     *
     * The fixed list owns one contiguous state block containing the list state
     * header, inline storage for each element state, and a trailing validity
     * bitmap. This mirrors the current value-layer fixed-list design while
     * keeping element lifecycle on the new builder/state system.
     */
    struct HGRAPH_EXPORT FixedListState final : ListStateBase
    {
        FixedListState(const value::TypeMeta &element_schema, size_t fixed_size) noexcept;
        ~FixedListState() override;

        [[nodiscard]] size_t              size() const noexcept override;
        [[nodiscard]] bool                is_fixed() const noexcept override;
        [[nodiscard]] bool                element_valid(size_t index) const override;
        [[nodiscard]] detail::ViewDispatch *element_dispatch(size_t index) override;
        [[nodiscard]] const detail::ViewDispatch *element_dispatch(size_t index) const override;
        void                              set_element_valid(size_t index, bool valid) override;
        void                              resize(size_t new_size, const ValueBuilder &element_builder) override;
        void                              clear(const ValueBuilder &element_builder) override;

        /**
         * Return the total allocation size required for a fixed-size list state
         * with the supplied element builder and element count.
         */
        [[nodiscard]] static size_t allocation_size(const ValueBuilder &element_builder, size_t fixed_size) noexcept;

        /**
         * Return the alignment required for a fixed-size list state with the
         * supplied element builder.
         */
        [[nodiscard]] static size_t allocation_alignment(const ValueBuilder &element_builder) noexcept;

      private:
        friend struct detail::FixedListStateOps;

        [[nodiscard]] size_t      element_stride() const noexcept;
        [[nodiscard]] size_t      elements_offset() const noexcept;
        [[nodiscard]] size_t      validity_offset() const noexcept;
        [[nodiscard]] std::byte  *element_memory(size_t index) noexcept;
        [[nodiscard]] const std::byte *element_memory(size_t index) const noexcept;
        [[nodiscard]] std::byte  *validity_memory() noexcept;
        [[nodiscard]] const std::byte *validity_memory() const noexcept;
        void                     construct_elements(const ValueBuilder &element_builder);
        void                     copy_from(const FixedListState &other, const ValueBuilder &element_builder);
        void                     move_from(FixedListState &other, const ValueBuilder &element_builder);
        void                     destroy_elements(const ValueBuilder &element_builder) noexcept;

        size_t m_fixed_size{0};
    };

    /**
     * Dynamic list state with heap-backed resizable element storage.
     *
     * The dynamic list keeps its own byte buffer and validity bitmap so it can
     * grow and shrink at runtime while still storing element states using the
     * new builder/state lifecycle.
     */
    struct HGRAPH_EXPORT DynamicListState final : ListStateBase
    {
        explicit DynamicListState(const value::TypeMeta &element_schema) noexcept;
        ~DynamicListState() override;

        [[nodiscard]] size_t              size() const noexcept override;
        [[nodiscard]] bool                is_fixed() const noexcept override;
        [[nodiscard]] bool                element_valid(size_t index) const override;
        [[nodiscard]] detail::ViewDispatch *element_dispatch(size_t index) override;
        [[nodiscard]] const detail::ViewDispatch *element_dispatch(size_t index) const override;
        void                              set_element_valid(size_t index, bool valid) override;
        void                              resize(size_t new_size, const ValueBuilder &element_builder) override;
        void                              clear(const ValueBuilder &element_builder) override;

      private:
        friend struct detail::DynamicListStateOps;

        [[nodiscard]] size_t      element_stride() const noexcept;
        [[nodiscard]] std::byte  *element_memory(size_t index) noexcept;
        [[nodiscard]] const std::byte *element_memory(size_t index) const noexcept;
        void                     reserve(size_t new_capacity, const ValueBuilder &element_builder);
        void                     destroy_range(size_t begin, size_t end, const ValueBuilder &element_builder) noexcept;
        void                     copy_from(const DynamicListState &other, const ValueBuilder &element_builder);
        void                     move_from(DynamicListState &other) noexcept;

        std::vector<std::byte> m_data;
        std::vector<std::byte> m_validity;
        size_t                 m_size{0};
        size_t                 m_capacity{0};
    };

    /**
     * Non-owning erased list view.
     *
     * `ListView` provides indexed navigation over the list state while keeping
     * the public surface aligned with the current value-layer list behavior:
     * fixed and dynamic lists share the same indexed operations, and dynamic
     * lists add resize-style mutation.
     */
    struct HGRAPH_EXPORT ListView : View
    {
        ListView() = default;
        explicit ListView(const View &view);
        explicit ListView(detail::ListViewDispatch &state, const value::TypeMeta *schema) noexcept;

        [[nodiscard]] size_t                 size() const;
        [[nodiscard]] bool                   empty() const;
        [[nodiscard]] bool                   is_fixed() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] View                   at(size_t index);
        [[nodiscard]] View                   at(size_t index) const;
        [[nodiscard]] View                   operator[](size_t index);
        [[nodiscard]] View                   operator[](size_t index) const;
        [[nodiscard]] View                   front();
        [[nodiscard]] View                   front() const;
        [[nodiscard]] View                   back();
        [[nodiscard]] View                   back() const;

        void set(size_t index, const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            View slot = at(index);
            slot.set(std::forward<T>(value));
            list_dispatch()->set_element_valid(index, true);
        }

        void resize(size_t new_size);
        void clear();

        void push_back(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void push_back(T &&value)
        {
            const size_t index = size();
            resize(index + 1);
            set(index, std::forward<T>(value));
        }

      private:
        [[nodiscard]] detail::ListViewDispatch *list_dispatch() noexcept;
        [[nodiscard]] const detail::ListViewDispatch *list_dispatch() const noexcept;
    };

    inline ListView View::as_list()
    {
        return ListView{*this};
    }

    inline ListView View::as_list() const
    {
        return ListView{*this};
    }

}  // namespace hgraph
