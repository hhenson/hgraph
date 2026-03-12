#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>
#include <hgraph/types/value/validity_bitmap.h>

#include <cstddef>
#include <type_traits>
#include <utility>
namespace hgraph
{

    struct ListMutationView;
    struct ListDeltaView;

    struct ValueBuilder;

    namespace detail
    {

        /**
         * Behavior-only dispatch for list storage.
         *
         * The list dispatch knows how to interpret a raw storage pointer as a
         * fixed-size or dynamic homogeneous collection. Element storage remains
         * plain data described by the element builder.
         *
         * For fixed-size lists, `clear()` does not change the list length. It
         * invalidates every slot, which is the closest behavioral match to
         * clearing a collection whose extent is part of its schema.
         *
         * Invalidating a slot must also release any non-trivial element payload
         * held in that slot while leaving the slot storage ready for a later
         * assignment. In practice that means list implementations destroy the
         * old element state when required, reconstruct default storage in place,
         * and then mark the slot invalid.
         */
        struct ListViewDispatch : ViewDispatch
        {
            /**
             * Start a new mutation epoch for this list.
             *
             * Delta tracking for lists is scoped to the outermost active
             * mutation. Entering a new outermost mutation clears the previous
             * updated/added markers so the delta surface always describes the
             * current logical batch of list edits.
             */
            virtual void begin_mutation(void *data) const = 0;
            /**
             * End the current mutation epoch for this list.
             */
            virtual void end_mutation(void *data) const = 0;
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual bool is_fixed() const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &element_dispatch() const noexcept = 0;
            [[nodiscard]] virtual void *element_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *element_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool element_valid(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool slot_updated(const void *data, size_t index) const noexcept = 0;
            [[nodiscard]] virtual bool slot_added(const void *data, size_t index) const noexcept = 0;
            virtual void set_element_valid(void *data, size_t index, bool valid) const = 0;
            virtual void resize(void *data, size_t new_size) const = 0;
            virtual void clear(void *data) const = 0;
        };

        /**
         * Return the cached builder for a list schema.
         *
         * List storage layout and dispatch are owned by the list implementation,
         * so the generic builder factory delegates list-schema resolution to
         * `list.cpp`.
         */
        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *list_builder_for(const value::TypeMeta *schema);

    }  // namespace detail

    /**
     * Dynamic list runtime data.
     *
     * Dynamic lists need runtime capacity management, so they keep their
     * backing storage, validity bitmap, and mutation-epoch delta bitmaps in
     * this plain data struct.
     */
    struct DynamicListState
    {
        std::byte *data{nullptr};
        std::byte *validity{nullptr};
        std::byte *updated{nullptr};
        std::byte *added{nullptr};
        size_t     size{0};
        size_t     capacity{0};
        size_t     mutation_depth{0};
    };

    /**
     * Non-owning erased list view.
     */
    struct HGRAPH_EXPORT ListView : View
    {
        explicit ListView(const View &view);

        /**
         * Return the mutable surface for this list view.
         *
         * The returned mutation view owns the matching `end_mutation()` call.
         * Nested scopes are allowed and are tracked with a depth count in the
         * underlying storage so helper routines can safely compose list
         * mutations without losing the current delta information.
         */
        ListMutationView begin_mutation();
        /**
         * Return the delta-inspection surface for the current mutation epoch.
         */
        [[nodiscard]] ListDeltaView delta();
        [[nodiscard]] ListDeltaView delta() const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool is_fixed() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] View operator[](size_t index);
        [[nodiscard]] View operator[](size_t index) const;
        [[nodiscard]] View front();
        [[nodiscard]] View front() const;
        [[nodiscard]] View back();
        [[nodiscard]] View back() const;

      protected:
        /**
         * Enter the underlying list mutation epoch.
         */
        void begin_mutation_scope();
        /**
         * Leave the underlying list mutation epoch.
         */
        void end_mutation_scope() noexcept;
        [[nodiscard]] const detail::ListViewDispatch *list_dispatch() const noexcept;
    };

    /**
     * Delta surface for list mutation epochs.
     *
     * Lists report the slots that were touched in the current mutation epoch.
     * Fixed-size lists only expose updated slots. Dynamic lists additionally
     * expose slots that were appended or introduced by growth during the same
     * epoch.
     */
    struct HGRAPH_EXPORT ListDeltaView : View
    {
        explicit ListDeltaView(const View &view);

        [[nodiscard]] Range<size_t> updated_indices() const;
        [[nodiscard]] Range<View>   updated_values() const;
        [[nodiscard]] Range<size_t> added_indices() const;
        [[nodiscard]] Range<View>   added_values() const;

      private:
        [[nodiscard]] static bool slot_is_updated(const void *context, size_t index);
        [[nodiscard]] static bool slot_is_added(const void *context, size_t index);
        [[nodiscard]] static size_t project_index(const void *context, size_t index);
        [[nodiscard]] static View project_value(const void *context, size_t index);
        [[nodiscard]] const detail::ListViewDispatch *list_dispatch() const noexcept;
    };

    /**
     * Mutable list surface.
     *
     * Mutation is split from `ListView` so callers must opt into the mutating
     * API explicitly. The wrapper is RAII-managed and owns the matching
     * `end_mutation()` call for the mutation depth it opens.
     */
    struct HGRAPH_EXPORT ListMutationView : ListView
    {
        /**
         * Open a mutable list surface over the supplied list view.
         */
        explicit ListMutationView(ListView &view);
        ListMutationView(const ListMutationView &) = delete;
        ListMutationView &operator=(const ListMutationView &) = delete;
        /**
         * Transfer responsibility for closing the mutation scope.
         */
        ListMutationView(ListMutationView &&other) noexcept;
        ListMutationView &operator=(ListMutationView &&other) = delete;
        /**
         * Close the owned mutation scope, if any.
         */
        ~ListMutationView();

        /**
         * Assign the supplied value to an existing list slot.
         */
        void set(size_t index, const View &value);

        /**
         * Assign the supplied value and return this mutation view for fluent
         * mutation chains when the per-operation result is not needed.
         */
        ListMutationView &setting(size_t index, const View &value)
        {
            set(index, value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            auto *dispatch = list_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("ListMutationView::set on invalid view"); }
            if (index >= size()) { throw std::out_of_range("ListMutationView::set index out of range"); }

            using TValue = std::remove_cvref_t<T>;
            void *slot = dispatch->element_data(data(), index);
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->element_dispatch().set_from_cpp(slot, std::addressof(value), value::scalar_type_meta<TValue>());
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->element_dispatch().move_from_cpp(slot, std::addressof(moved_value), value::scalar_type_meta<TValue>());
            }
            dispatch->set_element_valid(data(), index, true);
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        ListMutationView &setting(size_t index, T &&value)
        {
            set(index, std::forward<T>(value));
            return *this;
        }

        /**
         * Resize the list to the supplied logical length.
         */
        void resize(size_t new_size);

        /**
         * Resize the list and return this mutation view for fluent chains.
         */
        ListMutationView &resizing(size_t new_size)
        {
            resize(new_size);
            return *this;
        }

        /**
         * Clear the list contents.
         */
        void clear();

        /**
         * Clear the list and return this mutation view for fluent chains.
         */
        ListMutationView &clearing()
        {
            clear();
            return *this;
        }

        /**
         * Append a value to the end of a dynamic list.
         */
        void push_back(const View &value);

        /**
         * Append a value and return this mutation view for fluent chains.
         */
        ListMutationView &pushing_back(const View &value)
        {
            push_back(value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void push_back(T &&value)
        {
            const size_t index = size();
            resize(index + 1);
            set(index, std::forward<T>(value));
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        ListMutationView &pushing_back(T &&value)
        {
            push_back(std::forward<T>(value));
            return *this;
        }

      private:
        bool m_owns_scope{true};
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
