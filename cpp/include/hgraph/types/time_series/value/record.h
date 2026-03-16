#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct TupleMutationView;
    struct BundleMutationView;
    struct TupleDeltaView;
    struct BundleDeltaView;

    struct ValueBuilder;

    namespace detail
    {

        /**
         * Behavior-only dispatch for tuple and bundle storage.
         *
         * Record-like values are heterogeneous fixed-shape collections. The
         * dispatch exposes field navigation while the raw storage remains a
         * schema-specific byte layout chosen by the record builder.
         */
        struct RecordViewDispatch : ViewDispatch
        {
            /**
             * Start a new mutation epoch for this record.
             *
             * Record deltas are defined over the current logical batch of field
             * edits. Entering a new outermost mutation clears the previous
             * updated markers.
             */
            virtual void begin_mutation(void *data) const = 0;
            /**
             * End the current mutation epoch for this record.
             */
            virtual void end_mutation(void *data) const = 0;
            [[nodiscard]] virtual size_t size() const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &field_schema(size_t index) const = 0;
            [[nodiscard]] virtual const ViewDispatch &field_dispatch(size_t index) const = 0;
            [[nodiscard]] virtual std::string_view field_name(size_t index) const noexcept = 0;
            [[nodiscard]] virtual void *field_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *field_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool field_valid(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool field_updated(const void *data, size_t index) const noexcept = 0;
            virtual void set_field_valid(void *data, size_t index, bool valid) const = 0;
            /**
             * Return the index of the named field, or `SIZE_MAX` when no
             * field with the supplied name exists.
             */
            [[nodiscard]] virtual size_t find_field(std::string_view name) const noexcept = 0;
        };

        /**
         * Return the cached builder for tuple and bundle schemas.
         *
         * Tuple and bundle layout is schema-specific because it depends on the
         * ordered child builders and the offsets chosen for their storage.
         */
        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *record_builder_for(
            const value::TypeMeta *schema, MutationTracking tracking);

    }  // namespace detail

    /**
     * Non-owning typed wrapper over a tuple value.
     *
     * A tuple is a positional heterogeneous collection. Bundle schemas are also
     * accepted here because bundles preserve the same positional record layout
     * and simply add field names on top of that tuple-compatible structure.
     * Child positions may be invalid even when the tuple itself is live, in
     * which case the child view is returned as an invalid view for the known
     * field schema.
     */
    struct HGRAPH_EXPORT TupleView : View
    {
        explicit TupleView(const View &view);

        /**
         * Return the mutable surface for this tuple-like view.
         *
         * The returned mutation view owns the matching `end_mutation()` call.
         * Nested scopes are allowed and are tracked with a depth count in the
         * underlying storage so helper routines can compose record mutation
         * without losing the current delta.
         */
        TupleMutationView begin_mutation();
        /**
         * Return the delta-inspection surface for the current mutation epoch.
         */
        [[nodiscard]] TupleDeltaView delta();
        [[nodiscard]] TupleDeltaView delta() const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] View operator[](size_t index);
        [[nodiscard]] View operator[](size_t index) const;

      protected:
        /**
         * Enter the underlying record mutation epoch.
         */
        void begin_mutation_scope();
        /**
         * Leave the underlying record mutation epoch.
         */
        void end_mutation_scope() noexcept;
        /**
         * Return the shared record dispatch surface for this tuple-like view.
         */
        [[nodiscard]] const detail::RecordViewDispatch *record_dispatch() const noexcept;
    };

    /**
     * Delta surface for tuple-compatible records.
     *
     * Tuples report the field positions updated during the current mutation
     * epoch together with the current value now stored in those positions.
     */
    struct HGRAPH_EXPORT TupleDeltaView : View
    {
        explicit TupleDeltaView(const View &view);

        [[nodiscard]] Range<size_t> updated_indices() const;
        [[nodiscard]] Range<View>   updated_values() const;

      protected:
        [[nodiscard]] static bool slot_is_updated(const void *context, size_t index);
        [[nodiscard]] static size_t project_index(const void *context, size_t index);
        [[nodiscard]] static View project_value(const void *context, size_t index);
        [[nodiscard]] const detail::RecordViewDispatch *record_dispatch() const noexcept;
    };

    /**
     * Non-owning typed wrapper over a bundle value.
     *
     * A bundle is a named heterogeneous collection. The bundle view supports
     * both positional and name-based access over the same underlying storage.
     */
    struct HGRAPH_EXPORT BundleView : TupleView
    {
        explicit BundleView(const View &view);

        /**
         * Return the mutable surface for this bundle view.
         */
        BundleMutationView begin_mutation();
        /**
         * Return the delta-inspection surface for the current mutation epoch.
         */
        [[nodiscard]] BundleDeltaView delta();
        [[nodiscard]] BundleDeltaView delta() const;
        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] View field(std::string_view name);
        [[nodiscard]] View field(std::string_view name) const;

      protected:
        [[nodiscard]] size_t field_index(std::string_view name) const;
    };

    /**
     * Delta surface for bundle mutation epochs.
     *
     * Bundles expose updated field names as keys and the current values stored
     * at those fields. The positional tuple-style delta remains available via
     * the inherited `updated_indices()` and `updated_values()` methods.
     */
    struct HGRAPH_EXPORT BundleDeltaView : TupleDeltaView
    {
        explicit BundleDeltaView(const View &view);

        [[nodiscard]] Range<std::string_view> updated_keys() const;

      private:
        [[nodiscard]] static std::string_view project_key(const void *context, size_t index);
    };

    /**
     * Mutable tuple surface.
     */
    struct HGRAPH_EXPORT TupleMutationView : TupleView
    {
        /**
         * Open a mutable tuple surface over the supplied tuple view.
         */
        explicit TupleMutationView(TupleView &view);
        TupleMutationView(const TupleMutationView &) = delete;
        TupleMutationView &operator=(const TupleMutationView &) = delete;
        /**
         * Transfer the mutation surface to a new wrapper.
         *
         * Transfer responsibility for closing the mutation scope.
         */
        TupleMutationView(TupleMutationView &&other) noexcept;
        TupleMutationView &operator=(TupleMutationView &&other) = delete;
        /**
         * Close the owned mutation scope, if any.
         */
        ~TupleMutationView();

        /**
         * Assign the supplied value to the tuple field at the given index.
         */
        void set(size_t index, const View &value);

        /**
         * Assign the supplied value and return this mutation view for fluent
         * tuple mutation chains.
         */
        TupleMutationView &setting(size_t index, const View &value)
        {
            set(index, value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            auto *dispatch = record_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("TupleMutationView::set on invalid view"); }
            if (index >= dispatch->size()) { throw std::out_of_range("TupleMutationView::set index out of range"); }

            using TValue = std::remove_cvref_t<T>;
            void *slot = dispatch->field_data(data(), index);
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                dispatch->field_dispatch(index).set_from_cpp(slot, std::addressof(value), value::scalar_type_meta<TValue>());
            } else {
                TValue moved_value = std::forward<T>(value);
                dispatch->field_dispatch(index).move_from_cpp(slot, std::addressof(moved_value), value::scalar_type_meta<TValue>());
            }
            dispatch->set_field_valid(data(), index, true);
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        TupleMutationView &setting(size_t index, T &&value)
        {
            set(index, std::forward<T>(value));
            return *this;
        }

      private:
        bool m_owns_scope{true};
    };

    /**
     * Mutable bundle surface.
     */
    struct HGRAPH_EXPORT BundleMutationView : BundleView
    {
        /**
         * Open a mutable bundle surface over the supplied bundle view.
         */
        explicit BundleMutationView(BundleView &view);
        BundleMutationView(const BundleMutationView &) = delete;
        BundleMutationView &operator=(const BundleMutationView &) = delete;
        /**
         * Transfer the mutation surface to a new wrapper.
         *
         * Transfer responsibility for closing the mutation scope.
         */
        BundleMutationView(BundleMutationView &&other) noexcept;
        BundleMutationView &operator=(BundleMutationView &&other) = delete;
        /**
         * Close the owned mutation scope, if any.
         */
        ~BundleMutationView();

        /**
         * Assign the supplied value to the bundle field at the given index.
         */
        void set(size_t index, const View &value);

        /**
         * Assign the supplied value and return this mutation view for fluent
         * bundle mutation chains.
         */
        BundleMutationView &setting(size_t index, const View &value)
        {
            set(index, value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            auto tuple_mutation = TupleMutationView{*this};
            tuple_mutation.set(index, std::forward<T>(value));
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        BundleMutationView &setting(size_t index, T &&value)
        {
            set(index, std::forward<T>(value));
            return *this;
        }

        /**
         * Assign the supplied value to the named bundle field.
         */
        void set_field(std::string_view name, const View &value);

        /**
         * Assign the supplied value to the named bundle field and return this
         * mutation view for fluent bundle mutation chains.
         */
        BundleMutationView &setting_field(std::string_view name, const View &value)
        {
            set_field(name, value);
            return *this;
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set_field(std::string_view name, T &&value)
        {
            const size_t index = field_index(name);
            set(index, std::forward<T>(value));
        }

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        BundleMutationView &setting_field(std::string_view name, T &&value)
        {
            set_field(name, std::forward<T>(value));
            return *this;
        }

      private:
        bool m_owns_scope{true};
    };

    inline TupleView View::as_tuple()
    {
        return TupleView{*this};
    }

    inline TupleView View::as_tuple() const
    {
        return TupleView{*this};
    }

    inline BundleView View::as_bundle()
    {
        return BundleView{*this};
    }

    inline BundleView View::as_bundle() const
    {
        return BundleView{*this};
    }

}  // namespace hgraph
