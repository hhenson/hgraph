#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{

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
            [[nodiscard]] virtual size_t size() const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &field_schema(size_t index) const = 0;
            [[nodiscard]] virtual const ViewDispatch &field_dispatch(size_t index) const = 0;
            [[nodiscard]] virtual std::string_view field_name(size_t index) const noexcept = 0;
            [[nodiscard]] virtual void *field_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *field_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool field_valid(const void *data, size_t index) const = 0;
            virtual void set_field_valid(void *data, size_t index, bool valid) const = 0;
        };

        /**
         * Return the cached builder for tuple and bundle schemas.
         *
         * Tuple and bundle layout is schema-specific because it depends on the
         * ordered child builders and the offsets chosen for their storage.
         */
        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *record_builder_for(const value::TypeMeta *schema);

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

        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] View operator[](size_t index);
        [[nodiscard]] View operator[](size_t index) const;

        void set(size_t index, const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set(size_t index, T &&value)
        {
            auto *dispatch = record_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("TupleView::set on invalid view"); }
            if (index >= dispatch->size()) { throw std::out_of_range("TupleView::set index out of range"); }

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

      protected:
        /**
         * Return the shared record dispatch surface for this tuple-like view.
         */
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

        [[nodiscard]] bool has_field(std::string_view name) const noexcept;
        [[nodiscard]] View field(std::string_view name);
        [[nodiscard]] View field(std::string_view name) const;
        void set_field(std::string_view name, const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        void set_field(std::string_view name, T &&value)
        {
            const size_t index = field_index(name);
            set(index, std::forward<T>(value));
        }

      private:
        [[nodiscard]] size_t field_index(std::string_view name) const;
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
