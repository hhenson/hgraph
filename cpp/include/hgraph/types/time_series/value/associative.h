#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>

namespace hgraph
{

    struct ValueBuilder;

    namespace detail
    {

        /**
         * Behavior-only dispatch for set storage.
         *
         * Sets own homogeneous elements with uniqueness enforced by the set
         * operations. Iteration order is storage order and is not part of the
         * public semantic contract.
         */
        struct SetViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &element_dispatch() const noexcept = 0;
            [[nodiscard]] virtual void *element_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *element_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool contains(const void *data, const void *element) const = 0;
            [[nodiscard]] virtual bool add(void *data, const void *element) const = 0;
            [[nodiscard]] virtual bool remove(void *data, const void *element) const = 0;
            virtual void clear(void *data) const = 0;
        };

        /**
         * Behavior-only dispatch for map storage.
         *
         * Maps own homogeneous keys and values. Values may be invalid for a
         * present key, so lookup can return an invalid value view for the known
         * value schema.
         */
        struct MapViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &key_schema() const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &value_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &key_dispatch() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &value_dispatch() const noexcept = 0;
            [[nodiscard]] virtual size_t find(const void *data, const void *key) const = 0;
            [[nodiscard]] virtual void *key_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *key_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual void *value_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *value_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool value_valid(const void *data, size_t index) const = 0;
            virtual void set_value_valid(void *data, size_t index, bool valid) const = 0;
            virtual bool set_item(void *data, const void *key, const void *value, bool value_valid) const = 0;
            virtual bool remove(void *data, const void *key) const = 0;
            virtual void clear(void *data) const = 0;
        };

        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *associative_builder_for(const value::TypeMeta *schema);

    }  // namespace detail

    /**
     * Non-owning typed wrapper over a set value.
     */
    struct HGRAPH_EXPORT SetView : View
    {
        explicit SetView(const View &view);

        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] bool contains(const View &value) const;
        bool add(const View &value);
        bool remove(const View &value);
        void clear();

      private:
        [[nodiscard]] const detail::SetViewDispatch *set_dispatch() const noexcept;
    };

    /**
     * Non-owning typed wrapper over a map value.
     */
    struct HGRAPH_EXPORT MapView : View
    {
        explicit MapView(const View &view);

        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *key_schema() const;
        [[nodiscard]] const value::TypeMeta *value_schema() const;
        [[nodiscard]] bool contains(const View &key) const;
        [[nodiscard]] View at(const View &key);
        [[nodiscard]] View at(const View &key) const;
        void set(const View &key, const View &value);
        bool remove(const View &key);
        void clear();

      private:
        [[nodiscard]] const detail::MapViewDispatch *map_dispatch() const noexcept;
    };

    inline SetView View::as_set()
    {
        return SetView{*this};
    }

    inline SetView View::as_set() const
    {
        return SetView{*this};
    }

    inline MapView View::as_map()
    {
        return MapView{*this};
    }

    inline MapView View::as_map() const
    {
        return MapView{*this};
    }

}  // namespace hgraph
