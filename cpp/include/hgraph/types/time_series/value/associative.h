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
         * operations. The storage keeps stable slots so removed payloads can be
         * retained internally until reuse or clear, but only live elements are
         * visible through the public set API. Iteration order is storage order
         * and is not part of the public semantic contract.
         */
        struct SetViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t slot_capacity(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &element_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &element_dispatch() const noexcept = 0;
            [[nodiscard]] virtual void *element_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *element_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual bool slot_occupied(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual void *slot_data(void *data, size_t slot) const = 0;
            [[nodiscard]] virtual const void *slot_data(const void *data, size_t slot) const = 0;
            [[nodiscard]] virtual bool contains(const void *data, const void *element) const = 0;
            [[nodiscard]] virtual bool add(void *data, const void *element) const = 0;
            [[nodiscard]] virtual bool remove(void *data, const void *element) const = 0;
            virtual void clear(void *data) const = 0;
        };

        /**
         * Behavior-only dispatch for map storage.
         *
         * Maps own homogeneous keys and values. Keys live in stable slots and
         * values are stored in parallel by slot. A present key always has a
         * present value; there is no separate logical-invalid state for map
         * values in this layer.
         */
        struct MapViewDispatch : ViewDispatch
        {
            [[nodiscard]] virtual size_t size(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t slot_capacity(const void *data) const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &key_schema() const noexcept = 0;
            [[nodiscard]] virtual const value::TypeMeta &value_schema() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &key_dispatch() const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &value_dispatch() const noexcept = 0;
            [[nodiscard]] virtual size_t find(const void *data, const void *key) const = 0;
            [[nodiscard]] virtual bool slot_occupied(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual void *key_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *key_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual void *value_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *value_data(const void *data, size_t index) const = 0;
            virtual bool set_item(void *data, const void *key, const void *value) const = 0;
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
        [[nodiscard]] size_t slot_capacity() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        [[nodiscard]] View at(size_t index);
        [[nodiscard]] View at(size_t index) const;
        [[nodiscard]] bool slot_occupied(size_t slot) const;
        [[nodiscard]] View at_slot(size_t slot);
        [[nodiscard]] View at_slot(size_t slot) const;
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
        [[nodiscard]] size_t slot_capacity() const;
        [[nodiscard]] const value::TypeMeta *key_schema() const;
        [[nodiscard]] const value::TypeMeta *value_schema() const;
        [[nodiscard]] bool slot_occupied(size_t slot) const;
        [[nodiscard]] View key_at_slot(size_t slot);
        [[nodiscard]] View key_at_slot(size_t slot) const;
        [[nodiscard]] View value_at_slot(size_t slot);
        [[nodiscard]] View value_at_slot(size_t slot) const;
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
