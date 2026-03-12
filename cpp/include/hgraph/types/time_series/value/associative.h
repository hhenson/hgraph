#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>

namespace hgraph
{

    struct SetMutationView;
    struct MapMutationView;

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
            /**
             * Start a new mutation epoch.
             *
             * Removed slots are released here so their payloads remain
             * inspectable by slot id until the next mutation begins.
             */
            virtual void begin_mutation(void *data) const = 0;
            /**
             * End the current mutation epoch.
             */
            virtual void end_mutation(void *data) const = 0;
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
            /**
             * Start a new mutation epoch.
             *
             * Removed slots are released here so erased key/value payloads stay
             * inspectable by slot id until the next mutation begins.
             */
            virtual void begin_mutation(void *data) const = 0;
            /**
             * End the current mutation epoch.
             */
            virtual void end_mutation(void *data) const = 0;
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

        /**
         * Start a mutation scope over this set.
         *
         * The returned mutation view owns the matching `end_mutation()` call.
         * Nested scopes are allowed and are tracked with a depth count in the
         * underlying storage so callers can build larger operations from
         * smaller helpers without prematurely releasing removed slots.
         */
        [[nodiscard]] SetMutationView begin_mutation();
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

      protected:
        /**
         * Enter the underlying mutation epoch.
         *
         * This is protected so only the RAII mutation wrapper can expose the
         * mutating surface.
         */
        void begin_mutation_scope();
        /**
         * Leave the underlying mutation epoch.
         *
         * This is protected so the matching `end_mutation()` stays coupled to
         * the RAII mutation wrapper rather than becoming part of the general
         * read-only view surface.
         */
        void end_mutation_scope() noexcept;
        [[nodiscard]] const detail::SetViewDispatch *set_dispatch() const noexcept;
    };

    /**
     * RAII mutation scope for a set value.
     *
     * A mutation scope guarantees that `end_mutation()` runs when the scope is
     * destroyed, even when mutation exits through an exception. The scope is
     * move-only so there is exactly one owner responsible for closing the
     * mutation depth it opened.
     */
    struct HGRAPH_EXPORT SetMutationView : SetView
    {
        /**
         * Open a mutation scope over the supplied set view.
         */
        explicit SetMutationView(SetView &view);
        SetMutationView(const SetMutationView &) = delete;
        SetMutationView &operator=(const SetMutationView &) = delete;
        /**
         * Transfer responsibility for closing the mutation scope.
         */
        SetMutationView(SetMutationView &&other) noexcept;
        SetMutationView &operator=(SetMutationView &&other) = delete;
        /**
         * Close the owned mutation scope, if any.
         */
        ~SetMutationView();

        /**
         * Insert a new live element into the set.
         */
        [[nodiscard]] bool add(const View &value);
        /**
         * Insert a new live element into the set and return this mutation scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was inserted" result from `add(...)`.
         */
        [[nodiscard]] SetMutationView &adding(const View &value);
        /**
         * Remove a live element from the set.
         */
        [[nodiscard]] bool remove(const View &value);
        /**
         * Remove a live element from the set and return this mutation scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was removed" result from `remove(...)`.
         */
        [[nodiscard]] SetMutationView &removing(const View &value);
        /**
         * Remove every live element from the set.
         */
        void clear();
        /**
         * Remove every live element from the set and return this mutation
         * scope.
         */
        [[nodiscard]] SetMutationView &clearing();

      private:
        /**
         * Tracks whether this RAII wrapper still owns the matching
         * `end_mutation()` call.
         */
        bool m_owns_scope{true};
    };

    /**
     * Non-owning typed wrapper over a map value.
     */
    struct HGRAPH_EXPORT MapView : View
    {
        explicit MapView(const View &view);

        /**
         * Start a mutation scope over this map.
         *
         * The returned mutation view owns the matching `end_mutation()` call.
         * Nested scopes are allowed and are tracked with a depth count in the
         * underlying storage so callers can build larger operations from
         * smaller helpers without prematurely releasing removed slots.
         */
        [[nodiscard]] MapMutationView begin_mutation();
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

      protected:
        /**
         * Enter the underlying mutation epoch.
         *
         * This is protected so only the RAII mutation wrapper can expose the
         * mutating surface.
         */
        void begin_mutation_scope();
        /**
         * Leave the underlying mutation epoch.
         *
         * This is protected so the matching `end_mutation()` stays coupled to
         * the RAII mutation wrapper rather than becoming part of the general
         * read-only view surface.
         */
        void end_mutation_scope() noexcept;
        [[nodiscard]] const detail::MapViewDispatch *map_dispatch() const noexcept;
    };

    /**
     * RAII mutation scope for a map value.
     *
     * A mutation scope guarantees that `end_mutation()` runs when the scope is
     * destroyed, even when mutation exits through an exception. The scope is
     * move-only so there is exactly one owner responsible for closing the
     * mutation depth it opened.
     */
    struct HGRAPH_EXPORT MapMutationView : MapView
    {
        /**
         * Open a mutation scope over the supplied map view.
         */
        explicit MapMutationView(MapView &view);
        MapMutationView(const MapMutationView &) = delete;
        MapMutationView &operator=(const MapMutationView &) = delete;
        /**
         * Transfer responsibility for closing the mutation scope.
         */
        MapMutationView(MapMutationView &&other) noexcept;
        MapMutationView &operator=(MapMutationView &&other) = delete;
        /**
         * Close the owned mutation scope, if any.
         */
        ~MapMutationView();

        /**
         * Insert or replace the value for a key.
         */
        void set(const View &key, const View &value);
        /**
         * Insert or replace the value for a key and return this mutation
         * scope.
         */
        [[nodiscard]] MapMutationView &setting(const View &key, const View &value);
        /**
         * Remove a live key and its value from the map.
         */
        [[nodiscard]] bool remove(const View &key);
        /**
         * Remove a live key and its value from the map and return this mutation
         * scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was removed" result from `remove(...)`.
         */
        [[nodiscard]] MapMutationView &removing(const View &key);
        /**
         * Remove every live key/value pair from the map.
         */
        void clear();
        /**
         * Remove every live key/value pair from the map and return this
         * mutation scope.
         */
        [[nodiscard]] MapMutationView &clearing();

      private:
        /**
         * Tracks whether this RAII wrapper still owns the matching
         * `end_mutation()` call.
         */
        bool m_owns_scope{true};
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
