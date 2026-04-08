#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/slot_observer.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/view.h>

#include <cstddef>
#include <type_traits>
#include <utility>

namespace hgraph
{

    struct SetMutationView;
    struct MapMutationView;
    struct SetDeltaView;
    struct MapDeltaView;

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
         *
         * Plain builders omit the added/removed mutation journal and destroy
         * removed payloads immediately. Delta builders keep the extra state
         * needed to expose removed payloads and net added/removed ranges for
         * the current mutation epoch.
         */
        struct SetViewDispatch : ViewDispatch
        {
            /**
             * Start a new mutation epoch.
             *
             * Removed slots are released here so their payloads remain
             * inspectable by slot id until the next mutation begins. Plain
             * builders do not retain removed payloads, so this is a no-op for
             * them.
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
            [[nodiscard]] virtual bool slot_live(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_occupied(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_added(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_removed(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual void *slot_data(void *data, size_t slot) const = 0;
            [[nodiscard]] virtual const void *slot_data(const void *data, size_t slot) const = 0;
            [[nodiscard]] virtual bool contains(const void *data, const void *element) const = 0;
            /**
             * Ensure the set can hold at least `capacity` live elements
             * without shrinking or allocating beyond the requested capacity.
             *
             * This explicit reserve path is intended for callers that know
             * expected set cardinality in advance and want to avoid the
             * amortized growth policy used by ordinary insertion.
             */
            virtual void reserve(void *data, size_t capacity) const = 0;
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
         *
         * Plain builders omit the added/removed/updated mutation journal and
         * destroy removed payloads immediately. Delta builders retain removed
         * key/value payloads and track the net added/removed/updated slots for
         * the current mutation epoch.
         */
        struct MapViewDispatch : ViewDispatch
        {
            /**
             * Start a new mutation epoch.
             *
             * Removed slots are released here so erased key/value payloads stay
             * inspectable by slot id until the next mutation begins. Plain
             * builders do not retain removed payloads, so this is a no-op for
             * them.
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
            [[nodiscard]] virtual size_t first_live_slot(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t next_live_slot(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_occupied(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_added(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_removed(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual bool slot_updated(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual size_t first_added_slot(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t next_added_slot(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual size_t first_removed_slot(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t next_removed_slot(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual size_t first_updated_slot(const void *data) const noexcept = 0;
            [[nodiscard]] virtual size_t next_updated_slot(const void *data, size_t slot) const noexcept = 0;
            [[nodiscard]] virtual void *key_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *key_data(const void *data, size_t index) const = 0;
            [[nodiscard]] virtual void *value_data(void *data, size_t index) const = 0;
            [[nodiscard]] virtual const void *value_data(const void *data, size_t index) const = 0;
            virtual void add_slot_observer(void *data, SlotObserver *observer) const = 0;
            virtual void remove_slot_observer(void *data, SlotObserver *observer) const = 0;
            /**
             * Ensure the map can hold at least `capacity` live key/value pairs
             * without shrinking or allocating beyond the requested capacity.
             */
            virtual void reserve(void *data, size_t capacity) const = 0;
            virtual bool set_item(void *data, const void *key, const void *value) const = 0;
            virtual bool remove(void *data, const void *key) const = 0;
            virtual void clear(void *data) const = 0;
        };

        [[nodiscard]] HGRAPH_EXPORT const ValueBuilder *associative_builder_for(
            const value::TypeMeta *schema, MutationTracking tracking);

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
        SetMutationView begin_mutation();
        /**
         * Return the delta-inspection surface for the current mutation epoch.
         */
        [[nodiscard]] SetDeltaView delta();
        [[nodiscard]] SetDeltaView delta() const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *element_schema() const;
        /**
         * Return the live set elements as a storage-backed range.
         *
         * Set iteration uses the shared range abstraction used elsewhere in
         * the value layer rather than carrying a one-off container iterator
         * type on `SetView`.
         */
        [[nodiscard]] Range<View> values() const;
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

      private:
        [[nodiscard]] static bool slot_is_live(const void *context, size_t slot);
        [[nodiscard]] static View project_live_slot(const void *context, size_t slot);
    };

    /**
     * Slot-oriented delta surface for a set value.
     *
     * Delta inspection is separated from the normal set API for the same
     * reason mutation is separated: slot retention and added/removed flags are
     * time-series-oriented concerns rather than ordinary value-navigation
     * concerns.
     */
    struct HGRAPH_EXPORT SetDeltaView : View
    {
        explicit SetDeltaView(const View &view);

        [[nodiscard]] Range<View> added() const;
        [[nodiscard]] Range<View> removed() const;
        [[nodiscard]] size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(size_t slot) const;
        [[nodiscard]] bool slot_added(size_t slot) const;
        [[nodiscard]] bool slot_removed(size_t slot) const;
        [[nodiscard]] View at_slot(size_t slot);
        [[nodiscard]] View at_slot(size_t slot) const;

      private:
        [[nodiscard]] static bool slot_is_added(const void *context, size_t slot);
        [[nodiscard]] static bool slot_is_removed(const void *context, size_t slot);
        [[nodiscard]] static View project_slot(const void *context, size_t slot);
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
         * Ensure the set can hold at least `capacity` live elements without
         * shrinking or over-allocating beyond the requested capacity.
         */
        void reserve(size_t capacity);

        /**
         * Reserve capacity and return this mutation scope for fluent chains.
         */
        SetMutationView &reserving(size_t capacity)
        {
            reserve(capacity);
            return *this;
        }

        /**
         * Insert a new live element into the set.
         */
        [[nodiscard]] bool add(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        [[nodiscard]] bool add(T &&value)
        {
            auto *dispatch = set_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("SetMutationView::add on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("SetMutationView::add requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                return dispatch->add(data(), std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                return dispatch->add(data(), std::addressof(moved_value));
            }
        }
        /**
         * Insert a new live element into the set and return this mutation scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was inserted" result from `add(...)`.
         */
        SetMutationView &adding(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        SetMutationView &adding(T &&value)
        {
            static_cast<void>(add(std::forward<T>(value)));
            return *this;
        }
        /**
         * Remove a live element from the set.
         */
        [[nodiscard]] bool remove(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        [[nodiscard]] bool remove(T &&value)
        {
            auto *dispatch = set_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("SetMutationView::remove on invalid view"); }
            using TValue = std::remove_cvref_t<T>;
            if (&dispatch->element_schema() != value::scalar_type_meta<TValue>()) {
                throw std::invalid_argument("SetMutationView::remove requires a matching atomic element schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                return dispatch->remove(data(), std::addressof(value));
            } else {
                TValue moved_value = std::forward<T>(value);
                return dispatch->remove(data(), std::addressof(moved_value));
            }
        }
        /**
         * Remove a live element from the set and return this mutation scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was removed" result from `remove(...)`.
         */
        SetMutationView &removing(const View &value);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        SetMutationView &removing(T &&value)
        {
            static_cast<void>(remove(std::forward<T>(value)));
            return *this;
        }
        /**
         * Remove every live element from the set.
         */
        void clear();
        /**
         * Remove every live element from the set and return this mutation
         * scope.
         */
        SetMutationView &clearing();

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
        MapMutationView begin_mutation();
        /**
         * Return the delta-inspection surface for the current mutation epoch.
         */
        [[nodiscard]] MapDeltaView delta();
        [[nodiscard]] MapDeltaView delta() const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] const value::TypeMeta *key_schema() const;
        [[nodiscard]] const value::TypeMeta *value_schema() const;
        [[nodiscard]] size_t first_live_slot() const;
        [[nodiscard]] size_t next_live_slot(size_t slot) const;
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
     * Slot-oriented delta surface for a map value.
     *
     * This view exposes retained removed payloads and the per-slot added /
     * removed flags for the current mutation epoch without widening the normal
     * live-map API.
     */
    struct HGRAPH_EXPORT MapDeltaView : View
    {
        explicit MapDeltaView(const View &view);

        [[nodiscard]] Range<View> added_keys() const;
        [[nodiscard]] Range<View> removed_keys() const;
        [[nodiscard]] Range<View> updated_keys() const;
        [[nodiscard]] Range<View> added_values() const;
        [[nodiscard]] Range<View> removed_values() const;
        [[nodiscard]] Range<View> updated_values() const;
        [[nodiscard]] Range<std::pair<View, View>> added_items() const;
        [[nodiscard]] Range<std::pair<View, View>> removed_items() const;
        [[nodiscard]] Range<std::pair<View, View>> updated_items() const;
        [[nodiscard]] size_t slot_capacity() const;
        [[nodiscard]] bool slot_occupied(size_t slot) const;
        [[nodiscard]] bool slot_added(size_t slot) const;
        [[nodiscard]] bool slot_removed(size_t slot) const;
        [[nodiscard]] bool slot_updated(size_t slot) const;
        [[nodiscard]] size_t first_added_slot() const;
        [[nodiscard]] size_t next_added_slot(size_t slot) const;
        [[nodiscard]] size_t first_removed_slot() const;
        [[nodiscard]] size_t next_removed_slot(size_t slot) const;
        [[nodiscard]] size_t first_updated_slot() const;
        [[nodiscard]] size_t next_updated_slot(size_t slot) const;
        [[nodiscard]] View key_at_slot(size_t slot);
        [[nodiscard]] View key_at_slot(size_t slot) const;
        [[nodiscard]] View value_at_slot(size_t slot);
        [[nodiscard]] View value_at_slot(size_t slot) const;

      private:
        [[nodiscard]] static bool slot_is_added(const void *context, size_t slot);
        [[nodiscard]] static bool slot_is_removed(const void *context, size_t slot);
        [[nodiscard]] static bool slot_is_updated(const void *context, size_t slot);
        [[nodiscard]] static View project_key(const void *context, size_t slot);
        [[nodiscard]] static View project_value(const void *context, size_t slot);
        [[nodiscard]] static std::pair<View, View> project_item(const void *context, size_t slot);
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
         * Ensure the map can hold at least `capacity` live key/value pairs
         * without shrinking or over-allocating beyond the requested capacity.
         */
        void reserve(size_t capacity);

        /**
         * Reserve capacity and return this mutation scope for fluent chains.
         */
        MapMutationView &reserving(size_t capacity)
        {
            reserve(capacity);
            return *this;
        }

        /**
         * Insert or replace the value for a key.
         */
        void set(const View &key, const View &value);

        template <typename TKey, typename TValue>
            requires(!std::derived_from<std::remove_cvref_t<TKey>, View> &&
                     !std::derived_from<std::remove_cvref_t<TValue>, View>)
        void set(TKey &&key, TValue &&value)
        {
            auto *dispatch = map_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("MapMutationView::set on invalid view"); }
            using TKeyValue = std::remove_cvref_t<TKey>;
            using TMappedValue = std::remove_cvref_t<TValue>;
            if (&dispatch->key_schema() != value::scalar_type_meta<TKeyValue>()) {
                throw std::invalid_argument("MapMutationView::set requires a matching atomic key schema");
            }
            if (&dispatch->value_schema() != value::scalar_type_meta<TMappedValue>()) {
                throw std::invalid_argument("MapMutationView::set requires a matching atomic value schema");
            }

            if constexpr (std::is_lvalue_reference_v<TKey &&> && std::is_lvalue_reference_v<TValue &&>) {
                dispatch->set_item(data(), std::addressof(key), std::addressof(value));
            } else if constexpr (std::is_lvalue_reference_v<TKey &&>) {
                TMappedValue mapped_value = std::forward<TValue>(value);
                dispatch->set_item(data(), std::addressof(key), std::addressof(mapped_value));
            } else if constexpr (std::is_lvalue_reference_v<TValue &&>) {
                TKeyValue key_value = std::forward<TKey>(key);
                dispatch->set_item(data(), std::addressof(key_value), std::addressof(value));
            } else {
                TKeyValue key_value = std::forward<TKey>(key);
                TMappedValue mapped_value = std::forward<TValue>(value);
                dispatch->set_item(data(), std::addressof(key_value), std::addressof(mapped_value));
            }
        }
        /**
         * Insert or replace the value for a key and return this mutation
         * scope.
         */
        MapMutationView &setting(const View &key, const View &value);

        template <typename TKey, typename TValue>
            requires(!std::derived_from<std::remove_cvref_t<TKey>, View> &&
                     !std::derived_from<std::remove_cvref_t<TValue>, View>)
        MapMutationView &setting(TKey &&key, TValue &&value)
        {
            set(std::forward<TKey>(key), std::forward<TValue>(value));
            return *this;
        }
        /**
         * Remove a live key and its value from the map.
         */
        [[nodiscard]] bool remove(const View &key);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        [[nodiscard]] bool remove(T &&key)
        {
            auto *dispatch = map_dispatch();
            if (dispatch == nullptr) { throw std::runtime_error("MapMutationView::remove on invalid view"); }
            using TKeyValue = std::remove_cvref_t<T>;
            if (&dispatch->key_schema() != value::scalar_type_meta<TKeyValue>()) {
                throw std::invalid_argument("MapMutationView::remove requires a matching atomic key schema");
            }
            if constexpr (std::is_lvalue_reference_v<T &&>) {
                return dispatch->remove(data(), std::addressof(key));
            } else {
                TKeyValue key_value = std::forward<T>(key);
                return dispatch->remove(data(), std::addressof(key_value));
            }
        }
        /**
         * Remove a live key and its value from the map and return this mutation
         * scope.
         *
         * This supports fluent mutation chains when the caller does not need
         * the boolean "was removed" result from `remove(...)`.
         */
        MapMutationView &removing(const View &key);

        template <typename T>
            requires(!std::derived_from<std::remove_cvref_t<T>, View>)
        MapMutationView &removing(T &&key)
        {
            static_cast<void>(remove(std::forward<T>(key)));
            return *this;
        }
        /**
         * Remove every live key/value pair from the map.
         */
        void clear();
        /**
         * Remove every live key/value pair from the map and return this
         * mutation scope.
         */
        MapMutationView &clearing();

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
