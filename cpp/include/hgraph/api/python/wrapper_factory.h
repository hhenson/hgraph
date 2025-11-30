//
// Wrapper Factory - Creates appropriate wrapper based on runtime type inspection
//

#ifndef HGRAPH_WRAPPER_FACTORY_H
#define HGRAPH_WRAPPER_FACTORY_H

#include "py_time_series.h"
// Ensure Nanobind core types are available for iterator helpers below
#include <nanobind/nanobind.h>

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_graph.h>
#include <hgraph/api/python/py_node.h>
#include <memory>
#include <type_traits>

namespace hgraph
{
    // Forward declarations
    struct Node;
    struct Graph;
    struct NodeScheduler;
    struct TimeSeriesInput;
    struct TimeSeriesOutput;

    /**
     * Wrap a Node pointer in a PyNode.
     * Creates a new wrapper for the node.
     */
    nb::object wrap_node(const hgraph::Node *impl, const control_block_ptr &control_block);

    nb::object wrap_node(const Node *impl);

    // Overload for shared_ptr - avoids creating new shared_ptr when one already exists
    nb::object wrap_node(const node_ptr &impl);

    /**
     * Wrap a Graph pointer in a PyGraph.
     * Creates a new wrapper for the graph.
     */
    nb::object wrap_graph(const hgraph::Graph *impl, const control_block_ptr &control_block);

    // Overload for shared_ptr - avoids creating new shared_ptr when one already exists
    nb::object wrap_graph(const graph_ptr &impl);

    /**
     * Wrap a Traits pointer in a PyTraits.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_traits(const hgraph::Traits *impl, const control_block_ptr &control_block);

    nb::object wrap_traits(const traits_ptr &traits);

    /**
     * Wrap a NodeScheduler pointer in a PyNodeScheduler.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_node_scheduler(const hgraph::NodeScheduler *impl, const control_block_ptr &control_block);

    /**
     * Wrap a TimeSeriesInput pointer in the appropriate PyTimeSeriesXxxInput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     * Caches the created wrapper for future use.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    nb::object wrap_input(const hgraph::TimeSeriesInput *impl, const control_block_ptr &control_block);

    nb::object wrap_input(const TimeSeriesInput *impl);

    // Overload for shared_ptr - avoids creating new shared_ptr when one already exists
    nb::object wrap_input(const time_series_input_ptr &impl);

    /**
     * Wrap a TimeSeriesOutput pointer in the appropriate PyTimeSeriesXxxOutput wrapper.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Uses dynamic_cast to determine actual runtime type and creates specialized wrapper.
     * Caches the created wrapper for future use.
     *
     * Handles: TS, Signal, TSL, TSB, TSD, TSS, TSW, REF and their specializations.
     */
    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl, const control_block_ptr &control_block);

    nb::object wrap_output(const hgraph::TimeSeriesOutput *impl);

    // Overload for shared_ptr - avoids creating new shared_ptr when one already exists
    nb::object wrap_output(const time_series_output_ptr &impl);

    nb::object wrap_time_series(const TimeSeriesInput *impl, const control_block_ptr &control_block);

    nb::object wrap_time_series(const TimeSeriesOutput *impl, const control_block_ptr &control_block);

    nb::object wrap_time_series(const TimeSeriesInput *impl);

    nb::object wrap_time_series(const TimeSeriesOutput *impl);

    // Overloads for shared_ptr - avoids creating new shared_ptr when one already exists
    nb::object wrap_time_series(const time_series_input_ptr &impl);

    nb::object wrap_time_series(const time_series_output_ptr &impl);

    // ---------------------------------------------------------------------
    // Lightweight Nanobind iterator helpers for time series wrapping
    // ---------------------------------------------------------------------
    // These helpers create Python iterators that transform underlying C++
    // iterator elements into wrapped time series Python objects using the
    // provided control block. Implemented as templates fully in the header
    // to keep usage simple and avoid separate adapter iterator types.

    namespace detail
    {
        // Accessors used by iterator helpers
        struct tsd_identity_accessor
        {
            template <typename Iterator> static decltype(auto) get(Iterator &it) { return (*it); }
        };

        struct tsd_second_accessor
        {
            template <typename Iterator> static decltype(auto) get(Iterator &it) { return (*it).second; }
        };

        template <typename Iterator, typename Sentinel, typename Accessor> struct tsd_mapped_iter_state
        {
            Iterator it;
            Sentinel end;
            bool     first_or_done;
        };

        // State that stores the collection to ensure iterator lifetime
        template <typename Collection, typename Accessor> struct tsd_collection_iter_state
        {
            std::shared_ptr<Collection>   collection; // Store the collection in shared_ptr
            typename Collection::iterator it;
            typename Collection::iterator end;
            bool                          first_or_done;
        };

        template <typename Iterator, typename Sentinel, typename Accessor,
                  typename ValueType = typename nb::detail::iterator_access<Iterator>::result_type>
        inline nb::typed<nb::iterator, ValueType> make_time_series_iterator_ex(nb::handle scope, const char *name, Iterator first,
                                                                               Sentinel   last, control_block_ptr cb) {
            using State = tsd_mapped_iter_state<Iterator, Sentinel, Accessor>;

            static nb::ft_mutex mu;
            nb::ft_lock_guard   lock(mu);
            if (!nb::type<State>().is_valid()) {
                nb::class_<State>(scope, name)
                    .def("__iter__", [](nb::handle h) { return h; })
                    .def("__next__", [](State &s) -> nb::object {
                        if (!s.first_or_done) ++s.it;
                        else s.first_or_done = false;

                        if (s.it == s.end) {
                            s.first_or_done = true;
                            throw nb::stop_iteration();
                        }

                        // Project element and wrap into a Python time series
                        auto &&elem = Accessor::get(s.it);
                        return wrap_time_series(elem);
                    });
            }

            return nb::borrow<nb::typed<nb::iterator, nb::object> >(
                nb::cast(State{std::move(first), std::move(last), true}));
        }
    } // namespace detail

    // Direct-iteration version: wraps elements yielded by the iterator
    // (use when the iterator dereferences to a time series pointer/ref)
    template <typename Iterator, typename Sentinel,
              typename ValueType = typename nb::detail::iterator_access<Iterator>::result_type>
    inline nb::typed<nb::iterator, ValueType> make_time_series_iterator(nb::handle scope, const char *     name, Iterator first,
                                                                        Sentinel   last, control_block_ptr cb) {
        return detail::make_time_series_iterator_ex<Iterator, Sentinel, detail::tsd_identity_accessor>(
            scope, name, std::move(first), std::move(last), std::move(cb));
    }

    // Collection-based version: stores the collection to ensure iterator lifetime
    template <typename Collection,
              typename ValueType = typename nb::detail::iterator_access<decltype(std::declval<Collection>().begin())>::result_type>
    inline nb::typed<nb::iterator, ValueType>
    make_time_series_iterator(nb::handle scope, const char *name, Collection &&collection) {
        // Store the collection in the state to ensure lifetime
        using CollectionType = std::decay_t<Collection>;
        using State          = detail::tsd_collection_iter_state<CollectionType, detail::tsd_identity_accessor>;

        static nb::ft_mutex mu;
        nb::ft_lock_guard   lock(mu);
        if (!nb::type<State>().is_valid()) {
            nb::class_<State>(scope, name)
                .def("__iter__", [](nb::handle h) { return h; })
                .def("__next__", [](State &s) -> nb::object {
                    if (!s.first_or_done) ++s.it;
                    else s.first_or_done = false;

                    if (s.it == s.end) {
                        s.first_or_done = true;
                        throw nb::stop_iteration();
                    }

                    auto &&elem = detail::tsd_identity_accessor::get(s.it);
                    return wrap_time_series(elem);
                });
        }

        // Move the collection into a shared_ptr to extend its lifetime
        auto coll_ptr = std::make_shared<CollectionType>(std::forward<Collection>(collection));
        return nb::borrow<nb::typed<nb::iterator, ValueType> >(
            nb::cast(State{coll_ptr, coll_ptr->begin(), coll_ptr->end(), true}));
    }

    // Value-iteration version: wraps the `.second` of pair-like iterators
    template <typename Iterator, typename Sentinel,
              typename ValueType = typename nb::detail::iterator_access<Iterator>::result_type>
    inline nb::typed<nb::iterator, ValueType> make_time_series_value_iterator(nb::handle scope, const char *name, Iterator first,
                                                                              Sentinel   last, control_block_ptr cb) {
        return detail::make_time_series_iterator_ex<Iterator, Sentinel, detail::tsd_second_accessor>(
            scope, name, std::move(first), std::move(last), std::move(cb));
    }

    // Collection-based version: stores the collection to ensure iterator lifetime
    template <typename Collection,
              typename ValueType = typename nb::detail::iterator_access<decltype(std::declval<Collection>().begin())>::result_type>
    inline nb::typed<nb::iterator, ValueType> make_time_series_value_iterator(nb::handle   scope, const char *           name,
                                                                              Collection &&collection, control_block_ptr cb) {
        // Store the collection in the state to ensure lifetime
        using CollectionType = std::decay_t<Collection>;
        using State          = detail::tsd_collection_iter_state<CollectionType, detail::tsd_second_accessor>;

        static nb::ft_mutex mu;
        nb::ft_lock_guard   lock(mu);
        if (!nb::type<State>().is_valid()) {
            nb::class_<State>(scope, name)
                .def("__iter__", [](nb::handle h) { return h; })
                .def("__next__", [](State &s) -> nb::object {
                    if (!s.first_or_done) ++s.it;
                    else s.first_or_done = false;

                    if (s.it == s.end) {
                        s.first_or_done = true;
                        throw nb::stop_iteration();
                    }

                    auto &&elem = detail::tsd_second_accessor::get(s.it);
                    // elem is a shared_ptr, need to get raw pointer
                    return wrap_time_series(elem);
                });
        }

        // Move the collection into a shared_ptr to extend its lifetime
        auto coll_ptr = std::make_shared<CollectionType>(std::forward<Collection>(collection));
        return nb::borrow<nb::typed<nb::iterator, ValueType> >(
            nb::cast(State{coll_ptr, coll_ptr->begin(), coll_ptr->end(), std::move(cb), true}));
    }

    namespace detail
    {
        // Items iterator state mirroring the mapped iterator style above
        template <typename Iterator, typename Sentinel> struct tsd_items_iter_state
        {
            Iterator it;
            Sentinel end;
            bool     first_or_done;
        };

        // State that stores the collection for items iterator to ensure iterator lifetime
        template <typename Collection> struct tsd_collection_items_iter_state
        {
            std::shared_ptr<Collection>   collection; // Store the collection in shared_ptr
            typename Collection::iterator it;
            typename Collection::iterator end;
            bool                          first_or_done;
        };

        template <typename Iterator, typename Sentinel,
                  typename ValueType = typename nb::detail::iterator_access<Iterator>::result_type>
        inline nb::typed<nb::iterator, ValueType> make_time_series_items_iterator_ex(nb::handle scope, const char *name,
                                                                                     Iterator   first, Sentinel    last) {
            using State = tsd_items_iter_state<Iterator, Sentinel>;

            static nb::ft_mutex mu;
            nb::ft_lock_guard   lock(mu);
            if (!nb::type<State>().is_valid()) {
                nb::class_<State>(scope, name)
                    .def("__iter__", [](nb::handle h) { return h; })
                    .def("__next__", [](State &s) -> nb::object {
                        if (!s.first_or_done) ++s.it;
                        else s.first_or_done = false;

                        if (s.it == s.end) {
                            s.first_or_done = true;
                            throw nb::stop_iteration();
                        }

                        // Build (key, wrapped_value) tuple
                        // Note: many range views (filter/transform) yield proxy objects on dereference.
                        // Do not bind to a non-const lvalue reference; take by value to avoid dangling refs.
                        const auto &[key, value] = *s.it; // pair-like element (key, value)
                        nb::object  key_obj      = nb::cast(key);
                        // value is a shared_ptr, need to get raw pointer
                        nb::object val_obj = wrap_time_series(value);
                        return nb::make_tuple(std::move(key_obj), std::move(val_obj));
                    });
            }

            return nb::borrow<nb::typed<nb::iterator, ValueType> >(
                nb::cast(State{std::move(first), std::move(last), true}));
        }
    } // namespace detail

    // Items-iteration version: returns (key, wrapped(value)) tuples for pair-like iterators
    // The key is converted using nb::cast; the value is wrapped with the provided control block.
    template <typename Iterator, typename Sentinel,
              typename ValueType = typename nb::detail::iterator_access<Iterator>::result_type>
    inline nb::typed<nb::iterator, ValueType> make_time_series_items_iterator(nb::handle scope, const char *name, Iterator first,
                                                                              Sentinel   last) {
        return detail::make_time_series_items_iterator_ex(scope, name, std::move(first), std::move(last));
    }

    // Collection-based version: stores the collection to ensure iterator lifetime
    template <typename Collection,
              typename ValueType = typename nb::detail::iterator_access<decltype(std::declval<Collection>().begin())>::result_type>
    inline nb::typed<nb::iterator, ValueType> make_time_series_items_iterator(nb::handle   scope, const char *name,
                                                                              Collection &&collection) {
        // Store the collection in the state to ensure lifetime
        using CollectionType  = std::decay_t<Collection>;
        using CollectionState = detail::tsd_collection_items_iter_state<CollectionType>;
        auto coll_ptr         = std::make_shared<CollectionType>(std::forward<Collection>(collection));

        static nb::ft_mutex mu;
        nb::ft_lock_guard   lock(mu);
        if (!nb::type<CollectionState>().is_valid()) {
            nb::class_<CollectionState>(scope, name)
                .def("__iter__", [](nb::handle h) { return h; })
                .def("__next__", [](CollectionState &s) -> nb::object {
                    if (!s.first_or_done) ++s.it;
                    else s.first_or_done = false;

                    if (s.it == s.end) {
                        s.first_or_done = true;
                        throw nb::stop_iteration();
                    }

                    // Build (key, wrapped_value) tuple
                    const auto &[key, value] = *s.it;
                    nb::object  key_obj      = nb::cast(key);
                    // value is a shared_ptr, need to get raw pointer
                    nb::object val_obj = wrap_time_series(value);
                    return nb::make_tuple(std::move(key_obj), std::move(val_obj));
                });
        }

        return nb::borrow<nb::typed<nb::iterator, ValueType> >(
            nb::cast(CollectionState{coll_ptr, coll_ptr->begin(), coll_ptr->end(), true}));
    }

    /**
     * Extract Node shared_ptr from PyNode wrapper.
     * Returns nullptr if obj is not a PyNode.
     */
    node_ptr unwrap_node(const nb::handle &obj);

    /**
     * Extract TimeSeriesInput shared_ptr from PyTimeSeriesInput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesInput.
     */
    time_series_input_ptr unwrap_input(const nb::handle &obj);

    template <typename T> requires std::is_base_of_v<TimeSeriesInput, T>
    std::shared_ptr<T> unwrap_input_as(const nb::handle &obj) {
        auto ptr = unwrap_input(obj);
        return ptr ? std::dynamic_pointer_cast<T>(ptr) : nullptr;
    }

    time_series_input_ptr unwrap_input(const PyTimeSeriesInput &input_);

    /**
     * Extract TimeSeriesOutput shared_ptr from PyTimeSeriesOutput wrapper.
     * Returns nullptr if obj is not a PyTimeSeriesOutput.
     */
    time_series_output_ptr unwrap_output(const nb::handle &obj);

    time_series_output_ptr unwrap_output(const PyTimeSeriesOutput &output_);

    template <typename T> requires std::is_base_of_v<TimeSeriesOutput, T>
    std::shared_ptr<T> unwrap_output_as(const nb::handle &obj) {
        auto ptr = unwrap_output(obj);
        return ptr ? std::dynamic_pointer_cast<T>(ptr) : nullptr;
    }

    /**
     * Wrap an EvaluationEngineApi pointer in a PyEvaluationEngineApi.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_evaluation_engine_api(const hgraph::EvaluationEngineApi *impl, control_block_ptr control_block);

    /**
     * Wrap an EvaluationClock pointer in a PyEvaluationClock.
     * Uses cached Python wrapper if available (via intrusive_base::self_py()).
     * Creates and caches new wrapper if not.
     */
    nb::object wrap_evaluation_clock(const hgraph::EvaluationClock *impl, control_block_ptr control_block);

} // namespace hgraph

#endif  // HGRAPH_WRAPPER_FACTORY_H