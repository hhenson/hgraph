//
// Time Series Visitor Pattern Support
//
// This file provides both CRTP and Acyclic visitor pattern support for TimeSeriesInput/Output.
//
// Design Philosophy:
// - CRTP visitors: For internal, performance-critical code with zero overhead (compile-time dispatch)
// - Acyclic visitors: For extensions, plugins, and Python bindings (runtime dispatch)
// - Both patterns are supported simultaneously via function overloading and concepts
//
// Usage:
// 1. For internal code: Derive from TimeSeriesOutputVisitorCRTP/TimeSeriesInputVisitorCRTP
// 2. For extensions: Derive from TimeSeriesVisitor + specific TimeSeriesOutputVisitor<T>/TimeSeriesInputVisitor<T>
// 3. Call accept() on any TimeSeriesInput/Output - the compiler selects the right dispatch
//

#ifndef TIME_SERIES_VISITOR_H
#define TIME_SERIES_VISITOR_H

#include <hgraph/hgraph_forward_declarations.h>
#include <type_traits>

namespace hgraph {
    // Forward declarations needed for visitor interfaces
    // (Most are already in hgraph_forward_declarations.h)

    // Value types (TS[T])
    template<typename T> struct TimeSeriesValueInput;
    template<typename T> struct TimeSeriesValueOutput;

    // Bundle types (TSB)
    struct TimeSeriesBundleInput;
    struct TimeSeriesBundleOutput;

    // Dict types (TSD[K, V])
    struct TimeSeriesDictInput;
    struct TimeSeriesDictOutput;
    template<typename K> struct TimeSeriesDictInput_T;
    template<typename K> struct TimeSeriesDictOutput_T;

    // List types (TSL)
    struct TimeSeriesListInput;
    struct TimeSeriesListOutput;

    // Set types (TSS)
    struct TimeSeriesSetInput;
    struct TimeSeriesSetOutput;
    template<typename T> struct TimeSeriesSetInput_T;
    template<typename T> struct TimeSeriesSetOutput_T;

    // Reference types (REF)
    struct TimeSeriesReferenceInput;
    struct TimeSeriesReferenceOutput;

    // ============================================================================
    // CRTP Visitors - For internal, performance-critical code (zero overhead)
    // ============================================================================

    /**
     * @brief CRTP base for visiting TimeSeriesOutput types
     *
     * Use this for internal operations that need maximum performance.
     * The derived class must implement visit() methods for each type.
     *
     * @tparam Derived The derived visitor class (CRTP pattern)
     *
     * Example:
     * @code
     * struct MyCRTPVisitor : TimeSeriesOutputVisitorCRTP<MyCRTPVisitor> {
     *     template<typename T>
     *     void visit(TimeSeriesValueOutput<T>& output) {
     *         // Handle value output
     *     }
     *
     *     void visit(TimeSeriesBundleOutput& output) {
     *         // Handle bundle output
     *     }
     * };
     *
     * MyCRTPVisitor visitor;
     * output->accept(visitor);  // Zero-overhead compile-time dispatch
     * @endcode
     */
    template<typename Derived>
    struct TimeSeriesOutputVisitorCRTP {
        /**
         * @brief Function call operator for visiting an output
         * Dispatches to the derived class's visit() method
         */
        template<typename TS>
        decltype(auto) operator()(TS& ts) {
            return static_cast<Derived*>(this)->visit(ts);
        }

        template<typename TS>
        decltype(auto) operator()(const TS& ts) const {
            return static_cast<const Derived*>(this)->visit(ts);
        }
    };

    /**
     * @brief CRTP base for visiting TimeSeriesInput types
     *
     * @tparam Derived The derived visitor class (CRTP pattern)
     *
     * @see TimeSeriesOutputVisitorCRTP for usage example
     */
    template<typename Derived>
    struct TimeSeriesInputVisitorCRTP {
        template<typename TS>
        decltype(auto) operator()(TS& ts) {
            return static_cast<Derived*>(this)->visit(ts);
        }

        template<typename TS>
        decltype(auto) operator()(const TS& ts) const {
            return static_cast<const Derived*>(this)->visit(ts);
        }
    };

    // ============================================================================
    // Acyclic Visitors - For extensions, plugins, and Python bindings
    // ============================================================================

    /**
     * @brief Base interface for acyclic visitors (empty tag interface)
     *
     * This serves as a marker interface to distinguish acyclic visitors from CRTP visitors.
     * All acyclic visitors must inherit from this interface.
     */
    struct HGRAPH_EXPORT TimeSeriesVisitor {
        virtual ~TimeSeriesVisitor() = default;
    };

    /**
     * @brief Typed visitor interface for a specific TimeSeriesOutput type
     *
     * Inherit from this interface for each concrete type you want to visit.
     *
     * @tparam T The concrete TimeSeriesOutput type
     *
     * Example:
     * @code
     * struct MyAcyclicVisitor : TimeSeriesVisitor,
     *                           TimeSeriesOutputVisitor<TimeSeriesValueOutput<int>>,
     *                           TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {
     *
     *     void visit(TimeSeriesValueOutput<int>& output) override {
     *         // Handle int output
     *     }
     *
     *     void visit(TimeSeriesBundleOutput& output) override {
     *         // Handle bundle output
     *     }
     * };
     *
     * MyAcyclicVisitor visitor;
     * output->accept(visitor);  // Runtime dispatch via dynamic_cast
     * @endcode
     */
    template<typename T>
    struct TimeSeriesOutputVisitor {
        virtual void visit(T& output) = 0;
        virtual ~TimeSeriesOutputVisitor() = default;
    };

    /**
     * @brief Typed visitor interface for a specific TimeSeriesInput type
     *
     * @tparam T The concrete TimeSeriesInput type
     *
     * @see TimeSeriesOutputVisitor for usage example
     */
    template<typename T>
    struct TimeSeriesInputVisitor {
        virtual void visit(T& input) = 0;
        virtual ~TimeSeriesInputVisitor() = default;
    };

    /**
     * @brief Const variant for read-only visiting of TimeSeriesOutput
     *
     * Use this when your visitor doesn't need to modify the time series.
     *
     * @tparam T The concrete TimeSeriesOutput type
     */
    template<typename T>
    struct ConstTimeSeriesOutputVisitor {
        virtual void visit(const T& output) = 0;
        virtual ~ConstTimeSeriesOutputVisitor() = default;
    };

    /**
     * @brief Const variant for read-only visiting of TimeSeriesInput
     *
     * @tparam T The concrete TimeSeriesInput type
     */
    template<typename T>
    struct ConstTimeSeriesInputVisitor {
        virtual void visit(const T& input) = 0;
        virtual ~ConstTimeSeriesInputVisitor() = default;
    };

    // Note: TimeSeriesOutputVisitable and TimeSeriesInputVisitable are defined
    // in time_series_type.h to avoid circular dependencies

    // ============================================================================
    // Helper Functions and Utilities
    // ============================================================================

    /**
     * @brief Helper to automatically choose the right dispatch mechanism
     *
     * This function automatically selects CRTP or Acyclic dispatch based on the visitor type.
     *
     * @tparam Visitor The visitor type
     * @tparam TS The time series type
     * @param visitor The visitor instance
     * @param ts The time series instance
     * @return Whatever the visitor returns (for CRTP visitors)
     */
    template<typename Visitor, typename TS>
    decltype(auto) visit_timeseries(Visitor& visitor, TS& ts) {
        if constexpr (std::is_base_of_v<TimeSeriesVisitor, Visitor>) {
            // Acyclic visitor - use runtime dispatch
            ts.accept(static_cast<TimeSeriesVisitor&>(visitor));
        } else {
            // CRTP visitor - use compile-time dispatch
            return ts.accept(visitor);
        }
    }

    /**
     * @brief Const variant of visit_timeseries
     */
    template<typename Visitor, typename TS>
    decltype(auto) visit_timeseries(Visitor& visitor, const TS& ts) {
        if constexpr (std::is_base_of_v<TimeSeriesVisitor, Visitor>) {
            ts.accept(static_cast<TimeSeriesVisitor&>(visitor));
        } else {
            return ts.accept(visitor);
        }
    }

} // namespace hgraph

#endif // TIME_SERIES_VISITOR_H
