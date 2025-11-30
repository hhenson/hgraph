//
// Time Series Visitor Pattern Support
//
// Simple double dispatch visitor pattern:
// - Base visitor interface with visit() methods for each concrete type
// - Concrete types override accept() to call visitor.visit(*this)
// - This provides runtime dispatch based on the concrete type
//

#ifndef TIME_SERIES_VISITOR_H
#define TIME_SERIES_VISITOR_H

#include <hgraph/hgraph_forward_declarations.h>

namespace hgraph {
    // Forward declarations for concrete types
    template<typename T> struct TimeSeriesValueInput;
    template<typename T> struct TimeSeriesValueOutput;
    struct TimeSeriesSignalInput;
    struct TimeSeriesListInput;
    struct TimeSeriesBundleInput;
    template<typename K> struct TimeSeriesDictInput_T;
    template<typename K> struct TimeSeriesSetInput_T;
    template<typename T> struct TimeSeriesWindowInput;
    struct TimeSeriesReferenceInput;
    struct TimeSeriesReferenceOutput;
    struct TimeSeriesValueReferenceInput;
    struct TimeSeriesBundleReferenceInput;
    struct TimeSeriesSetReferenceInput;
    struct TimeSeriesListReferenceInput;
    struct TimeSeriesDictReferenceInput;
    struct TimeSeriesWindowReferenceInput;

    struct TimeSeriesListOutput;
    struct TimeSeriesBundleOutput;
    template<typename K> struct TimeSeriesDictOutput_T;
    template<typename K> struct TimeSeriesSetOutput_T;
    template<typename T> struct TimeSeriesFixedWindowOutput;
    template<typename T> struct TimeSeriesTimeWindowOutput;
    struct TimeSeriesValueReferenceOutput;
    struct TimeSeriesBundleReferenceOutput;
    struct TimeSeriesSetReferenceOutput;
    struct TimeSeriesListReferenceOutput;
    struct TimeSeriesDictReferenceOutput;
    struct TimeSeriesWindowReferenceOutput;

    /**
     * @brief Base visitor interface for TimeSeriesInput types
     * 
     * Visitors should inherit from this and implement visit() methods for the types they care about.
     * Concrete TimeSeriesInput types will call visitor.visit(*this) in their accept() method.
     */
    struct HGRAPH_EXPORT TimeSeriesInputVisitor {
        virtual ~TimeSeriesInputVisitor() = default;

        // Template visit methods for value types - these call virtual methods that can be overridden
        template<typename T>
        void visit(TimeSeriesValueInput<T>& input) {
            visit_value_input_impl(&input);
        }

        template<typename T>
        void visit(const TimeSeriesValueInput<T>& input) {
            visit_value_input_impl(const_cast<TimeSeriesValueInput<T>*>(&input));
        }

        // Virtual method that derived classes can override to handle value inputs
        virtual void visit_value_input_impl(TimeSeriesType* input) {
            // Default: do nothing, override in derived classes
        }

        // Visit methods for specific types
        virtual void visit(TimeSeriesSignalInput& input) {}
        virtual void visit(const TimeSeriesSignalInput& input) {}
        virtual void visit(TimeSeriesListInput& input) {}
        virtual void visit(const TimeSeriesListInput& input) {}
        virtual void visit(TimeSeriesBundleInput& input) {}
        virtual void visit(const TimeSeriesBundleInput& input) {}
        
        template<typename K>
        void visit(TimeSeriesDictInput_T<K>& input) {
            visit_dict_input_impl(&input);
        }
        
        template<typename K>
        void visit(const TimeSeriesDictInput_T<K>& input) {
            visit_dict_input_impl(const_cast<TimeSeriesDictInput_T<K>*>(&input));
        }
        
        template<typename K>
        void visit(TimeSeriesSetInput_T<K>& input) {
            visit_set_input_impl(&input);
        }
        
        template<typename K>
        void visit(const TimeSeriesSetInput_T<K>& input) {
            visit_set_input_impl(const_cast<TimeSeriesSetInput_T<K>*>(&input));
        }
        
        template<typename T>
        void visit(TimeSeriesWindowInput<T>& input) {
            visit_window_input_impl(&input);
        }
        
        template<typename T>
        void visit(const TimeSeriesWindowInput<T>& input) {
            visit_window_input_impl(const_cast<TimeSeriesWindowInput<T>*>(&input));
        }

        // Virtual methods that derived classes can override to handle template types
        virtual void visit_dict_input_impl(TimeSeriesType* input) {
            // Default: do nothing, override in derived classes
        }

        virtual void visit_set_input_impl(TimeSeriesType* input) {
            // Default: do nothing, override in derived classes
        }

        virtual void visit_window_input_impl(TimeSeriesType* input) {
            // Default: do nothing, override in derived classes
        }
        
        // Base reference input type - must be before specialized types
        virtual void visit(TimeSeriesReferenceInput& input) {}
        virtual void visit(const TimeSeriesReferenceInput& input) {}
        
        // Specialized reference input types
        virtual void visit(TimeSeriesValueReferenceInput& input) {}
        virtual void visit(const TimeSeriesValueReferenceInput& input) {}
        virtual void visit(TimeSeriesBundleReferenceInput& input) {}
        virtual void visit(const TimeSeriesBundleReferenceInput& input) {}
        virtual void visit(TimeSeriesSetReferenceInput& input) {}
        virtual void visit(const TimeSeriesSetReferenceInput& input) {}
        virtual void visit(TimeSeriesListReferenceInput& input) {}
        virtual void visit(const TimeSeriesListReferenceInput& input) {}
        virtual void visit(TimeSeriesDictReferenceInput& input) {}
        virtual void visit(const TimeSeriesDictReferenceInput& input) {}
        virtual void visit(TimeSeriesWindowReferenceInput& input) {}
        virtual void visit(const TimeSeriesWindowReferenceInput& input) {}
    };

    /**
     * @brief Base visitor interface for TimeSeriesOutput types
     * 
     * Visitors should inherit from this and implement visit() methods for the types they care about.
     * Concrete TimeSeriesOutput types will call visitor.visit(*this) in their accept() method.
     */
    struct HGRAPH_EXPORT TimeSeriesOutputVisitor {
        virtual ~TimeSeriesOutputVisitor() = default;

        // Template visit methods for value types - these call virtual methods that can be overridden
        template<typename T>
        void visit(TimeSeriesValueOutput<T>& output) {
            visit_value_output_impl(&output);
        }

        template<typename T>
        void visit(const TimeSeriesValueOutput<T>& output) {
            visit_value_output_impl(const_cast<TimeSeriesValueOutput<T>*>(&output));
        }

        // Virtual method that derived classes can override to handle value outputs
        virtual void visit_value_output_impl(TimeSeriesType* output) {
            // Default: do nothing, override in derived classes
        }

        // Visit methods for specific types
        virtual void visit(TimeSeriesListOutput& output) {}
        virtual void visit(const TimeSeriesListOutput& output) {}
        virtual void visit(TimeSeriesBundleOutput& output) {}
        virtual void visit(const TimeSeriesBundleOutput& output) {}
        
        template<typename K>
        void visit(TimeSeriesDictOutput_T<K>& output) {
            visit_dict_output_impl(&output);
        }
        
        template<typename K>
        void visit(const TimeSeriesDictOutput_T<K>& output) {
            visit_dict_output_impl(const_cast<TimeSeriesDictOutput_T<K>*>(&output));
        }
        
        template<typename K>
        void visit(TimeSeriesSetOutput_T<K>& output) {
            visit_set_output_impl(&output);
        }
        
        template<typename K>
        void visit(const TimeSeriesSetOutput_T<K>& output) {
            visit_set_output_impl(const_cast<TimeSeriesSetOutput_T<K>*>(&output));
        }
        
        template<typename T>
        void visit(TimeSeriesFixedWindowOutput<T>& output) {
            visit_fixed_window_output_impl(&output);
        }
        
        template<typename T>
        void visit(const TimeSeriesFixedWindowOutput<T>& output) {
            visit_fixed_window_output_impl(const_cast<TimeSeriesFixedWindowOutput<T>*>(&output));
        }
        
        template<typename T>
        void visit(TimeSeriesTimeWindowOutput<T>& output) {
            visit_time_window_output_impl(&output);
        }
        
        template<typename T>
        void visit(const TimeSeriesTimeWindowOutput<T>& output) {
            visit_time_window_output_impl(const_cast<TimeSeriesTimeWindowOutput<T>*>(&output));
        }

        // Virtual methods that derived classes can override to handle template types
        virtual void visit_dict_output_impl(TimeSeriesType* output) {
            // Default: do nothing, override in derived classes
        }

        virtual void visit_set_output_impl(TimeSeriesType* output) {
            // Default: do nothing, override in derived classes
        }

        virtual void visit_fixed_window_output_impl(TimeSeriesType* output) {
            // Default: do nothing, override in derived classes
        }

        virtual void visit_time_window_output_impl(TimeSeriesType* output) {
            // Default: do nothing, override in derived classes
        }
        
        // Base reference output type - must be before specialized types
        virtual void visit(TimeSeriesReferenceOutput& output) {}
        virtual void visit(const TimeSeriesReferenceOutput& output) {}
        
        // Specialized reference output types
        virtual void visit(TimeSeriesValueReferenceOutput& output) {}
        virtual void visit(const TimeSeriesValueReferenceOutput& output) {}
        virtual void visit(TimeSeriesBundleReferenceOutput& output) {}
        virtual void visit(const TimeSeriesBundleReferenceOutput& output) {}
        virtual void visit(TimeSeriesSetReferenceOutput& output) {}
        virtual void visit(const TimeSeriesSetReferenceOutput& output) {}
        virtual void visit(TimeSeriesListReferenceOutput& output) {}
        virtual void visit(const TimeSeriesListReferenceOutput& output) {}
        virtual void visit(TimeSeriesDictReferenceOutput& output) {}
        virtual void visit(const TimeSeriesDictReferenceOutput& output) {}
        virtual void visit(TimeSeriesWindowReferenceOutput& output) {}
        virtual void visit(const TimeSeriesWindowReferenceOutput& output) {}
    };

} // namespace hgraph

#endif // TIME_SERIES_VISITOR_H
