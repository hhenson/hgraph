//
// Example Visitor Implementations
//
// This file provides several example visitor implementations to demonstrate
// both CRTP and Acyclic visitor patterns for TimeSeriesInput/Output.
//

#ifndef EXAMPLE_VISITORS_H
#define EXAMPLE_VISITORS_H

#include <hgraph/types/time_series_visitor.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsl.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/ref.h>
#include <iostream>
#include <sstream>
#include <vector>

namespace hgraph {
namespace visitors {

    // ============================================================================
    // CRTP Visitor Examples - For internal, performance-critical code
    // ============================================================================

    /**
     * @brief Type information collector using CRTP (zero overhead)
     *
     * Collects type names of all visited time series.
     * This uses compile-time dispatch for maximum performance.
     */
    struct TypeInfoCollector : TimeSeriesOutputVisitorCRTP<TypeInfoCollector> {
        std::vector<std::string> type_names;

        // Visit value outputs
        template<typename T>
        void visit(TimeSeriesValueOutput<T>& output) {
            std::ostringstream oss;
            oss << "TS[" << typeid(T).name() << "]";
            type_names.push_back(oss.str());
        }

        // Visit bundle output
        void visit(TimeSeriesBundleOutput& output) {
            type_names.push_back("TSB");
            // Optionally recurse into bundle items
            for (auto& [key, value] : output.items()) {
                value->accept(*this);
            }
        }

        // Visit dict output
        template<typename K>
        void visit(TimeSeriesDictOutput_T<K>& output) {
            std::ostringstream oss;
            oss << "TSD[" << typeid(K).name() << "]";
            type_names.push_back(oss.str());
        }

        // Visit list output
        void visit(TimeSeriesListOutput& output) {
            type_names.push_back("TSL");
        }

        // Visit set output
        template<typename T>
        void visit(TimeSeriesSetOutput_T<T>& output) {
            std::ostringstream oss;
            oss << "TSS[" << typeid(T).name() << "]";
            type_names.push_back(oss.str());
        }

        // Visit reference output
        void visit(TimeSeriesReferenceOutput& output) {
            type_names.push_back("REF");
        }

        [[nodiscard]] std::string to_string() const {
            std::ostringstream oss;
            for (size_t i = 0; i < type_names.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << type_names[i];
            }
            return oss.str();
        }
    };

    /**
     * @brief Deep copy visitor using CRTP
     *
     * Performs a deep copy from one time series output to another.
     * This is the fastest way to copy because it uses compile-time dispatch.
     */
    struct DeepCopyVisitor : TimeSeriesOutputVisitorCRTP<DeepCopyVisitor> {
        TimeSeriesOutput& target;

        explicit DeepCopyVisitor(TimeSeriesOutput& t) : target(t) {}

        template<typename T>
        void visit(TimeSeriesValueOutput<T>& source) {
            auto& dest = static_cast<TimeSeriesValueOutput<T>&>(target);
            if (source.valid()) {
                dest.set_value(source.value());
            }
        }

        void visit(TimeSeriesBundleOutput& source) {
            auto& dest = static_cast<TimeSeriesBundleOutput&>(target);
            for (auto& [key, value] : source.items()) {
                DeepCopyVisitor sub_visitor(*dest[key]);
                value->accept(sub_visitor);
            }
        }

        template<typename K>
        void visit(TimeSeriesDictOutput_T<K>& source) {
            auto& dest = static_cast<TimeSeriesDictOutput_T<K>&>(target);
            for (const auto& [key, value] : source.value()) {
                auto dest_value = dest[key];
                DeepCopyVisitor sub_visitor(*dest_value);
                value->accept(sub_visitor);
            }
        }

        // Add similar handlers for other types...
    };

    /**
     * @brief Validity checker using CRTP
     *
     * Checks if all time series in a structure are valid.
     * Returns true only if all visited time series are valid.
     */
    struct ValidityChecker : TimeSeriesOutputVisitorCRTP<ValidityChecker> {
        bool all_valid = true;

        template<typename TS>
        void visit(TS& ts) {
            all_valid &= ts.valid();

            // Recursively check composite types
            if constexpr (requires { ts.items(); }) {
                for (auto& [key, value] : ts.items()) {
                    if (value) {
                        value->accept(*this);
                    }
                }
            }
        }
    };

    /**
     * @brief Value extraction visitor using CRTP
     *
     * Extracts the Python value from any time series type.
     */
    struct ValueExtractor : TimeSeriesOutputVisitorCRTP<ValueExtractor> {
        nb::object extracted_value;
        bool found = false;

        template<typename TS>
        void visit(TS& ts) {
            if (ts.valid()) {
                extracted_value = ts.py_value();
                found = true;
            }
        }
    };

    // ============================================================================
    // Acyclic Visitor Examples - For extensions and plugins
    // ============================================================================

    /**
     * @brief Logging visitor using Acyclic pattern
     *
     * Logs information about visited time series.
     * This uses runtime dispatch and can be extended at runtime.
     */
    struct LoggingVisitor : TimeSeriesVisitor,
                           TimeSeriesOutputVisitor<TimeSeriesValueOutput<int>>,
                           TimeSeriesOutputVisitor<TimeSeriesValueOutput<double>>,
                           TimeSeriesOutputVisitor<TimeSeriesValueOutput<std::string>>,
                           TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {

        std::ostream& out;
        int indent_level = 0;

        explicit LoggingVisitor(std::ostream& o = std::cout) : out(o) {}

        void visit(TimeSeriesValueOutput<int>& output) override {
            print_indent();
            out << "TS[int] = ";
            if (output.valid()) {
                out << output.value();
            } else {
                out << "<invalid>";
            }
            out << "\n";
        }

        void visit(TimeSeriesValueOutput<double>& output) override {
            print_indent();
            out << "TS[double] = ";
            if (output.valid()) {
                out << output.value();
            } else {
                out << "<invalid>";
            }
            out << "\n";
        }

        void visit(TimeSeriesValueOutput<std::string>& output) override {
            print_indent();
            out << "TS[string] = ";
            if (output.valid()) {
                out << "\"" << output.value() << "\"";
            } else {
                out << "<invalid>";
            }
            out << "\n";
        }

        void visit(TimeSeriesBundleOutput& output) override {
            print_indent();
            out << "TSB with " << output.size() << " keys:\n";
            indent_level++;
            for (auto& [key, value] : output.items()) {
                print_indent();
                out << key << ": ";
                if (value) {
                    value->accept(*this);
                } else {
                    out << "<null>\n";
                }
            }
            indent_level--;
        }

    private:
        void print_indent() {
            for (int i = 0; i < indent_level; ++i) {
                out << "  ";
            }
        }
    };

    /**
     * @brief Statistics collector using Acyclic pattern
     *
     * Collects statistics about time series types encountered.
     */
    struct StatisticsCollector : TimeSeriesVisitor,
                                 TimeSeriesOutputVisitor<TimeSeriesValueOutput<int>>,
                                 TimeSeriesOutputVisitor<TimeSeriesValueOutput<double>>,
                                 TimeSeriesOutputVisitor<TimeSeriesBundleOutput> {

        size_t total_count = 0;
        size_t valid_count = 0;
        size_t invalid_count = 0;
        std::unordered_map<std::string, size_t> type_counts;

        void visit(TimeSeriesValueOutput<int>& output) override {
            count_type("TS[int]", output);
        }

        void visit(TimeSeriesValueOutput<double>& output) override {
            count_type("TS[double]", output);
        }

        void visit(TimeSeriesBundleOutput& output) override {
            count_type("TSB", output);
            // Recurse into bundle
            for (auto& [key, value] : output.items()) {
                if (value) {
                    value->accept(*this);
                }
            }
        }

        void print_stats(std::ostream& out = std::cout) const {
            out << "Statistics:\n";
            out << "  Total: " << total_count << "\n";
            out << "  Valid: " << valid_count << "\n";
            out << "  Invalid: " << invalid_count << "\n";
            out << "  By type:\n";
            for (const auto& [type, count] : type_counts) {
                out << "    " << type << ": " << count << "\n";
            }
        }

    private:
        template<typename TS>
        void count_type(const std::string& type_name, TS& ts) {
            total_count++;
            if (ts.valid()) {
                valid_count++;
            } else {
                invalid_count++;
            }
            type_counts[type_name]++;
        }
    };

} // namespace visitors
} // namespace hgraph

#endif // EXAMPLE_VISITORS_H
