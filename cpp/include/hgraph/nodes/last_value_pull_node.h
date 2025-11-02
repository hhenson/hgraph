//
// Created by Claude Code
//

#ifndef LAST_VALUE_PULL_NODE_H
#define LAST_VALUE_PULL_NODE_H

#include <hgraph/types/node.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/tsb.h>
#include <hgraph/types/tsl.h>
#include <optional>
#include <functional>
#include <variant>

namespace hgraph {
    /**
     * LastValuePullNode implementation in C++
     * This node type is used for pull source nodes that cache delta values
     * and combine them when multiple values are received before evaluation.
     */
    struct HGRAPH_EXPORT LastValuePullNode : Node {
        using Node::Node;

        /**
         * Copy a value from an input TimeSeriesOutput
         * This is called when the node needs to pull a value from another output
         */
        void copy_from_input(const TimeSeriesInput &input);

        void copy_from_output(const TimeSeriesOutput &output);

        /**
         * Apply a value directly to the node
         * This is used when setting a default value or when the node receives a new value
         */
        void apply_value(const nb::object &new_value);

        // Lifecycle hooks required by the base class
        void initialise() override; // no-op
        void dispose() override; // no-op

        static void register_with_nanobind(nb::module_ &m);

    protected:
        void do_eval() override;

        void do_start() override;

        void do_stop() override;

        /**
         * Combine two delta values based on the output type
         * Different time series types have different combination strategies
         */
        nb::object combine_delta_values(const nb::object &old_delta, const nb::object &new_delta);

    private:
        std::optional<nb::object> _delta_value;
        std::function<nb::object(const nb::object &, const nb::object &)> _delta_combine_fn;

        // Type-specific combine functions
        static nb::object _combine_tss_delta(const nb::object &old_delta, const nb::object &new_delta);

        static nb::object _combine_tsd_delta(const nb::object &old_delta, const nb::object &new_delta);

        static nb::object _combine_tsb_delta(const nb::object &old_delta, const nb::object &new_delta);

        static nb::object _combine_tsl_delta_value(const nb::object &old_delta, const nb::object &new_delta);

        void _setup_combine_function();
    };
} // namespace hgraph

#endif  // LAST_VALUE_PULL_NODE_H