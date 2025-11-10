//
// Specialized Reference Type Builders
// Created to match Python specialized reference classes
//

#ifndef SPECIALIZED_REF_BUILDERS_H
#define SPECIALIZED_REF_BUILDERS_H

#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>

namespace hgraph {
    // ============================================================
    // Specialized Reference Input Builders
    // ============================================================

    struct HGRAPH_EXPORT TimeSeriesValueRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesListRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesBundleRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesDictRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesSetRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesWindowRefInputBuilder : InputBuilder {
        using InputBuilder::InputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Builders
    // ============================================================

    struct HGRAPH_EXPORT TimeSeriesValueRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesListRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesBundleRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesDictRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesSetRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesWindowRefOutputBuilder : OutputBuilder {
        using OutputBuilder::OutputBuilder;

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        static void register_with_nanobind(nb::module_ &m);
    };

} // namespace hgraph

#endif // SPECIALIZED_REF_BUILDERS_H

