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
        InputBuilder::ptr value_builder;  // Builder for child items
        size_t size;                      // Fixed size of the list

        TimeSeriesListRefInputBuilder(InputBuilder::ptr value_builder, size_t size);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        [[nodiscard]] bool is_same_type(const Builder &other) const override;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesBundleRefInputBuilder : InputBuilder {
        time_series_schema_ptr schema;              // Schema for bundle fields
        std::vector<InputBuilder::ptr> field_builders;  // Builders for each field

        TimeSeriesBundleRefInputBuilder(time_series_schema_ptr schema, std::vector<InputBuilder::ptr> field_builders);

        time_series_input_ptr make_instance(node_ptr owning_node) const override;
        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;
        bool has_reference() const override { return true; }
        [[nodiscard]] bool is_same_type(const Builder &other) const override;
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
        OutputBuilder::ptr value_builder;  // Builder for child items
        size_t size;                       // Fixed size of the list

        TimeSeriesListRefOutputBuilder(OutputBuilder::ptr value_builder, size_t size);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        [[nodiscard]] bool is_same_type(const Builder &other) const override;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesBundleRefOutputBuilder : OutputBuilder {
        time_series_schema_ptr schema;                  // Schema for bundle fields
        std::vector<OutputBuilder::ptr> field_builders; // Builders for each field

        TimeSeriesBundleRefOutputBuilder(time_series_schema_ptr schema, std::vector<OutputBuilder::ptr> field_builders);

        time_series_output_ptr make_instance(node_ptr owning_node) const override;
        time_series_output_ptr make_instance(time_series_output_ptr owning_output) const override;
        bool has_reference() const override { return true; }
        [[nodiscard]] bool is_same_type(const Builder &other) const override;
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

