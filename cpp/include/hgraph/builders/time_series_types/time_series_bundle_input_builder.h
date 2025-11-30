//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_BUNDLE_INPUT_BUILDER_H
#define TIME_SERIES_BUNDLE_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <vector>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesBundleInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesBundleInputBuilder>;

        TimeSeriesBundleInputBuilder(time_series_schema_ptr schema, std::vector<InputBuilder::ptr> input_builders);

        time_series_input_ptr make_instance(node_ptr owning_node, std::shared_ptr<void> buffer = nullptr, size_t* offset = nullptr) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input, std::shared_ptr<void> buffer = nullptr, size_t* offset = nullptr) const override;

        bool has_reference() const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

        [[nodiscard]] size_t memory_size() const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        time_series_input_ptr make_and_set_inputs(std::shared_ptr<TimeSeriesBundleInput> input, std::shared_ptr<void> buffer, size_t* offset) const;

        time_series_schema_ptr schema;
        std::vector<InputBuilder::ptr> input_builders;
    };
} // namespace hgraph

#endif  // TIME_SERIES_BUNDLE_INPUT_BUILDER_H