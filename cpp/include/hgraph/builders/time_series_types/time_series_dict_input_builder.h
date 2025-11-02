//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_DICT_INPUT_BUILDER_H
#define TIME_SERIES_DICT_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesDictInputBuilder>;
        input_builder_ptr ts_builder;

        explicit TimeSeriesDictInputBuilder(input_builder_ptr ts_builder);

        bool has_reference() const override { return ts_builder->has_reference(); }
    };

    template<typename T>
    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder_T : TimeSeriesDictInputBuilder {
        using TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder;

        time_series_input_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;
    };

    void time_series_dict_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_DICT_INPUT_BUILDER_H