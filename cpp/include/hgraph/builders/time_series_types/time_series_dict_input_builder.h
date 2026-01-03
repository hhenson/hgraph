//
// Created by Howard Henson on 27/12/2024.
//

#ifndef TIME_SERIES_DICT_INPUT_BUILDER_H
#define TIME_SERIES_DICT_INPUT_BUILDER_H

#include <hgraph/builders/input_builder.h>
#include <hgraph/types/value/type_meta.h>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesDictInputBuilder : InputBuilder {
        using ptr = nb::ref<TimeSeriesDictInputBuilder>;
        input_builder_s_ptr ts_builder;
        const value::TypeMeta* key_type_meta;

        explicit TimeSeriesDictInputBuilder(input_builder_s_ptr ts_builder, const value::TypeMeta* key_type_meta);

        bool has_reference() const override { return ts_builder->has_reference(); }

        time_series_input_s_ptr make_instance(node_ptr owning_node) const override;

        time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;

        [[nodiscard]] bool is_same_type(const Builder &other) const override;

        void release_instance(time_series_input_ptr item) const override;

        [[nodiscard]] size_t memory_size() const override;

        [[nodiscard]] size_t type_alignment() const override;
    };

    void time_series_dict_input_builder_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TIME_SERIES_DICT_INPUT_BUILDER_H