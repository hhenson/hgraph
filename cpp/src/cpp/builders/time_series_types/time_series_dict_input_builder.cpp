#include <hgraph/builders/time_series_types/time_series_dict_input_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <utility>

namespace hgraph {
    TimeSeriesDictInputBuilder::TimeSeriesDictInputBuilder(input_builder_s_ptr ts_builder,
                                                           const value::TypeMeta* key_type_meta)
        : InputBuilder(), ts_builder{std::move(ts_builder)}, key_type_meta{key_type_meta} {
    }

    time_series_input_s_ptr TimeSeriesDictInputBuilder::make_instance(node_ptr owning_node) const {
        return arena_make_shared_as<TimeSeriesDictInputImpl, TimeSeriesInput>(
            owning_node, ts_builder, key_type_meta);
    }

    time_series_input_s_ptr TimeSeriesDictInputBuilder::make_instance(time_series_input_ptr owning_input) const {
        return arena_make_shared_as<TimeSeriesDictInputImpl, TimeSeriesInput>(
            owning_input, ts_builder, key_type_meta);
    }

    bool TimeSeriesDictInputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesDictInputBuilder *>(&other)) {
            return ts_builder->is_same_type(*other_b->ts_builder) &&
                   key_type_meta == other_b->key_type_meta;
        }
        return false;
    }

    void TimeSeriesDictInputBuilder::release_instance(time_series_input_ptr item) const {
        auto dict = dynamic_cast<TimeSeriesDictInputImpl *>(item);
        if (dict == nullptr) {
            throw std::runtime_error("TimeSeriesDictInputBuilder::release_instance: expected TimeSeriesDictInputImpl but got different type");
        }
        // if (dict->output()) dict->output_t().remove_key_observer(dict);
        for (auto &value: dict->_ts_values) { ts_builder->release_instance(value.second.get()); }
        for (auto &value: dict->_removed_items) { ts_builder->release_instance(value.second.first.get()); }
        InputBuilder::release_instance(&dict->key_set());
        InputBuilder::release_instance(item);
    }

    size_t TimeSeriesDictInputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesDictInputImpl));
    }

    size_t TimeSeriesDictInputBuilder::type_alignment() const {
        return alignof(TimeSeriesDictInputImpl);
    }

    void time_series_dict_input_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictInputBuilder, InputBuilder>(m, "InputBuilder_TSD")
                .def(nb::init<input_builder_s_ptr, const value::TypeMeta*>(),
                     "ts_builder"_a, "key_type_meta"_a)
                .def_ro("ts_builder", &TimeSeriesDictInputBuilder::ts_builder);
    }

} // namespace hgraph