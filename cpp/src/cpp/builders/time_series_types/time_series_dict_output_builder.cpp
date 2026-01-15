#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>
#include <hgraph/types/node.h>
#include <hgraph/types/tsd.h>
#include <hgraph/util/arena_enable_shared_from_this.h>

#include <utility>

namespace hgraph {
    TimeSeriesDictOutputBuilder::TimeSeriesDictOutputBuilder(output_builder_s_ptr ts_builder,
                                                             output_builder_s_ptr ts_ref_builder,
                                                             const value::TypeMeta* key_type_meta)
        : OutputBuilder(), ts_builder{std::move(ts_builder)}, ts_ref_builder{std::move(ts_ref_builder)},
          key_type_meta{key_type_meta} {
    }

    bool TimeSeriesDictOutputBuilder::is_same_type(const Builder &other) const {
        if (auto other_b = dynamic_cast<const TimeSeriesDictOutputBuilder *>(&other)) {
            return ts_builder->is_same_type(*other_b->ts_builder) &&
                   key_type_meta == other_b->key_type_meta;
        }
        return false;
    }

    size_t TimeSeriesDictOutputBuilder::memory_size() const {
        return add_canary_size(sizeof(TimeSeriesDictOutputImpl));
    }

    size_t TimeSeriesDictOutputBuilder::type_alignment() const {
        return alignof(TimeSeriesDictOutputImpl);
    }

    void time_series_dict_output_builder_register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesDictOutputBuilder, OutputBuilder>(m, "OutputBuilder_TSD")
                .def(nb::init<output_builder_s_ptr, output_builder_s_ptr, const value::TypeMeta*>(),
                     "ts_builder"_a, "ts_ref_builder"_a, "key_type_meta"_a);
    }

} // namespace hgraph