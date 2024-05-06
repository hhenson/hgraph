//
// Created by Howard Henson on 06/05/2024.
//

#ifndef REF_TYPE_H
#define REF_TYPE_H

#include<hgraph/types/time_series_type.h>

namespace hgraph {
    struct TimeSeriesReference {
        TimeSeriesReference() = default;

        explicit TimeSeriesReference(TimeSeriesInput *input);

        explicit TimeSeriesReference(TimeSeriesOutput *output);

        [[nodiscard]] TimeSeriesOutput *output() const { return _output; }

        void bind_input(TimeSeriesInput *input);

    private:
        TimeSeriesOutput *_output{};
        std::vector<TimeSeriesReference*> _items{};
        py::type _tp{py::type::of(py::none())};
        bool _has_peer{false};
        bool _valid{false};
    };

    struct TimeSeriesReferenceOutput : TimeSeriesOutput {

        [[nodiscard]] TimeSeriesReference &value() const;
        void set_value(TimeSeriesReference &&value);

        void observe_reference(TimeSeriesInput *input);

        void stop_observing_reference(TimeSeriesInput *input);
    };

    struct TimeSeriesReferenceInput : TimeSeriesInput {
        [[nodiscard]] const TimeSeriesReference &value() const;
    };
}

#endif //REF_TYPE_H
