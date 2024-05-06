//
// Created by Howard Henson on 05/05/2024.
//

#ifndef TIME_SERIES_TYPES_H
#define TIME_SERIES_TYPES_H

#include<hgraph/python/pyb.h>

#include "hgraph/util/date_time.h"

namespace hgraph {
    struct Node;
    struct Graph;

    struct TimeSeries {
        virtual ~TimeSeries() = default;

        [[nodiscard]] virtual Node *owning_node() const = 0;

        [[nodiscard]] Graph *owning_graph() const;

        [[nodiscard]] virtual py::object py_value() const = 0;

        [[nodiscard]] virtual py::object py_delta_value() const { return py_value(); };

        [[nodiscard]] virtual bool modified() const = 0;

        [[nodiscard]] virtual bool valid() const = 0;

        [[nodiscard]] virtual bool all_valid() const = 0;

        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

        virtual void re_parent(Node *parent) = 0;

        virtual void re_parent(TimeSeries *parent) = 0;

    protected:
        std::optional<Node *> _owning_node;
    };

    struct TimeSeriesInput;

    struct TimeSeriesOutput : TimeSeries {
        [[nodiscard]] TimeSeriesOutput *parent_output() const;

        [[nodiscard]] bool has_parent_output() const;

        void re_parent(TimeSeriesOutput *parent);

        virtual void set_py_value(py::object value) = 0;

        void apply_result(py::object value);

        virtual void invaliate() = 0;

        virtual void mark_invalidate() = 0;

        virtual void mark_modified() = 0;

        void subscribe_node(Node *node);

        void un_subscribe_node(Node *node);

        virtual void copy_from_output(TimeSeriesOutput &output) = 0;

        virtual void copy_from_input(TimeSeriesInput &input) = 0;
    };

    struct TimeSeriesInput : TimeSeries {
        TimeSeriesInput() = default;

        [[nodiscard]] Node *owning_node() const override;

        [[nodiscard]] const TimeSeriesInput *parent_input() const { return *_parent_input; }

        [[nodiscard]] bool has_parent_input() const { return (bool) _parent_input; };

        void re_parent(Node *parent) override;

        void re_parent(TimeSeriesInput *parent);

        [[nodiscard]] bool bound() const { return _output.has_value(); };

        [[nodiscard]] virtual bool has_peer() const { return _output.has_value(); };

        [[nodiscard]] TimeSeriesOutput *output() const { return _output.value(); };

        virtual bool bind_output(TimeSeriesOutput *output);

        virtual bool do_bind_output(TimeSeriesOutput *value);

        void un_bind_output();

        virtual void do_un_bind_output(TimeSeriesOutput *value) {
        };

        [[nodiscard]] bool active() const;

        virtual void make_active() = 0;

        virtual void make_passive() = 0;

    protected:
        [[nodiscard]] virtual bool active_un_bound() const { return _active; }

    private:
        std::optional<TimeSeriesInput *> _parent_input;
        std::optional<TimeSeriesOutput *> _output;
        std::optional<TimeSeriesOutput *> _reference_output;
        bool _active{false};
        engine_time_t _sample_time{MIN_DT};
    };


    struct TimeSeriesSignalInput : TimeSeriesInput {
        [[nodiscard]] py::object py_value() const override;
    };
}

#endif //TIME_SERIES_TYPES_H
