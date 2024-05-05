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

        Node *owning_node{};
        Graph *owning_graph{};

        [[nodiscard]] virtual py::object py_value() const = 0;

        [[nodiscard]] virtual py::object py_delta_value() const { return py_value(); };

        [[nodiscard]] virtual bool modified() const = 0;

        [[nodiscard]] virtual bool valid() const = 0;

        [[nodiscard]] virtual bool all_valid() const = 0;

        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

        virtual void re_parent(Node *parent) const = 0;

        virtual void re_parent(TimeSeries *parent) const = 0;
    };

    struct TimeSeriesInput;

    struct TimeSeriesOutput : TimeSeries {
        [[nodiscard]] TimeSeriesOutput *parent_output() const;

        [[nodiscard]] bool has_parent_output() const;

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

        [[nodiscard]] TimeSeriesInput *parent_input() const;

        [[nodiscard]] bool has_parent_input() const;

        [[nodiscard]] virtual bool bound() const;

        [[nodiscard]] virtual bool has_peer() const;

        [[nodiscard]] TimeSeriesOutput *output() const;

        virtual bool bind_output(TimeSeriesOutput *value);

        virtual void do_bind_output(TimeSeriesOutput *value);

        void un_bind_output();

        virtual void do_un_bind_output(TimeSeriesOutput *value) {
        };

        [[nodiscard]] virtual bool active() = 0;

        virtual void make_active() = 0;

        virtual void make_passive() = 0;
    };

    struct TimeSeriesSignalInput : TimeSeriesInput {
        [[nodiscard]] py::object py_value() const override;
    };
}

#endif //TIME_SERIES_TYPES_H
