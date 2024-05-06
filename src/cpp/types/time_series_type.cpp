//
// Created by Howard Henson on 06/05/2024.
//
#include<hgraph/types/time_series_type.h>
#include<hgraph/types/node.h>
#include<optional>

#include <hgraph/types/ref_type.h>

#include "hgraph/types/graph.h"

namespace hgraph {
    Graph *TimeSeries::owning_graph() const {
        return owning_node()->graph;
    }

    Node *TimeSeriesInput::owning_node() const {
        return _owning_node
                   ? *_owning_node
                   : _parent_input.transform([](auto v) { return v->owning_node(); }).value_or(nullptr);
    }

    void TimeSeriesInput::re_parent(Node *parent) {
        _owning_node.emplace(parent);
        _parent_input.reset();
    }

    void TimeSeriesInput::re_parent(TimeSeriesInput *parent) {
        _owning_node.reset();
        _parent_input.emplace(parent);
    }

    bool TimeSeriesInput::bind_output(TimeSeriesOutput *output) {
        auto ref_value{dynamic_cast<TimeSeriesReferenceOutput *>(output)};
        bool peer{false};
        if (ref_value) {
            if (ref_value->valid()) {
                ref_value->value().bind_input(this);
                ref_value->observe_reference(this);
                _reference_output = ref_value;
                peer = false;
            }
        } else {
            peer = do_bind_output(output);
        }
        auto owning_node_{owning_node()};
        if ((owning_node_->is_started() || owning_node_->is_starting())
            && _output.transform([](auto o_) { o_->valid(); })) {
            _sample_time = owning_node_->graph->evaluation_clock()->evaluation_time();
            if (active()) {
                owning_node_->notify();
            }
        }

        return peer;
    }

    bool TimeSeriesInput::active() const {
        if (bound()) {
            return _active;
        } else {
            return active_un_bound();
        }
    }
}
