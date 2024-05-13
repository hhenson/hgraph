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

    Node * TimeSeriesOutput::owning_node() const {
        if(_parent_output.has_value()) {
            return _parent_output.value()->owning_node();
        } else {
            return _owning_node.value();
        }
    }

    TimeSeriesOutput * TimeSeriesOutput::parent_output() const {
        return *_parent_output;
    }

    bool TimeSeriesOutput::has_parent_output() const {
        return _parent_output.has_value();
    }

    void TimeSeriesOutput::re_parent(Node *parent) {
        _owning_node.emplace(parent);
        _parent_output.reset();
    }

    void TimeSeriesOutput::re_parent(TimeSeriesOutput *parent) {
        _owning_node.reset();
        _parent_output.emplace(parent);
    }

    void TimeSeriesOutput::apply_result(py::object value) {
        if(!value.is_none()) {
            set_py_value(std::move(value));
        }
    }

    bool TimeSeriesOutput::modified() const {
        return owning_graph()->evaluation_clock()->evaluation_time() == _last_modified_time;
    }

    bool TimeSeriesOutput::valid() const {
        return _last_modified_time > MIN_DT;
    }

    bool TimeSeriesOutput::all_valid() const {
        return valid();  // By default, all valid is the same as valid
    }

    engine_time_t TimeSeriesOutput::last_modified_time() const {
        return _last_modified_time;
    }

    void TimeSeriesOutput::mark_invalidate() {
        if(_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            _notify();
        }
    }

    void TimeSeriesOutput::mark_modified() {
        auto clock{owning_graph()->evaluation_clock()};
        auto et{clock->evaluation_time()};
        if(_last_modified_time < et) {
            _last_modified_time = et;
            if(_parent_output.has_value()) {
                _parent_output.value()->mark_modified();
            }
            _notify();
        }
    }

    void TimeSeriesOutput::subscribe_node(Node *node) {
        _subscribers.subscribe(node);
    }

    void TimeSeriesOutput::un_subscribe_node(Node *node) {
        _subscribers.un_subscribe(node);
    }

    void TimeSeriesOutput::_notify() {
        _subscribers.apply([](Node *node) { node->notify(); });
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

    bool TimeSeriesInput::bind_output(TimeSeriesOutput *value) {
        auto ref_value{dynamic_cast<TimeSeriesReferenceOutput *>(value)};
        bool peer{false};
        if (ref_value) {
            if (ref_value->valid()) {
                ref_value->value().bind_input(this);
                ref_value->observe_reference(this);
                _reference_output = ref_value;
                peer = false;
            }
        } else {
            peer = do_bind_output(value);
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

    bool TimeSeriesInput::do_bind_output(TimeSeriesOutput *value) {
        auto active_{active()};
        make_passive();
        _output = value;
        if (active()) {
            make_active();
        }
        return true;
    }

    void TimeSeriesInput::un_bind_output() {
        auto valid_{valid()};
        if (bound()) {
            auto output_{dynamic_cast<TimeSeriesReferenceOutput *>(*_output)};
            if (output_) {
                output_->stop_observing_reference(this);
                _reference_output.reset();
            }
            do_un_bind_output(*_output);

            if (owning_node()->is_started() and valid_) {
                _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
                if (active()) {
                    owning_node()->notify();
                }
            }
        }
    }

    void TimeSeriesInput::do_un_bind_output(TimeSeriesOutput *value) {
        if (active()) {
            value->un_subscribe_node(owning_node());
        }
        _output.reset();
    }

    bool TimeSeriesInput::active() const {
        if (bound()) {
            return _active;
        } else {
            return active_un_bound();
        }
    }

    bool TimeSeriesInput::modified() const {
        return _output.has_value() && ((*_output)->modified() || sampled());
    }

    bool TimeSeriesInput::valid() const {
        return bound() && (*_output)->valid();
    }

    bool TimeSeriesInput::all_valid() const {
        return bound() && (*_output)->all_valid();
    }

    engine_time_t TimeSeriesInput::last_modified_time() const {
        return bound() ? std::max((*_output)->last_modified_time(), _sample_time) : MIN_DT;
    }

    bool TimeSeriesInput::sampled() const {
        return _sample_time != MIN_DT && _sample_time == owning_graph()->evaluation_clock()->evaluation_time();
    }
}
