#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>

#include <utility>

namespace hgraph {
    /*
     * The python code sets the node and unsets the parent_input, we have an optional with a
     * variant so we just need to set the _parent_ts_or_node property
     */
    void TimeSeriesType::re_parent(const Node::ptr &parent) { _parent_ts_or_node = parent; }

    void TimeSeriesType::re_parent(const ptr &parent) { _parent_ts_or_node = parent; }

    bool TimeSeriesType::is_reference() const { return false; }

    bool TimeSeriesType::has_reference() const { return false; }

    void TimeSeriesType::reset_parent_or_node() { _parent_ts_or_node.reset(); }

    void TimeSeriesType::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesType, nb::intrusive_base>(m, "TimeSeriesType")
                .def_prop_ro("owning_node",
                             static_cast<node_ptr (TimeSeriesType::*)() const>(&TimeSeriesType::owning_node))
                .def_prop_ro("owning_graph",
                             static_cast<graph_ptr (TimeSeriesType::*)() const>(&TimeSeriesType::owning_graph))
                .def_prop_ro("value", &TimeSeriesType::py_value)
                .def_prop_ro("delta_value", &TimeSeriesType::py_delta_value)
                .def_prop_ro("modified", &TimeSeriesType::modified)
                .def_prop_ro("valid", &TimeSeriesType::valid)
                .def_prop_ro("all_valid", &TimeSeriesType::all_valid)
                .def_prop_ro("last_modified_time", &TimeSeriesType::last_modified_time)
                .def("re_parent", static_cast<void (TimeSeriesType::*)(const Node::ptr &)>(&TimeSeriesType::re_parent))
                .def("re_parent", static_cast<void (TimeSeriesType::*)(const ptr &)>(&TimeSeriesType::re_parent))
                .def("is_reference", &TimeSeriesType::is_reference)
                .def("has_reference", &TimeSeriesType::has_reference)
                .def("__str__", [](const TimeSeriesType &self) {
                    return fmt::format("TimeSeriesType@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                })
                .def("__repr__", [](const TimeSeriesType &self) {
                    return fmt::format("TimeSeriesType@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                });
    }

    TimeSeriesType::ptr &TimeSeriesType::_parent_time_series() const {
        return const_cast<TimeSeriesType *>(this)->_parent_time_series();
    }

    TimeSeriesType::ptr &TimeSeriesType::_parent_time_series() {
        if (_parent_ts_or_node.has_value()) {
            return std::get<ptr>(_parent_ts_or_node.value());
        }
        return null_ptr;
    }

    bool TimeSeriesType::_has_parent_time_series() const {
        if (_parent_ts_or_node.has_value()) {
            return std::holds_alternative<ptr>(_parent_ts_or_node.value());
        } else {
            return false;
        }
    }

    void TimeSeriesType::_set_parent_time_series(TimeSeriesType *ts) { _parent_ts_or_node = ptr{ts}; }

    bool TimeSeriesType::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool TimeSeriesType::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<Node::ptr>(*_parent_ts_or_node)) {
                return std::get<Node::ptr>(*_parent_ts_or_node) != Node::ptr{};
            }
            return std::get<ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    graph_ptr TimeSeriesType::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    graph_ptr TimeSeriesType::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : graph_ptr{};
    }

    void TimeSeriesOutput::clear() {
    }

    void TimeSeriesOutput::invalidate() { mark_invalid(); }

    void TimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesOutput, TimeSeriesType>(m, "TimeSeriesOutput")
                .def_prop_ro("parent_output",
                             [](const TimeSeriesOutput &ts) -> nb::object {
                                 if (ts.has_parent_output()) { return nb::cast(ts.parent_output()); }
                                 return nb::none();
                             })
                .def_prop_ro("has_parent_output", &TimeSeriesOutput::has_parent_output)
                .def_prop_rw("value", &TimeSeriesOutput::py_value, &TimeSeriesOutput::py_set_value,
                             nb::arg("value").none())
                .def("can_apply_result", &TimeSeriesOutput::can_apply_result)
                .def("apply_result", &TimeSeriesOutput::apply_result, nb::arg("value").none())
                .def("invalidate", &TimeSeriesOutput::invalidate)
                .def("mark_invalid", &TimeSeriesOutput::mark_invalid)
                .def("mark_modified", static_cast<void (TimeSeriesOutput::*)()>(&TimeSeriesOutput::mark_modified))
                .def("mark_modified",
                     static_cast<void (TimeSeriesOutput::*)(engine_time_t)>(&TimeSeriesOutput::mark_modified))
                .def("subscribe", &TimeSeriesOutput::subscribe)
                .def("unsubscribe", &TimeSeriesOutput::un_subscribe)
                .def("copy_from_output", &TimeSeriesOutput::copy_from_output)
                .def("copy_from_input", &TimeSeriesOutput::copy_from_input)
                .def("__str__", [](const TimeSeriesOutput &self) {
                    return fmt::format("TimeSeriesOutput@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                })
                .def("__repr__", [](const TimeSeriesOutput &self) {
                    return fmt::format("TimeSeriesOutput@{:p}[valid={}, modified={}]",
                                       static_cast<const void *>(&self), self.valid(), self.modified());
                });
    }

    node_ptr TimeSeriesType::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> node_ptr {
                    using T = std::decay_t<T_>; // Get the actual type
                    if constexpr (std::is_same_v<T, TimeSeriesType::ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T, Node::ptr>) {
                        return value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    TimeSeriesType::TimeSeriesType(const node_ptr &parent) : _parent_ts_or_node{parent} {
    }

    TimeSeriesType::TimeSeriesType(const ptr &parent) : _parent_ts_or_node{parent} {
    }

    node_ptr TimeSeriesType::owning_node() { return _owning_node(); }

    node_ptr TimeSeriesType::owning_node() const { return _owning_node(); }

    TimeSeriesInput::ptr TimeSeriesInput::parent_input() const {
        return static_cast<TimeSeriesInput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool TimeSeriesInput::has_parent_input() const { return _has_parent_time_series(); }

    bool TimeSeriesInput::bound() const { return _output != nullptr; }

    bool TimeSeriesInput::has_peer() const {
        // By default, we assume that if there is an output, then we are peered.
        // This is not always True but is a good general assumption.
        return _output != nullptr;
    }

    time_series_output_ptr TimeSeriesInput::output() const { return _output; }

    bool TimeSeriesInput::has_output() const { return _output.get() != nullptr; }

    bool TimeSeriesInput::bind_output(time_series_output_ptr output_) {
        bool peer;
        bool was_bound = bound(); // Track if input was previously bound (matches Python behavior)

        if (auto ref_output = dynamic_cast<TimeSeriesReferenceOutput *>(output_.get())) {
            // Is a TimeseriesReferenceOutput
            // Match Python behavior: only check if value exists (truthy), bind if it does
            if (ref_output->valid() && ref_output->value()) { ref_output->value()->bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer = false;
        } else {
            if (output_ == _output) { return has_peer(); }
            peer = do_bind_output(output_);
        }

        // Notify if the node is started/starting and either:
        // - The input was previously bound (rebinding case), OR
        // - The new output is valid
        // This matches the Python implementation: (was_bound or self._output.valid)
        if ((owning_node()->is_started() || owning_node()->is_starting()) && _output.get() && (was_bound || _output->
                valid())) {
            _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
            if (active()) {
                notify(_sample_time);
                // TODO: This might belong to make_active, or not? There is a race with setting sample_time too.
            }
        }

        return peer;
    }

    void TimeSeriesInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();

        // Handle reference output unbinding conditionally based on unbind_refs parameter
        if (unbind_refs && _reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }

        if (bound()) {
            do_un_bind_output(unbind_refs);

            if (owning_node()->is_started() && was_valid) {
                _sample_time = owning_graph()->evaluation_clock()->evaluation_time();
                if (active()) {
                    // Notify as the state of the node has changed from bound to un_bound
                    owning_node()->notify(_sample_time);
                }
            }
        }
    }

    bool TimeSeriesInput::active() const { return _active; }

    void TimeSeriesInput::make_active() {
        if (!_active) {
            _active = true;
            if (_output != nullptr) {
                output()->subscribe(this);
                if (output()->valid() && output()->modified()) {
                    notify(output()->last_modified_time());
                    return; // If the output is modified, we do not need to check if sampled
                }
            }

            if (sampled()) { notify(_sample_time); }
        }
    }

    void TimeSeriesInput::make_passive() {
        if (_active) {
            _active = false;
            if (_output != nullptr) { output()->un_subscribe(this); }
        }
    }

    nb::object TimeSeriesInput::py_value() const {
        if (_output != nullptr) {
            return _output->py_value();
        } else {
            return nb::none();
        }
    }

    nb::object TimeSeriesInput::py_delta_value() const {
        if (_output != nullptr) {
            return _output->py_delta_value();
        } else {
            return nb::none();
        }
    }

    void TimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesInput, TimeSeriesType>(m, "TimeSeriesInput")
                .def_prop_ro("parent_input",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 if (ts.has_parent_input()) { return nb::cast(ts.parent_input()); }
                                 return nb::none();
                             })
                .def_prop_ro("has_parent_input", &TimeSeriesInput::has_parent_input)
                .def_prop_ro("bound", &TimeSeriesInput::bound)
                .def_prop_ro("has_peer", &TimeSeriesInput::has_peer)
                .def_prop_ro("output",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 if (ts.has_output()) { return nb::cast(ts.output()); }
                                 return nb::none();
                             })
                .def_prop_ro("reference_output",
                             [](const TimeSeriesInput &ts) -> nb::object {
                                 auto ref = ts.reference_output();
                                 if (ref != nullptr) { return nb::cast(ref); }
                                 return nb::none();
                             })
                .def_prop_ro("active", &TimeSeriesInput::active)
                .def("bind_output", &TimeSeriesInput::bind_output, "output"_a)
                .def("un_bind_output", &TimeSeriesInput::un_bind_output, "unbind_refs"_a = false)
                .def("make_active", &TimeSeriesInput::make_active)
                .def("make_passive", &TimeSeriesInput::make_passive)
                .def("__str__", [](const TimeSeriesInput &self) {
                    return fmt::format("TimeSeriesInput@{:p}[bound={}, valid={}, active={}]",
                                       static_cast<const void *>(&self), self.bound(), self.valid(), self.active());
                })
                .def("__repr__", [](const TimeSeriesInput &self) {
                    return fmt::format("TimeSeriesInput@{:p}[bound={}, valid={}, active={}]",
                                       static_cast<const void *>(&self), self.bound(), self.valid(), self.active());
                });
    }

    bool TimeSeriesInput::do_bind_output(time_series_output_ptr &output_) {
        auto active_{active()};
        make_passive(); // Ensure we are unsubscribed from the old output.
        _output = output_;
        if (active_) {
            make_active(); // If we were active now subscribe to the new output,
            // this is important even if we were not bound previously as this will ensure the new output gets
            // subscribed to
        }
        return true;
    }

    auto TimeSeriesInput::notify(engine_time_t modified_time) -> void { // NOLINT(*-no-recursion)
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                parent_input()->notify_parent(this, modified_time);
            } else {
                owning_node()->notify(modified_time);
            }
        }
    }

    void TimeSeriesInput::do_un_bind_output(bool unbind_refs) {
        if (_active) { output()->un_subscribe(this); }
        _output = nullptr;
    }

    // Minimal-teardown helper: avoid consulting owning_node/graph
    void TimeSeriesInput::builder_release_cleanup() {
        if (_output.get() != nullptr && _active) {
            // Unsubscribe from output without triggering any node notifications
            _output->un_subscribe(this);
        }
        _active = false;
        if (_reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output.reset();
        }
        _output = nullptr;
    }

    void TimeSeriesInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        notify(modified_time);
    } // NOLINT(*-no-recursion)

    void TimeSeriesInput::set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }

    engine_time_t TimeSeriesInput::sample_time() const { return _sample_time; }

    bool TimeSeriesInput::sampled() const {
        return _sample_time != MIN_DT && _sample_time == owning_graph()->evaluation_clock()->evaluation_time();
    }

    time_series_reference_output_ptr TimeSeriesInput::reference_output() const { return _reference_output; }

    const TimeSeriesInput *TimeSeriesInput::get_input(size_t index) const {
        return const_cast<TimeSeriesInput *>(this)->get_input(index);
    }

    TimeSeriesInput *TimeSeriesInput::get_input(size_t index) {
        throw std::runtime_error("TimeSeriesInput [] not supported");
    }


    void TimeSeriesInput::reset_output() { _output = nullptr; }

    void TimeSeriesInput::set_output(time_series_output_ptr output) { _output = std::move(output); }

    void TimeSeriesInput::set_active(bool active) { _active = active; }

    TimeSeriesOutput::ptr TimeSeriesOutput::parent_output() const {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    TimeSeriesOutput::ptr TimeSeriesOutput::parent_output() {
        return static_cast<TimeSeriesOutput *>(_parent_time_series().get()); // NOLINT(*-pro-type-static-cast-downcast)
    }

    bool TimeSeriesOutput::has_parent_output() const { return _has_parent_time_series(); }

    bool TimeSeriesOutput::can_apply_result(nb::object value) {
        return not
        modified();
    }

    // Minimal-teardown helper: avoid consulting owning_node/graph
    void TimeSeriesOutput::builder_release_cleanup() {
        // Clear subscribers safely without notifications
        _subscribers.clear();
        // Reset modification state to a neutral value without touching evaluation_clock
        _reset_last_modified_time();
    }

    bool TimeSeriesOutput::modified() const {
        auto g = owning_graph();
        if (!g) { return false; }
        return g->evaluation_clock()->evaluation_time() == _last_modified_time;
    }

    bool TimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool TimeSeriesOutput::all_valid() const {
        return valid(); // By default, all valid is the same as valid
    }

    engine_time_t TimeSeriesOutput::last_modified_time() const { return _last_modified_time; }

    void TimeSeriesOutput::mark_invalid() {
        if (_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            auto g = owning_graph();
            if (g) {
                _notify(g->evaluation_clock()->evaluation_time());
            } else {
                // Owning graph not yet attached; skip notify to avoid dereferencing null during start/recover
            }
        }
    }

    void TimeSeriesOutput::mark_modified() {
        if (has_parent_or_node()) {
            auto g = owning_graph();
            if (g != nullptr) {
                mark_modified(g->evaluation_clock()->evaluation_time());
            } else {
                // Graph not yet attached; mark with a maximal time to preserve monotonicity without dereferencing
                // This is a bad situation, I would probably prefer to find out why,
                // TODO: find the root cause of why this could be called without a bound graph.
            }
        } else {
            mark_modified(MAX_ET);
        }
    }

    void TimeSeriesOutput::mark_modified(engine_time_t modified_time) { // NOLINT(*-no-recursion)
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            if (has_parent_output()) { parent_output()->mark_child_modified(*this, modified_time); }
            _notify(modified_time);
        }
    }

    void TimeSeriesOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        mark_modified(modified_time);
    } // NOLINT(*-no-recursion)

    void TimeSeriesOutput::subscribe(Notifiable *notifiable) { _subscribers.insert(notifiable); }

    void TimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.erase(notifiable); }

    void TimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void TimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    bool TimeSeriesInput::modified() const { return _output != nullptr && (_output->modified() || sampled()); }

    bool TimeSeriesInput::valid() const { return bound() && _output != nullptr && _output->valid(); }

    bool TimeSeriesInput::all_valid() const { return bound() && _output != nullptr && _output->all_valid(); }

    engine_time_t TimeSeriesInput::last_modified_time() const {
        return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
    }
} // namespace hgraph