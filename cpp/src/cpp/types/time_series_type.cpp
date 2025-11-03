#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/base_time_series_input.h>
#include <hgraph/types/base_time_series_output.h>

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

    TimeSeriesType::TimeSeriesType(const ptr &parent) : _parent_ts_or_node{parent} {}

    engine_time_t TimeSeriesType::current_engine_time() const {
        auto owning_graph_{owning_graph()};
        if (owning_graph_ != nullptr) { return owning_graph_->evaluation_clock()->evaluation_time(); }
        return MIN_DT;
    }

    node_ptr TimeSeriesType::owning_node() { return _owning_node(); }

    node_ptr TimeSeriesType::owning_node() const { return _owning_node(); }


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


} // namespace hgraph