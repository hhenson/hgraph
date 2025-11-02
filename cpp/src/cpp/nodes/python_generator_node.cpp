#include <hgraph/nodes/python_generator_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>

namespace hgraph {
    void PythonGeneratorNode::register_with_nanobind(nb::module_ &m) {
        nb::class_<PythonGeneratorNode, BasePythonNode>(m, "PythonGeneratorNode");
    }

    void PythonGeneratorNode::do_eval() {
        auto et = graph()->evaluation_clock()->evaluation_time();
        auto next_time{MIN_DT};
        auto sentinel{nb::iterator::sentinel()};
        nb::object out;
        for (nb::iterator v = ++generator; v != sentinel; ++v) {
            // Returns NULL if there are no new values
            auto tpl = *v;
            if (v.is_none()) {
                out = nb::none();
                break;
            }
            auto time = nb::cast<nb::object>(tpl[0]);
            out = nb::cast<nb::object>(tpl[1]);

            // Import datetime module
            static auto datetime = nb::module_::import_("datetime");
            static auto timedelta = datetime.attr("timedelta");
            static auto datetime_type = datetime.attr("datetime");

            // Robustly handle either a timedelta (duration) or a datetime (time_point)
            if (nb::isinstance(time, timedelta)) {
                // Handle timedelta
                auto delta = nb::cast<engine_time_delta_t>(time);
                next_time = et + delta;
            } else if (nb::isinstance(time, datetime_type)) {
                // Handle datetime
                next_time = nb::cast<engine_time_t>(time);
            } else {
                // Raise if time is not of a correct type
                throw std::runtime_error("Type of time value not recognised");
            }
            if (next_time >= et && !out.is_none()) { break; }
        }

        // If next_time > MIN_TD then we entered the extraction loop and extracted a value
        // If next_time <= et then we are expecting to schedule the task.
        if (next_time > MIN_DT && next_time <= et) {
            // If we have a duplicate time, this will pick it up
            if (output()->last_modified_time() == next_time) {
                throw std::runtime_error(
                    fmt::format("Duplicate time produced by generator: [{:%FT%T%z}] - {}", next_time,
                                nb::str(out).c_str()));
            }
            // If next_time is less than et we will schedule at et anyhow.
            output()->apply_result(out);
            next_value = nb::none();
            do_eval(); // We are going to apply now! Prepare next step
            return;
        }

        // If we get here, it may be that we are scheduled, let's see if there is anything pending delivery.
        if (next_value.is_valid() && !next_value.is_none()) {
            // There is, set the value and reset the next_value
            output()->apply_result(next_value);
            next_value = nb::none();
        }

        // OK, now see if there is a new value to process.
        if (next_time != MIN_DT) {
            // There is so cache the value and schedule ourselves for the next time.
            next_value = out;
            graph()->schedule_node(node_ndx(), next_time);
        }
    }

    void PythonGeneratorNode::start() {
        BasePythonNode::_initialise_kwargs();
        generator = nb::cast<nb::iterator>(_eval_fn(**_kwargs));
        graph()->schedule_node(node_ndx(), graph()->evaluation_clock()->evaluation_time());
    }
}