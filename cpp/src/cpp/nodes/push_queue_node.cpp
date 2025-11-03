#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsd.h>
#include <hgraph/types/constants.h>

namespace hgraph {
    void PushQueueNode::do_eval() {
    }

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        if (_batch) {
            // Batch mode: accumulate messages into a tuple
            auto output_ptr = output();

            // Check if output is a TimeSeriesDictOutput (TSD)
            if (auto tsd_output = dynamic_cast<TimeSeriesDictOutput *>(output_ptr.get())) {
                // For TSD outputs, iterate over message dict
                auto msg_dict = nb::cast<nb::dict>(message);
                for (auto [key, val]: msg_dict) {
                    // Handle REMOVE and REMOVE_IF_EXISTS (TODO: check if these symbols are available)
                    // For now, skip removal handling
                    auto child_output = tsd_output->py_get_or_create(nb::cast<nb::object>(key));

                    // Get the modified state by checking if the value property exists and has been set
                    bool is_modified = nb::hasattr(child_output, "modified") && nb::cast<bool>(
                                           child_output.attr("modified"));

                    if (is_modified) {
                        // Append to existing tuple
                        auto existing = child_output.attr("value");
                        size_t existing_len = PyTuple_Size(existing.ptr());
                        nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
                        for (size_t i = 0; i < existing_len; ++i) {
                            PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
                        }
                        PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, nb::borrow(val).release().ptr());
                        child_output.attr("value") = new_tuple;
                    } else {
                        // Create new tuple with single element
                        nb::tuple new_tuple = nb::make_tuple(val);
                        child_output.attr("value") = new_tuple;
                    }
                }
            } else {
                // For non-TSD outputs, accumulate messages into a tuple
                if (output_ptr->modified()) {
                    // Append to existing tuple
                    auto existing = output_ptr->py_value();
                    size_t existing_len = PyTuple_Size(existing.ptr());
                    nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
                    for (size_t i = 0; i < existing_len; ++i) {
                        PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
                    }
                    PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, message.release().ptr());
                    output_ptr->py_set_value(new_tuple);
                } else {
                    // Create new tuple with single element
                    nb::tuple new_tuple = nb::make_tuple(message);
                    output_ptr->py_set_value(new_tuple);
                }
            }

            ++_messages_dequeued;
            return true;
        }

        if (_elide || output()->can_apply_result(message)) {
            output()->apply_result(std::move(message));
            ++_messages_dequeued;
            return true;
        }
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::do_start() {
        _receiver = &graph()->receiver();
        _elide = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;

        // If an eval function was provided (from push_queue decorator), call it with a sender and scalar kwargs
        if (_eval_fn.is_valid() && !_eval_fn.is_none()) {
            // Create a Python-callable sender that enqueues messages into this node
            // The sender will be called from a Python thread, so it needs to acquire the GIL
            nb::object sender = nb::cpp_function([this](nb::object m) {
                // Acquire GIL in case we're being called from a thread that doesn't have it
                nb::gil_scoped_acquire gil;
                this->enqueue_message(std::move(m));
            });
            // Call eval_fn(sender, **scalars)
            try {
                _eval_fn(sender, **scalars());
            } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During push-queue start"); }
        }
    }
} // namespace hgraph
