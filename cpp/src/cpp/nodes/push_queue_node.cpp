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

            if (_is_tsd) {
                auto tsd_output = static_cast<TimeSeriesDictOutput *>(output_ptr.get());

                auto remove = get_remove();
                auto remove_if_exist = get_remove_if_exists();

                // For TSD outputs, iterate over message dict
                auto msg_dict = nb::cast<nb::dict>(message);
                for (auto [key, val]: msg_dict) {
                    if (val.is(remove) || val.is(remove_if_exist)) {
                        auto child_output = tsd_output->py_get(nb::cast<nb::object>(key), nb::none());
                        if (!child_output.is_none()) {
                            if (nb::cast<TimeSeriesOutput::ptr>(child_output)->modified()) {
                                return false; // reject message because cannot remove when there is unprocessed data
                           }
                        }
                    }
                }
                for (auto [key, val]: msg_dict) {
                    if (!val.is(remove) && !val.is(remove_if_exist)) {
                        auto child_output = nb::cast<TimeSeriesOutput::ptr>(tsd_output->py_get_or_create(nb::cast<nb::object>(key)));

                        if (child_output->modified()) {
                            // Append to existing tuple
                            auto existing = child_output->py_value();
                            size_t existing_len = PyTuple_Size(existing.ptr());
                            nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
                            for (size_t i = 0; i < existing_len; ++i) {
                                PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
                            }
                            PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, nb::borrow(val).release().ptr());
                            child_output->py_set_value(new_tuple);
                        } else {
                            // Create new tuple with single element
                            nb::tuple new_tuple = nb::make_tuple(val);
                            child_output->py_set_value(new_tuple);
                        }
                    } else {
                        tsd_output->py_pop(nb::cast<nb::object>(key), nb::none());
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
        _is_tsd = dynamic_cast<TimeSeriesDictOutput *>(output().get()) != nullptr;

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
