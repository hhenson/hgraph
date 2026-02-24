#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>

namespace hgraph {
    namespace {
        value::Value key_from_python(const value::TypeMeta *key_type, const nb::handle &key_obj) {
            value::Value key_value(key_type);
            key_value.emplace();
            key_type->ops().from_python(key_value.data(), nb::borrow(key_obj), key_type);
            return key_value;
        }

        nb::tuple append_tuple(const nb::object &existing, const nb::object &value) {
            size_t existing_len = PyTuple_Size(existing.ptr());
            nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
            for (size_t i = 0; i < existing_len; ++i) {
                PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
            }
            PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, nb::borrow(value).release().ptr());
            return new_tuple;
        }
    }  // namespace

    void PushQueueNode::do_eval() {
    }

    void PushQueueNode::enqueue_message(nb::object message) {
        auto sender_context = _sender_context;
        if (!sender_context || !sender_context->active.load(std::memory_order_acquire)) {
            return;
        }
        if (sender_context->receiver == nullptr) {
            return;
        }

        sender_context->receiver->enqueue({sender_context->node_ndx, std::move(message)});
        sender_context->queued.fetch_add(1, std::memory_order_relaxed);
    }

    bool PushQueueNode::apply_message(nb::object message) {
        // Push queue payloads are Python objects coming from external threads.
        // Message application runs on the graph thread, so re-acquire the GIL
        // before any nanobind/Python API interaction.
        nb::gil_scoped_acquire gil;

        auto out_view = output();
        if (!out_view) {
            return false;
        }

        if (_batch) {
            // Batch mode: accumulate messages into a tuple
            if (_is_tsd) {
                if (!out_view.valid()) {
                    out_view.from_python(nb::dict{});
                }

                auto remove = get_remove();
                auto remove_if_exist = get_remove_if_exists();
                auto ts_meta = out_view.ts_meta();
                if (ts_meta == nullptr || ts_meta->kind != TSKind::TSD || ts_meta->key_type() == nullptr) {
                    return false;
                }
                auto key_type = ts_meta->key_type();
                auto tsd_view = out_view.as_dict();

                // For TSD outputs, iterate over message dict
                nb::dict msg_dict = nb::borrow<nb::dict>(message);
                for (auto [key, val]: msg_dict) {
                    if (val.is(remove) || val.is(remove_if_exist)) {
                        auto key_value = key_from_python(key_type, key);
                        auto child_output = tsd_view.at_key(key_value.view());
                        if (child_output && child_output.modified()) {
                            return false; // reject message because cannot remove when there is unprocessed data
                        }
                    }
                }
                for (auto [key, val]: msg_dict) {
                    auto key_value = key_from_python(key_type, key);
                    if (!val.is(remove) && !val.is(remove_if_exist)) {
                        auto child_output = tsd_view.at_key(key_value.view());
                        if (!child_output) {
                            child_output = tsd_view.create(key_value.view());
                        }

                        if (child_output && child_output.modified()) {
                            // Append to existing tuple
                            auto existing = child_output.to_python();
                            child_output.from_python(append_tuple(existing, nb::borrow(val)));
                        } else {
                            // Create new tuple with single element
                            nb::tuple new_tuple = nb::make_tuple(val);
                            child_output.from_python(new_tuple);
                        }
                    } else {
                        tsd_view.remove(key_value.view());
                    }
                }
            } else {
                // For non-TSD outputs, accumulate messages into a tuple
                if (out_view.modified()) {
                    // Append to existing tuple
                    auto existing = out_view.to_python();
                    out_view.from_python(append_tuple(existing, message));
                } else {
                    // Create new tuple with single element
                    nb::tuple new_tuple = nb::make_tuple(message);
                    out_view.from_python(new_tuple);
                }
            }

            _messages_dequeued.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        if (_elide) {
            out_view.from_python(std::move(message));
            _messages_dequeued.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        // Python parity: in non-elide mode, only accept one message per cycle.
        // If output already has an unconsumed value, keep this message in queue.
        if (out_view.modified()) {
            return false;
        }

        try {
            out_view.from_python(std::move(message));
            _messages_dequeued.fetch_add(1, std::memory_order_relaxed);
            return true;
        } catch (...) {}
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const {
        const auto sender_context = _sender_context;
        const int64_t queued =
            sender_context ? sender_context->queued.load(std::memory_order_relaxed) : 0;
        const int64_t dequeued = _messages_dequeued.load(std::memory_order_relaxed);
        return queued - dequeued;
    }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) {
        if (!_sender_context) {
            _sender_context = std::make_shared<SenderContext>();
        }
        _sender_context->receiver = value;
        _sender_context->node_ndx = node_ndx();
    }

    void PushQueueNode::do_start() {
        if (!_sender_context) {
            _sender_context = std::make_shared<SenderContext>();
        }
        _sender_context->receiver = &graph()->receiver();
        _sender_context->node_ndx = node_ndx();
        _sender_context->queued.store(0, std::memory_order_relaxed);
        _sender_context->active.store(true, std::memory_order_release);
        _messages_dequeued.store(0, std::memory_order_relaxed);

        _elide = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;
        auto out_view = output();
        _is_tsd = out_view && out_view.ts_meta() != nullptr && out_view.ts_meta()->kind == TSKind::TSD;

        // If an eval function was provided (from push_queue decorator), call it with a sender and scalar kwargs
        if (_eval_fn.is_valid() && !_eval_fn.is_none()) {
            // Create a Python-callable sender that enqueues messages into this node
            // The sender must not capture `this` because it can outlive node teardown.
            auto weak_sender_context = std::weak_ptr<SenderContext>(_sender_context);
            nb::object sender = nb::cpp_function([weak_sender_context](nb::object m) {
                // Acquire GIL in case we're being called from a thread that doesn't have it
                nb::gil_scoped_acquire gil;
                auto sender_context = weak_sender_context.lock();
                if (!sender_context || !sender_context->active.load(std::memory_order_acquire)) {
                    return;
                }
                if (sender_context->receiver == nullptr || sender_context->receiver->stopped()) {
                    return;
                }

                sender_context->receiver->enqueue({sender_context->node_ndx, std::move(m)});
                sender_context->queued.fetch_add(1, std::memory_order_relaxed);
            });
            // Call eval_fn(sender, **scalars)
            try {
                _eval_fn(sender, **scalars());
            } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During push-queue start"); }
        }
    }

    void PushQueueNode::do_stop() {
        if (_sender_context) {
            _sender_context->active.store(false, std::memory_order_release);
        }
    }
} // namespace hgraph
