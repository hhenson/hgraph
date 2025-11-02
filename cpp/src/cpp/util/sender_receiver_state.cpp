#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/util/sender_receiver_state.h>

namespace hgraph {
    void SenderReceiverState::set_evaluation_clock(engine_evalaution_clock_ptr clock) { evaluation_clock = clock; }

    void SenderReceiverState::operator()(value_type value) {
        // Replace `int` with the appropriate type.
        enqueue(std::move(value));
    }

    void SenderReceiverState::enqueue(value_type value) {
        // Replace `int` with the appropriate type.
        LockGuard guard(lock);
        if (stopped()) { throw std::runtime_error("Cannot enqueue into a stopped receiver"); }
        queue.push_back(std::move(value));
        if (evaluation_clock) { evaluation_clock->mark_push_node_requires_scheduling(); }
    }

    void SenderReceiverState::enqueue_front(value_type value) {
        LockGuard guard(lock);
        if (stopped()) { throw std::runtime_error("Cannot enqueue into a stopped receiver"); }
        queue.push_front(std::move(value));
    }

    std::optional<SenderReceiverState::value_type> SenderReceiverState::dequeue() {
        // Replace `int` with the appropriate type.
        LockGuard guard(lock);
        if (!queue.empty()) {
            auto value = queue.front();
            queue.pop_front();
            return value;
        }
        return std::nullopt;
    }

    SenderReceiverState::operator bool() const {
        LockGuard guard(lock);
        return !queue.empty();
    }

    bool SenderReceiverState::stopped() const { return _stopped; }

    void SenderReceiverState::mark_stopped() { _stopped = true; }

    auto SenderReceiverState::guard() const -> LockGuard { return LockGuard(lock); }
} // namespace hgraph