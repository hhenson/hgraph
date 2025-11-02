//
// Created by Howard Henson on 26/12/2024.
//

#ifndef SENDER_RECEIVER_STATE_H
#define SENDER_RECEIVER_STATE_H

#include <hgraph/hgraph_base.h>
#include <deque>
#include <mutex>

namespace hgraph {
    struct SenderReceiverState {
        using ptr = SenderReceiverState *;
        using LockType = std::recursive_mutex;
        using LockGuard = std::lock_guard<LockType>;
        using value_type = std::pair<int64_t, nb::object>;

        SenderReceiverState() = default;

        void set_evaluation_clock(engine_evalaution_clock_ptr clock);

        void operator()(value_type value);

        void enqueue(value_type value);

        void enqueue_front(value_type value);

        std::optional<value_type> dequeue();

        explicit operator bool() const;

        [[nodiscard]] bool stopped() const;

        void mark_stopped();

        /**
         * This can replace with ``with ...`` clause just graph the guard and in an appropriate
         * context.
         */
        [[nodiscard]] auto guard() const -> LockGuard;

    private:
        mutable LockType lock;
        std::deque<value_type> queue;
        engine_evalaution_clock_ptr evaluation_clock{};
        bool _stopped{false};
    };
} // namespace hgraph

#endif  // SENDER_RECEIVER_STATE_H