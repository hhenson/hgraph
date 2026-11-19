#include "window_expiry_service.h"

#include <hgraph/util/scope.h>

namespace hgraph
{
    WindowExpiryAlarm::WindowExpiryAlarm(const GraphView &graph, void *subscriber, Expire expire, Refresh refresh)
        : service_(root_window_expiry_service(graph)), subscriber_(subscriber), expire_(expire), refresh_(refresh),
          sequence_(service_.lock()->next_sequence_++)
    {
    }

    WindowExpiryAlarm::~WindowExpiryAlarm()
    {
        if (auto service = service_.lock())
        {
            service->remove(*this);
            service->unmark_dirty(*this);
        }
    }

    void WindowExpiryAlarm::set_deadline(DateTime when)
    {
        if (when == deadline_) { return; }
        if (auto service = service_.lock()) { service->set_deadline(*this, when); }
    }

    void WindowExpiryAlarm::request_refresh() noexcept
    {
        if (dirty_) { return; }
        if (auto service = service_.lock()) { service->mark_dirty(*this); }
    }

    bool WindowExpiryService::earlier(const WindowExpiryAlarm &left, const WindowExpiryAlarm &right) noexcept
    {
        return left.deadline_ != right.deadline_ ? left.deadline_ < right.deadline_ : left.sequence_ < right.sequence_;
    }

    void WindowExpiryService::reorder(std::size_t index) noexcept
    {
        auto *alarm = heap_[index];
        while (index > 0)
        {
            const auto parent = (index - 1) / 2;
            if (!earlier(*alarm, *heap_[parent])) { break; }
            heap_[index] = heap_[parent];
            heap_[index]->heap_index_ = index;
            index = parent;
        }
        while (index < heap_.size() / 2)
        {
            auto child = 2 * index + 1;
            if (child + 1 < heap_.size() && earlier(*heap_[child + 1], *heap_[child])) { ++child; }
            if (!earlier(*heap_[child], *alarm)) { break; }
            heap_[index] = heap_[child];
            heap_[index]->heap_index_ = index;
            index = child;
        }
        heap_[index] = alarm;
        alarm->heap_index_ = index;
    }

    void WindowExpiryService::remove(WindowExpiryAlarm &alarm) noexcept
    {
        const auto index = alarm.heap_index_;
        if (index == WindowExpiryAlarm::unarmed) { return; }
        auto *last = heap_.back();
        heap_.pop_back();
        if (index < heap_.size())
        {
            heap_[index] = last;
            reorder(index);
        }
        alarm.heap_index_ = WindowExpiryAlarm::unarmed;
        alarm.deadline_ = MAX_DT;
    }

    void WindowExpiryService::set_deadline(WindowExpiryAlarm &alarm, DateTime when)
    {
        // Previously armed siblings remain due on this pass. A callback that
        // supplies an already-due entry again must not spin the root in place.
        if (pulling_time_ != MIN_DT && when != MAX_DT && when <= pulling_time_ && alarm.deadline_ > pulling_time_)
            when = pulling_time_ < MAX_DT ? pulling_time_ + MIN_TD : MAX_DT;
        if (when == alarm.deadline_) { return; }
        if (when == MAX_DT) { remove(alarm); return; }
        if (alarm.heap_index_ == WindowExpiryAlarm::unarmed)
        {
            heap_.push_back(&alarm);
            alarm.heap_index_ = heap_.size() - 1;
        }
        alarm.deadline_ = when;
        reorder(alarm.heap_index_);
    }

    void WindowExpiryService::mark_dirty(WindowExpiryAlarm &alarm) noexcept
    {
        if (alarm.dirty_) { return; }
        alarm.dirty_ = true;
        alarm.dirty_prev_ = dirty_tail_;
        if (dirty_tail_) { dirty_tail_->dirty_next_ = &alarm; }
        else { dirty_head_ = &alarm; }
        dirty_tail_ = &alarm;
    }

    void WindowExpiryService::unmark_dirty(WindowExpiryAlarm &alarm) noexcept
    {
        if (!alarm.dirty_) { return; }
        if (alarm.dirty_prev_) { alarm.dirty_prev_->dirty_next_ = alarm.dirty_next_; }
        else { dirty_head_ = alarm.dirty_next_; }
        if (alarm.dirty_next_) { alarm.dirty_next_->dirty_prev_ = alarm.dirty_prev_; }
        else { dirty_tail_ = alarm.dirty_prev_; }
        alarm.dirty_prev_ = alarm.dirty_next_ = nullptr;
        alarm.dirty_ = false;
    }

    DateTime WindowExpiryService::next_time(DateTime graph_time) const noexcept
    {
        if (heap_.empty()) { return MAX_DT; }
        const auto due = heap_.front()->deadline_;
        if (due > graph_time) { return due; }
        // Pre-start restored history can be due immediately. A historical
        // replacement installed by a normal producer has already passed the
        // expiry phase; never evaluate that producer twice at the same time.
        if (finished_time_ != graph_time) { return graph_time; }
        return graph_time < MAX_DT ? graph_time + MIN_TD : MAX_DT;
    }

    void WindowExpiryService::evaluate(DateTime now)
    {
        if (heap_.empty() || heap_.front()->deadline_ > now) { return; }
        // A throwing value copy can leave the alarm due. Refresh live dirty
        // registrations for a retained graph; a destroyed one has unlinked itself.
        auto refresh_on_failure = UnwindCleanupGuard([&] { refresh(); });
        pulling_time_ = now;
        auto reset_pull = make_scope_exit([&]() noexcept { pulling_time_ = MIN_DT; });
        while (!heap_.empty() && heap_.front()->deadline_ <= now)
        {
            auto &alarm = *heap_.front();
            remove(alarm);
            // The callback may destroy its own registration, cancel a sibling,
            // or publish a change which re-enters the service.
            mark_dirty(alarm);
            alarm.expire_(alarm.subscriber_, now);
        }
    }

    void WindowExpiryService::refresh()
    {
        while (dirty_head_)
        {
            auto &alarm = *dirty_head_;
            const auto sequence = alarm.sequence_;
            // Keep it dirty if refreshing throws. The callback can also
            // detach and destroy it; only inspect the current head afterwards.
            alarm.refresh_(alarm.subscriber_);
            if (dirty_head_ && dirty_head_->sequence_ == sequence) { unmark_dirty(*dirty_head_); }
        }
    }

    void WindowExpiryService::finish_cycle(DateTime now)
    {
        refresh();
        finished_time_ = now;
    }
}
