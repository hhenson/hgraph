#include <hgraph/types/utils/slot_observer.h>

#include <hgraph/util/scope.h>

#include <hgraph/types/utils/impl/observer_list.h>

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    struct SlotObserverList::ObserverList : detail::ObserverListStorage<SlotObserver>
    {
    };

    SlotObserverList::SlotObserverList(const SlotObserverList &other)
    {
        auto rollback = make_scope_exit([this] { clear(); });
        other.for_each([this](SlotObserver *observer) { add(observer); });
        rollback.release();
    }

    SlotObserverList &SlotObserverList::operator=(const SlotObserverList &other)
    {
        if (this != &other)
        {
            clear();
            other.for_each([this](SlotObserver *observer) { add(observer); });
        }
        return *this;
    }

    SlotObserverList::SlotObserverList(SlotObserverList &&other) noexcept
        : observers_(std::exchange(other.observers_, ObserverStorage{}))
    {
    }

    SlotObserverList &SlotObserverList::operator=(SlotObserverList &&other) noexcept
    {
        if (this != &other)
        {
            clear();
            observers_ = std::exchange(other.observers_, ObserverStorage{});
        }
        return *this;
    }

    SlotObserverList::~SlotObserverList() noexcept
    {
        clear();
    }

    void SlotObserverList::add(SlotObserver *observer)
    {
        if (observer == nullptr) { return; }

        if (!observers_)
        {
            set_single(observer);
            return;
        }

        if (auto *entry = single(); entry != nullptr)
        {
            assert(entry != observer && "slot observer registered twice");
            if (entry == observer) { return; }

            auto entries = std::make_unique<ObserverList>();
            entries->entries.reserve(2);
            static_cast<void>(entries->add(entry));
            static_cast<void>(entries->add(observer));
            set_many(entries.release());
            return;
        }

        auto *entries = many();
        assert(entries != nullptr && "slot observer storage is corrupt");
        if (entries == nullptr) { throw std::logic_error("slot observer storage is corrupt"); }

        const bool inserted = entries->add(observer);
        assert(inserted && "slot observer registered twice");
        static_cast<void>(inserted);
    }

    void SlotObserverList::remove(SlotObserver *observer)
    {
        if (observer == nullptr) { return; }

        if (auto *entry = single(); entry != nullptr)
        {
            assert(entry == observer && "removing unregistered slot observer");
            if (entry == observer) { observers_.clear(); }
            return;
        }

        auto *entries = many();
        if (entries == nullptr)
        {
            assert(false && "removing unregistered slot observer");
            return;
        }

        const bool removed = entries->remove(observer);
        assert(removed && "removing unregistered slot observer");
        if (!removed || entries->notify_depth != 0) { return; }
        compact_many(*entries);
    }

    bool SlotObserverList::empty() const noexcept
    {
        return !observers_;
    }

    std::size_t SlotObserverList::size() const noexcept
    {
        if (single() != nullptr) { return 1; }
        const auto *entries = many();
        if (entries == nullptr) { return 0; }
        return entries->size();
    }

    bool SlotObserverList::contains(const SlotObserver *observer) const noexcept
    {
        if (observer == nullptr) { return false; }
        if (auto *entry = single(); entry != nullptr) { return entry == observer; }
        const auto *entries = many();
        return entries != nullptr && entries->find(observer) != ObserverList::not_found;
    }

    DynamicStorageMetrics SlotObserverList::dynamic_storage_metrics() const noexcept
    {
        const auto *entries = many();
        if (entries == nullptr) { return {}; }
        return DynamicStorageMetrics{sizeof(ObserverList), sizeof(ObserverList)} + entries->buffer_metrics();
    }

    void SlotObserverList::clear() noexcept
    {
        if (auto *entries = many(); entries != nullptr)
        {
            if (entries->notify_depth == 0) { delete entries; }
            else
            {
                entries->clear_entries();
            }
        }
        observers_.clear();
    }

    void SlotObserverList::notify_capacity(std::size_t old_capacity, std::size_t new_capacity) const
    {
        for_each([=](SlotObserver *observer) { observer->on_capacity(old_capacity, new_capacity); });
    }

    void SlotObserverList::notify_insert(std::size_t slot) const
    {
        for_each([=](SlotObserver *observer) { observer->on_insert(slot); });
    }

    void SlotObserverList::notify_remove(std::size_t slot) const
    {
        for_each([=](SlotObserver *observer) { observer->on_remove(slot); });
    }

    void SlotObserverList::notify_erase(std::size_t slot) const
    {
        for_each([=](SlotObserver *observer) { observer->on_erase(slot); });
    }

    void SlotObserverList::notify_clear() const
    {
        for_each([](SlotObserver *observer) { observer->on_clear(); });
    }

    SlotObserver *SlotObserverList::single() const noexcept
    {
        return observers_.has_enum(Representation::single)
                   ? observers_.as<SlotObserver>()
                   : nullptr;
    }

    SlotObserverList::ObserverList *SlotObserverList::many() const noexcept
    {
        return observers_.has_enum(Representation::many)
                   ? observers_.as<ObserverList>()
                   : nullptr;
    }

    void SlotObserverList::set_single(SlotObserver *observer) noexcept
    {
        observers_.set(observer, Representation::single);
    }

    void SlotObserverList::set_many(ObserverList *observers) noexcept
    {
        observers_.set(observers, Representation::many);
    }

    void SlotObserverList::compact_many(ObserverList &observers) noexcept
    {
        if (observers.notify_depth != 0)
        {
            observers.compact_pending = true;
            return;
        }

        observers.compact();

        if (observers.entries.empty())
        {
            delete &observers;
            observers_.clear();
            return;
        }

        if (observers.entries.size() == 1)
        {
            auto *remaining = observers.entries.front();
            delete &observers;
            set_single(remaining);
        }
    }

    void SlotObserverList::for_each_erased(void *context, ErasedVisitor visitor) const
    {
        if (context == nullptr || visitor == nullptr) { return; }

        if (auto *entry = single(); entry != nullptr)
        {
            visitor(context, entry);
            return;
        }

        auto *entries = many();
        if (entries == nullptr) { return; }

        ++entries->notify_depth;
        auto guard = make_scope_exit([this, entries]() noexcept {
            --entries->notify_depth;
            if (entries->notify_depth == 0 && entries->compact_pending)
            {
                auto *self = const_cast<SlotObserverList *>(this);
                if (self->many() == entries) { self->compact_many(*entries); }
                else { delete entries; }
            }
        });

        const auto limit = entries->entries.size();
        for (std::size_t index = 0; index < limit; ++index)
        {
            auto *observer = entries->entries[index];
            if (observer != nullptr) { visitor(context, observer); }
        }
    }
}  // namespace hgraph
