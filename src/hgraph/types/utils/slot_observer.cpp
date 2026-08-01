#include <hgraph/types/utils/slot_observer.h>

#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    struct SlotObserverList::ObserverList
    {
        std::vector<SlotObserver *> entries{};
        std::size_t                 notify_depth{0};
        bool                        compact_pending{false};
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
            entries->entries.push_back(entry);
            entries->entries.push_back(observer);
            set_many(entries.release());
            return;
        }

        auto *entries = many();
        assert(entries != nullptr && "slot observer storage is corrupt");
        if (entries == nullptr) { throw std::logic_error("slot observer storage is corrupt"); }

        const auto it = std::find(entries->entries.begin(), entries->entries.end(), observer);
        assert(it == entries->entries.end() && "slot observer registered twice");
        if (it == entries->entries.end()) { entries->entries.push_back(observer); }
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

        const auto it = std::find(entries->entries.begin(), entries->entries.end(), observer);
        assert(it != entries->entries.end() && "removing unregistered slot observer");
        if (it == entries->entries.end()) { return; }

        if (entries->notify_depth != 0)
        {
            *it = nullptr;
            entries->compact_pending = true;
            return;
        }

        *it = entries->entries.back();
        entries->entries.pop_back();
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
        return static_cast<std::size_t>(
            std::count_if(entries->entries.begin(), entries->entries.end(),
                          [](const SlotObserver *observer) { return observer != nullptr; }));
    }

    bool SlotObserverList::contains(const SlotObserver *observer) const noexcept
    {
        if (observer == nullptr) { return false; }
        if (auto *entry = single(); entry != nullptr) { return entry == observer; }
        const auto *entries = many();
        return entries != nullptr &&
               std::find(entries->entries.begin(), entries->entries.end(), observer) != entries->entries.end();
    }

    DynamicStorageMetrics SlotObserverList::dynamic_storage_metrics() const noexcept
    {
        const auto *entries = many();
        if (entries == nullptr) { return {}; }
        return {
            .live_bytes = sizeof(ObserverList) + entries->entries.size() * sizeof(SlotObserver *),
            .reserved_bytes = sizeof(ObserverList) + entries->entries.capacity() * sizeof(SlotObserver *),
        };
    }

    void SlotObserverList::clear() noexcept
    {
        if (auto *entries = many(); entries != nullptr)
        {
            if (entries->notify_depth == 0) { delete entries; }
            else
            {
                std::ranges::fill(entries->entries, nullptr);
                entries->compact_pending = true;
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

        for (std::size_t index = 0; index < observers.entries.size();)
        {
            if (observers.entries[index] != nullptr)
            {
                ++index;
                continue;
            }
            observers.entries[index] = observers.entries.back();
            observers.entries.pop_back();
        }
        observers.compact_pending = false;

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
