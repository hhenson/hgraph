//
// Created by Howard Henson on 03/04/2021.
//

#ifndef HGRAPH_REFERENCE_COUNT_SUBSCRIBER_H
#define HGRAPH_REFERENCE_COUNT_SUBSCRIBER_H

#include <unordered_map>

namespace hgraph {
    struct Notifiable {
        virtual ~Notifiable() = default;

        virtual void notify(engine_time_t et) = 0;
    };

    template<typename T>
    class ReferenceCountSubscriber {
    public:
        void subscribe(T subscriber);

        void un_subscribe(T subscriber);

        template<typename Op>
        void apply(Op op) {
            for (const auto &[k, _]: m_subscriptions) { op(k); }
        }

    private:
        std::unordered_map<T, std::size_t> m_subscriptions{};
    };

    template<typename T>
    void ReferenceCountSubscriber<T>::subscribe(T subscriber) {
        auto [it, success] = m_subscriptions.insert({subscriber, 1});
        if (!success) { ++(it->second); }
    }

    template<typename T>
    void ReferenceCountSubscriber<T>::un_subscribe(T subscriber) {
        auto it{m_subscriptions.find(subscriber)};
        if (it != m_subscriptions.end()) {
            if (--(it->second) == 0) { m_subscriptions.erase(it); }
        }
    }
}
#endif  // HGRAPH_REFERENCE_COUNT_SUBSCRIBER_H