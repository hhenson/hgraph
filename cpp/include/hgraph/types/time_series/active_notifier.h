#pragma once

#include <hgraph/types/notifiable.h>

namespace hgraph {

/**
 * Wrapper used by link payloads to forward activation notifications.
 *
 * The target notifier is owner-local state (set/cleared by TSInput activation).
 */
struct HGRAPH_EXPORT ActiveNotifier : Notifiable {
    ActiveNotifier() = default;

    void set_target(Notifiable* target) noexcept { target_ = target; }
    [[nodiscard]] Notifiable* target() const noexcept { return target_; }
    [[nodiscard]] bool active() const noexcept { return target_ != nullptr; }

    void notify(engine_time_t et) override {
        if (target_ != nullptr) {
            target_->notify(et);
        }
    }

private:
    Notifiable* target_{nullptr};
};

}  // namespace hgraph

