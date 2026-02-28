#pragma once

#include <hgraph/types/time_series/active_notifier.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/view_data.h>

#include <vector>

namespace hgraph {

enum class LinkObserverNotifyPolicy : uint8_t {
    None = 0,
    RefToNonRefDynamicTarget = 1 << 0,
    SignalRefToNonRefTarget = 1 << 1,
    SignalWrapperWrite = 1 << 2,
    SignalRefWrapperWrite = 1 << 3,
    NonRefObserverWrapperWrite = 1 << 4,
};

inline constexpr uint8_t link_observer_notify_policy_bit(LinkObserverNotifyPolicy policy) {
    return static_cast<uint8_t>(policy);
}

/**
 * LinkTarget is the input-side binding payload.
 *
 * Target-data fields are copied during bind/rebind.
 * Structural fields stay local to owner hierarchy.
 */
struct HGRAPH_EXPORT LinkTarget : Notifiable {
    // Target-data fields (copied on bind)
    bool is_linked{false};
    ShortPath target_path{};
    void* value_data{nullptr};
    void* time_data{nullptr};
    void* observer_data{nullptr};
    void* delta_data{nullptr};
    void* link_data{nullptr};
    const engine_time_t* engine_time_ptr{nullptr};
    TSLinkObserverRegistry* link_observer_registry{nullptr};
    ViewProjection projection{ViewProjection::NONE};
    const ts_ops* ops{nullptr};
    const TSMeta* meta{nullptr};

    // Structural fields (owner-local)
    engine_time_t* owner_time_ptr{nullptr};
    LinkTarget* parent_link{nullptr};
    ActiveNotifier active_notifier{};
    ViewData observer_view{};
    bool peered{false};
    // For non-REF consumers bound via REF wrappers, wrapper-local writes should
    // not drive notifications when the resolved target is unchanged.
    bool notify_on_ref_wrapper_write{true};
    bool observer_is_signal{false};
    // REF inputs bound to non-REF targets should not receive downstream target
    // value-write notifications; they only tick on rebind/sample events.
    bool observer_ref_to_nonref_target{false};
    uint8_t notify_policy{0};
    engine_time_t last_rebind_time{MIN_DT};
    bool has_previous_target{false};
    ViewData previous_target{};
    bool has_resolved_target{false};
    ViewData resolved_target{};
    std::vector<ViewData> fan_in_targets{};

    LinkTarget();
    ~LinkTarget() override;

    LinkTarget(const LinkTarget& other);
    LinkTarget& operator=(const LinkTarget& other);
    LinkTarget(LinkTarget&& other) noexcept;
    LinkTarget& operator=(LinkTarget&& other) noexcept;

    void bind(const ViewData& target, engine_time_t current_time = MIN_DT);
    void unbind(engine_time_t current_time = MIN_DT);

    [[nodiscard]] bool valid() const { return is_linked; }
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] ViewData as_view_data(bool sampled = false) const;
    [[nodiscard]] ViewData previous_view_data(bool sampled = false) const;

    void notify_time(engine_time_t et);
    void notify_active(engine_time_t et);
    void notify(engine_time_t et) override;

private:
    void copy_target_data_from(const LinkTarget& other);
    void move_target_data_from(LinkTarget&& other) noexcept;
    void clear_target_data();
};

HGRAPH_EXPORT bool is_live_link_target(const LinkTarget* link_target) noexcept;

}  // namespace hgraph
