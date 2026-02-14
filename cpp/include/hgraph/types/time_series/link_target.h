#pragma once

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/view_data.h>

namespace hgraph {

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
    const ts_ops* ops{nullptr};
    const TSMeta* meta{nullptr};

    // Structural fields (owner-local)
    engine_time_t* owner_time_ptr{nullptr};
    LinkTarget* parent_link{nullptr};
    Notifiable* active_notifier{nullptr};
    bool peered{false};

    LinkTarget() = default;
    ~LinkTarget() override = default;

    LinkTarget(const LinkTarget& other);
    LinkTarget& operator=(const LinkTarget& other);
    LinkTarget(LinkTarget&& other) noexcept;
    LinkTarget& operator=(LinkTarget&& other) noexcept;

    void bind(const ViewData& target);
    void unbind();

    [[nodiscard]] bool valid() const { return is_linked; }
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] ViewData as_view_data(bool sampled = false) const;

    void notify(engine_time_t et) override;

private:
    void copy_target_data_from(const LinkTarget& other);
    void move_target_data_from(LinkTarget&& other) noexcept;
    void clear_target_data();
};

}  // namespace hgraph

