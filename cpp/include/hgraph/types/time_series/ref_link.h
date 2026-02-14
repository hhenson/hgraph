#pragma once

#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/view_data.h>

namespace hgraph {

/**
 * REFLink is the output/alternative link payload that can track REF source changes.
 *
 * This scaffolding implementation supports bind/unbind semantics and target rebinding hooks.
 */
struct HGRAPH_EXPORT REFLink : Notifiable {
    bool is_linked{false};

    ViewData source{};
    ViewData target{};

    Notifiable* active_notifier{nullptr};
    engine_time_t last_rebind_time{MIN_DT};

    REFLink() = default;
    ~REFLink() override = default;

    REFLink(const REFLink& other);
    REFLink& operator=(const REFLink& other);
    REFLink(REFLink&& other) noexcept;
    REFLink& operator=(REFLink&& other) noexcept;

    void bind(const ViewData& source_view);
    void bind_to_ref(const ViewData& source_view);
    void bind_target(const ViewData& target_view);
    void unbind();

    [[nodiscard]] bool has_target() const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool modified(engine_time_t current_time) const;
    [[nodiscard]] ViewData resolved_view_data() const;

    void notify(engine_time_t et) override;
};

}  // namespace hgraph
