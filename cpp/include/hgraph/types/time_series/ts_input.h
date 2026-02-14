#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph {

class TSOutput;

/**
 * Input endpoint owning TSValue with input-link schema and active-state tree.
 */
class HGRAPH_EXPORT TSInput : public Notifiable {
public:
    TSInput() = default;
    TSInput(const TSMeta* meta, node_ptr owning_node);

    TSInput(const TSInput&) = delete;
    TSInput& operator=(const TSInput&) = delete;
    TSInput(TSInput&&) noexcept = default;
    TSInput& operator=(TSInput&&) noexcept = default;
    ~TSInput() override = default;

    [[nodiscard]] TSView view(engine_time_t current_time);
    [[nodiscard]] TSView view(engine_time_t current_time, const TSMeta* schema);
    [[nodiscard]] TSInputView input_view(engine_time_t current_time);
    [[nodiscard]] TSInputView input_view(engine_time_t current_time, const TSMeta* schema);

    void bind(TSOutput& output, engine_time_t current_time);
    void unbind(engine_time_t current_time);

    void set_active(bool active);
    [[nodiscard]] bool active() const noexcept { return active_root_; }
    void set_active(const TSView& ts_view, bool active);
    [[nodiscard]] bool active(const TSView& ts_view) const;

    [[nodiscard]] value::View active_view() const;
    [[nodiscard]] value::ValueView active_view_mut();

    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }
    [[nodiscard]] node_ptr owning_node() const noexcept { return owning_node_; }

    [[nodiscard]] TSValue& value() noexcept { return value_; }
    [[nodiscard]] const TSValue& value() const noexcept { return value_; }

    [[nodiscard]] ShortPath root_path() const {
        return ShortPath{owning_node_, PortType::INPUT, {}};
    }

    [[nodiscard]] FQPath to_fq_path(const TSView& view) const {
        ViewData root = view.view_data();
        root.path = root_path();
        return view.short_path().to_fq(root);
    }

    void notify(engine_time_t et) override;

private:
    void set_active_recursive(const TSView& ts_view, value::ValueView active_view, bool active);

    TSValue value_;
    value::Value active_;
    const TSMeta* meta_{nullptr};
    node_ptr owning_node_{nullptr};
    bool active_root_{false};
};

}  // namespace hgraph
