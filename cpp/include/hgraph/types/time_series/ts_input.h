#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>

#include <memory>
#include <vector>

namespace hgraph {

class TSOutput;

/**
 * Input endpoint owning TSValue with input-link schema and active-state tree.
 */
class HGRAPH_EXPORT TSInput : public Notifiable {
public:
    TSInput();
    TSInput(const TSMeta* meta, node_ptr owning_node, size_t port_index = 0);

    TSInput(const TSInput&) = delete;
    TSInput& operator=(const TSInput&) = delete;
    TSInput(TSInput&&) noexcept;
    TSInput& operator=(TSInput&&) noexcept;
    ~TSInput() override;

    [[nodiscard]] TSView view();
    [[nodiscard]] TSInputView input_view();

    void bind(TSOutput& output);
    void unbind();

    void set_active(bool active);
    [[nodiscard]] bool active() const noexcept { return active_root_; }
    void set_active(const TSView& ts_view, bool active);
    [[nodiscard]] bool active(const TSView& ts_view) const;

    [[nodiscard]] value::View active_view() const;
    [[nodiscard]] value::ValueView active_view_mut();

    [[nodiscard]] const TSMeta* meta() const noexcept { return meta_; }
    [[nodiscard]] node_ptr owning_node() const noexcept { return owning_node_; }
    [[nodiscard]] size_t port_index() const noexcept { return port_index_; }

    [[nodiscard]] TSValue& value() noexcept { return value_; }
    [[nodiscard]] const TSValue& value() const noexcept { return value_; }

    void set_signal_input_impl_flags(std::vector<bool> flags);
    [[nodiscard]] bool signal_input_has_impl(const std::vector<size_t>& path_indices) const;

    [[nodiscard]] ShortPath root_path() const {
        return ShortPath{owning_node_, PortType::INPUT, {}};
    }

    [[nodiscard]] ShortPath endpoint_root_path() const {
        if (port_index_ == 0) {
            return root_path();
        }
        return ShortPath{owning_node_, PortType::INPUT, {port_index_}};
    }

    [[nodiscard]] ShortPath to_short_path(const TSView& view) const {
        ShortPath out = view.short_path();
        out.node = owning_node_;
        out.port_type = PortType::INPUT;
        if (port_index_ != 0 && (out.indices.empty() || out.indices.front() != port_index_)) {
            out.indices.insert(out.indices.begin(), port_index_);
        }
        return out;
    }

    [[nodiscard]] FQPath to_fq_path(const TSView& view) const {
        ShortPath fq_path = to_short_path(view);
        ViewData root = view.view_data();
        root.path = endpoint_root_path();
        return fq_path.to_fq(root);
    }

    void notify(engine_time_t et) override;

private:
    [[nodiscard]] const engine_time_t* owner_engine_time_ptr() const noexcept;
    void set_active_recursive(const TSView& ts_view, value::ValueView active_view, bool active);

    // Must outlive TSValue teardown because LinkTarget/REFLink destructors
    // unregister against this registry.
    std::shared_ptr<TSLinkObserverRegistry> link_observer_registry_{};
    TSValue value_;
    value::Value active_;
    std::vector<bool> signal_input_impl_flags_;
    const TSMeta* meta_{nullptr};
    node_ptr owning_node_{nullptr};
    size_t port_index_{0};
    bool active_root_{false};
};

HGRAPH_EXPORT bool is_live_ts_input(const TSInput* input) noexcept;

}  // namespace hgraph
