#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>

#include <memory>
#include <unordered_map>

namespace hgraph {

/**
 * Output endpoint owning native TSValue plus optional alternative schemas.
 */
class HGRAPH_EXPORT TSOutput {
public:
    TSOutput() = default;
    TSOutput(const TSMeta* meta, node_ptr owning_node, size_t port_index = 0);

    TSOutput(const TSOutput&) = delete;
    TSOutput& operator=(const TSOutput&) = delete;
    TSOutput(TSOutput&&) noexcept = default;
    TSOutput& operator=(TSOutput&&) noexcept = default;
    ~TSOutput() = default;

    [[nodiscard]] TSView view(engine_time_t current_time);
    [[nodiscard]] TSView view(engine_time_t current_time, const TSMeta* schema);
    [[nodiscard]] TSView view(const engine_time_t* engine_time_ptr);
    [[nodiscard]] TSView view(const engine_time_t* engine_time_ptr, const TSMeta* schema);
    [[nodiscard]] TSOutputView output_view(engine_time_t current_time);
    [[nodiscard]] TSOutputView output_view(engine_time_t current_time, const TSMeta* schema);
    [[nodiscard]] TSOutputView output_view(const engine_time_t* engine_time_ptr);
    [[nodiscard]] TSOutputView output_view(const engine_time_t* engine_time_ptr, const TSMeta* schema);

    [[nodiscard]] node_ptr owning_node() const noexcept { return owning_node_; }
    [[nodiscard]] size_t port_index() const noexcept { return port_index_; }

    [[nodiscard]] const TSMeta* meta() const noexcept { return native_value_.meta(); }

    [[nodiscard]] TSValue& native_value() noexcept { return native_value_; }
    [[nodiscard]] const TSValue& native_value() const noexcept { return native_value_; }

    [[nodiscard]] bool valid() const noexcept { return native_value_.meta() != nullptr; }

    [[nodiscard]] ShortPath root_path() const {
        // Runtime TS paths are schema-local; output port prefix is only for FQ serialization.
        return ShortPath{owning_node_, PortType::OUTPUT, {}};
    }

    [[nodiscard]] FQPath to_fq_path(const TSView& view) const {
        ShortPath fq_path = view.short_path();
        fq_path.node = owning_node_;
        fq_path.port_type = PortType::OUTPUT;
        fq_path.indices.insert(fq_path.indices.begin(), port_index_);

        ViewData root = view.view_data();
        root.path = ShortPath{owning_node_, PortType::OUTPUT, {port_index_}};
        return fq_path.to_fq(root);
    }

private:
    [[nodiscard]] const engine_time_t* owner_engine_time_ptr() const noexcept;
    TSValue& get_or_create_alternative(const TSMeta* schema);
    void establish_default_binding(TSValue& alternative);

    // Must outlive TSValue teardown because LinkTarget/REFLink destructors
    // unregister against this registry.
    std::shared_ptr<TSLinkObserverRegistry> link_observer_registry_{};
    TSValue native_value_;
    std::unordered_map<const TSMeta*, TSValue> alternatives_;
    node_ptr owning_node_{nullptr};
    size_t port_index_{0};
};

}  // namespace hgraph
