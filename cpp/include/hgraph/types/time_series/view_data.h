#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace hgraph {

struct ts_ops;
struct LinkTarget;
struct REFLink;
struct TSLinkObserverRegistry;
struct ViewData;

/**
 * Port side for path ownership.
 */
enum class PortType : uint8_t {
    INPUT = 0,
    OUTPUT = 1,
};

/**
 * Optional view projection mode used by runtime wiring.
 */
enum class ViewProjection : uint8_t {
    NONE = 0,
    TSD_KEY_SET = 1,
};

/**
 * Fully-qualified path element.
 */
struct FQPathElement {
    using element_type = std::variant<std::string, size_t>;

    element_type element{};

    static FQPathElement field(std::string name) {
        return FQPathElement{std::move(name)};
    }

    static FQPathElement index(size_t index) {
        return FQPathElement{index};
    }

    [[nodiscard]] std::string to_string() const;
};

/**
 * Serialized path representation.
 */
struct FQPath {
    uint64_t node_id{0};
    PortType port_type{PortType::OUTPUT};
    std::vector<FQPathElement> path;

    [[nodiscard]] std::string to_string() const;
};

/**
 * Runtime path representation.
 */
struct ShortPath {
    node_ptr node{nullptr};
    PortType port_type{PortType::OUTPUT};
    std::vector<size_t> indices;

    [[nodiscard]] ShortPath child(size_t index) const;
    [[nodiscard]] FQPath to_fq() const;
    [[nodiscard]] FQPath to_fq(const ViewData& root) const;
    [[nodiscard]] std::string to_string() const;
};

/**
 * Shared data contract for TS views.
 *
 * This matches the locked full contract: path + five storage pointers + flags + ops/meta.
 */
struct ViewData {
    ShortPath path;
    const engine_time_t* engine_time_ptr{nullptr};

    void* value_data{nullptr};
    void* time_data{nullptr};
    void* observer_data{nullptr};
    void* delta_data{nullptr};
    void* link_data{nullptr};
    TSLinkObserverRegistry* link_observer_registry{nullptr};

    bool sampled{false};
    bool uses_link_target{false};
    ViewProjection projection{ViewProjection::NONE};

    const ts_ops* ops{nullptr};
    const TSMeta* meta{nullptr};

    [[nodiscard]] bool is_null() const {
        return value_data == nullptr && time_data == nullptr && observer_data == nullptr &&
               delta_data == nullptr && link_data == nullptr;
    }

    [[nodiscard]] LinkTarget* as_link_target() const;
    [[nodiscard]] REFLink* as_ref_link() const;
};

/**
 * Debug-only consistency checks for discriminator and payload assumptions.
 */
HGRAPH_EXPORT void debug_assert_view_data_consistency(const ViewData& vd);

}  // namespace hgraph
