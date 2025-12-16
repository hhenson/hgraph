//
// TsbInput - Bundle Input for V2 Architecture
//
// TimeSeriesBundleInput implementation using the V2 TSValue shared state model.
// Contains named child inputs, each following the V2 pattern.
//

#ifndef HGRAPH_TSB_INPUT_V2_H
#define HGRAPH_TSB_INPUT_V2_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/v2/ts_context.h>
#include <hgraph/api/python/py_schema.h>
#include <unordered_map>
#include <vector>
#include <functional>
#include <ranges>

namespace hgraph {
namespace ts {

/**
 * TsbInput - Bundle input for V2 architecture.
 *
 * A collection of named time-series inputs (like a struct/schema).
 * Each child input uses the V2 TSValue shared state model.
 *
 * Key differences from V1:
 * - Children are created via TSTypeMeta::make_input()
 * - Uses TSContext for navigation
 * - No BaseTimeSeriesInput inheritance chain
 */
struct TsbInput final : TimeSeriesInput {
    using s_ptr = std::shared_ptr<TsbInput>;
    using key_type = std::string;
    using child_ptr = time_series_input_s_ptr;
    using collection_type = std::vector<child_ptr>;
    using key_collection_type = std::vector<std::reference_wrapper<const std::string>>;

    // Construction with node owner
    TsbInput(node_ptr parent, TimeSeriesSchema* schema);

    // Construction with parent time-series owner
    TsbInput(TimeSeriesInput* parent, TimeSeriesSchema* schema);

    // Destructor
    ~TsbInput() override;

    // === TimeSeriesType interface (delegates to context) ===
    [[nodiscard]] node_ptr owning_node() override { return _ctx.owning_node(); }
    [[nodiscard]] node_ptr owning_node() const override { return _ctx.owning_node(); }
    [[nodiscard]] graph_ptr owning_graph() override { return _ctx.owning_graph(); }
    [[nodiscard]] graph_ptr owning_graph() const override { return _ctx.owning_graph(); }
    [[nodiscard]] bool has_parent_or_node() const override { return _ctx.has_owner(); }
    [[nodiscard]] bool has_owning_node() const override { return owning_node() != nullptr; }

    // === Value access (aggregates from children) ===
    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;

    // === Re-parenting ===
    void re_parent(node_ptr parent) override { _ctx.re_parent(parent); }
    void re_parent(time_series_type_ptr parent) override { _ctx.re_parent(parent); }
    void reset_parent_or_node() override { _ctx.reset(); }
    void builder_release_cleanup() override;

    // === Type checking ===
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool is_reference() const override { return false; }
    [[nodiscard]] bool has_reference() const override;

    // === TimeSeriesInput interface ===
    [[nodiscard]] TimeSeriesInput::s_ptr parent_input() const override;
    [[nodiscard]] bool has_parent_input() const override { return _ctx.is_parent_owner(); }

    [[nodiscard]] bool active() const override { return _active; }
    void make_active() override;
    void make_passive() override;

    [[nodiscard]] bool bound() const override;
    [[nodiscard]] bool has_peer() const override;
    [[nodiscard]] time_series_output_s_ptr output() const override { return _bound_output; }
    [[nodiscard]] bool has_output() const override { return _bound_output != nullptr; }
    bool bind_output(time_series_output_s_ptr output_) override;
    void un_bind_output(bool unbind_refs) override;

    [[nodiscard]] time_series_reference_output_s_ptr reference_output() const override { return nullptr; }
    [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

    // === Notifiable interface ===
    void notify(engine_time_t modified_time) override;

    // === Bundle-specific interface ===
    [[nodiscard]] size_t size() const { return _children.size(); }
    [[nodiscard]] child_ptr& operator[](size_t index);
    [[nodiscard]] const child_ptr& operator[](size_t index) const;
    [[nodiscard]] child_ptr& operator[](const std::string& key);
    [[nodiscard]] const child_ptr& operator[](const std::string& key) const;
    [[nodiscard]] bool contains(const std::string& key) const;

    [[nodiscard]] const TimeSeriesSchema& schema() const { return *_schema; }
    [[nodiscard]] key_collection_type keys() const;
    [[nodiscard]] key_collection_type valid_keys() const;
    [[nodiscard]] key_collection_type modified_keys() const;
    [[nodiscard]] const collection_type& values() const { return _children; }

    // Provide items() for iteration over key-value pairs
    // Returns a generator/view that yields (key, child) pairs
    [[nodiscard]] auto items() const {
        return std::views::transform(
            std::views::iota(size_t{0}, _children.size()),
            [this](size_t i) {
                return std::make_pair(std::cref(_schema->keys()[i]), std::cref(_children[i]));
            }
        );
    }

    // Set children (called by builder)
    void set_children(collection_type children);

    // Copy with new parent (used by nested graph infrastructure)
    s_ptr copy_with(node_ptr parent, collection_type children);

    VISITOR_SUPPORT()

private:
    TSContext _ctx;
    TimeSeriesSchema* _schema;
    collection_type _children;
    std::unordered_map<std::string, size_t> _key_to_index;
    time_series_output_s_ptr _bound_output;  // Keep bound output alive
    bool _active{false};
    engine_time_t _sample_time{MIN_DT};
};

} // namespace ts
} // namespace hgraph

#endif // HGRAPH_TSB_INPUT_V2_H
