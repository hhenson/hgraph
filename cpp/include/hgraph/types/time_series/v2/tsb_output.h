//
// TsbOutput - Bundle Output for V2 Architecture
//
// TimeSeriesBundleOutput implementation using the V2 TSValue shared state model.
// Contains named child outputs, each following the V2 pattern.
//

#ifndef HGRAPH_TSB_OUTPUT_V2_H
#define HGRAPH_TSB_OUTPUT_V2_H

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/time_series/v2/ts_context.h>
#include <hgraph/api/python/py_schema.h>
#include <unordered_map>
#include <vector>
#include <functional>

namespace hgraph {
namespace ts {

/**
 * TsbOutput - Bundle output for V2 architecture.
 *
 * A collection of named time-series outputs (like a struct/schema).
 * Each child output uses the V2 TSValue shared state model.
 *
 * Key differences from V1:
 * - Children are created via TSTypeMeta::make_output()
 * - Uses TSContext for navigation
 * - No BaseTimeSeriesOutput inheritance chain
 */
struct TsbOutput final : TimeSeriesOutput {
    using s_ptr = std::shared_ptr<TsbOutput>;
    using key_type = std::string;
    using child_ptr = time_series_output_s_ptr;
    using collection_type = std::vector<child_ptr>;
    using key_collection_type = std::vector<std::reference_wrapper<const std::string>>;

    // Construction with node owner
    TsbOutput(node_ptr parent, TimeSeriesSchema* schema);

    // Construction with parent time-series owner
    TsbOutput(TimeSeriesOutput* parent, TimeSeriesSchema* schema);

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

    // === TimeSeriesOutput interface ===
    [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() const override;
    [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() override;
    [[nodiscard]] bool has_parent_output() const override { return _ctx.is_parent_owner(); }

    void subscribe(Notifiable* n) override;
    void un_subscribe(Notifiable* n) override;

    // === Mutation ===
    void apply_result(const nb::object& value) override;
    void py_set_value(const nb::object& value) override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;

    void clear() override;
    void invalidate() override;
    void mark_invalid() override;
    void mark_modified() override;
    void mark_modified(engine_time_t modified_time) override;
    void mark_child_modified(TimeSeriesOutput& child, engine_time_t modified_time) override;
    bool can_apply_result(const nb::object& value) override;

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

    // Set children (called by builder)
    void set_children(collection_type children);

    VISITOR_SUPPORT()

private:
    TSContext _ctx;
    TimeSeriesSchema* _schema;
    collection_type _children;
    std::unordered_map<std::string, size_t> _key_to_index;
    std::unordered_set<Notifiable*> _subscribers;
    engine_time_t _last_modified{MIN_DT};
};

} // namespace ts
} // namespace hgraph

#endif // HGRAPH_TSB_OUTPUT_V2_H
