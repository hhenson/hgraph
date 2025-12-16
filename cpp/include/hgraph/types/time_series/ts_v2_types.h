//
// Created by Claude on 15/12/2025.
//
// TypeMeta-based Time-Series Types
//
// These are simplified, type-erased time-series types designed to work with
// TypeMeta-based construction. They store all values as nb::object for
// Python interoperability while providing accurate memory sizing for arena allocation.
//

#ifndef HGRAPH_TS_META_TYPES_H
#define HGRAPH_TS_META_TYPES_H

#include <hgraph/api/python/py_schema.h>
#include <hgraph/types/base_time_series.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <vector>

namespace hgraph {

// Forward declarations
struct TimeSeriesTypeMeta;

// ============================================================================
// TsOutput - Simple scalar time-series output
// ============================================================================

struct TsOutput final : BaseTimeSeriesOutput {
    using s_ptr = std::shared_ptr<TsOutput>;

    TsOutput(node_ptr parent, const TSTypeMeta* meta);
    TsOutput(time_series_output_ptr parent, const TSTypeMeta* meta);

    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    void py_set_value(const nb::object& value) override;
    void apply_result(const nb::object& value) override;
    void mark_invalid() override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

    VISITOR_SUPPORT()

private:
    const TSTypeMeta* _meta;
    nb::object _value;
};

// ============================================================================
// TsInput - Simple scalar time-series input
// ============================================================================

struct TsInput final : BaseTimeSeriesInput {
    using s_ptr = std::shared_ptr<TsInput>;

    TsInput(node_ptr parent, const TSTypeMeta* meta);
    TsInput(time_series_input_ptr parent, const TSTypeMeta* meta);

    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

    VISITOR_SUPPORT()

private:
    const TSTypeMeta* _meta;
};

// ============================================================================
// TssOutput - Time-series set output
// ============================================================================

struct TssOutput final : BaseTimeSeriesOutput {
    using s_ptr = std::shared_ptr<TssOutput>;

    TssOutput(node_ptr parent, const TSSTypeMeta* meta);
    TssOutput(time_series_output_ptr parent, const TSSTypeMeta* meta);

    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    void py_set_value(const nb::object& value) override;
    void apply_result(const nb::object& value) override;
    void mark_invalid() override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

    // TSS-specific methods
    [[nodiscard]] nb::object added() const;
    [[nodiscard]] nb::object removed() const;

    VISITOR_SUPPORT()

private:
    const TSSTypeMeta* _meta;
    nb::object _value;      // frozenset - the current value
    nb::object _added;      // frozenset - elements added this tick
    nb::object _removed;    // frozenset - elements removed this tick
};

// ============================================================================
// TssInput - Time-series set input
// ============================================================================

struct TssInput final : BaseTimeSeriesInput {
    using s_ptr = std::shared_ptr<TssInput>;

    TssInput(node_ptr parent, const TSSTypeMeta* meta);
    TssInput(time_series_input_ptr parent, const TSSTypeMeta* meta);

    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;

    // TSS-specific methods (delegate to output)
    [[nodiscard]] nb::object added() const;
    [[nodiscard]] nb::object removed() const;

    VISITOR_SUPPORT()

private:
    const TSSTypeMeta* _meta;
};

// ============================================================================
// TslOutput - Time-series list output (indexed)
// ============================================================================

struct TslOutput final : BaseTimeSeriesOutput {
    using s_ptr = std::shared_ptr<TslOutput>;

    TslOutput(node_ptr parent, const TSLTypeMeta* meta);
    TslOutput(time_series_output_ptr parent, const TSLTypeMeta* meta);

    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    void py_set_value(const nb::object& value) override;
    void apply_result(const nb::object& value) override;
    void mark_invalid() override;
    void invalidate() override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool all_valid() const override;
    [[nodiscard]] bool has_reference() const override;

    // Indexed access
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] TimeSeriesOutput::s_ptr operator[](size_t ndx);

    VISITOR_SUPPORT()

private:
    const TSLTypeMeta* _meta;
    std::vector<TimeSeriesOutput::s_ptr> _elements;
};

// ============================================================================
// TslInput - Time-series list input (indexed)
// ============================================================================

struct TslInput final : BaseTimeSeriesInput {
    using s_ptr = std::shared_ptr<TslInput>;

    TslInput(node_ptr parent, const TSLTypeMeta* meta);
    TslInput(time_series_input_ptr parent, const TSLTypeMeta* meta);

    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    [[nodiscard]] bool bound() const override;
    [[nodiscard]] bool active() const override;
    [[nodiscard]] bool has_reference() const override;
    void make_active() override;
    void make_passive() override;
    [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

    // Indexed access
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](size_t ndx);
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](size_t ndx) const;

    VISITOR_SUPPORT()

protected:
    bool do_bind_output(time_series_output_s_ptr value) override;
    void do_un_bind_output(bool unbind_refs) override;

private:
    const TSLTypeMeta* _meta;
    std::vector<TimeSeriesInput::s_ptr> _elements;
};

// ============================================================================
// TsbOutput - Time-series bundle output
// ============================================================================

struct TsbOutput final : BaseTimeSeriesOutput {
    using s_ptr = std::shared_ptr<TsbOutput>;

    // Collection types (match TimeSeriesBundle interface)
    using key_collection_type = std::vector<c_string_ref>;
    using key_value_collection_type = std::vector<std::pair<c_string_ref, TimeSeriesOutput::s_ptr>>;
    using raw_key_collection_type = std::vector<std::string>;
    using raw_key_const_iterator = raw_key_collection_type::const_iterator;

    TsbOutput(node_ptr parent, const TSBTypeMeta* meta);
    TsbOutput(time_series_output_ptr parent, const TSBTypeMeta* meta);

    [[nodiscard]] nb::object py_value() const override;
    [[nodiscard]] nb::object py_delta_value() const override;
    void py_set_value(const nb::object& value) override;
    void apply_result(const nb::object& value) override;
    void mark_invalid() override;
    void invalidate() override;
    void copy_from_output(const TimeSeriesOutput& output) override;
    void copy_from_input(const TimeSeriesInput& input) override;
    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool all_valid() const override;
    [[nodiscard]] bool has_reference() const override;

    // Named field access (existing)
    [[nodiscard]] size_t size() const;
    [[nodiscard]] TimeSeriesOutput::s_ptr operator[](size_t ndx);
    [[nodiscard]] TimeSeriesOutput::s_ptr operator[](const std::string& name);

    // Bundle interface methods (for PyTimeSeriesBundle compatibility)
    [[nodiscard]] bool empty() const;
    [[nodiscard]] bool contains(const std::string& key) const;
    [[nodiscard]] const PyTimeSeriesSchema& schema() const;
    [[nodiscard]] PyTimeSeriesSchema& schema();
    [[nodiscard]] key_collection_type keys() const;
    [[nodiscard]] key_collection_type valid_keys() const;
    [[nodiscard]] key_collection_type modified_keys() const;
    [[nodiscard]] std::vector<TimeSeriesOutput::s_ptr> values() const;
    [[nodiscard]] std::vector<TimeSeriesOutput::s_ptr> valid_values() const;
    [[nodiscard]] std::vector<TimeSeriesOutput::s_ptr> modified_values() const;
    [[nodiscard]] key_value_collection_type items() const;
    [[nodiscard]] key_value_collection_type valid_items() const;
    [[nodiscard]] key_value_collection_type modified_items() const;
    [[nodiscard]] const std::string& key_from_value(TimeSeriesOutput::s_ptr value) const;
    [[nodiscard]] raw_key_const_iterator begin() const;
    [[nodiscard]] raw_key_const_iterator end() const;

    VISITOR_SUPPORT()

private:
    const TSBTypeMeta* _meta;
    std::vector<TimeSeriesOutput::s_ptr> _fields;
    mutable PyTimeSeriesSchema::ptr _schema;  // Lazily created
    mutable raw_key_collection_type _keys_cache;  // For begin()/end() iteration
};

// ============================================================================
// TsbInput - Time-series bundle input
// ============================================================================

struct TsbInput final : BaseTimeSeriesInput {
    using s_ptr = std::shared_ptr<TsbInput>;

    // Collection types (match TimeSeriesBundle interface)
    using key_collection_type = std::vector<c_string_ref>;
    using key_value_collection_type = std::vector<std::pair<c_string_ref, TimeSeriesInput::s_ptr>>;
    using raw_key_collection_type = std::vector<std::string>;
    using raw_key_const_iterator = raw_key_collection_type::const_iterator;

    TsbInput(node_ptr parent, const TSBTypeMeta* meta);
    TsbInput(time_series_input_ptr parent, const TSBTypeMeta* meta);

    [[nodiscard]] bool is_same_type(const TimeSeriesType* other) const override;
    [[nodiscard]] bool modified() const override;
    [[nodiscard]] bool valid() const override;
    [[nodiscard]] bool all_valid() const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    [[nodiscard]] bool bound() const override;
    [[nodiscard]] bool active() const override;
    [[nodiscard]] bool has_reference() const override;
    void make_active() override;
    void make_passive() override;
    [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override;

    // Named field access (existing)
    [[nodiscard]] size_t size() const;
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](size_t ndx);
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](size_t ndx) const;
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](const std::string& name);
    [[nodiscard]] TimeSeriesInput::s_ptr operator[](const std::string& name) const;

    // Bundle interface methods (for PyTimeSeriesBundle compatibility)
    [[nodiscard]] bool empty() const;
    [[nodiscard]] bool contains(const std::string& key) const;
    [[nodiscard]] const PyTimeSeriesSchema& schema() const;
    [[nodiscard]] PyTimeSeriesSchema& schema();
    [[nodiscard]] key_collection_type keys() const;
    [[nodiscard]] key_collection_type valid_keys() const;
    [[nodiscard]] key_collection_type modified_keys() const;
    [[nodiscard]] std::vector<TimeSeriesInput::s_ptr> values() const;
    [[nodiscard]] std::vector<TimeSeriesInput::s_ptr> valid_values() const;
    [[nodiscard]] std::vector<TimeSeriesInput::s_ptr> modified_values() const;
    [[nodiscard]] key_value_collection_type items() const;
    [[nodiscard]] key_value_collection_type valid_items() const;
    [[nodiscard]] key_value_collection_type modified_items() const;
    [[nodiscard]] const std::string& key_from_value(TimeSeriesInput::s_ptr value) const;
    [[nodiscard]] raw_key_const_iterator begin() const;
    [[nodiscard]] raw_key_const_iterator end() const;

    VISITOR_SUPPORT()

protected:
    bool do_bind_output(time_series_output_s_ptr value) override;
    void do_un_bind_output(bool unbind_refs) override;

private:
    const TSBTypeMeta* _meta;
    std::vector<TimeSeriesInput::s_ptr> _fields;
    mutable PyTimeSeriesSchema::ptr _schema;  // Lazily created
    mutable raw_key_collection_type _keys_cache;  // For begin()/end() iteration
};

} // namespace hgraph

#endif // HGRAPH_TS_META_TYPES_H
