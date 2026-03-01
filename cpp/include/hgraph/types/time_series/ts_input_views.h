#pragma once

/**
 * @file ts_input_views.h
 * @brief Input-owned time-series view wrappers.
 *
 * TSInputView and its kind-specific subclasses provide input-context
 * path conversion and subscription control for TSInput endpoints.
 */

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

#include <nanobind/nanobind.h>

#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace hgraph {

class TSInput;
class TSOutputView;

class TSWInputView;
class TSSInputView;
class TSDInputView;
class TSIndexedInputView;
class TSLInputView;
class TSBInputView;

/**
 * Input-owned TS cursor wrapper with owner-context path conversion.
 */
class HGRAPH_EXPORT TSInputView {
public:
    TSInputView() = default;
    TSInputView(TSInput* owner, TSView ts_view) noexcept
        : owner_(owner), ts_view_(std::move(ts_view)) {}

    explicit operator bool() const noexcept { return static_cast<bool>(ts_view_); }

    [[nodiscard]] const TSView& as_ts_view() const noexcept { return ts_view_; }
    [[nodiscard]] TSView& as_ts_view() noexcept { return ts_view_; }
    [[nodiscard]] ShortPath short_path() const;

    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return ts_view_.ts_meta(); }
    [[nodiscard]] engine_time_t current_time() const noexcept { return ts_view_.current_time(); }
    void set_current_time_ptr(const engine_time_t* engine_time_ptr) noexcept { ts_view_.set_current_time_ptr(engine_time_ptr); }

    [[nodiscard]] FQPath fq_path() const;

    [[nodiscard]] engine_time_t last_modified_time() const { return ts_view_.last_modified_time(); }
    [[nodiscard]] bool modified() const { return ts_view_.modified(); }
    [[nodiscard]] bool valid() const { return ts_view_.valid(); }
    [[nodiscard]] bool all_valid() const { return ts_view_.all_valid(); }
    [[nodiscard]] bool has_delta() const { return ts_view_.has_delta(); }
    [[nodiscard]] bool sampled() const { return ts_view_.sampled(); }

    [[nodiscard]] value::View value() const { return ts_view_.value(); }
    [[nodiscard]] value::View delta_value() const { return ts_view_.delta_value(); }
    [[nodiscard]] DeltaView delta_view() const { return ts_view_.delta_view(); }
    [[nodiscard]] nb::object to_python() const { return ts_view_.to_python(); }
    [[nodiscard]] nb::object delta_to_python() const { return ts_view_.delta_to_python(); }

    void bind(const TSOutputView& target);
    void unbind();
    [[nodiscard]] bool is_bound() const;
    void make_active();
    void make_passive();
    [[nodiscard]] bool active() const;

    [[nodiscard]] TSInputView field(std::string_view name) const;
    [[nodiscard]] TSInputView at(size_t index) const;
    [[nodiscard]] TSInputView at_key(const value::View& key) const;
    [[nodiscard]] TSInputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] TSInputView operator[](const value::View& key) const { return at_key(key); }

    [[nodiscard]] std::optional<TSWInputView> try_as_window() const;
    [[nodiscard]] std::optional<TSSInputView> try_as_set() const;
    [[nodiscard]] std::optional<TSDInputView> try_as_dict() const;
    [[nodiscard]] std::optional<TSLInputView> try_as_list() const;
    [[nodiscard]] std::optional<TSBInputView> try_as_bundle() const;

    [[nodiscard]] TSWInputView as_window() const;
    [[nodiscard]] TSSInputView as_set() const;
    [[nodiscard]] TSDInputView as_dict() const;
    [[nodiscard]] TSLInputView as_list() const;
    [[nodiscard]] TSBInputView as_bundle() const;

protected:
    TSInput* owner_{nullptr};
    TSView ts_view_{};
};

class HGRAPH_EXPORT TSWInputView : public TSInputView {
public:
    TSWInputView() = default;
    explicit TSWInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}

    [[nodiscard]] const engine_time_t* value_times() const;
    [[nodiscard]] size_t value_times_count() const;
    [[nodiscard]] engine_time_t first_modified_time() const;
    [[nodiscard]] bool has_removed_value() const;
    [[nodiscard]] size_t removed_value_count() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t min_size() const;
    [[nodiscard]] size_t length() const;
};

class HGRAPH_EXPORT TSSInputView : public TSInputView {
public:
    TSSInputView() = default;
    explicit TSSInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}
};

class HGRAPH_EXPORT TSDInputView : public TSInputView {
public:
    using item_type = std::pair<value::Value, TSInputView>;

    TSDInputView() = default;
    explicit TSDInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}

    [[nodiscard]] bool contains(const value::View& key) const;
    [[nodiscard]] std::vector<value::Value> keys() const;
    [[nodiscard]] std::vector<value::Value> valid_keys() const;
    [[nodiscard]] std::vector<value::Value> modified_keys() const;
    [[nodiscard]] std::vector<value::Value> added_keys() const;
    [[nodiscard]] std::vector<value::Value> removed_keys() const;
    [[nodiscard]] bool has_added() const;
    [[nodiscard]] bool has_removed() const;
    [[nodiscard]] bool was_added(const value::View& key) const;
    [[nodiscard]] bool was_removed(const value::View& key) const;
    [[nodiscard]] bool was_modified(const value::View& key) const;
    [[nodiscard]] std::vector<TSInputView> values() const;
    [[nodiscard]] std::vector<TSInputView> valid_values() const;
    [[nodiscard]] std::vector<TSInputView> modified_values() const;
    [[nodiscard]] std::vector<TSInputView> added_values() const;
    [[nodiscard]] std::vector<TSInputView> removed_values() const;
    [[nodiscard]] std::vector<item_type> items() const;
    [[nodiscard]] std::vector<item_type> valid_items() const;
    [[nodiscard]] std::vector<item_type> modified_items() const;
    [[nodiscard]] std::vector<item_type> added_items() const;
    [[nodiscard]] std::vector<item_type> removed_items() const;
    [[nodiscard]] TSInputView at_key(const value::View& key) const;
    [[nodiscard]] TSInputView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] std::optional<value::Value> key_at_slot(size_t slot) const;
    [[nodiscard]] std::optional<value::Value> key_for_child(const TSInputView& child) const;
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSIndexedInputView : public TSInputView {
public:
    TSIndexedInputView() = default;
    explicit TSIndexedInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}
    [[nodiscard]] TSInputView at(size_t index) const;
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSLInputView : public TSIndexedInputView {
public:
    TSLInputView() = default;
    explicit TSLInputView(TSInputView base) noexcept : TSIndexedInputView(std::move(base)) {}
    [[nodiscard]] TSInputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] std::vector<size_t> indices() const;
    [[nodiscard]] std::vector<size_t> valid_indices() const;
    [[nodiscard]] std::vector<size_t> modified_indices() const;
};

class HGRAPH_EXPORT TSBInputView : public TSIndexedInputView {
public:
    using item_type = std::pair<std::string_view, TSInputView>;

    TSBInputView() = default;
    explicit TSBInputView(TSInputView base) noexcept : TSIndexedInputView(std::move(base)) {}

    using TSIndexedInputView::at;
    [[nodiscard]] TSInputView field(std::string_view name) const;
    [[nodiscard]] TSInputView at(std::string_view name) const { return field(name); }
    [[nodiscard]] std::optional<size_t> index_of(std::string_view name) const;
    [[nodiscard]] std::string_view name_at(size_t index) const;
    [[nodiscard]] std::optional<std::string_view> name_for_child(const TSInputView& child) const;
    [[nodiscard]] bool contains(std::string_view name) const;
    [[nodiscard]] nb::list keys() const;
    [[nodiscard]] nb::list valid_keys() const;
    [[nodiscard]] nb::list modified_keys() const;
    [[nodiscard]] std::vector<TSInputView> values() const;
    [[nodiscard]] std::vector<TSInputView> valid_values() const;
    [[nodiscard]] std::vector<TSInputView> modified_values() const;
    [[nodiscard]] std::vector<item_type> items() const;
    [[nodiscard]] std::vector<item_type> valid_items() const;
    [[nodiscard]] std::vector<item_type> modified_items() const;
    [[nodiscard]] std::vector<size_t> indices() const;
    [[nodiscard]] std::vector<size_t> valid_indices() const;
    [[nodiscard]] std::vector<size_t> modified_indices() const;
};

}  // namespace hgraph
