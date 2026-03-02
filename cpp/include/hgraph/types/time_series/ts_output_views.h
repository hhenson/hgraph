#pragma once

/**
 * @file ts_output_views.h
 * @brief Output-owned time-series view wrappers.
 *
 * TSOutputView and its kind-specific subclasses provide output-context
 * path conversion and owner-aware navigation for TSOutput endpoints.
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

class TSOutput;
class TSInputView;

class TSWOutputView;
class TSSOutputView;
class TSDOutputView;
class TSIndexedOutputView;
class TSLOutputView;
class TSBOutputView;

/**
 * Output-owned TS cursor wrapper with owner-context path conversion.
 */
class HGRAPH_EXPORT TSOutputView {
public:
    TSOutputView() = default;
    TSOutputView(TSOutput* owner, TSView ts_view) noexcept
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
    [[nodiscard]] nb::object to_python() const;
    [[nodiscard]] nb::object delta_to_python() const;

    void set_value(const value::View& src);
    void from_python(const nb::object& src);
    void copy_from_input(const TSInputView& input);
    void copy_from_output(const TSOutputView& output);
    void apply_delta(const value::View& delta);
    void apply_delta(const DeltaView& delta);
    void invalidate();

    [[nodiscard]] TSOutputView field(std::string_view name) const;
    [[nodiscard]] TSOutputView at(size_t index) const;
    [[nodiscard]] TSOutputView at_key(const value::View& key) const;
    [[nodiscard]] TSOutputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] TSOutputView operator[](const value::View& key) const { return at_key(key); }

    [[nodiscard]] std::optional<TSWOutputView> try_as_window() const;
    [[nodiscard]] std::optional<TSSOutputView> try_as_set() const;
    [[nodiscard]] std::optional<TSDOutputView> try_as_dict() const;
    [[nodiscard]] std::optional<TSLOutputView> try_as_list() const;
    [[nodiscard]] std::optional<TSBOutputView> try_as_bundle() const;

    [[nodiscard]] TSWOutputView as_window() const;
    [[nodiscard]] TSSOutputView as_set() const;
    [[nodiscard]] TSDOutputView as_dict() const;
    [[nodiscard]] TSLOutputView as_list() const;
    [[nodiscard]] TSBOutputView as_bundle() const;

protected:
    TSOutput* owner_{nullptr};
    TSView ts_view_{};
};

class HGRAPH_EXPORT TSWOutputView : public TSOutputView {
public:
    TSWOutputView() = default;
    explicit TSWOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] const engine_time_t* value_times() const;
    [[nodiscard]] size_t value_times_count() const;
    [[nodiscard]] engine_time_t first_modified_time() const;
    [[nodiscard]] bool has_removed_value() const;
    [[nodiscard]] value::View removed_value() const;
    [[nodiscard]] size_t removed_value_count() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t min_size() const;
    [[nodiscard]] size_t length() const;
};

class HGRAPH_EXPORT TSSOutputView : public TSOutputView {
public:
    TSSOutputView() = default;
    explicit TSSOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] bool contains(const value::View& elem) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] std::vector<value::View> values() const;
    [[nodiscard]] std::vector<value::View> added() const;
    [[nodiscard]] std::vector<value::View> removed() const;
    bool add(const value::View& elem);
    bool remove(const value::View& elem);
    void clear();
};

class HGRAPH_EXPORT TSDOutputView : public TSOutputView {
public:
    using item_type = std::pair<value::Value, TSOutputView>;

    TSDOutputView() = default;
    explicit TSDOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] TSSOutputView key_set() const;
    [[nodiscard]] TSOutputView get(const value::View& key) const;
    [[nodiscard]] TSOutputView get(const value::View& key, TSOutputView default_value) const;
    [[nodiscard]] TSOutputView get_or_create(const value::View& key);
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
    [[nodiscard]] std::vector<TSOutputView> values() const;
    [[nodiscard]] std::vector<TSOutputView> valid_values() const;
    [[nodiscard]] std::vector<TSOutputView> modified_values() const;
    [[nodiscard]] std::vector<TSOutputView> added_values() const;
    [[nodiscard]] std::vector<TSOutputView> removed_values() const;
    [[nodiscard]] std::vector<item_type> items() const;
    [[nodiscard]] std::vector<item_type> valid_items() const;
    [[nodiscard]] std::vector<item_type> modified_items() const;
    [[nodiscard]] std::vector<item_type> added_items() const;
    [[nodiscard]] std::vector<item_type> removed_items() const;
    [[nodiscard]] TSOutputView at_key(const value::View& key) const;
    [[nodiscard]] TSOutputView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] std::optional<value::Value> key_at_slot(size_t slot) const;
    [[nodiscard]] std::optional<value::Value> key_for_child(const TSOutputView& child) const;
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }

    bool remove(const value::View& key);
    [[nodiscard]] TSOutputView create(const value::View& key);
    [[nodiscard]] TSOutputView set(const value::View& key, const value::View& value);
};

class HGRAPH_EXPORT TSIndexedOutputView : public TSOutputView {
public:
    TSIndexedOutputView() = default;
    explicit TSIndexedOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}
    [[nodiscard]] TSOutputView at(size_t index) const;
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSLOutputView : public TSIndexedOutputView {
public:
    using item_type = std::pair<size_t, TSOutputView>;

    TSLOutputView() = default;
    explicit TSLOutputView(TSOutputView base) noexcept : TSIndexedOutputView(std::move(base)) {}
    [[nodiscard]] TSOutputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] std::vector<size_t> keys() const;
    [[nodiscard]] std::vector<size_t> valid_keys() const;
    [[nodiscard]] std::vector<size_t> modified_keys() const;
    [[nodiscard]] std::vector<TSOutputView> values() const;
    [[nodiscard]] std::vector<TSOutputView> valid_values() const;
    [[nodiscard]] std::vector<TSOutputView> modified_values() const;
    [[nodiscard]] std::vector<item_type> items() const;
    [[nodiscard]] std::vector<item_type> valid_items() const;
    [[nodiscard]] std::vector<item_type> modified_items() const;
    [[nodiscard]] std::vector<size_t> indices() const;
    [[nodiscard]] std::vector<size_t> valid_indices() const;
    [[nodiscard]] std::vector<size_t> modified_indices() const;
};

class HGRAPH_EXPORT TSBOutputView : public TSIndexedOutputView {
public:
    using item_type = std::pair<std::string_view, TSOutputView>;

    TSBOutputView() = default;
    explicit TSBOutputView(TSOutputView base) noexcept : TSIndexedOutputView(std::move(base)) {}

    using TSIndexedOutputView::at;
    [[nodiscard]] TSOutputView field(std::string_view name) const;
    [[nodiscard]] TSOutputView at(std::string_view name) const { return field(name); }
    [[nodiscard]] std::optional<size_t> index_of(std::string_view name) const;
    [[nodiscard]] std::string_view name_at(size_t index) const;
    [[nodiscard]] std::optional<std::string_view> name_for_child(const TSOutputView& child) const;
    [[nodiscard]] bool contains(std::string_view name) const;
    [[nodiscard]] nb::list keys() const;
    [[nodiscard]] nb::list valid_keys() const;
    [[nodiscard]] nb::list modified_keys() const;
    [[nodiscard]] std::vector<TSOutputView> values() const;
    [[nodiscard]] std::vector<TSOutputView> valid_values() const;
    [[nodiscard]] std::vector<TSOutputView> modified_values() const;
    [[nodiscard]] std::vector<item_type> items() const;
    [[nodiscard]] std::vector<item_type> valid_items() const;
    [[nodiscard]] std::vector<item_type> modified_items() const;
    [[nodiscard]] std::vector<size_t> indices() const;
    [[nodiscard]] std::vector<size_t> valid_indices() const;
    [[nodiscard]] std::vector<size_t> modified_indices() const;
};

}  // namespace hgraph
