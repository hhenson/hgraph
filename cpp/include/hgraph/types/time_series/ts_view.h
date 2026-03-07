#pragma once

/**
 * @file ts_view.h
 * @brief Base time-series view classes.
 *
 * TSView is the non-owning time-series cursor (ViewData + engine-time reference).
 * Kind-specific subclasses (TSWView, TSSView, TSDView, TSIndexedView, TSLView, TSBView)
 * provide convenient typed access.
 *
 * Output and input view wrappers are in separate headers:
 * - ts_output_views.h: TSOutputView hierarchy
 * - ts_input_views.h:  TSInputView hierarchy
 */

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/delta_view.h>
#include <hgraph/types/time_series/ts_iterable.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value_view.h>

#include <nanobind/nanobind.h>

#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace nb = nanobind;

namespace hgraph {

class TSInput;
class TSOutput;
class TSValue;
class TSWView;
class TSSView;
class TSDView;
class TSIndexedView;
class TSLView;
class TSBView;

/**
 * Non-owning time-series cursor (ViewData + engine-time reference).
 */
class HGRAPH_EXPORT TSView {
public:
    TSView() = default;
    TSView(ViewData view_data, const engine_time_t* engine_time_ptr) noexcept;

    TSView(const TSValue& value, const engine_time_t* engine_time_ptr, ShortPath path = {});

    explicit operator bool() const noexcept {
        return view_data_.meta != nullptr && view_data_.ops != nullptr;
    }

    [[nodiscard]] const TSMeta* ts_meta() const noexcept;

    [[nodiscard]] engine_time_t current_time() const noexcept {
        return view_data_.engine_time_ptr != nullptr ? *view_data_.engine_time_ptr : MIN_DT;
    }
    void set_current_time_ptr(const engine_time_t* engine_time_ptr) noexcept {
        view_data_.engine_time_ptr = engine_time_ptr;
    }
    [[nodiscard]] DeltaSemantics delta_semantics() const noexcept { return view_data_.delta_semantics; }
    void set_delta_semantics(DeltaSemantics semantics) noexcept { view_data_.delta_semantics = semantics; }

    [[nodiscard]] const ShortPath& short_path() const noexcept { return view_data_.path; }
    [[nodiscard]] FQPath fq_path() const { return view_data_.path.to_fq(); }

    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool modified() const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool all_valid() const;
    [[nodiscard]] bool has_delta() const;
    [[nodiscard]] bool sampled() const;

    [[nodiscard]] value::View value() const;
    // Unified delta accessor for authoring APIs.
    [[nodiscard]] DeltaView delta_value() const;
    // Raw underlying delta payload.
    [[nodiscard]] value::View delta_payload() const;
    [[nodiscard]] nb::object to_python() const;
    [[nodiscard]] nb::object delta_to_python() const;

    void set_value(const value::View& src);
    template<typename T>
    void set_value(const T& src) {
        using U = std::remove_cvref_t<T>;
        set_value(value::View(&src, value::scalar_type_meta<U>()));
    }
    void from_python(const nb::object& src);
    void apply_delta(const value::View& delta);
    void apply_delta(const DeltaView& delta);
    void invalidate();

    [[nodiscard]] TSView child_at(size_t index) const;
    [[nodiscard]] TSView child_by_name(std::string_view name) const;
    [[nodiscard]] TSView child_by_key(const value::View& key) const;
    [[nodiscard]] std::optional<size_t> child_slot_for(const TSView& child) const;
    [[nodiscard]] size_t child_count() const;
    [[nodiscard]] size_t size() const { return child_count(); }

    [[nodiscard]] TSKind kind() const noexcept { return ts_meta() != nullptr ? ts_meta()->kind : TSKind::SIGNAL; }
    [[nodiscard]] bool is_window() const noexcept { return kind() == TSKind::TSW; }
    [[nodiscard]] bool is_set() const noexcept { return kind() == TSKind::TSS; }
    [[nodiscard]] bool is_dict() const noexcept { return kind() == TSKind::TSD; }
    [[nodiscard]] bool is_list() const noexcept { return kind() == TSKind::TSL; }
    [[nodiscard]] bool is_bundle() const noexcept { return kind() == TSKind::TSB; }

    [[nodiscard]] std::optional<TSIndexedView> try_as_indexed() const;
    [[nodiscard]] std::optional<TSWView> try_as_window() const;
    [[nodiscard]] std::optional<TSSView> try_as_set() const;
    [[nodiscard]] std::optional<TSDView> try_as_dict() const;
    [[nodiscard]] std::optional<TSLView> try_as_list() const;
    [[nodiscard]] std::optional<TSBView> try_as_bundle() const;

    [[nodiscard]] TSIndexedView as_indexed_unchecked() const noexcept;
    [[nodiscard]] TSIndexedView as_indexed() const;
    [[nodiscard]] TSWView as_window() const;
    [[nodiscard]] TSSView as_set() const;
    [[nodiscard]] TSDView as_dict() const;
    [[nodiscard]] TSLView as_list() const;
    [[nodiscard]] TSBView as_bundle() const;

    void bind(const TSView& target);
    void unbind();
    [[nodiscard]] bool is_bound() const;

    [[nodiscard]] ViewData& view_data() noexcept { return view_data_; }
    [[nodiscard]] const ViewData& view_data() const noexcept { return view_data_; }

private:
    ViewData view_data_{};
};

using ViewRange = TSIterable<value::View>;
using TSViewRange = TSIterable<TSView>;
using TSViewPair = std::pair<value::View, TSView>;
using TSViewPairRange = TSIterable<TSViewPair>;

class HGRAPH_EXPORT TSWView : public TSView {
public:
    TSWView() = default;
    explicit TSWView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}
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

class HGRAPH_EXPORT TSSView : public TSView {
public:
    TSSView() = default;
    explicit TSSView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}
    [[nodiscard]] bool contains(const value::View& elem) const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] bool empty() const { return size() == 0; }
    [[nodiscard]] TSIterable<value::View> values() const;
    [[nodiscard]] TSIterable<value::View> added() const;
    [[nodiscard]] TSIterable<value::View> removed() const;
    [[nodiscard]] bool was_added(const value::View& elem) const;
    [[nodiscard]] bool was_removed(const value::View& elem) const;
    bool add(const value::View& elem);
    bool remove(const value::View& elem);
    void clear();
};

class HGRAPH_EXPORT TSDView : public TSView {
public:
    using item_type = std::pair<value::View, TSView>;

    TSDView() = default;
    explicit TSDView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}

    [[nodiscard]] TSSView key_set() const;
    [[nodiscard]] TSView get(const value::View& key) const;
    [[nodiscard]] TSView get(const value::View& key, TSView default_value) const;
    [[nodiscard]] TSView get_or_create(const value::View& key);
    [[nodiscard]] bool contains(const value::View& key) const;
    [[nodiscard]] TSIterable<value::View> keys() const;
    [[nodiscard]] TSIterable<value::View> valid_keys() const;
    [[nodiscard]] TSIterable<value::View> modified_keys() const;
    [[nodiscard]] TSIterable<value::View> added_keys() const;
    [[nodiscard]] TSIterable<value::View> removed_keys() const;
    [[nodiscard]] bool has_added() const;
    [[nodiscard]] bool has_removed() const;
    [[nodiscard]] bool was_added(const value::View& key) const;
    [[nodiscard]] bool was_removed(const value::View& key) const;
    [[nodiscard]] bool was_modified(const value::View& key) const;
    [[nodiscard]] TSIterable<TSView> values() const;
    [[nodiscard]] TSIterable<TSView> valid_values() const;
    [[nodiscard]] TSIterable<TSView> modified_values() const;
    [[nodiscard]] TSIterable<TSView> added_values() const;
    [[nodiscard]] TSIterable<TSView> removed_values() const;
    [[nodiscard]] TSIterable<item_type> items() const;
    [[nodiscard]] TSIterable<item_type> valid_items() const;
    [[nodiscard]] TSIterable<item_type> modified_items() const;
    [[nodiscard]] TSIterable<item_type> added_items() const;
    [[nodiscard]] TSIterable<item_type> removed_items() const;
    [[nodiscard]] TSView at_key(const value::View& key) const { return child_by_key(key); }
    [[nodiscard]] TSView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] std::optional<value::Value> key_at_slot(size_t slot) const;
    [[nodiscard]] std::optional<value::Value> key_for_child(const TSView& child) const;
    [[nodiscard]] size_t count() const { return child_count(); }
    [[nodiscard]] size_t size() const { return count(); }

    bool remove(const value::View& key);
    [[nodiscard]] TSView create(const value::View& key);
    [[nodiscard]] TSView set(const value::View& key, const value::View& value);
};

class HGRAPH_EXPORT TSIndexedView : public TSView {
public:
    TSIndexedView() = default;
    explicit TSIndexedView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}
    [[nodiscard]] TSView at(size_t index) const { return child_at(index); }
    [[nodiscard]] TSView operator[](size_t index) const { return at(index); }
    [[nodiscard]] size_t count() const { return child_count(); }
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSLView : public TSIndexedView {
public:
    using item_type = std::pair<value::View, TSView>;

    TSLView() = default;
    explicit TSLView(TSView ts_view) noexcept : TSIndexedView(std::move(ts_view)) {}

    [[nodiscard]] TSIterable<value::View> keys() const;
    [[nodiscard]] TSIterable<value::View> valid_keys() const;
    [[nodiscard]] TSIterable<value::View> modified_keys() const;
    [[nodiscard]] TSIterable<TSView> values() const;
    [[nodiscard]] TSIterable<TSView> valid_values() const;
    [[nodiscard]] TSIterable<TSView> modified_values() const;
    [[nodiscard]] TSIterable<item_type> items() const;
    [[nodiscard]] TSIterable<item_type> valid_items() const;
    [[nodiscard]] TSIterable<item_type> modified_items() const;
    [[nodiscard]] TSIterable<value::View> indices() const;
    [[nodiscard]] TSIterable<value::View> valid_indices() const;
    [[nodiscard]] TSIterable<value::View> modified_indices() const;
};

class HGRAPH_EXPORT TSBView : public TSIndexedView {
public:
    using item_type = std::pair<value::View, TSView>;

    TSBView() = default;
    explicit TSBView(TSView ts_view) noexcept : TSIndexedView(std::move(ts_view)) {}

    using TSIndexedView::at;
    [[nodiscard]] TSView field(std::string_view name) const { return child_by_name(name); }
    [[nodiscard]] TSView at(std::string_view name) const { return field(name); }
    [[nodiscard]] std::optional<size_t> index_of(std::string_view name) const;
    [[nodiscard]] std::string_view name_at(size_t index) const;
    [[nodiscard]] std::optional<std::string_view> name_for_child(const TSView& child) const;
    [[nodiscard]] bool contains(std::string_view name) const;
    [[nodiscard]] TSIterable<value::View> keys() const;
    [[nodiscard]] TSIterable<value::View> valid_keys() const;
    [[nodiscard]] TSIterable<value::View> modified_keys() const;
    [[nodiscard]] TSIterable<TSView> values() const;
    [[nodiscard]] TSIterable<TSView> valid_values() const;
    [[nodiscard]] TSIterable<TSView> modified_values() const;
    [[nodiscard]] TSIterable<item_type> items() const;
    [[nodiscard]] TSIterable<item_type> valid_items() const;
    [[nodiscard]] TSIterable<item_type> modified_items() const;
    [[nodiscard]] TSIterable<size_t> indices() const;
    [[nodiscard]] TSIterable<size_t> valid_indices() const;
    [[nodiscard]] TSIterable<size_t> modified_indices() const;
};

}  // namespace hgraph

// Include output and input view headers for backward compatibility.
// New code should include these directly when only output or input views are needed.
#include <hgraph/types/time_series/ts_output_views.h>
#include <hgraph/types/time_series/ts_input_views.h>
