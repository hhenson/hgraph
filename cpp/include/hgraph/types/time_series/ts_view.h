#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/view_data.h>
#include <hgraph/types/value/value_view.h>

#include <nanobind/nanobind.h>

#include <optional>
#include <string_view>
#include <utility>

namespace nb = nanobind;

namespace hgraph {

class TSInput;
class TSOutput;
class TSValue;
class TSWView;
class TSSView;
class TSDView;
class TSLView;
class TSBView;
class TSWOutputView;
class TSSOutputView;
class TSDOutputView;
class TSLOutputView;
class TSBOutputView;
class TSWInputView;
class TSSInputView;
class TSDInputView;
class TSLInputView;
class TSBInputView;
class TSInputView;
class TSOutputView;

/**
 * Non-owning time-series cursor (ViewData + current time).
 */
class HGRAPH_EXPORT TSView {
public:
    TSView() = default;
    TSView(ViewData view_data, engine_time_t current_time) noexcept
        : view_data_(std::move(view_data)), current_time_(current_time) {}

    TSView(const TSValue& value, engine_time_t current_time, ShortPath path = {});

    explicit operator bool() const noexcept {
        return view_data_.meta != nullptr && view_data_.ops != nullptr;
    }

    [[nodiscard]] const TSMeta* ts_meta() const noexcept;

    [[nodiscard]] engine_time_t current_time() const noexcept { return current_time_; }
    void set_current_time(engine_time_t time) noexcept { current_time_ = time; }

    [[nodiscard]] const ShortPath& short_path() const noexcept { return view_data_.path; }
    [[nodiscard]] FQPath fq_path() const { return view_data_.path.to_fq(); }

    [[nodiscard]] engine_time_t last_modified_time() const;
    [[nodiscard]] bool modified() const;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] bool all_valid() const;
    [[nodiscard]] bool has_delta() const;
    [[nodiscard]] bool sampled() const;

    [[nodiscard]] value::View value() const;
    [[nodiscard]] value::View delta_value() const;
    [[nodiscard]] nb::object to_python() const;
    [[nodiscard]] nb::object delta_to_python() const;

    void set_value(const value::View& src);
    void from_python(const nb::object& src);
    void apply_delta(const value::View& delta);
    void invalidate();

    [[nodiscard]] TSView child_at(size_t index) const;
    [[nodiscard]] TSView child_by_name(std::string_view name) const;
    [[nodiscard]] TSView child_by_key(const value::View& key) const;
    [[nodiscard]] size_t child_count() const;
    [[nodiscard]] size_t size() const { return child_count(); }

    [[nodiscard]] TSKind kind() const noexcept { return ts_meta() != nullptr ? ts_meta()->kind : TSKind::SIGNAL; }
    [[nodiscard]] bool is_window() const noexcept { return kind() == TSKind::TSW; }
    [[nodiscard]] bool is_set() const noexcept { return kind() == TSKind::TSS; }
    [[nodiscard]] bool is_dict() const noexcept { return kind() == TSKind::TSD; }
    [[nodiscard]] bool is_list() const noexcept { return kind() == TSKind::TSL; }
    [[nodiscard]] bool is_bundle() const noexcept { return kind() == TSKind::TSB; }

    [[nodiscard]] std::optional<TSWView> try_as_window() const;
    [[nodiscard]] std::optional<TSSView> try_as_set() const;
    [[nodiscard]] std::optional<TSDView> try_as_dict() const;
    [[nodiscard]] std::optional<TSLView> try_as_list() const;
    [[nodiscard]] std::optional<TSBView> try_as_bundle() const;

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
    engine_time_t current_time_{MIN_DT};
};

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
    bool add(const value::View& elem);
    bool remove(const value::View& elem);
    void clear();
};

class HGRAPH_EXPORT TSDView : public TSView {
public:
    TSDView() = default;
    explicit TSDView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}

    [[nodiscard]] TSView at_key(const value::View& key) const { return child_by_key(key); }
    [[nodiscard]] TSView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] size_t count() const { return child_count(); }
    [[nodiscard]] size_t size() const { return count(); }

    bool remove(const value::View& key);
    [[nodiscard]] TSView create(const value::View& key);
    [[nodiscard]] TSView set(const value::View& key, const value::View& value);
};

class HGRAPH_EXPORT TSLView : public TSView {
public:
    TSLView() = default;
    explicit TSLView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}

    [[nodiscard]] TSView at(size_t index) const { return child_at(index); }
    [[nodiscard]] TSView operator[](size_t index) const { return at(index); }
    [[nodiscard]] size_t count() const { return child_count(); }
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSBView : public TSView {
public:
    TSBView() = default;
    explicit TSBView(TSView ts_view) noexcept : TSView(std::move(ts_view)) {}

    [[nodiscard]] TSView at(size_t index) const { return child_at(index); }
    [[nodiscard]] TSView field(std::string_view name) const { return child_by_name(name); }
    [[nodiscard]] TSView at(std::string_view name) const { return field(name); }
    [[nodiscard]] size_t count() const { return child_count(); }
    [[nodiscard]] size_t size() const { return count(); }
};

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
    [[nodiscard]] const ShortPath& short_path() const noexcept { return ts_view_.short_path(); }

    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return ts_view_.ts_meta(); }
    [[nodiscard]] engine_time_t current_time() const noexcept { return ts_view_.current_time(); }
    void set_current_time(engine_time_t time) noexcept { ts_view_.set_current_time(time); }

    [[nodiscard]] FQPath fq_path() const;

    [[nodiscard]] engine_time_t last_modified_time() const { return ts_view_.last_modified_time(); }
    [[nodiscard]] bool modified() const { return ts_view_.modified(); }
    [[nodiscard]] bool valid() const { return ts_view_.valid(); }
    [[nodiscard]] bool all_valid() const { return ts_view_.all_valid(); }
    [[nodiscard]] bool has_delta() const { return ts_view_.has_delta(); }
    [[nodiscard]] bool sampled() const { return ts_view_.sampled(); }

    [[nodiscard]] value::View value() const { return ts_view_.value(); }
    [[nodiscard]] value::View delta_value() const { return ts_view_.delta_value(); }
    [[nodiscard]] nb::object to_python() const { return ts_view_.to_python(); }
    [[nodiscard]] nb::object delta_to_python() const { return ts_view_.delta_to_python(); }

    void set_value(const value::View& src) { ts_view_.set_value(src); }
    void from_python(const nb::object& src) { ts_view_.from_python(src); }
    void copy_from_input(const TSInputView& input);
    void copy_from_output(const TSOutputView& output);
    void apply_delta(const value::View& delta) { ts_view_.apply_delta(delta); }
    void invalidate() { ts_view_.invalidate(); }

    [[nodiscard]] TSOutputView child_at(size_t index) const;
    [[nodiscard]] TSOutputView child_by_name(std::string_view name) const;
    [[nodiscard]] TSOutputView child_by_key(const value::View& key) const;
    [[nodiscard]] size_t child_count() const { return ts_view_.child_count(); }

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
    [[nodiscard]] size_t removed_value_count() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t min_size() const;
    [[nodiscard]] size_t length() const;
};

class HGRAPH_EXPORT TSSOutputView : public TSOutputView {
public:
    TSSOutputView() = default;
    explicit TSSOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    bool add(const value::View& elem);
    bool remove(const value::View& elem);
    void clear();
};

class HGRAPH_EXPORT TSDOutputView : public TSOutputView {
public:
    TSDOutputView() = default;
    explicit TSDOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] TSOutputView at_key(const value::View& key) const;
    [[nodiscard]] TSOutputView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }

    bool remove(const value::View& key);
    [[nodiscard]] TSOutputView create(const value::View& key);
    [[nodiscard]] TSOutputView set(const value::View& key, const value::View& value);
};

class HGRAPH_EXPORT TSLOutputView : public TSOutputView {
public:
    TSLOutputView() = default;
    explicit TSLOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] TSOutputView at(size_t index) const;
    [[nodiscard]] TSOutputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSBOutputView : public TSOutputView {
public:
    TSBOutputView() = default;
    explicit TSBOutputView(TSOutputView base) noexcept : TSOutputView(std::move(base)) {}

    [[nodiscard]] TSOutputView at(size_t index) const;
    [[nodiscard]] TSOutputView field(std::string_view name) const;
    [[nodiscard]] TSOutputView at(std::string_view name) const { return field(name); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

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
    [[nodiscard]] const ShortPath& short_path() const noexcept { return ts_view_.short_path(); }

    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return ts_view_.ts_meta(); }
    [[nodiscard]] engine_time_t current_time() const noexcept { return ts_view_.current_time(); }
    void set_current_time(engine_time_t time) noexcept { ts_view_.set_current_time(time); }

    [[nodiscard]] FQPath fq_path() const;

    [[nodiscard]] engine_time_t last_modified_time() const { return ts_view_.last_modified_time(); }
    [[nodiscard]] bool modified() const { return ts_view_.modified(); }
    [[nodiscard]] bool valid() const { return ts_view_.valid(); }
    [[nodiscard]] bool all_valid() const { return ts_view_.all_valid(); }
    [[nodiscard]] bool has_delta() const { return ts_view_.has_delta(); }
    [[nodiscard]] bool sampled() const { return ts_view_.sampled(); }

    [[nodiscard]] value::View value() const { return ts_view_.value(); }
    [[nodiscard]] value::View delta_value() const { return ts_view_.delta_value(); }
    [[nodiscard]] nb::object to_python() const { return ts_view_.to_python(); }
    [[nodiscard]] nb::object delta_to_python() const { return ts_view_.delta_to_python(); }

    [[nodiscard]] TSInputView child_at(size_t index) const;
    [[nodiscard]] TSInputView child_by_name(std::string_view name) const;
    [[nodiscard]] TSInputView child_by_key(const value::View& key) const;
    [[nodiscard]] size_t child_count() const { return ts_view_.child_count(); }

    void bind(const TSOutputView& target) { ts_view_.bind(target.as_ts_view()); }
    void unbind() { ts_view_.unbind(); }
    [[nodiscard]] bool is_bound() const { return ts_view_.is_bound(); }
    void make_active();
    void make_passive();
    [[nodiscard]] bool active() const;

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
    TSDInputView() = default;
    explicit TSDInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}

    [[nodiscard]] TSInputView at_key(const value::View& key) const;
    [[nodiscard]] TSInputView by_key(const value::View& key) const { return at_key(key); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSLInputView : public TSInputView {
public:
    TSLInputView() = default;
    explicit TSLInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}

    [[nodiscard]] TSInputView at(size_t index) const;
    [[nodiscard]] TSInputView operator[](size_t index) const { return at(index); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

class HGRAPH_EXPORT TSBInputView : public TSInputView {
public:
    TSBInputView() = default;
    explicit TSBInputView(TSInputView base) noexcept : TSInputView(std::move(base)) {}

    [[nodiscard]] TSInputView at(size_t index) const;
    [[nodiscard]] TSInputView field(std::string_view name) const;
    [[nodiscard]] TSInputView at(std::string_view name) const { return field(name); }
    [[nodiscard]] size_t count() const;
    [[nodiscard]] size_t size() const { return count(); }
};

}  // namespace hgraph
