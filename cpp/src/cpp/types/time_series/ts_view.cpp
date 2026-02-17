#include <hgraph/types/time_series/ts_view.h>

#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_value.h>

#include <stdexcept>

namespace hgraph {
namespace {

const ts_window_ops* resolve_window_ops(const ViewData& view_data) {
    if (view_data.ops == nullptr) {
        return nullptr;
    }
    return view_data.ops->window_ops();
}

const ts_set_ops* resolve_set_ops(const ViewData& view_data) {
    if (view_data.ops == nullptr) {
        return nullptr;
    }
    return view_data.ops->set_ops();
}

const ts_dict_ops* resolve_dict_ops(const ViewData& view_data) {
    if (view_data.ops == nullptr) {
        return nullptr;
    }
    return view_data.ops->dict_ops();
}

const ts_list_ops* resolve_list_ops(const ViewData& view_data) {
    if (view_data.ops == nullptr) {
        return nullptr;
    }
    return view_data.ops->list_ops();
}

const ts_bundle_ops* resolve_bundle_ops(const ViewData& view_data) {
    if (view_data.ops == nullptr) {
        return nullptr;
    }
    return view_data.ops->bundle_ops();
}

const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
    const TSMeta* meta = root;
    for (size_t index : indices) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            return nullptr;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                if (meta->fields() == nullptr || index >= meta->field_count()) {
                    return nullptr;
                }
                meta = meta->fields()[index].ts_type;
                break;
            case TSKind::TSL:
            case TSKind::TSD:
                meta = meta->element_ts();
                break;
            default:
                return nullptr;
        }
    }
    return meta;
}

value::View resolve_navigation_value(const ViewData& view_data) {
    if (view_data.ops == nullptr || view_data.ops->value == nullptr) {
        return {};
    }
    return view_data.ops->value(view_data);
}

TSView child_at_impl(const ViewData& view_data, size_t index, engine_time_t current_time) {
    ViewData child = view_data;
    child.path.indices.push_back(index);
    return TSView(child, current_time);
}

TSView child_by_name_impl(const ViewData& view_data, std::string_view name, engine_time_t current_time) {
    const TSMeta* current = meta_at_path(view_data.meta, view_data.path.indices);
    if (current == nullptr || current->kind != TSKind::TSB || current->fields() == nullptr) {
        return {};
    }

    for (size_t i = 0; i < current->field_count(); ++i) {
        if (name == current->fields()[i].name) {
            return child_at_impl(view_data, i, current_time);
        }
    }
    return {};
}

TSView child_by_key_impl(const ViewData& view_data, const value::View& key, engine_time_t current_time) {
    value::View v = resolve_navigation_value(view_data);
    if (!v.valid() || !v.is_map()) {
        return {};
    }

    auto map = v.as_map();
    const bool debug_child_key = std::getenv("HGRAPH_DEBUG_CHILD_KEY") != nullptr;
    if (debug_child_key) {
        std::string key_s = nb::cast<std::string>(nb::repr(key.to_python()));
        std::fprintf(stderr,
                     "[child_by_key] path=%s key=%s map_size=%zu\n",
                     view_data.path.to_string().c_str(),
                     key_s.c_str(),
                     map.size());
    }
    size_t slot = 0;
    for (value::View map_key : map.keys()) {
        if (debug_child_key) {
            std::string map_key_s = nb::cast<std::string>(nb::repr(map_key.to_python()));
            std::fprintf(stderr,
                         "  [child_by_key] probe slot=%zu map_key=%s\n",
                         slot,
                         map_key_s.c_str());
        }
        if (map_key.schema() == key.schema() && map_key.equals(key)) {
            if (debug_child_key) {
                std::fprintf(stderr, "  [child_by_key] match slot=%zu\n", slot);
            }
            return child_at_impl(view_data, slot, current_time);
        }
        ++slot;
    }
    return {};
}

size_t child_count_impl(const ViewData& view_data) {
    const TSMeta* current = meta_at_path(view_data.meta, view_data.path.indices);
    if (current == nullptr) {
        return 0;
    }

    switch (current->kind) {
        case TSKind::TSB:
            return current->field_count();
        case TSKind::TSL:
            if (current->fixed_size() > 0) {
                return current->fixed_size();
            } else {
                value::View v = resolve_navigation_value(view_data);
                return (v.valid() && v.is_list()) ? v.as_list().size() : 0;
            }
        case TSKind::TSD: {
            value::View v = resolve_navigation_value(view_data);
            return (v.valid() && v.is_map()) ? v.as_map().size() : 0;
        }
        default:
            return 0;
    }
}

}  // namespace

TSView::TSView(const TSValue& value, engine_time_t current_time, ShortPath path)
    : view_data_(value.make_view_data(std::move(path))), current_time_(current_time) {}

const TSMeta* TSView::ts_meta() const noexcept {
    if (view_data_.ops != nullptr) {
        if (const TSMeta* meta = view_data_.ops->ts_meta(view_data_); meta != nullptr) {
            return meta;
        }
    }
    return view_data_.meta;
}

engine_time_t TSView::last_modified_time() const {
    if (view_data_.ops == nullptr) {
        return MIN_DT;
    }
    return view_data_.ops->last_modified_time(view_data_);
}

bool TSView::modified() const {
    if (view_data_.ops == nullptr) {
        return false;
    }
    return view_data_.ops->modified(view_data_, current_time_);
}

bool TSView::valid() const {
    if (view_data_.ops == nullptr) {
        return false;
    }
    return view_data_.ops->valid(view_data_);
}

bool TSView::all_valid() const {
    if (view_data_.ops == nullptr) {
        return false;
    }
    return view_data_.ops->all_valid(view_data_);
}

bool TSView::has_delta() const {
    if (view_data_.ops == nullptr) {
        return false;
    }
    return view_data_.ops->has_delta(view_data_);
}

bool TSView::sampled() const {
    if (view_data_.ops == nullptr || view_data_.ops->sampled == nullptr) {
        return view_data_.sampled;
    }
    return view_data_.ops->sampled(view_data_);
}

value::View TSView::value() const {
    if (view_data_.ops == nullptr) {
        return {};
    }
    return view_data_.ops->value(view_data_);
}

value::View TSView::delta_value() const {
    if (view_data_.ops == nullptr) {
        return {};
    }
    return view_data_.ops->delta_value(view_data_);
}

nb::object TSView::to_python() const {
    if (view_data_.ops == nullptr || view_data_.ops->to_python == nullptr) {
        return nb::none();
    }
    return view_data_.ops->to_python(view_data_);
}

nb::object TSView::delta_to_python() const {
    if (view_data_.ops == nullptr || view_data_.ops->delta_to_python == nullptr) {
        return nb::none();
    }
    return view_data_.ops->delta_to_python(view_data_, current_time_);
}

void TSView::set_value(const value::View& src) {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->set_value(view_data_, src, current_time_);
}

void TSView::from_python(const nb::object& src) {
    if (view_data_.ops == nullptr || view_data_.ops->from_python == nullptr) {
        return;
    }
    view_data_.ops->from_python(view_data_, src, current_time_);
}

void TSView::apply_delta(const value::View& delta) {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->apply_delta(view_data_, delta, current_time_);
}

void TSView::invalidate() {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->invalidate(view_data_);
}

TSView TSView::child_at(size_t index) const {
    return child_at_impl(view_data_, index, current_time_);
}

TSView TSView::child_by_name(std::string_view name) const {
    return child_by_name_impl(view_data_, name, current_time_);
}

TSView TSView::child_by_key(const value::View& key) const {
    if (view_data_.meta == nullptr) {
        return {};
    }
    return child_by_key_impl(view_data_, key, current_time_);
}

size_t TSView::child_count() const {
    return child_count_impl(view_data_);
}

std::optional<TSIndexedView> TSView::try_as_indexed() const {
    if (!is_list() && !is_bundle()) {
        return std::nullopt;
    }
    return TSIndexedView(*this);
}

std::optional<TSWView> TSView::try_as_window() const {
    if (!is_window()) {
        return std::nullopt;
    }
    return TSWView(*this);
}

std::optional<TSSView> TSView::try_as_set() const {
    if (!is_set()) {
        return std::nullopt;
    }
    return TSSView(*this);
}

std::optional<TSDView> TSView::try_as_dict() const {
    if (!is_dict()) {
        return std::nullopt;
    }
    return TSDView(*this);
}

std::optional<TSLView> TSView::try_as_list() const {
    if (!is_list()) {
        return std::nullopt;
    }
    return TSLView(*this);
}

std::optional<TSBView> TSView::try_as_bundle() const {
    if (!is_bundle()) {
        return std::nullopt;
    }
    return TSBView(*this);
}

TSIndexedView TSView::as_indexed() const {
    if (!is_list() && !is_bundle()) {
        throw std::runtime_error("TSView is not an indexed view");
    }
    return TSIndexedView(*this);
}

TSIndexedView TSView::as_indexed_unchecked() const noexcept {
    return TSIndexedView(*this);
}

TSWView TSView::as_window() const {
    auto v = try_as_window();
    if (!v.has_value()) {
        throw std::runtime_error("TSView is not a TSW view");
    }
    return *v;
}

TSSView TSView::as_set() const {
    auto v = try_as_set();
    if (!v.has_value()) {
        throw std::runtime_error("TSView is not a TSS view");
    }
    return *v;
}

TSDView TSView::as_dict() const {
    auto v = try_as_dict();
    if (!v.has_value()) {
        throw std::runtime_error("TSView is not a TSD view");
    }
    return *v;
}

TSLView TSView::as_list() const {
    auto v = try_as_list();
    if (!v.has_value()) {
        throw std::runtime_error("TSView is not a TSL view");
    }
    return *v;
}

TSBView TSView::as_bundle() const {
    auto v = try_as_bundle();
    if (!v.has_value()) {
        throw std::runtime_error("TSView is not a TSB view");
    }
    return *v;
}

const engine_time_t* TSWView::value_times() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->value_times == nullptr) {
        return nullptr;
    }
    return ops->value_times(view_data());
}

size_t TSWView::value_times_count() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->value_times_count == nullptr) {
        return 0;
    }
    return ops->value_times_count(view_data());
}

engine_time_t TSWView::first_modified_time() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->first_modified_time == nullptr) {
        return MIN_DT;
    }
    return ops->first_modified_time(view_data());
}

bool TSWView::has_removed_value() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->has_removed_value == nullptr) {
        return false;
    }
    return ops->has_removed_value(view_data());
}

value::View TSWView::removed_value() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->removed_value == nullptr) {
        return {};
    }
    return ops->removed_value(view_data());
}

size_t TSWView::removed_value_count() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->removed_value_count == nullptr) {
        return 0;
    }
    return ops->removed_value_count(view_data());
}

size_t TSWView::size() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->size == nullptr) {
        return 0;
    }
    return ops->size(view_data());
}

size_t TSWView::min_size() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->min_size == nullptr) {
        return 0;
    }
    return ops->min_size(view_data());
}

size_t TSWView::length() const {
    const ts_window_ops* ops = resolve_window_ops(view_data());
    if (ops == nullptr || ops->length == nullptr) {
        return 0;
    }
    return ops->length(view_data());
}

bool TSSView::add(const value::View& elem) {
    const ts_set_ops* ops = resolve_set_ops(view_data());
    if (ops == nullptr || ops->add == nullptr) {
        return false;
    }
    return ops->add(view_data(), elem, current_time());
}

bool TSSView::remove(const value::View& elem) {
    const ts_set_ops* ops = resolve_set_ops(view_data());
    if (ops == nullptr || ops->remove == nullptr) {
        return false;
    }
    return ops->remove(view_data(), elem, current_time());
}

void TSSView::clear() {
    const ts_set_ops* ops = resolve_set_ops(view_data());
    if (ops == nullptr || ops->clear == nullptr) {
        return;
    }
    ops->clear(view_data(), current_time());
}

bool TSDView::remove(const value::View& key) {
    const ts_dict_ops* ops = resolve_dict_ops(view_data());
    if (ops == nullptr || ops->remove == nullptr) {
        return false;
    }
    return ops->remove(view_data(), key, current_time());
}

TSView TSDView::create(const value::View& key) {
    const ts_dict_ops* ops = resolve_dict_ops(view_data());
    if (ops == nullptr || ops->create == nullptr) {
        return {};
    }
    return ops->create(view_data(), key, current_time());
}

TSView TSDView::set(const value::View& key, const value::View& value) {
    const ts_dict_ops* ops = resolve_dict_ops(view_data());
    if (ops == nullptr || ops->set == nullptr) {
        return {};
    }
    return ops->set(view_data(), key, value, current_time());
}

void TSView::bind(const TSView& target) {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->bind(view_data_, target.view_data_, current_time_);
}

void TSView::unbind() {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->unbind(view_data_, current_time_);
}

bool TSView::is_bound() const {
    if (view_data_.ops == nullptr) {
        return false;
    }
    return view_data_.ops->is_bound(view_data_);
}

FQPath TSOutputView::fq_path() const {
    if (owner_ == nullptr) {
        return ts_view_.fq_path();
    }
    return owner_->to_fq_path(ts_view_);
}

void TSOutputView::copy_from_input(const TSInputView& input) {
    ViewData dst = ts_view_.view_data();
    const ViewData& src = input.as_ts_view().view_data();
    copy_view_data_value(dst, src, current_time());
}

void TSOutputView::copy_from_output(const TSOutputView& output) {
    ViewData dst = ts_view_.view_data();
    const ViewData& src = output.as_ts_view().view_data();
    copy_view_data_value(dst, src, current_time());
}

std::optional<TSWOutputView> TSOutputView::try_as_window() const {
    if (!ts_view_.is_window()) {
        return std::nullopt;
    }
    return TSWOutputView(*this);
}

std::optional<TSSOutputView> TSOutputView::try_as_set() const {
    if (!ts_view_.is_set()) {
        return std::nullopt;
    }
    return TSSOutputView(*this);
}

std::optional<TSDOutputView> TSOutputView::try_as_dict() const {
    if (!ts_view_.is_dict()) {
        return std::nullopt;
    }
    return TSDOutputView(*this);
}

std::optional<TSLOutputView> TSOutputView::try_as_list() const {
    if (!ts_view_.is_list()) {
        return std::nullopt;
    }
    return TSLOutputView(*this);
}

std::optional<TSBOutputView> TSOutputView::try_as_bundle() const {
    if (!ts_view_.is_bundle()) {
        return std::nullopt;
    }
    return TSBOutputView(*this);
}

TSWOutputView TSOutputView::as_window() const {
    auto out = try_as_window();
    if (!out.has_value()) {
        throw std::runtime_error("TSOutputView is not a TSW output view");
    }
    return *out;
}

TSSOutputView TSOutputView::as_set() const {
    auto out = try_as_set();
    if (!out.has_value()) {
        throw std::runtime_error("TSOutputView is not a TSS output view");
    }
    return *out;
}

TSDOutputView TSOutputView::as_dict() const {
    auto out = try_as_dict();
    if (!out.has_value()) {
        throw std::runtime_error("TSOutputView is not a TSD output view");
    }
    return *out;
}

TSLOutputView TSOutputView::as_list() const {
    auto out = try_as_list();
    if (!out.has_value()) {
        throw std::runtime_error("TSOutputView is not a TSL output view");
    }
    return *out;
}

TSBOutputView TSOutputView::as_bundle() const {
    auto out = try_as_bundle();
    if (!out.has_value()) {
        throw std::runtime_error("TSOutputView is not a TSB output view");
    }
    return *out;
}

const engine_time_t* TSWOutputView::value_times() const {
    return as_ts_view().as_window().value_times();
}

size_t TSWOutputView::value_times_count() const {
    return as_ts_view().as_window().value_times_count();
}

engine_time_t TSWOutputView::first_modified_time() const {
    return as_ts_view().as_window().first_modified_time();
}

bool TSWOutputView::has_removed_value() const {
    return as_ts_view().as_window().has_removed_value();
}

size_t TSWOutputView::removed_value_count() const {
    return as_ts_view().as_window().removed_value_count();
}

size_t TSWOutputView::size() const {
    return as_ts_view().as_window().size();
}

size_t TSWOutputView::min_size() const {
    return as_ts_view().as_window().min_size();
}

size_t TSWOutputView::length() const {
    return as_ts_view().as_window().length();
}

bool TSSOutputView::add(const value::View& elem) {
    return as_ts_view().as_set().add(elem);
}

bool TSSOutputView::remove(const value::View& elem) {
    return as_ts_view().as_set().remove(elem);
}

void TSSOutputView::clear() {
    as_ts_view().as_set().clear();
}

TSOutputView TSDOutputView::at_key(const value::View& key) const {
    return TSOutputView(owner_, as_ts_view().as_dict().at_key(key));
}

size_t TSDOutputView::count() const {
    return as_ts_view().as_dict().count();
}

bool TSDOutputView::remove(const value::View& key) {
    return as_ts_view().as_dict().remove(key);
}

TSOutputView TSDOutputView::create(const value::View& key) {
    return TSOutputView(owner_, as_ts_view().as_dict().create(key));
}

TSOutputView TSDOutputView::set(const value::View& key, const value::View& value) {
    return TSOutputView(owner_, as_ts_view().as_dict().set(key, value));
}

TSOutputView TSIndexedOutputView::at(size_t index) const {
    return TSOutputView(owner_, as_ts_view().as_indexed_unchecked().at(index));
}

size_t TSIndexedOutputView::count() const {
    return as_ts_view().as_indexed_unchecked().count();
}

TSOutputView TSBOutputView::field(std::string_view name) const {
    return TSOutputView(owner_, as_ts_view().as_bundle().field(name));
}

FQPath TSInputView::fq_path() const {
    if (owner_ == nullptr) {
        return ts_view_.fq_path();
    }
    return owner_->to_fq_path(ts_view_);
}

std::optional<TSWInputView> TSInputView::try_as_window() const {
    if (!ts_view_.is_window()) {
        return std::nullopt;
    }
    return TSWInputView(*this);
}

std::optional<TSSInputView> TSInputView::try_as_set() const {
    if (!ts_view_.is_set()) {
        return std::nullopt;
    }
    return TSSInputView(*this);
}

std::optional<TSDInputView> TSInputView::try_as_dict() const {
    if (!ts_view_.is_dict()) {
        return std::nullopt;
    }
    return TSDInputView(*this);
}

std::optional<TSLInputView> TSInputView::try_as_list() const {
    if (!ts_view_.is_list()) {
        return std::nullopt;
    }
    return TSLInputView(*this);
}

std::optional<TSBInputView> TSInputView::try_as_bundle() const {
    if (!ts_view_.is_bundle()) {
        return std::nullopt;
    }
    return TSBInputView(*this);
}

TSWInputView TSInputView::as_window() const {
    auto out = try_as_window();
    if (!out.has_value()) {
        throw std::runtime_error("TSInputView is not a TSW input view");
    }
    return *out;
}

TSSInputView TSInputView::as_set() const {
    auto out = try_as_set();
    if (!out.has_value()) {
        throw std::runtime_error("TSInputView is not a TSS input view");
    }
    return *out;
}

TSDInputView TSInputView::as_dict() const {
    auto out = try_as_dict();
    if (!out.has_value()) {
        throw std::runtime_error("TSInputView is not a TSD input view");
    }
    return *out;
}

TSLInputView TSInputView::as_list() const {
    auto out = try_as_list();
    if (!out.has_value()) {
        throw std::runtime_error("TSInputView is not a TSL input view");
    }
    return *out;
}

TSBInputView TSInputView::as_bundle() const {
    auto out = try_as_bundle();
    if (!out.has_value()) {
        throw std::runtime_error("TSInputView is not a TSB input view");
    }
    return *out;
}

const engine_time_t* TSWInputView::value_times() const {
    return as_ts_view().as_window().value_times();
}

size_t TSWInputView::value_times_count() const {
    return as_ts_view().as_window().value_times_count();
}

engine_time_t TSWInputView::first_modified_time() const {
    return as_ts_view().as_window().first_modified_time();
}

bool TSWInputView::has_removed_value() const {
    return as_ts_view().as_window().has_removed_value();
}

size_t TSWInputView::removed_value_count() const {
    return as_ts_view().as_window().removed_value_count();
}

size_t TSWInputView::size() const {
    return as_ts_view().as_window().size();
}

size_t TSWInputView::min_size() const {
    return as_ts_view().as_window().min_size();
}

size_t TSWInputView::length() const {
    return as_ts_view().as_window().length();
}

TSInputView TSDInputView::at_key(const value::View& key) const {
    return TSInputView(owner_, as_ts_view().as_dict().at_key(key));
}

size_t TSDInputView::count() const {
    return as_ts_view().as_dict().count();
}

TSInputView TSIndexedInputView::at(size_t index) const {
    return TSInputView(owner_, as_ts_view().as_indexed_unchecked().at(index));
}

size_t TSIndexedInputView::count() const {
    return as_ts_view().as_indexed_unchecked().count();
}

TSInputView TSBInputView::field(std::string_view name) const {
    return TSInputView(owner_, as_ts_view().as_bundle().field(name));
}

void TSInputView::make_active() {
    if (owner_ != nullptr) {
        owner_->set_active(ts_view_, true);
    }
}

void TSInputView::make_passive() {
    if (owner_ != nullptr) {
        owner_->set_active(ts_view_, false);
    }
}

bool TSInputView::active() const {
    return owner_ != nullptr && owner_->active(ts_view_);
}

}  // namespace hgraph
