#include <hgraph/api/python/py_ts_runtime_internal.h>

#include <hgraph/python/chrono.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/time_series/link_observer_registry.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_meta_schema_cache.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace hgraph {
namespace {

using value::View;

std::vector<size_t> ts_path_to_link_path(const TSMeta* root_meta, const std::vector<size_t>& ts_path) {
    std::vector<size_t> out;
    const TSMeta* meta = root_meta;
    bool crossed_dynamic_boundary = false;

    for (size_t index : ts_path) {
        if (meta == nullptr) {
            break;
        }

        if (crossed_dynamic_boundary) {
            switch (meta->kind) {
                case TSKind::TSB:
                    if (meta->fields() == nullptr || index >= meta->field_count()) {
                        return out;
                    }
                    meta = meta->fields()[index].ts_type;
                    break;
                case TSKind::TSL:
                case TSKind::TSD:
                case TSKind::REF:
                    meta = meta->element_ts();
                    break;
                default:
                    return out;
            }
            continue;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                out.push_back(index + 1);  // slot 0 is container link
                if (meta->fields() == nullptr || index >= meta->field_count()) {
                    return out;
                }
                meta = meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
                if (meta->fixed_size() > 0) {
                    out.push_back(1);
                    out.push_back(index);
                }
                if (meta->fixed_size() == 0) {
                    crossed_dynamic_boundary = true;
                }
                meta = meta->element_ts();
                break;

            case TSKind::TSD:
                crossed_dynamic_boundary = true;
            case TSKind::REF:
                meta = meta->element_ts();
                break;

            default:
                return out;
        }
    }

    if (!crossed_dynamic_boundary && meta != nullptr && meta->kind == TSKind::TSB) {
        out.push_back(0);
    } else if (!crossed_dynamic_boundary && meta != nullptr && meta->kind == TSKind::TSL && meta->fixed_size() > 0) {
        out.push_back(0);
    }

    return out;
}

std::optional<View> navigate_const(View view, const std::vector<size_t>& indices) {
    View current = view;
    for (size_t index : indices) {
        if (!current.valid()) {
            return std::nullopt;
        }

        if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index >= tuple.size()) {
                return std::nullopt;
            }
            current = tuple.at(index);
            continue;
        }

        if (current.is_list()) {
            auto list = current.as_list();
            if (index >= list.size()) {
                return std::nullopt;
            }
            current = list.at(index);
            continue;
        }

        return std::nullopt;
    }
    return current;
}

std::optional<View> resolve_link_payload(const TSView& ts_view) {
    const ViewData& vd = ts_view.view_data();
    auto* link_root = static_cast<const value::Value*>(vd.link_data);
    if (link_root == nullptr || !link_root->has_value()) {
        return std::nullopt;
    }

    const auto link_path = ts_path_to_link_path(vd.meta, vd.path.indices);
    return navigate_const(link_root->view(), link_path);
}

std::vector<size_t> linked_source_indices(const TSView& ts_view) {
    auto payload = resolve_link_payload(ts_view);
    if (!payload.has_value() || !payload->valid()) {
        return {};
    }

    const value::TypeMeta* ref_link_meta = value::TypeRegistry::instance().get_by_name("REFLink");
    if (ref_link_meta == nullptr || payload->schema() != ref_link_meta) {
        return {};
    }

    const auto* link = static_cast<const REFLink*>(payload->data());
    if (link == nullptr || !link->is_linked) {
        return {};
    }
    return link->source.path.indices;
}

std::vector<size_t> linked_target_indices(const TSView& ts_view) {
    auto payload = resolve_link_payload(ts_view);
    if (!payload.has_value() || !payload->valid()) {
        return {};
    }

    const value::TypeMeta* link_target_meta = value::TypeRegistry::instance().get_by_name("LinkTarget");
    if (link_target_meta == nullptr || payload->schema() != link_target_meta) {
        return {};
    }

    const auto* link = static_cast<const LinkTarget*>(payload->data());
    if (link == nullptr || !link->is_linked) {
        return {};
    }
    return link->target_path.indices;
}

std::optional<value::Value> dict_key_value_from_python(const TSView& ts_view, const nb::object& key_obj) {
    const TSMeta* meta = ts_view.ts_meta();
    if (meta == nullptr || meta->kind != TSKind::TSD || meta->key_type() == nullptr) {
        return std::nullopt;
    }

    value::Value out(meta->key_type());
    out.emplace();
    meta->key_type()->ops().from_python(out.data(), key_obj, meta->key_type());
    return out;
}

TSInputView dict_input_at_key_object(const TSInputView& self, const nb::object& key_obj) {
    auto dict = self.try_as_dict();
    if (!dict.has_value()) {
        return {};
    }
    auto key_value = dict_key_value_from_python(self.as_ts_view(), key_obj);
    if (!key_value.has_value()) {
        return {};
    }
    return dict->at_key(key_value->view());
}

TSOutputView dict_output_at_key_object(const TSOutputView& self, const nb::object& key_obj) {
    auto dict = self.try_as_dict();
    if (!dict.has_value()) {
        return {};
    }
    auto key_value = dict_key_value_from_python(self.as_ts_view(), key_obj);
    if (!key_value.has_value()) {
        return {};
    }
    return dict->at_key(key_value->view());
}

bool dict_contains_python_key(const TSView& ts_view, const nb::object& key_obj) {
    auto key_value = dict_key_value_from_python(ts_view, key_obj);
    if (!key_value.has_value()) {
        return false;
    }

    View value = ts_view.value();
    if (!value.valid() || !value.is_map()) {
        return false;
    }
    return value.as_map().contains(key_value->view());
}

nb::list tsd_keys_python(const TSView& ts_view) {
    nb::list out;
    View value = ts_view.value();
    if (!value.valid() || !value.is_map()) {
        return out;
    }
    for (View key : value.as_map().keys()) {
        out.append(key.to_python());
    }
    return out;
}

nb::list tsd_modified_keys_python(const TSView& ts_view) {
    nb::list out;
    View delta = ts_view.delta_value();
    if (!delta.valid() || !delta.is_tuple()) {
        return out;
    }

    auto tuple = delta.as_tuple();
    if (tuple.size() == 0 || !tuple.at(0).valid() || !tuple.at(0).is_map()) {
        return out;
    }

    for (View key : tuple.at(0).as_map().keys()) {
        out.append(key.to_python());
    }
    return out;
}

nb::list tsd_added_keys_python(const TSView& ts_view) {
    nb::list out;
    View delta = ts_view.delta_value();
    if (!delta.valid() || !delta.is_tuple()) {
        return out;
    }

    auto tuple = delta.as_tuple();
    if (tuple.size() <= 1 || !tuple.at(1).valid() || !tuple.at(1).is_set()) {
        return out;
    }

    for (View key : tuple.at(1).as_set()) {
        out.append(key.to_python());
    }
    return out;
}

nb::list tsd_removed_keys_python(const TSView& ts_view) {
    nb::list out;
    View delta = ts_view.delta_value();
    if (!delta.valid() || !delta.is_tuple()) {
        return out;
    }

    auto tuple = delta.as_tuple();
    if (tuple.size() <= 2 || !tuple.at(2).valid() || !tuple.at(2).is_set()) {
        return out;
    }

    for (View key : tuple.at(2).as_set()) {
        out.append(key.to_python());
    }
    return out;
}

TSInputView tsd_key_set_input_view(const TSInputView& self) {
    if (self.as_ts_view().kind() != TSKind::TSD) {
        return {};
    }
    TSInputView out = self;
    out.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
    return out;
}

TSOutputView tsd_key_set_output_view(const TSOutputView& self) {
    if (self.as_ts_view().kind() != TSKind::TSD) {
        return {};
    }
    TSOutputView out = self;
    out.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
    return out;
}

nb::list tsd_input_values(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    for (const auto& key : keys) {
        out.append(dict_input_at_key_object(self, nb::cast<nb::object>(key)));
    }
    return out;
}

nb::list tsd_output_values(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    for (const auto& key : keys) {
        out.append(dict_output_at_key_object(self, nb::cast<nb::object>(key)));
    }
    return out;
}

nb::list tsd_input_items(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        out.append(nb::make_tuple(key_obj, dict_input_at_key_object(self, key_obj)));
    }
    return out;
}

nb::list tsd_output_items(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        out.append(nb::make_tuple(key_obj, dict_output_at_key_object(self, key_obj)));
    }
    return out;
}

nb::object tsd_input_delta_to_python(const TSDInputView& self) {
    nb::object delta = self.as_ts_view().delta_to_python();
    if (delta.is_none()) {
        return get_frozendict()(nb::dict{});
    }
    return delta;
}

nb::list tsl_keys_python(const TSView& ts_view) {
    nb::list out;
    if (ts_view.kind() != TSKind::TSL) {
        return out;
    }
    const size_t n = ts_view.child_count();
    for (size_t i = 0; i < n; ++i) {
        out.append(nb::int_(i));
    }
    return out;
}

nb::list tsb_keys_python(const TSView& ts_view) {
    nb::list out;
    const TSMeta* meta = ts_view.ts_meta();
    if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
        return out;
    }

    for (size_t i = 0; i < meta->field_count(); ++i) {
        const char* name = meta->fields()[i].name;
        if (name != nullptr) {
            out.append(nb::str(name));
        }
    }
    return out;
}

nb::list tsl_input_values(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    auto list = self.try_as_list();
    if (!list.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        out.append(list->at(nb::cast<size_t>(key)));
    }
    return out;
}

nb::list tsl_output_values(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    auto list = self.try_as_list();
    if (!list.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        out.append(list->at(nb::cast<size_t>(key)));
    }
    return out;
}

nb::list tsl_input_items(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    auto list = self.try_as_list();
    if (!list.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        out.append(nb::make_tuple(key_obj, list->at(nb::cast<size_t>(key_obj))));
    }
    return out;
}

nb::list tsl_output_items(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    auto list = self.try_as_list();
    if (!list.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        out.append(nb::make_tuple(key_obj, list->at(nb::cast<size_t>(key_obj))));
    }
    return out;
}

nb::list tsb_input_values(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    auto bundle = self.try_as_bundle();
    if (!bundle.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        std::string key_str = nb::cast<std::string>(key);
        out.append(bundle->field(key_str));
    }
    return out;
}

nb::list tsb_output_values(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    auto bundle = self.try_as_bundle();
    if (!bundle.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        std::string key_str = nb::cast<std::string>(key);
        out.append(bundle->field(key_str));
    }
    return out;
}

nb::list tsb_input_items(const TSInputView& self, const nb::list& keys) {
    nb::list out;
    auto bundle = self.try_as_bundle();
    if (!bundle.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        std::string key_str = nb::cast<std::string>(key_obj);
        out.append(nb::make_tuple(key_obj, bundle->field(key_str)));
    }
    return out;
}

nb::list tsb_output_items(const TSOutputView& self, const nb::list& keys) {
    nb::list out;
    auto bundle = self.try_as_bundle();
    if (!bundle.has_value()) {
        return out;
    }
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        std::string key_str = nb::cast<std::string>(key_obj);
        out.append(nb::make_tuple(key_obj, bundle->field(key_str)));
    }
    return out;
}

template <typename ViewT, typename ChildByKeyFn, typename IncludeFn>
nb::list tsd_keys_filtered(const ViewT& self, ChildByKeyFn&& child_by_key, IncludeFn&& include) {
    nb::list out;
    if (self.as_ts_view().kind() != TSKind::TSD) {
        return out;
    }

    const nb::list keys = tsd_keys_python(self.as_ts_view());
    for (const auto& key : keys) {
        nb::object key_obj = nb::cast<nb::object>(key);
        auto child = child_by_key(self, key_obj);
        if (include(child)) {
            out.append(key_obj);
        }
    }
    return out;
}

template <typename ViewT, typename IncludeFn>
nb::list tsl_keys_filtered(const ViewT& self, IncludeFn&& include) {
    nb::list out;
    if (self.as_ts_view().kind() != TSKind::TSL) {
        return out;
    }

    auto list = self.try_as_list();
    if (!list.has_value()) {
        return out;
    }
    const size_t n = list->count();
    for (size_t i = 0; i < n; ++i) {
        auto child = list->at(i);
        if (include(child)) {
            out.append(nb::int_(i));
        }
    }
    return out;
}

template <typename ViewT, typename IncludeFn>
nb::list tsb_keys_filtered(const ViewT& self, IncludeFn&& include) {
    nb::list out;
    const TSMeta* meta = self.as_ts_view().ts_meta();
    if (meta == nullptr || meta->kind != TSKind::TSB || meta->fields() == nullptr) {
        return out;
    }

    auto bundle = self.try_as_bundle();
    if (!bundle.has_value()) {
        return out;
    }

    for (size_t i = 0; i < meta->field_count(); ++i) {
        auto child = bundle->at(i);
        if (!include(child)) {
            continue;
        }
        const char* name = meta->fields()[i].name;
        if (name != nullptr) {
            out.append(nb::str(name));
        }
    }
    return out;
}

nb::list tsl_modified_keys_input(const TSInputView& self) {
    return tsl_keys_filtered(self, [](const TSInputView& child) { return child.modified(); });
}

nb::list tsl_modified_keys_output(const TSOutputView& self) {
    return tsl_keys_filtered(self, [](const TSOutputView& child) { return child.modified(); });
}

nb::list tsb_modified_keys_input(const TSInputView& self) {
    return tsb_keys_filtered(self, [](const TSInputView& child) { return child.modified(); });
}

nb::list tsb_modified_keys_output(const TSOutputView& self) {
    return tsb_keys_filtered(self, [](const TSOutputView& child) { return child.modified(); });
}

nb::list tsd_valid_keys_input(const TSInputView& self) {
    return tsd_keys_filtered(
        self,
        [](const TSInputView& parent, const nb::object& key_obj) { return dict_input_at_key_object(parent, key_obj); },
        [](const TSInputView& child) { return child.valid(); });
}

nb::list tsd_valid_keys_output(const TSOutputView& self) {
    return tsd_keys_filtered(
        self,
        [](const TSOutputView& parent, const nb::object& key_obj) { return dict_output_at_key_object(parent, key_obj); },
        [](const TSOutputView& child) { return child.valid(); });
}

nb::list tsl_valid_keys_input(const TSInputView& self) {
    return tsl_keys_filtered(self, [](const TSInputView& child) { return child.valid(); });
}

nb::list tsl_valid_keys_output(const TSOutputView& self) {
    return tsl_keys_filtered(self, [](const TSOutputView& child) { return child.valid(); });
}

nb::list tsb_valid_keys_input(const TSInputView& self) {
    return tsb_keys_filtered(self, [](const TSInputView& child) { return child.valid(); });
}

nb::list tsb_valid_keys_output(const TSOutputView& self) {
    return tsb_keys_filtered(self, [](const TSOutputView& child) { return child.valid(); });
}

nb::object set_delta_attr_or_empty(const nb::object& delta, const char* attr_name) {
    if (delta.is_none()) {
        return nb::set{};
    }

    nb::object attr = nb::getattr(delta, attr_name, nb::none());
    if (attr.is_none()) {
        return nb::set{};
    }
    return attr;
}

bool set_output_add(TSOutputView& self, const nb::object& elem_obj) {
    if (self.as_ts_view().kind() != TSKind::TSS) {
        return false;
    }

    nb::set values;
    values.add(elem_obj);
    self.from_python(values);
    return true;
}

bool set_output_remove(TSOutputView& self, const nb::object& elem_obj) {
    if (self.as_ts_view().kind() != TSKind::TSS) {
        return false;
    }

    nb::object removed_cls = nb::module_::import_("hgraph").attr("Removed");
    nb::set values;
    values.add(removed_cls(elem_obj));
    self.from_python(values);
    return true;
}

struct ContainsOutputState {
    std::shared_ptr<TSValue> output_value;
    bool has_cached_value{false};
    bool cached_value{false};
    std::unordered_set<const void*> requesters{};
};

std::optional<value::Value> value_from_python(const value::TypeMeta* schema, const nb::object& obj) {
    if (schema == nullptr) {
        return std::nullopt;
    }

    value::Value out(schema);
    out.emplace();
    try {
        schema->ops().from_python(out.data(), obj, schema);
        return out;
    } catch (...) {
        return std::nullopt;
    }
}

bool view_data_identity_equals(const ViewData& lhs, const ViewData& rhs) {
    return lhs.path.indices == rhs.path.indices &&
           lhs.value_data == rhs.value_data &&
           lhs.time_data == rhs.time_data &&
           lhs.observer_data == rhs.observer_data &&
           lhs.delta_data == rhs.delta_data &&
           lhs.link_data == rhs.link_data &&
           lhs.projection == rhs.projection &&
           lhs.ops == rhs.ops &&
           lhs.meta == rhs.meta;
}

struct TSDRefOutputState {
    std::shared_ptr<TSValue> output_value;
    std::unordered_set<const void*> requesters{};
    bool has_cached_target{false};
    bool cached_has_target{false};
    ViewData cached_target{};
};

class TSDRefFeatureObserver final : public LinkTarget {
public:
    explicit TSDRefFeatureObserver(ViewData source)
        : source_(std::move(source)) {
        TSView source_view(source_, MIN_DT);
        const TSMeta* meta = source_view.ts_meta();
        if (meta != nullptr && meta->kind == TSKind::TSD) {
            key_type_ = meta->key_type();
            element_meta_ = meta->element_ts();
            if (element_meta_ != nullptr) {
                ref_meta_ = TSTypeRegistry::instance().ref(element_meta_);
            }
        }

        bind(source_, MIN_DT);
        register_ts_link_observer(*this);
        registered_ = true;
    }

    ~TSDRefFeatureObserver() override {
        detach();
    }

    void detach() {
        if (!registered_) {
            return;
        }
        unregister_ts_link_observer(*this);
        registered_ = false;
    }

    TSOutputView get_ref_output(const nb::object& key_obj, const nb::object& requester, engine_time_t current_time) {
        if (key_type_ == nullptr || ref_meta_ == nullptr) {
            return {};
        }

        value::Value key_value(key_type_);
        key_value.emplace();
        key_type_->ops().from_python(key_value.data(), key_obj, key_type_);
        const value::View key_view = key_value.view();

        auto it = ref_outputs_.find(key_view);
        if (it == ref_outputs_.end()) {
            TSDRefOutputState state{};
            state.output_value = make_ref_output();
            auto [inserted_it, inserted] = ref_outputs_.emplace(std::move(key_value), std::move(state));
            (void)inserted;
            it = inserted_it;
        }

        it->second.requesters.insert(requester.ptr());
        update_ref_output(it->first.view(), it->second, current_time);
        return TSOutputView(nullptr, it->second.output_value->ts_view(current_time));
    }

    void release_ref_output(const nb::object& key_obj, const nb::object& requester) {
        if (key_type_ == nullptr) {
            return;
        }

        value::Value key_value(key_type_);
        key_value.emplace();
        key_type_->ops().from_python(key_value.data(), key_obj, key_type_);

        auto it = ref_outputs_.find(key_value.view());
        if (it == ref_outputs_.end()) {
            return;
        }
        it->second.requesters.erase(requester.ptr());
        if (it->second.requesters.empty()) {
            ref_outputs_.erase(it);
        }
    }

    [[nodiscard]] bool has_consumers() const {
        return !ref_outputs_.empty();
    }

    void notify(engine_time_t et) override {
        for (auto& [key, state] : ref_outputs_) {
            update_ref_output(key.view(), state, et);
        }
    }

private:
    [[nodiscard]] std::shared_ptr<TSValue> make_ref_output() const {
        if (ref_meta_ == nullptr) {
            return {};
        }
        auto out = std::make_shared<TSValue>(ref_meta_);
        out->set_link_observer_registry(output_registry_.get());
        return out;
    }

    [[nodiscard]] std::optional<ViewData> resolve_target_for_key(const value::View& key, engine_time_t current_time) const {
        TSView source_view(source_, current_time);
        if (!source_view || source_view.kind() != TSKind::TSD) {
            return std::nullopt;
        }

        TSView child = source_view.child_by_key(key);
        if (!child) {
            return std::nullopt;
        }
        return child.view_data();
    }

    void set_output_reference(const std::shared_ptr<TSValue>& output_value,
                              const std::optional<ViewData>& target,
                              engine_time_t current_time) const {
        if (!output_value) {
            return;
        }
        TSView out = output_value->ts_view(current_time);
        if (target.has_value()) {
            out.from_python(nb::cast(TimeSeriesReference::make(*target)));
            return;
        }
        out.from_python(nb::cast(TimeSeriesReference::make()));
    }

    void update_ref_output(const value::View& key, TSDRefOutputState& state, engine_time_t current_time) {
        const std::optional<ViewData> target = resolve_target_for_key(key, current_time);
        const bool target_present = target.has_value();

        bool changed = !state.has_cached_target;
        if (!changed && state.cached_has_target != target_present) {
            changed = true;
        }
        if (!changed && target_present) {
            changed = !view_data_identity_equals(state.cached_target, *target);
        }

        if (!changed) {
            return;
        }

        set_output_reference(state.output_value, target, current_time);
        state.has_cached_target = true;
        state.cached_has_target = target_present;
        if (target_present) {
            state.cached_target = *target;
        } else {
            state.cached_target = {};
        }
    }

    ViewData source_{};
    const value::TypeMeta* key_type_{nullptr};
    const TSMeta* element_meta_{nullptr};
    const TSMeta* ref_meta_{nullptr};
    std::unordered_map<value::Value, TSDRefOutputState, ValueHash, ValueEqual> ref_outputs_{};
    bool registered_{false};
    std::shared_ptr<TSLinkObserverRegistry> output_registry_{std::make_shared<TSLinkObserverRegistry>()};
};

class TSSFeatureObserver final : public LinkTarget {
public:
    explicit TSSFeatureObserver(ViewData source)
        : source_(std::move(source)) {
        TSView source_view(source_, MIN_DT);
        const TSMeta* meta = source_view.ts_meta();
        if (meta != nullptr && meta->kind == TSKind::TSS && meta->value_type != nullptr) {
            // TSS element type is carried on the value schema (set[element_type]).
            element_type_ = meta->value_type->element_type;
        }

        bind(source_, MIN_DT);
        register_ts_link_observer(*this);
        registered_ = true;
    }

    ~TSSFeatureObserver() override {
        detach();
    }

    void detach() {
        if (!registered_) {
            return;
        }
        unregister_ts_link_observer(*this);
        registered_ = false;
    }

    TSOutputView get_contains_output(const nb::object& item, const nb::object& requester, engine_time_t current_time) {
        auto maybe_key = value_from_python(resolve_element_type(current_time), item);
        if (!maybe_key.has_value()) {
            return {};
        }

        const value::View key_view = maybe_key->view();
        const void* requester_key = requester.ptr();
        auto it = contains_outputs_.find(key_view);
        if (it == contains_outputs_.end()) {
            ContainsOutputState state{};
            state.output_value = make_bool_output();
            auto [inserted_it, inserted] = contains_outputs_.emplace(std::move(*maybe_key), std::move(state));
            (void)inserted;
            it = inserted_it;
        }

        it->second.requesters.insert(requester_key);
        update_contains_output(it->first.view(), it->second, current_time);
        return TSOutputView(nullptr, it->second.output_value->ts_view(current_time));
    }

    void release_contains_output(const nb::object& item, const nb::object& requester, engine_time_t current_time) {
        (void)current_time;
        auto maybe_key = value_from_python(element_type_, item);
        if (!maybe_key.has_value()) {
            return;
        }

        auto it = contains_outputs_.find(maybe_key->view());
        if (it == contains_outputs_.end()) {
            return;
        }

        it->second.requesters.erase(requester.ptr());
    }

    TSOutputView get_is_empty_output(engine_time_t current_time) {
        has_is_empty_consumer_ = true;
        if (!is_empty_output_) {
            is_empty_output_ = make_bool_output();
        }
        update_is_empty_output(current_time);
        return TSOutputView(nullptr, is_empty_output_->ts_view(current_time));
    }

    bool has_consumers() const {
        if (has_is_empty_consumer_) {
            return true;
        }
        return !contains_outputs_.empty();
    }

    void notify(engine_time_t et) override {
        for (auto& [key, state] : contains_outputs_) {
            update_contains_output(key.view(), state, et);
        }
        update_is_empty_output(et);
    }

private:
    [[nodiscard]] const value::TypeMeta* resolve_element_type(engine_time_t current_time) const {
        TSView source_view(source_, current_time);
        value::View source_value = source_view.value();
        if (!source_value.valid()) {
            return element_type_;
        }
        if (source_.projection == ViewProjection::TSD_KEY_SET && source_value.is_map()) {
            return source_value.as_map().key_type();
        }
        if (source_value.is_set()) {
            return source_value.as_set().element_type();
        }
        return element_type_;
    }

    std::shared_ptr<TSValue> make_bool_output() const {
        const TSMeta* bool_meta = TSTypeRegistry::instance().ts(value::scalar_type_meta<bool>());
        auto out = std::make_shared<TSValue>(bool_meta);
        out->set_link_observer_registry(output_registry_.get());
        return out;
    }

    bool contains_key(const value::View& key, engine_time_t current_time) const {
        TSView source_view(source_, current_time);
        nb::object source_obj = source_view.to_python();
        if (source_obj.is_none()) {
            return false;
        }

        nb::object key_obj = key.to_python();
        const int contains = PySequence_Contains(source_obj.ptr(), key_obj.ptr());
        if (contains < 0) {
            PyErr_Clear();
            return false;
        }
        return contains == 1;
    }

    bool is_empty(engine_time_t current_time) const {
        TSView source_view(source_, current_time);
        nb::object source_obj = source_view.to_python();
        if (source_obj.is_none()) {
            return true;
        }

        const Py_ssize_t size = PyObject_Size(source_obj.ptr());
        if (size < 0) {
            PyErr_Clear();
            return true;
        }
        return size == 0;
    }

    void set_output_value(const std::shared_ptr<TSValue>& output_value, bool value, engine_time_t current_time) const {
        if (!output_value) {
            return;
        }
        TSView out = output_value->ts_view(current_time);
        out.from_python(nb::bool_(value));
    }

    void update_contains_output(const value::View& key, ContainsOutputState& state, engine_time_t current_time) {
        const bool contains = contains_key(key, current_time);
        if (!state.has_cached_value || state.cached_value != contains) {
            set_output_value(state.output_value, contains, current_time);
            state.has_cached_value = true;
            state.cached_value = contains;
        }
    }

    void update_is_empty_output(engine_time_t current_time) {
        if (!has_is_empty_consumer_ || !is_empty_output_) {
            return;
        }
        const bool empty = is_empty(current_time);
        if (!has_is_empty_cached_ || is_empty_cached_ != empty) {
            set_output_value(is_empty_output_, empty, current_time);
            has_is_empty_cached_ = true;
            is_empty_cached_ = empty;
        }
    }

    ViewData source_{};
    const value::TypeMeta* element_type_{nullptr};

    std::unordered_map<value::Value, ContainsOutputState, ValueHash, ValueEqual> contains_outputs_{};

    std::shared_ptr<TSValue> is_empty_output_{};
    bool has_is_empty_consumer_{false};
    bool has_is_empty_cached_{false};
    bool is_empty_cached_{false};
    bool registered_{false};
    std::shared_ptr<TSLinkObserverRegistry> output_registry_{std::make_shared<TSLinkObserverRegistry>()};
};

std::string tss_source_runtime_key_for(const TSOutputView& self) {
    const ViewData& vd = self.as_ts_view().view_data();
    std::string key;
    key.reserve(64 + vd.path.indices.size() * 12);
    key.append("value:");
    key.append(std::to_string(reinterpret_cast<uintptr_t>(vd.value_data)));
    key.append("|proj:");
    key.append(std::to_string(static_cast<unsigned>(vd.projection)));
    key.append("|path");
    for (size_t index : vd.path.indices) {
        key.push_back('/');
        key.append(std::to_string(index));
    }
    return key;
}

std::string tsd_ref_source_runtime_key_for(const TSOutputView& self) {
    return std::string("tsd_ref:") + tss_source_runtime_key_for(self);
}

std::shared_ptr<TSSFeatureObserver> ensure_tss_feature_observer(const TSOutputView& self) {
    TSLinkObserverRegistry* registry = self.as_ts_view().view_data().link_observer_registry;
    if (registry == nullptr) {
        return {};
    }

    const std::string key = tss_source_runtime_key_for(self);
    std::shared_ptr<void> existing = registry->feature_state(key);
    if (existing) {
        return std::static_pointer_cast<TSSFeatureObserver>(std::move(existing));
    }

    auto observer = std::make_shared<TSSFeatureObserver>(self.as_ts_view().view_data());
    registry->set_feature_state(key, observer);
    return observer;
}

std::shared_ptr<TSDRefFeatureObserver> ensure_tsd_ref_feature_observer(const TSOutputView& self) {
    if (self.as_ts_view().kind() != TSKind::TSD) {
        return {};
    }

    TSLinkObserverRegistry* registry = self.as_ts_view().view_data().link_observer_registry;
    if (registry == nullptr) {
        return {};
    }

    const std::string key = tsd_ref_source_runtime_key_for(self);
    std::shared_ptr<void> existing = registry->feature_state(key);
    if (existing) {
        return std::static_pointer_cast<TSDRefFeatureObserver>(std::move(existing));
    }

    auto observer = std::make_shared<TSDRefFeatureObserver>(self.as_ts_view().view_data());
    registry->set_feature_state(key, observer);
    return observer;
}

TSOutputView tsd_get_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester) {
    auto observer = ensure_tsd_ref_feature_observer(self);
    if (!observer) {
        return {};
    }
    return observer->get_ref_output(key, requester, self.current_time());
}

void tsd_release_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester) {
    if (self.as_ts_view().kind() != TSKind::TSD) {
        return;
    }

    TSLinkObserverRegistry* registry = self.as_ts_view().view_data().link_observer_registry;
    if (registry == nullptr) {
        return;
    }

    const std::string feature_key = tsd_ref_source_runtime_key_for(self);
    std::shared_ptr<void> existing = registry->feature_state(feature_key);
    if (!existing) {
        return;
    }

    auto observer = std::static_pointer_cast<TSDRefFeatureObserver>(std::move(existing));
    observer->release_ref_output(key, requester);
    if (!observer->has_consumers()) {
        observer->detach();
        registry->clear_feature_state(feature_key);
    }
}

TSOutputView tss_get_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester) {
    if (self.as_ts_view().kind() != TSKind::TSS) {
        return {};
    }
    auto observer = ensure_tss_feature_observer(self);
    if (!observer) {
        return {};
    }
    return observer->get_contains_output(item, requester, self.current_time());
}

void tss_release_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester) {
    if (self.as_ts_view().kind() != TSKind::TSS) {
        return;
    }

    TSLinkObserverRegistry* registry = self.as_ts_view().view_data().link_observer_registry;
    if (registry == nullptr) {
        return;
    }

    const std::string key = tss_source_runtime_key_for(self);
    std::shared_ptr<void> existing = registry->feature_state(key);
    if (!existing) {
        return;
    }

    auto observer = std::static_pointer_cast<TSSFeatureObserver>(std::move(existing));
    observer->release_contains_output(item, requester, self.current_time());
    if (!observer->has_consumers()) {
        observer->detach();
        registry->clear_feature_state(key);
    }
}

TSOutputView tss_get_is_empty_output(TSOutputView& self) {
    if (self.as_ts_view().kind() != TSKind::TSS) {
        return {};
    }
    auto observer = ensure_tss_feature_observer(self);
    if (!observer) {
        return {};
    }
    return observer->get_is_empty_output(self.current_time());
}

void clear_output(TSOutputView& self) {
    switch (self.as_ts_view().kind()) {
        case TSKind::TSS: {
            self.from_python(nb::frozenset(nb::set{}));
            return;
        }

        case TSKind::TSL: {
            auto list = self.try_as_list();
            if (!list.has_value()) {
                return;
            }
            const size_t n = list->count();
            for (size_t i = 0; i < n; ++i) {
                TSOutputView child = list->at(i);
                clear_output(child);
            }
            return;
        }

        case TSKind::TSB: {
            auto bundle = self.try_as_bundle();
            if (!bundle.has_value()) {
                return;
            }
            const size_t n = bundle->count();
            for (size_t i = 0; i < n; ++i) {
                TSOutputView child = bundle->at(i);
                clear_output(child);
            }
            return;
        }

        case TSKind::TSD: {
            auto dict = self.try_as_dict();
            if (!dict.has_value()) {
                return;
            }

            nb::list keys = tsd_keys_python(self.as_ts_view());
            for (const auto& key : keys) {
                nb::object key_obj = nb::cast<nb::object>(key);
                auto key_value = dict_key_value_from_python(self.as_ts_view(), key_obj);
                if (key_value.has_value()) {
                    dict->remove(key_value->view());
                }
            }
            return;
        }

        default:
            return;
    }
}

}  // namespace

TSOutputView runtime_tsd_get_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester) {
    return tsd_get_ref_output(self, key, requester);
}

void runtime_tsd_release_ref_output(TSOutputView& self, const nb::object& key, const nb::object& requester) {
    tsd_release_ref_output(self, key, requester);
}

TSOutputView runtime_tss_get_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester) {
    return tss_get_contains_output(self, item, requester);
}

void runtime_tss_release_contains_output(TSOutputView& self, const nb::object& item, const nb::object& requester) {
    tss_release_contains_output(self, item, requester);
}

TSOutputView runtime_tss_get_is_empty_output(TSOutputView& self) {
    return tss_get_is_empty_output(self);
}

void reset_ts_runtime_feature_observers() {
    // Runtime observers are stored on endpoint registries and cleaned up with endpoint lifetime.
}

void ts_runtime_internal_register_with_nanobind(nb::module_& m) {
    using namespace nanobind::literals;

    auto test_mod = m.def_submodule("_ts_runtime", "Private TS runtime scaffolding bindings for tests");

    test_mod.def(
        "ops_ptr_for_kind",
        [](TSKind kind) { return reinterpret_cast<uintptr_t>(get_ts_ops(kind)); },
        "kind"_a);
    test_mod.def(
        "ops_ptr_for_meta",
        [](const TSMeta* meta) { return reinterpret_cast<uintptr_t>(get_ts_ops(meta)); },
        "meta"_a);
    test_mod.def(
        "ops_kind_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->kind; },
        "kind"_a);
    test_mod.def(
        "ops_has_window_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->window_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_set_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->set_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_dict_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->dict_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_list_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->list_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "ops_has_bundle_for_kind",
        [](TSKind kind) { return get_ts_ops(kind)->bundle_ops() != nullptr; },
        "kind"_a);
    test_mod.def(
        "schema_value_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).value_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_time_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).time_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_observer_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).observer_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_delta_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).delta_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_link_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).link_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_input_link_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).input_link_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);
    test_mod.def(
        "schema_active_meta",
        [](const TSMeta* meta) -> const value::TypeMeta* {
            if (meta == nullptr) {
                return nullptr;
            }
            return TSMetaSchemaCache::instance().get(meta).active_schema;
        },
        "meta"_a,
        nb::rv_policy::reference);

    nb::class_<TSOutput>(test_mod, "TSOutput")
        .def("__init__", [](TSOutput* self, const TSMeta* meta, size_t port_index) {
            new (self) TSOutput(meta, nullptr, port_index);
        }, "meta"_a, "port_index"_a = 0)
        .def("output_view",
             [](TSOutput& self, engine_time_t current_time) { return self.output_view(current_time); },
             "current_time"_a, nb::keep_alive<0, 1>())
        .def("output_view",
             [](TSOutput& self, engine_time_t current_time, const TSMeta* schema) {
                 return self.output_view(current_time, schema);
             },
             "current_time"_a, "schema"_a, nb::keep_alive<0, 1>());

    nb::class_<TSInput>(test_mod, "TSInput")
        .def("__init__", [](TSInput* self, const TSMeta* meta) {
            new (self) TSInput(meta, nullptr);
        }, "meta"_a)
        .def("input_view",
             [](TSInput& self, engine_time_t current_time) { return self.input_view(current_time); },
             "current_time"_a, nb::keep_alive<0, 1>())
        .def("input_view",
             [](TSInput& self, engine_time_t current_time, const TSMeta* schema) {
                 return self.input_view(current_time, schema);
             },
             "current_time"_a, "schema"_a, nb::keep_alive<0, 1>())
        .def("bind", &TSInput::bind, "output"_a, "current_time"_a)
        .def("unbind", &TSInput::unbind, "current_time"_a)
        .def("set_active", nb::overload_cast<bool>(&TSInput::set_active), "active"_a)
        .def("active", nb::overload_cast<>(&TSInput::active, nb::const_))
        .def("active_at", [](const TSInput& self, const TSInputView& view) {
            return self.active(view.as_ts_view());
        }, "view"_a);

    auto ts_view_cls = nb::class_<TSView>(test_mod, "TSView")
        .def("__bool__", [](const TSView& self) { return static_cast<bool>(self); })
        .def("fq_path_str", [](const TSView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSView& self) { return self.short_path().indices; })
        .def("at", [](const TSView& self, size_t index) { return self.child_at(index); }, "index"_a, nb::keep_alive<0, 1>())
        .def("field", [](const TSView& self, std::string_view name) { return self.child_by_name(name); }, "name"_a, nb::keep_alive<0, 1>())
        .def("at_key", [](const TSView& self, const value::View& key) { return self.child_by_key(key); }, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSView::child_count)
        .def("size", &TSView::size)
        .def("to_python", &TSView::to_python)
        .def("delta_to_python", &TSView::delta_to_python)
        .def("set_value", &TSView::set_value, "value"_a)
        .def("from_python", &TSView::from_python, "value"_a)
        .def("kind", &TSView::kind)
        .def("is_window", &TSView::is_window)
        .def("is_set", &TSView::is_set)
        .def("is_dict", &TSView::is_dict)
        .def("is_list", &TSView::is_list)
        .def("is_bundle", &TSView::is_bundle);

    nb::class_<TSWView, TSView>(test_mod, "TSWView")
        .def("__bool__", [](const TSWView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSWView& self) { return self.to_python(); })
        .def("value_times_count", &TSWView::value_times_count)
        .def("value_times", [](const TSWView& self) {
            std::vector<engine_time_t> out;
            const engine_time_t* times = self.value_times();
            const size_t count = self.value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("first_modified_time", &TSWView::first_modified_time)
        .def("has_removed_value", &TSWView::has_removed_value)
        .def("removed_value", &TSWView::removed_value)
        .def("removed_value_count", &TSWView::removed_value_count)
        .def("size", &TSWView::size)
        .def("min_size", &TSWView::min_size)
        .def("length", &TSWView::length);

    nb::class_<TSSView, TSView>(test_mod, "TSSView")
        .def("__bool__", [](const TSSView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSSView& self) { return self.to_python(); })
        .def("add", &TSSView::add, "elem"_a)
        .def("remove", &TSSView::remove, "elem"_a)
        .def("clear", &TSSView::clear);

    nb::class_<TSDView, TSView>(test_mod, "TSDView")
        .def("__bool__", [](const TSDView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSDView& self) { return self.to_python(); })
        .def("at_key", &TSDView::by_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDView::count)
        .def("size", &TSDView::size)
        .def("remove", &TSDView::remove, "key"_a)
        .def("create", &TSDView::create, "key"_a, nb::keep_alive<0, 1>())
        .def("set", &TSDView::set, "key"_a, "value"_a, nb::keep_alive<0, 1>());

    nb::class_<TSIndexedView, TSView>(test_mod, "TSIndexedView")
        .def("__bool__", [](const TSIndexedView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSIndexedView& self) { return self.to_python(); })
        .def("at", &TSIndexedView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSIndexedView::count)
        .def("size", &TSIndexedView::size);

    nb::class_<TSLView, TSIndexedView>(test_mod, "TSLView")
        .def("__bool__", [](const TSLView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSLView& self) { return self.to_python(); });

    nb::class_<TSBView, TSIndexedView>(test_mod, "TSBView")
        .def("__bool__", [](const TSBView& self) { return static_cast<bool>(self); })
        .def("to_python", [](const TSBView& self) { return self.to_python(); })
        .def("field", &TSBView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("at", [](const TSBView& self, size_t index) { return self.TSIndexedView::at(index); },
             "index"_a, nb::keep_alive<0, 1>())
        .def("at", nb::overload_cast<std::string_view>(&TSBView::at, nb::const_), "name"_a, nb::keep_alive<0, 1>());

    ts_view_cls
        .def("try_as_indexed", &TSView::try_as_indexed, nb::keep_alive<0, 1>())
        .def("try_as_window", &TSView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_indexed", &TSView::as_indexed, nb::keep_alive<0, 1>())
        .def("as_window", &TSView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSView::as_bundle, nb::keep_alive<0, 1>());

    auto ts_output_view_cls = nb::class_<TSOutputView>(test_mod, "TSOutputView")
        .def("__bool__", [](const TSOutputView& self) { return static_cast<bool>(self); })
        .def("__len__", [](const TSOutputView& self) { return self.as_ts_view().child_count(); })
        .def("__getitem__", [](const TSOutputView& self, size_t index) {
            if (!self.as_ts_view().try_as_indexed().has_value()) {
                throw nb::index_error();
            }
            TSIndexedOutputView indexed = TSIndexedOutputView(self);
            if (index >= indexed.count()) {
                throw nb::index_error();
            }
            return indexed.at(index);
        }, "index"_a,
             nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSOutputView& self, std::string_view name) {
            if (self.as_ts_view().kind() == TSKind::TSB) {
                return self.as_bundle().field(name);
            }
            if (self.as_ts_view().kind() == TSKind::TSD) {
                return dict_output_at_key_object(self, nb::str(std::string(name).c_str()));
            }
            return TSOutputView{};
        }, "name"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSOutputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->at_key(key) : TSOutputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSOutputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() == TSKind::TSD) {
                return dict_output_at_key_object(self, key);
            }
            return TSOutputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("__setitem__", [](TSOutputView& self, const nb::object& key, const nb::object& value_obj) {
            if (self.as_ts_view().kind() != TSKind::TSD) {
                return;
            }
            auto key_value = dict_key_value_from_python(self.as_ts_view(), key);
            if (!key_value.has_value()) {
                return;
            }
            auto dict = self.try_as_dict();
            TSOutputView child = dict.has_value() ? dict->create(key_value->view()) : TSOutputView{};
            if (child) {
                child.from_python(value_obj);
            }
        }, "key"_a, "value"_a)
        .def("__delitem__", [](TSOutputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() != TSKind::TSD) {
                return;
            }
            auto key_value = dict_key_value_from_python(self.as_ts_view(), key);
            if (!key_value.has_value()) {
                return;
            }
            auto dict = self.try_as_dict();
            if (dict.has_value()) {
                dict->remove(key_value->view());
            }
        }, "key"_a)
        .def("__contains__", [](const TSOutputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() != TSKind::TSD) {
                return false;
            }
            return dict_contains_python_key(self.as_ts_view(), key);
        }, "key"_a)
        .def("fq_path_str", [](const TSOutputView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSOutputView& self) { return self.short_path().indices; })
        .def("set_current_time", &TSOutputView::set_current_time, "time"_a)
        .def("set_value", &TSOutputView::set_value, "value"_a)
        .def("to_python", [](const TSOutputView& self) { return self.to_python(); })
        .def("delta_to_python", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def("py_value", [](const TSOutputView& self) { return self.to_python(); })
        .def("py_delta_value", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def_prop_rw("value",
            [](const TSOutputView& self) { return self.to_python(); },
            [](TSOutputView& self, const nb::object& value_obj) { self.from_python(value_obj); })
        .def_prop_ro("delta_value", [](const TSOutputView& self) { return self.delta_to_python(); })
        .def_prop_ro("valid", &TSOutputView::valid)
        .def_prop_ro("all_valid", &TSOutputView::all_valid)
        .def_prop_ro("modified", &TSOutputView::modified)
        .def_prop_ro("last_modified_time", &TSOutputView::last_modified_time)
        .def("from_python", [](TSOutputView& self, const nb::object& value_obj) {
            self.from_python(value_obj);
        }, "value"_a)
        .def("copy_from_input", [](TSOutputView& self, const TSInputView& input) {
            self.copy_from_input(input);
        }, "input"_a)
        .def("copy_from_output", [](TSOutputView& self, const TSOutputView& output) {
            self.copy_from_output(output);
        }, "output"_a)
        .def("apply_result", [](TSOutputView& self, const nb::object& value_obj) {
            if (value_obj.is_none()) {
                return;
            }
            self.from_python(value_obj);
        }, "value"_a)
        .def("can_apply_result", [](const TSOutputView& self, const nb::object&) {
            return static_cast<bool>(self);
        }, "value"_a)
        .def("invalidate", &TSOutputView::invalidate)
        .def("sampled", [](const TSOutputView& self) { return self.as_ts_view().sampled(); })
        .def("kind", [](const TSOutputView& self) { return self.as_ts_view().kind(); })
        .def("is_window", [](const TSOutputView& self) { return self.as_ts_view().is_window(); })
        .def("is_set", [](const TSOutputView& self) { return self.as_ts_view().is_set(); })
        .def("is_dict", [](const TSOutputView& self) { return self.as_ts_view().is_dict(); })
        .def("is_list", [](const TSOutputView& self) { return self.as_ts_view().is_list(); })
        .def("is_bundle", [](const TSOutputView& self) { return self.as_ts_view().is_bundle(); })
        .def_prop_ro("is_reference", [](const TSOutputView& self) { return self.as_ts_view().kind() == TSKind::REF; })
        .def("set_sampled", [](TSOutputView& self, bool sampled) {
            self.as_ts_view().view_data().sampled = sampled;
        }, "sampled"_a)
        .def("is_bound", [](const TSOutputView& self) { return self.as_ts_view().is_bound(); })
        .def("has_window_ops", [](const TSOutputView& self) { return self.try_as_window().has_value(); })
        .def("window_value_times_count", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->value_times_count() : size_t{0};
        })
        .def("window_value_times", [](const TSOutputView& self) {
            std::vector<engine_time_t> out;
            auto window = self.try_as_window();
            if (!window.has_value()) {
                return out;
            }
            const engine_time_t* times = window->value_times();
            const size_t count = window->value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("window_first_modified_time", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->first_modified_time() : MIN_DT;
        })
        .def("window_has_removed_value", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() && window->has_removed_value();
        })
        .def("window_removed_value_count", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->removed_value_count() : size_t{0};
        })
        .def("window_size", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->size() : size_t{0};
        })
        .def("window_min_size", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->min_size() : size_t{0};
        })
        .def("window_length", [](const TSOutputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->length() : size_t{0};
        })
        .def("has_set_ops", [](const TSOutputView& self) { return self.try_as_set().has_value(); })
        .def("set_add", [](TSOutputView& self, const value::View& elem) {
            auto set = self.try_as_set();
            return set.has_value() && set->add(elem);
        }, "elem"_a)
        .def("set_remove", [](TSOutputView& self, const value::View& elem) {
            auto set = self.try_as_set();
            return set.has_value() && set->remove(elem);
        }, "elem"_a)
        .def("set_clear", [](TSOutputView& self) {
            auto set = self.try_as_set();
            if (set.has_value()) {
                set->clear();
            }
        })
        .def("add", [](TSOutputView& self, const nb::object& elem_obj) {
            return set_output_add(self, elem_obj);
        }, "elem"_a)
        .def("remove", [](TSOutputView& self, const nb::object& elem_obj) {
            if (self.as_ts_view().kind() == TSKind::TSS) {
                return set_output_remove(self, elem_obj);
            }
            if (self.as_ts_view().kind() == TSKind::TSD) {
                auto key_value = dict_key_value_from_python(self.as_ts_view(), elem_obj);
                if (!key_value.has_value()) {
                    return false;
                }
                auto dict = self.try_as_dict();
                return dict.has_value() && dict->remove(key_value->view());
            }
            return false;
        }, "elem"_a)
        .def("clear", [](TSOutputView& self) {
            clear_output(self);
        })
        .def("added", [](const TSOutputView& self) -> nb::object {
            if (self.as_ts_view().kind() != TSKind::TSS) {
                return nb::set{};
            }
            return set_delta_attr_or_empty(self.delta_to_python(), "added");
        })
        .def("removed", [](const TSOutputView& self) -> nb::object {
            if (self.as_ts_view().kind() != TSKind::TSS) {
                return nb::set{};
            }
            return set_delta_attr_or_empty(self.delta_to_python(), "removed");
        })
        .def("has_dict_ops", [](const TSOutputView& self) { return self.try_as_dict().has_value(); })
        .def("dict_remove", [](TSOutputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() && dict->remove(key);
        }, "key"_a)
        .def("dict_create", [](TSOutputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->create(key) : TSOutputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("dict_set", [](TSOutputView& self, const value::View& key, const value::View& value_view) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->set(key, value_view) : TSOutputView{};
        }, "key"_a, "value"_a, nb::keep_alive<0, 1>())
        .def("get_ref", [](TSOutputView& self, const nb::object& key, const nb::object& requester) {
            return tsd_get_ref_output(self, key, requester);
        }, "key"_a, "requester"_a, nb::keep_alive<0, 1>())
        .def("release_ref", [](TSOutputView& self, const nb::object& key, const nb::object& requester) {
            tsd_release_ref_output(self, key, requester);
        }, "key"_a, "requester"_a)
        .def("get_or_create", [](TSOutputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() != TSKind::TSD) {
                return TSOutputView{};
            }
            auto key_value = dict_key_value_from_python(self.as_ts_view(), key);
            if (!key_value.has_value()) {
                return TSOutputView{};
            }
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->create(key_value->view()) : TSOutputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("is_empty_output", [](TSOutputView& self) {
            return tss_get_is_empty_output(self);
        }, nb::keep_alive<0, 1>())
        .def("get_contains_output", [](TSOutputView& self, const nb::object& item, const nb::object& requester) {
            return tss_get_contains_output(self, item, requester);
        }, "item"_a, "requester"_a, nb::keep_alive<0, 1>())
        .def("release_contains_output", [](TSOutputView& self, const nb::object& item, const nb::object& requester) {
            tss_release_contains_output(self, item, requester);
        }, "item"_a, "requester"_a)
        .def("linked_source_indices", [](const TSOutputView& self) {
            return linked_source_indices(self.as_ts_view());
        });

    nb::class_<TSWOutputView, TSOutputView>(test_mod, "TSWOutputView")
        .def("__bool__", [](const TSWOutputView& self) { return static_cast<bool>(self); })
        .def("value_times_count", &TSWOutputView::value_times_count)
        .def("value_times", [](const TSWOutputView& self) {
            std::vector<engine_time_t> out;
            const engine_time_t* times = self.value_times();
            const size_t count = self.value_times_count();
            if (times == nullptr || count == 0) {
                return out;
            }
            out.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                out.push_back(times[i]);
            }
            return out;
        })
        .def("first_modified_time", &TSWOutputView::first_modified_time)
        .def("has_removed_value", &TSWOutputView::has_removed_value)
        .def("removed_value_count", &TSWOutputView::removed_value_count)
        .def("size", &TSWOutputView::size)
        .def("min_size", &TSWOutputView::min_size)
        .def("length", &TSWOutputView::length);

    nb::class_<TSSOutputView, TSOutputView>(test_mod, "TSSOutputView")
        .def("__bool__", [](const TSSOutputView& self) { return static_cast<bool>(self); })
        .def("add", &TSSOutputView::add, "elem"_a)
        .def("remove", &TSSOutputView::remove, "elem"_a)
        .def("clear", &TSSOutputView::clear);

    nb::class_<TSDOutputView, TSOutputView>(test_mod, "TSDOutputView")
        .def("__bool__", [](const TSDOutputView& self) { return static_cast<bool>(self); })
        .def("at_key", &TSDOutputView::at_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDOutputView::count)
        .def("size", &TSDOutputView::size)
        .def("keys", [](const TSDOutputView& self) { return tsd_keys_python(self.as_ts_view()); })
        .def("values", [](const TSDOutputView& self) {
            nb::list keys = tsd_keys_python(self.as_ts_view());
            return tsd_output_values(self, keys);
        })
        .def("items", [](const TSDOutputView& self) {
            nb::list keys = tsd_keys_python(self.as_ts_view());
            return tsd_output_items(self, keys);
        })
        .def("valid_keys", [](const TSDOutputView& self) { return tsd_valid_keys_output(self); })
        .def("valid_values", [](const TSDOutputView& self) {
            return tsd_output_values(self, tsd_valid_keys_output(self));
        })
        .def("valid_items", [](const TSDOutputView& self) {
            return tsd_output_items(self, tsd_valid_keys_output(self));
        })
        .def("modified_keys", [](const TSDOutputView& self) { return tsd_modified_keys_python(self.as_ts_view()); })
        .def("modified_values", [](const TSDOutputView& self) {
            return tsd_output_values(self, tsd_modified_keys_python(self.as_ts_view()));
        })
        .def("modified_items", [](const TSDOutputView& self) {
            return tsd_output_items(self, tsd_modified_keys_python(self.as_ts_view()));
        })
        .def("added_keys", [](const TSDOutputView& self) { return tsd_added_keys_python(self.as_ts_view()); })
        .def("added_values", [](const TSDOutputView& self) {
            return tsd_output_values(self, tsd_added_keys_python(self.as_ts_view()));
        })
        .def("added_items", [](const TSDOutputView& self) {
            return tsd_output_items(self, tsd_added_keys_python(self.as_ts_view()));
        })
        .def("removed_keys", [](const TSDOutputView& self) { return tsd_removed_keys_python(self.as_ts_view()); })
        .def("removed_values", [](const TSDOutputView& self) {
            return tsd_output_values(self, tsd_removed_keys_python(self.as_ts_view()));
        })
        .def("removed_items", [](const TSDOutputView& self) {
            return tsd_output_items(self, tsd_removed_keys_python(self.as_ts_view()));
        })
        .def_prop_ro("key_set", [](const TSDOutputView& self) {
            return tsd_key_set_output_view(self);
        }, nb::keep_alive<0, 1>())
        .def("remove", &TSDOutputView::remove, "key"_a)
        .def("create", &TSDOutputView::create, "key"_a, nb::keep_alive<0, 1>())
        .def("set", &TSDOutputView::set, "key"_a, "value"_a, nb::keep_alive<0, 1>())
        .def("dict_remove", [](TSDOutputView& self, const value::View& key) {
            return self.remove(key);
        }, "key"_a)
        .def("dict_create", [](TSDOutputView& self, const value::View& key) {
            return self.create(key);
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("dict_set", [](TSDOutputView& self, const value::View& key, const value::View& value_view) {
            return self.set(key, value_view);
        }, "key"_a, "value"_a, nb::keep_alive<0, 1>())
        .def("get_ref", [](TSDOutputView& self, const nb::object& key, const nb::object& requester) {
            TSOutputView base = self;
            return tsd_get_ref_output(base, key, requester);
        }, "key"_a, "requester"_a, nb::keep_alive<0, 1>())
        .def("release_ref", [](TSDOutputView& self, const nb::object& key, const nb::object& requester) {
            TSOutputView base = self;
            tsd_release_ref_output(base, key, requester);
        }, "key"_a, "requester"_a)
        .def("get_or_create", [](TSDOutputView& self, const nb::object& key) {
            auto key_value = dict_key_value_from_python(self.as_ts_view(), key);
            if (!key_value.has_value()) {
                return TSOutputView{};
            }
            return self.create(key_value->view());
        }, "key"_a, nb::keep_alive<0, 1>());

    nb::class_<TSIndexedOutputView, TSOutputView>(test_mod, "TSIndexedOutputView")
        .def("__bool__", [](const TSIndexedOutputView& self) { return static_cast<bool>(self); })
        .def("at", &TSIndexedOutputView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSIndexedOutputView::count)
        .def("size", &TSIndexedOutputView::size);

    nb::class_<TSLOutputView, TSIndexedOutputView>(test_mod, "TSLOutputView")
        .def("__bool__", [](const TSLOutputView& self) { return static_cast<bool>(self); })
        .def("keys", [](const TSLOutputView& self) { return tsl_keys_python(self.as_ts_view()); })
        .def("values", [](const TSLOutputView& self) {
            nb::list keys = tsl_keys_python(self.as_ts_view());
            return tsl_output_values(self, keys);
        })
        .def("items", [](const TSLOutputView& self) {
            nb::list keys = tsl_keys_python(self.as_ts_view());
            return tsl_output_items(self, keys);
        })
        .def("valid_keys", [](const TSLOutputView& self) { return tsl_valid_keys_output(self); })
        .def("valid_values", [](const TSLOutputView& self) {
            return tsl_output_values(self, tsl_valid_keys_output(self));
        })
        .def("valid_items", [](const TSLOutputView& self) {
            return tsl_output_items(self, tsl_valid_keys_output(self));
        })
        .def("modified_keys", [](const TSLOutputView& self) { return tsl_modified_keys_output(self); })
        .def("modified_values", [](const TSLOutputView& self) {
            return tsl_output_values(self, tsl_modified_keys_output(self));
        })
        .def("modified_items", [](const TSLOutputView& self) {
            return tsl_output_items(self, tsl_modified_keys_output(self));
        });

    nb::class_<TSBOutputView, TSIndexedOutputView>(test_mod, "TSBOutputView")
        .def("__bool__", [](const TSBOutputView& self) { return static_cast<bool>(self); })
        .def("field", &TSBOutputView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("at", [](const TSBOutputView& self, size_t index) { return self.TSIndexedOutputView::at(index); },
             "index"_a, nb::keep_alive<0, 1>())
        .def("at", nb::overload_cast<std::string_view>(&TSBOutputView::at, nb::const_), "name"_a, nb::keep_alive<0, 1>())
        .def("keys", [](const TSBOutputView& self) { return tsb_keys_python(self.as_ts_view()); })
        .def("values", [](const TSBOutputView& self) {
            nb::list keys = tsb_keys_python(self.as_ts_view());
            return tsb_output_values(self, keys);
        })
        .def("items", [](const TSBOutputView& self) {
            nb::list keys = tsb_keys_python(self.as_ts_view());
            return tsb_output_items(self, keys);
        })
        .def("valid_keys", [](const TSBOutputView& self) { return tsb_valid_keys_output(self); })
        .def("valid_values", [](const TSBOutputView& self) {
            return tsb_output_values(self, tsb_valid_keys_output(self));
        })
        .def("valid_items", [](const TSBOutputView& self) {
            return tsb_output_items(self, tsb_valid_keys_output(self));
        })
        .def("modified_keys", [](const TSBOutputView& self) { return tsb_modified_keys_output(self); })
        .def("modified_values", [](const TSBOutputView& self) {
            return tsb_output_values(self, tsb_modified_keys_output(self));
        })
        .def("modified_items", [](const TSBOutputView& self) {
            return tsb_output_items(self, tsb_modified_keys_output(self));
        });

    ts_output_view_cls
        .def("try_as_window", &TSOutputView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSOutputView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSOutputView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSOutputView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSOutputView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_window", &TSOutputView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSOutputView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSOutputView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSOutputView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSOutputView::as_bundle, nb::keep_alive<0, 1>());

    auto ts_input_view_cls = nb::class_<TSInputView>(test_mod, "TSInputView")
        .def("__bool__", [](const TSInputView& self) { return static_cast<bool>(self); })
        .def("__len__", [](const TSInputView& self) { return self.as_ts_view().child_count(); })
        .def("__getitem__", [](const TSInputView& self, size_t index) {
            if (!self.as_ts_view().try_as_indexed().has_value()) {
                throw nb::index_error();
            }
            TSIndexedInputView indexed = TSIndexedInputView(self);
            if (index >= indexed.count()) {
                throw nb::index_error();
            }
            return indexed.at(index);
        }, "index"_a,
             nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSInputView& self, std::string_view name) {
            if (self.as_ts_view().kind() == TSKind::TSB) {
                return self.as_bundle().field(name);
            }
            if (self.as_ts_view().kind() == TSKind::TSD) {
                return dict_input_at_key_object(self, nb::str(std::string(name).c_str()));
            }
            return TSInputView{};
        }, "name"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSInputView& self, const value::View& key) {
            auto dict = self.try_as_dict();
            return dict.has_value() ? dict->at_key(key) : TSInputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("__getitem__", [](const TSInputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() == TSKind::TSD) {
                return dict_input_at_key_object(self, key);
            }
            return TSInputView{};
        }, "key"_a, nb::keep_alive<0, 1>())
        .def("__contains__", [](const TSInputView& self, const nb::object& key) {
            if (self.as_ts_view().kind() != TSKind::TSD) {
                return false;
            }
            return dict_contains_python_key(self.as_ts_view(), key);
        }, "key"_a)
        .def("fq_path_str", [](const TSInputView& self) { return self.fq_path().to_string(); })
        .def("short_indices", [](const TSInputView& self) { return self.short_path().indices; })
        .def("bind", &TSInputView::bind, "output"_a)
        .def("bind_output", &TSInputView::bind, "output"_a)
        .def("unbind", &TSInputView::unbind)
        .def("un_bind_output", [](TSInputView& self, bool) { self.unbind(); }, "unbind_refs"_a = true)
        .def("start", []() {})
        .def("to_python", [](const TSInputView& self) { return self.to_python(); })
        .def("delta_to_python", [](const TSInputView& self) { return self.delta_to_python(); })
        .def("py_value", [](const TSInputView& self) { return self.to_python(); })
        .def("py_delta_value", [](const TSInputView& self) { return self.delta_to_python(); })
        .def_prop_ro("value", [](const TSInputView& self) { return self.to_python(); })
        .def_prop_ro("delta_value", [](const TSInputView& self) { return self.delta_to_python(); })
        .def_prop_ro("valid", &TSInputView::valid)
        .def_prop_ro("all_valid", &TSInputView::all_valid)
        .def_prop_ro("modified", &TSInputView::modified)
        .def_prop_ro("last_modified_time", &TSInputView::last_modified_time)
        .def_prop_ro("bound", &TSInputView::is_bound)
        .def_prop_ro("has_peer", &TSInputView::is_bound)
        .def("sampled", [](const TSInputView& self) { return self.as_ts_view().sampled(); })
        .def("kind", [](const TSInputView& self) { return self.as_ts_view().kind(); })
        .def("is_window", [](const TSInputView& self) { return self.as_ts_view().is_window(); })
        .def("is_set", [](const TSInputView& self) { return self.as_ts_view().is_set(); })
        .def("is_dict", [](const TSInputView& self) { return self.as_ts_view().is_dict(); })
        .def("is_list", [](const TSInputView& self) { return self.as_ts_view().is_list(); })
        .def("is_bundle", [](const TSInputView& self) { return self.as_ts_view().is_bundle(); })
        .def("set_sampled", [](TSInputView& self, bool sampled) {
            self.as_ts_view().view_data().sampled = sampled;
        }, "sampled"_a)
        .def("is_bound", &TSInputView::is_bound)
        .def("has_window_ops", [](const TSInputView& self) { return self.try_as_window().has_value(); })
        .def("window_value_times_count", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->value_times_count() : size_t{0};
        })
        .def("window_first_modified_time", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->first_modified_time() : MIN_DT;
        })
        .def("window_has_removed_value", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() && window->has_removed_value();
        })
        .def("window_removed_value_count", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->removed_value_count() : size_t{0};
        })
        .def("window_size", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->size() : size_t{0};
        })
        .def("window_min_size", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->min_size() : size_t{0};
        })
        .def("window_length", [](const TSInputView& self) {
            auto window = self.try_as_window();
            return window.has_value() ? window->length() : size_t{0};
        })
        .def("has_set_ops", [](const TSInputView& self) { return self.try_as_set().has_value(); })
        .def("added", [](const TSInputView& self) -> nb::object {
            if (self.as_ts_view().kind() != TSKind::TSS) {
                return nb::set{};
            }
            return set_delta_attr_or_empty(self.delta_to_python(), "added");
        })
        .def("removed", [](const TSInputView& self) -> nb::object {
            if (self.as_ts_view().kind() != TSKind::TSS) {
                return nb::set{};
            }
            return set_delta_attr_or_empty(self.delta_to_python(), "removed");
        })
        .def("has_dict_ops", [](const TSInputView& self) { return self.try_as_dict().has_value(); })
        .def("make_active", &TSInputView::make_active)
        .def("make_passive", &TSInputView::make_passive)
        .def_prop_ro("active", &TSInputView::active)
        .def("linked_target_indices", [](const TSInputView& self) {
            return linked_target_indices(self.as_ts_view());
        });

    nb::class_<TSWInputView, TSInputView>(test_mod, "TSWInputView")
        .def("__bool__", [](const TSWInputView& self) { return static_cast<bool>(self); })
        .def("value_times_count", &TSWInputView::value_times_count)
        .def("first_modified_time", &TSWInputView::first_modified_time)
        .def("has_removed_value", &TSWInputView::has_removed_value)
        .def("removed_value_count", &TSWInputView::removed_value_count)
        .def("size", &TSWInputView::size)
        .def("min_size", &TSWInputView::min_size)
        .def("length", &TSWInputView::length);

    nb::class_<TSSInputView, TSInputView>(test_mod, "TSSInputView")
        .def("__bool__", [](const TSSInputView& self) { return static_cast<bool>(self); });

    nb::class_<TSDInputView, TSInputView>(test_mod, "TSDInputView")
        .def("__bool__", [](const TSDInputView& self) { return static_cast<bool>(self); })
        .def("at_key", &TSDInputView::at_key, "key"_a, nb::keep_alive<0, 1>())
        .def("count", &TSDInputView::count)
        .def("size", &TSDInputView::size)
        .def("delta_to_python", [](const TSDInputView& self) { return tsd_input_delta_to_python(self); })
        .def("py_delta_value", [](const TSDInputView& self) { return tsd_input_delta_to_python(self); })
        .def_prop_ro("delta_value", [](const TSDInputView& self) { return tsd_input_delta_to_python(self); })
        .def("keys", [](const TSDInputView& self) { return tsd_keys_python(self.as_ts_view()); })
        .def("values", [](const TSDInputView& self) {
            nb::list keys = tsd_keys_python(self.as_ts_view());
            return tsd_input_values(self, keys);
        })
        .def("items", [](const TSDInputView& self) {
            nb::list keys = tsd_keys_python(self.as_ts_view());
            return tsd_input_items(self, keys);
        })
        .def("valid_keys", [](const TSDInputView& self) { return tsd_valid_keys_input(self); })
        .def("valid_values", [](const TSDInputView& self) {
            return tsd_input_values(self, tsd_valid_keys_input(self));
        })
        .def("valid_items", [](const TSDInputView& self) {
            return tsd_input_items(self, tsd_valid_keys_input(self));
        })
        .def("modified_keys", [](const TSDInputView& self) { return tsd_modified_keys_python(self.as_ts_view()); })
        .def("modified_values", [](const TSDInputView& self) {
            return tsd_input_values(self, tsd_modified_keys_python(self.as_ts_view()));
        })
        .def("modified_items", [](const TSDInputView& self) {
            return tsd_input_items(self, tsd_modified_keys_python(self.as_ts_view()));
        })
        .def("added_keys", [](const TSDInputView& self) { return tsd_added_keys_python(self.as_ts_view()); })
        .def("added_values", [](const TSDInputView& self) {
            return tsd_input_values(self, tsd_added_keys_python(self.as_ts_view()));
        })
        .def("added_items", [](const TSDInputView& self) {
            return tsd_input_items(self, tsd_added_keys_python(self.as_ts_view()));
        })
        .def("removed_keys", [](const TSDInputView& self) { return tsd_removed_keys_python(self.as_ts_view()); })
        .def("removed_values", [](const TSDInputView& self) {
            return tsd_input_values(self, tsd_removed_keys_python(self.as_ts_view()));
        })
        .def("removed_items", [](const TSDInputView& self) {
            return tsd_input_items(self, tsd_removed_keys_python(self.as_ts_view()));
        })
        .def_prop_ro("key_set", [](const TSDInputView& self) {
            return tsd_key_set_input_view(self);
        }, nb::keep_alive<0, 1>());

    nb::class_<TSIndexedInputView, TSInputView>(test_mod, "TSIndexedInputView")
        .def("__bool__", [](const TSIndexedInputView& self) { return static_cast<bool>(self); })
        .def("at", &TSIndexedInputView::at, "index"_a, nb::keep_alive<0, 1>())
        .def("count", &TSIndexedInputView::count)
        .def("size", &TSIndexedInputView::size);

    nb::class_<TSLInputView, TSIndexedInputView>(test_mod, "TSLInputView")
        .def("__bool__", [](const TSLInputView& self) { return static_cast<bool>(self); })
        .def("keys", [](const TSLInputView& self) { return tsl_keys_python(self.as_ts_view()); })
        .def("values", [](const TSLInputView& self) {
            nb::list keys = tsl_keys_python(self.as_ts_view());
            return tsl_input_values(self, keys);
        })
        .def("items", [](const TSLInputView& self) {
            nb::list keys = tsl_keys_python(self.as_ts_view());
            return tsl_input_items(self, keys);
        })
        .def("valid_keys", [](const TSLInputView& self) { return tsl_valid_keys_input(self); })
        .def("valid_values", [](const TSLInputView& self) {
            return tsl_input_values(self, tsl_valid_keys_input(self));
        })
        .def("valid_items", [](const TSLInputView& self) {
            return tsl_input_items(self, tsl_valid_keys_input(self));
        })
        .def("modified_keys", [](const TSLInputView& self) { return tsl_modified_keys_input(self); })
        .def("modified_values", [](const TSLInputView& self) {
            return tsl_input_values(self, tsl_modified_keys_input(self));
        })
        .def("modified_items", [](const TSLInputView& self) {
            return tsl_input_items(self, tsl_modified_keys_input(self));
        });

    nb::class_<TSBInputView, TSIndexedInputView>(test_mod, "TSBInputView")
        .def("__bool__", [](const TSBInputView& self) { return static_cast<bool>(self); })
        .def("field", &TSBInputView::field, "name"_a, nb::keep_alive<0, 1>())
        .def("at", [](const TSBInputView& self, size_t index) { return self.TSIndexedInputView::at(index); },
             "index"_a, nb::keep_alive<0, 1>())
        .def("at", nb::overload_cast<std::string_view>(&TSBInputView::at, nb::const_), "name"_a, nb::keep_alive<0, 1>())
        .def("keys", [](const TSBInputView& self) { return tsb_keys_python(self.as_ts_view()); })
        .def("values", [](const TSBInputView& self) {
            nb::list keys = tsb_keys_python(self.as_ts_view());
            return tsb_input_values(self, keys);
        })
        .def("items", [](const TSBInputView& self) {
            nb::list keys = tsb_keys_python(self.as_ts_view());
            return tsb_input_items(self, keys);
        })
        .def("valid_keys", [](const TSBInputView& self) { return tsb_valid_keys_input(self); })
        .def("valid_values", [](const TSBInputView& self) {
            return tsb_input_values(self, tsb_valid_keys_input(self));
        })
        .def("valid_items", [](const TSBInputView& self) {
            return tsb_input_items(self, tsb_valid_keys_input(self));
        })
        .def("modified_keys", [](const TSBInputView& self) { return tsb_modified_keys_input(self); })
        .def("modified_values", [](const TSBInputView& self) {
            return tsb_input_values(self, tsb_modified_keys_input(self));
        })
        .def("modified_items", [](const TSBInputView& self) {
            return tsb_input_items(self, tsb_modified_keys_input(self));
        });

    ts_input_view_cls
        .def("try_as_window", &TSInputView::try_as_window, nb::keep_alive<0, 1>())
        .def("try_as_set", &TSInputView::try_as_set, nb::keep_alive<0, 1>())
        .def("try_as_dict", &TSInputView::try_as_dict, nb::keep_alive<0, 1>())
        .def("try_as_list", &TSInputView::try_as_list, nb::keep_alive<0, 1>())
        .def("try_as_bundle", &TSInputView::try_as_bundle, nb::keep_alive<0, 1>())
        .def("as_window", &TSInputView::as_window, nb::keep_alive<0, 1>())
        .def("as_set", &TSInputView::as_set, nb::keep_alive<0, 1>())
        .def("as_dict", &TSInputView::as_dict, nb::keep_alive<0, 1>())
        .def("as_list", &TSInputView::as_list, nb::keep_alive<0, 1>())
        .def("as_bundle", &TSInputView::as_bundle, nb::keep_alive<0, 1>());
}

}  // namespace hgraph
