#include <hgraph/types/time_series/ts_view.h>

#include <hgraph/api/python/py_ts_runtime_internal.h>
#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/value/map_storage.h>
#include "ts_ops/ts_ops_internal.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <type_traits>

namespace hgraph {
namespace {

const TSMeta* ts_view_meta_at_path(const TSMeta* root, const std::vector<size_t>& indices);

const ts_ops* resolve_kind_ops(const ViewData& view_data) {
    if (view_data.ops != nullptr) {
        return view_data.ops;
    }
    return get_ts_ops(ts_view_meta_at_path(view_data.meta, view_data.path.indices));
}

const ts_window_ops* resolve_window_ops(const ViewData& view_data) {
    const ts_ops* ops = resolve_kind_ops(view_data);
    return ops != nullptr ? ops->window_ops() : nullptr;
}

const ts_set_ops* resolve_set_ops(const ViewData& view_data) {
    const ts_ops* ops = resolve_kind_ops(view_data);
    return ops != nullptr ? ops->set_ops() : nullptr;
}

const ts_dict_ops* resolve_dict_ops(const ViewData& view_data) {
    const ts_ops* ops = resolve_kind_ops(view_data);
    return ops != nullptr ? ops->dict_ops() : nullptr;
}

TSView tsd_key_set_projection_view(const TSView& view) {
    if (!view || !view.is_dict()) {
        return {};
    }
    TSView out = view;
    out.view_data().projection = ViewProjection::TSD_KEY_SET;
    return out;
}

TSOutputView tsd_key_set_projection_view(const TSOutputView& view) {
    if (!view || !view.as_ts_view().is_dict()) {
        return {};
    }
    TSOutputView out = view;
    out.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
    return out;
}

TSInputView tsd_key_set_projection_view(const TSInputView& view) {
    if (!view || !view.as_ts_view().is_dict()) {
        return {};
    }
    TSInputView out = view;
    out.as_ts_view().view_data().projection = ViewProjection::TSD_KEY_SET;
    return out;
}

const TSMeta* ts_view_meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
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

value::View resolve_local_navigation_value(const ViewData& view_data) {
    auto* value_root = static_cast<const value::Value*>(view_data.value_data);
    if (value_root == nullptr || !value_root->has_value()) {
        return {};
    }

    const auto map_key_for_index = [](const value::View& map_view, size_t index) -> std::optional<value::View> {
        if (!map_view.valid() || !map_view.is_map()) {
            return std::nullopt;
        }

        auto map = map_view.as_map();
        if (const auto* storage = static_cast<const value::MapStorage*>(map.data()); storage != nullptr) {
            const auto& key_set = storage->key_set();
            if (key_set.is_alive(index)) {
                return value::View(storage->key_at_slot(index), map.key_type());
            }
        }
        return std::nullopt;
    };

    const auto child_value_by_index = [&](const value::View& current, size_t index) -> std::optional<value::View> {
        if (!current.valid()) {
            return std::nullopt;
        }

        if (current.is_bundle()) {
            auto bundle = current.as_bundle();
            if (index < bundle.size()) {
                return bundle.at(index);
            }
            return std::nullopt;
        }

        if (current.is_tuple()) {
            auto tuple = current.as_tuple();
            if (index < tuple.size()) {
                return tuple.at(index);
            }
            return std::nullopt;
        }

        if (current.is_list()) {
            auto list = current.as_list();
            if (index < list.size()) {
                return list.at(index);
            }
            return std::nullopt;
        }

        if (current.is_map()) {
            auto maybe_key = map_key_for_index(current, index);
            if (!maybe_key.has_value()) {
                return std::nullopt;
            }
            auto map = current.as_map();
            return map.at(*maybe_key);
        }

        return std::nullopt;
    };

    value::View current = value_root->view();
    for (size_t index : view_data.path.indices) {
        auto next = child_value_by_index(current, index);
        if (!next.has_value()) {
            return {};
        }
        current = *next;
    }
    return current;
}

std::optional<size_t> map_slot_for_key(const value::View& map_view, const value::View& key) {
    if (!map_view.valid() || !map_view.is_map()) {
        return std::nullopt;
    }

    auto map = map_view.as_map();
    if (!key.valid() || key.schema() != map.key_type()) {
        return std::nullopt;
    }

    const auto* storage = static_cast<const value::MapStorage*>(map.data());
    if (storage == nullptr) {
        return std::nullopt;
    }

    const size_t slot = storage->key_set().find(key.data());
    if (slot == static_cast<size_t>(-1)) {
        return std::nullopt;
    }
    return slot;
}

TSView child_at_impl(const ViewData& view_data, size_t index, const engine_time_t* engine_time_ptr) {
    ViewData child = view_data;
    child.path.indices.push_back(index);
    child.ops = get_ts_ops(ts_view_meta_at_path(child.meta, child.path.indices));
    return TSView(child, engine_time_ptr);
}

TSView child_by_name_impl(const ViewData& view_data, std::string_view name, const engine_time_t* engine_time_ptr) {
    const TSMeta* current = ts_view_meta_at_path(view_data.meta, view_data.path.indices);
    if (current == nullptr || current->kind != TSKind::TSB || current->fields() == nullptr) {
        return {};
    }

    for (size_t i = 0; i < current->field_count(); ++i) {
        if (name == current->fields()[i].name) {
            return child_at_impl(view_data, i, engine_time_ptr);
        }
    }
    return {};
}

std::optional<size_t> bundle_index_of_impl(const ViewData& view_data, std::string_view name) {
    const TSMeta* current = ts_view_meta_at_path(view_data.meta, view_data.path.indices);
    if (current == nullptr || current->kind != TSKind::TSB || current->fields() == nullptr) {
        return std::nullopt;
    }

    for (size_t i = 0; i < current->field_count(); ++i) {
        const char* field_name = current->fields()[i].name;
        if (field_name != nullptr && name == field_name) {
            return i;
        }
    }
    return std::nullopt;
}

std::string_view bundle_name_at_impl(const ViewData& view_data, size_t index) {
    const TSMeta* current = ts_view_meta_at_path(view_data.meta, view_data.path.indices);
    if (current == nullptr || current->kind != TSKind::TSB || current->fields() == nullptr || index >= current->field_count()) {
        return {};
    }
    const char* field_name = current->fields()[index].name;
    return field_name != nullptr ? std::string_view(field_name) : std::string_view{};
}

bool bundle_contains_impl(const ViewData& view_data, std::string_view name) {
    return bundle_index_of_impl(view_data, name).has_value();
}

std::optional<size_t> child_slot_from_short_path(const ShortPath& parent, const ShortPath& child) {
    if (parent.node != child.node || parent.port_type != child.port_type) {
        return std::nullopt;
    }
    if (child.indices.size() != parent.indices.size() + 1) {
        return std::nullopt;
    }
    if (!std::equal(parent.indices.begin(), parent.indices.end(), child.indices.begin())) {
        return std::nullopt;
    }
    return child.indices.back();
}

std::vector<size_t> dense_indices(size_t count) {
    std::vector<size_t> out;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        out.push_back(i);
    }
    return out;
}

bool tss_contains_for_view(const TSView& view, const value::View& elem) {
    if (!elem.valid()) {
        return false;
    }

    const value::View current = view.value();
    if (!current.valid()) {
        return false;
    }

    if (current.is_set()) {
        auto set = current.as_set();
        return elem.schema() == set.element_type() && set.contains(elem);
    }

    if (current.is_map()) {
        auto map = current.as_map();
        return elem.schema() == map.key_type() && map.contains(elem);
    }

    return false;
}

size_t tss_size_for_view(const TSView& view) {
    const value::View current = view.value();
    if (!current.valid()) {
        return 0;
    }
    if (current.is_set()) {
        return current.as_set().size();
    }
    if (current.is_map()) {
        return current.as_map().size();
    }
    return 0;
}

template <typename Range>
struct ValueViewIteratorState {
    Range range;
    decltype(std::declval<const Range&>().begin()) it{};
    decltype(std::declval<const Range&>().end()) end{};

    explicit ValueViewIteratorState(Range in_range)
        : range(std::move(in_range)),
          it(range.begin()),
          end(range.end()) {}

    [[nodiscard]] bool at_end() const { return it == end; }

    void next() { ++it; }

    [[nodiscard]] value::View value() const { return *it; }

    [[nodiscard]] size_t size() const { return range.size(); }
};

template <typename Range>
TSIterable<value::View> value_view_iterable_from(Range range) {
    return TSIterable<value::View>::from_state(ValueViewIteratorState<Range>{std::move(range)});
}

TSIterable<value::View> tss_values_for_view(const TSView& view) {
    const value::View current = view.value();
    if (!current.valid()) {
        return {};
    }

    if (current.is_set()) {
        return value_view_iterable_from(current.as_set());
    }

    if (current.is_map()) {
        return value_view_iterable_from(current.as_map().keys());
    }
    return {};
}

TSIterable<value::View> tss_delta_values_for_view(const TSView& view, size_t tuple_slot) {
    const value::View delta = view.delta_payload();
    if (!delta.valid() || !delta.is_tuple()) {
        if (tuple_slot == 0 && view.sampled()) {
            return tss_values_for_view(view);
        }
        return {};
    }

    auto tuple = delta.as_tuple();
    if (tuple_slot >= tuple.size()) {
        if (tuple_slot == 0 && view.sampled()) {
            return tss_values_for_view(view);
        }
        return {};
    }

    const value::View slot = tuple.at(tuple_slot);
    if (!slot.valid()) {
        return {};
    }

    if (slot.is_set()) {
        return value_view_iterable_from(slot.as_set());
    }

    if (slot.is_map()) {
        return value_view_iterable_from(slot.as_map().keys());
    }
    return {};
}

bool tss_was_delta_value_for_view(const TSView& view, const value::View& elem, size_t tuple_slot) {
    if (!elem.valid()) {
        return false;
    }

    const value::View delta = view.delta_payload();
    if (!delta.valid() || !delta.is_tuple()) {
        return tuple_slot == 0 && view.sampled() && tss_contains_for_view(view, elem);
    }

    auto tuple = delta.as_tuple();
    if (tuple_slot >= tuple.size()) {
        return tuple_slot == 0 && view.sampled() && tss_contains_for_view(view, elem);
    }

    const value::View slot = tuple.at(tuple_slot);
    if (!slot.valid()) {
        return false;
    }

    if (slot.is_set()) {
        auto set = slot.as_set();
        return elem.schema() == set.element_type() && set.contains(elem);
    }

    if (slot.is_map()) {
        auto map = slot.as_map();
        return elem.schema() == map.key_type() && map.contains(elem);
    }

    return false;
}

template <typename ListView>
using list_child_t = std::decay_t<decltype(std::declval<const ListView&>().at(size_t{}))>;

template <typename ListView>
using list_item_t = std::pair<value::View, list_child_t<ListView>>;

template <typename ListView>
bool ts_child_in_filter(const list_child_t<ListView>& child, TSCollectionFilter filter) {
    if (filter == TSCollectionFilter::All) {
        return true;
    }
    if (!child) {
        return false;
    }
    return filter == TSCollectionFilter::Valid ? child.valid() : child.modified();
}

template <typename ListView>
struct ListKeyIteratorState {
    ListView list;
    TSCollectionFilter filter;
    size_t index{0};
    mutable size_t key_value{0};

    ListKeyIteratorState(ListView in_list, TSCollectionFilter in_filter)
        : list(std::move(in_list)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < list.count()) {
            if (filter == TSCollectionFilter::All) {
                break;
            }
            auto child = list.at(index);
            if (ts_child_in_filter<ListView>(child, filter)) {
                break;
            }
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= list.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] value::View value() const {
        key_value = index;
        return {&key_value, value::scalar_type_meta<size_t>()};
    }
};

template <typename ListView>
struct ListValueIteratorState {
    ListView list;
    TSCollectionFilter filter;
    size_t index{0};

    ListValueIteratorState(ListView in_list, TSCollectionFilter in_filter)
        : list(std::move(in_list)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < list.count()) {
            auto child = list.at(index);
            if (filter == TSCollectionFilter::All) {
                break;
            }
            if (ts_child_in_filter<ListView>(child, filter)) {
                break;
            }
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= list.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] list_child_t<ListView> value() const { return list.at(index); }
};

template <typename ListView>
struct ListItemIteratorState {
    ListView list;
    TSCollectionFilter filter;
    size_t index{0};
    mutable size_t key_value{0};

    ListItemIteratorState(ListView in_list, TSCollectionFilter in_filter)
        : list(std::move(in_list)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < list.count()) {
            auto child = list.at(index);
            if (filter == TSCollectionFilter::All) {
                break;
            }
            if (ts_child_in_filter<ListView>(child, filter)) {
                break;
            }
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= list.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] list_item_t<ListView> value() const {
        key_value = index;
        return {{&key_value, value::scalar_type_meta<size_t>()}, list.at(index)};
    }
};

template <typename ListView>
TSIterable<value::View> ts_list_keys(const ListView& list, TSCollectionFilter filter) {
    return TSIterable<value::View>::from_state(ListKeyIteratorState<ListView>{list, filter});
}

template <typename ListView>
TSIterable<list_child_t<ListView>> ts_list_values(const ListView& list, TSCollectionFilter filter) {
    return TSIterable<list_child_t<ListView>>::from_state(ListValueIteratorState<ListView>{list, filter});
}

template <typename ListView>
TSIterable<list_item_t<ListView>> ts_list_items(const ListView& list, TSCollectionFilter filter) {
    return TSIterable<list_item_t<ListView>>::from_state(ListItemIteratorState<ListView>{list, filter});
}

template <typename BundleView>
using bundle_child_t = std::decay_t<decltype(std::declval<const BundleView&>().at(size_t{}))>;

template <typename BundleView>
using bundle_item_t = std::pair<value::View, bundle_child_t<BundleView>>;

template <typename BundleView>
bool ts_bundle_name_child_in_filter(const BundleView& bundle, size_t index, TSCollectionFilter filter) {
    const std::string_view name = bundle.name_at(index);
    if (name.empty()) {
        return false;
    }
    if (filter == TSCollectionFilter::All) {
        return true;
    }
    auto child = bundle.at(index);
    if (!child) {
        return false;
    }
    return filter == TSCollectionFilter::Valid ? child.valid() : child.modified();
}

template <typename BundleView>
bool ts_bundle_child_in_filter(const BundleView& bundle, size_t index, TSCollectionFilter filter) {
    auto child = bundle.at(index);
    if (!child) {
        return false;
    }
    if (filter == TSCollectionFilter::All) {
        return true;
    }
    return filter == TSCollectionFilter::Valid ? child.valid() : child.modified();
}

template <typename BundleView>
struct BundleKeyIteratorState {
    BundleView bundle;
    TSCollectionFilter filter;
    size_t index{0};
    mutable std::string key_value;

    BundleKeyIteratorState(BundleView in_bundle, TSCollectionFilter in_filter)
        : bundle(std::move(in_bundle)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < bundle.count() && !ts_bundle_name_child_in_filter(bundle, index, filter)) {
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= bundle.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] value::View value() const {
        key_value = bundle.name_at(index);
        return {&key_value, value::scalar_type_meta<std::string>()};
    }
};

template <typename BundleView>
struct BundleValueIteratorState {
    BundleView bundle;
    TSCollectionFilter filter;
    size_t index{0};

    BundleValueIteratorState(BundleView in_bundle, TSCollectionFilter in_filter)
        : bundle(std::move(in_bundle)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < bundle.count() && !ts_bundle_child_in_filter(bundle, index, filter)) {
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= bundle.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] bundle_child_t<BundleView> value() const { return bundle.at(index); }
};

template <typename BundleView>
struct BundleItemIteratorState {
    BundleView bundle;
    TSCollectionFilter filter;
    size_t index{0};
    mutable std::string key_value;

    BundleItemIteratorState(BundleView in_bundle, TSCollectionFilter in_filter)
        : bundle(std::move(in_bundle)), filter(in_filter) {
        seek();
    }

    void seek() {
        while (index < bundle.count()) {
            const std::string_view name = bundle.name_at(index);
            if (name.empty()) {
                ++index;
                continue;
            }
            if (ts_bundle_child_in_filter(bundle, index, filter)) {
                break;
            }
            ++index;
        }
    }

    [[nodiscard]] bool at_end() const { return index >= bundle.count(); }

    void next() {
        ++index;
        seek();
    }

    [[nodiscard]] bundle_item_t<BundleView> value() const {
        key_value = bundle.name_at(index);
        return {{&key_value, value::scalar_type_meta<std::string>()}, bundle.at(index)};
    }
};

template <typename BundleView>
TSIterable<value::View> ts_bundle_keys(const BundleView& bundle, TSCollectionFilter filter) {
    return TSIterable<value::View>::from_state(BundleKeyIteratorState<BundleView>{bundle, filter});
}

template <typename BundleView>
TSIterable<bundle_child_t<BundleView>> ts_bundle_values(const BundleView& bundle, TSCollectionFilter filter) {
    return TSIterable<bundle_child_t<BundleView>>::from_state(BundleValueIteratorState<BundleView>{bundle, filter});
}

template <typename BundleView>
TSIterable<bundle_item_t<BundleView>> ts_bundle_items(const BundleView& bundle, TSCollectionFilter filter) {
    return TSIterable<bundle_item_t<BundleView>>::from_state(BundleItemIteratorState<BundleView>{bundle, filter});
}

struct MapKeyValueIteratorState {
    value::KeySetView key_set;
    value::KeySetView::const_iterator it{};
    value::KeySetView::const_iterator end{};

    explicit MapKeyValueIteratorState(value::KeySetView in_key_set)
        : key_set(std::move(in_key_set)),
          it(key_set.begin()),
          end(key_set.end()) {}

    [[nodiscard]] bool at_end() const { return it == end; }

    void next() { ++it; }

    [[nodiscard]] value::View value() const { return *it; }

    [[nodiscard]] size_t size() const {
        return key_set.size();
    }
};

template <typename DictView>
using dict_child_t = std::decay_t<decltype(std::declval<const DictView&>().at_key(std::declval<const value::View&>()))>;

template <typename DictView>
using dict_item_t = std::pair<value::View, dict_child_t<DictView>>;

struct OwnedKeyViewIteratorState {
    std::shared_ptr<std::vector<value::Value>> keys;
    size_t index{0};

    explicit OwnedKeyViewIteratorState(std::vector<value::Value> in_keys)
        : keys(std::make_shared<std::vector<value::Value>>(std::move(in_keys))) {}

    [[nodiscard]] bool at_end() const { return !keys || index >= keys->size(); }

    void next() { ++index; }

    [[nodiscard]] value::View value() const { return (*keys)[index].view(); }

    [[nodiscard]] size_t size() const { return keys ? keys->size() : 0; }
};

TSIterable<value::View> key_view_iterable_from_values(std::vector<value::Value> keys) {
    return TSIterable<value::View>::from_state(OwnedKeyViewIteratorState{std::move(keys)});
}

template <typename Child>
struct OwnedKeyedChildIteratorState {
    std::shared_ptr<std::vector<value::Value>> keys;
    std::shared_ptr<std::vector<Child>> children;
    size_t index{0};

    OwnedKeyedChildIteratorState(std::vector<value::Value> in_keys, std::vector<Child> in_children)
        : keys(std::make_shared<std::vector<value::Value>>(std::move(in_keys))),
          children(std::make_shared<std::vector<Child>>(std::move(in_children))) {}

    [[nodiscard]] bool at_end() const {
        return !keys || !children || index >= keys->size() || index >= children->size();
    }

    void next() { ++index; }

    [[nodiscard]] std::pair<value::View, Child> value() const {
        return {(*keys)[index].view(), (*children)[index]};
    }

    [[nodiscard]] size_t size() const {
        if (!keys || !children) {
            return 0;
        }
        return std::min(keys->size(), children->size());
    }
};

template <typename Child>
TSIterable<std::pair<value::View, Child>> item_iterable_from_owned_keys(
    std::vector<value::Value> keys,
    std::vector<Child> children) {
    return TSIterable<std::pair<value::View, Child>>::from_state(
        OwnedKeyedChildIteratorState<Child>{std::move(keys), std::move(children)});
}

template <typename DictView>
struct DictValidKeyIteratorState {
    DictView dict;
    TSIterable<value::View> keys;
    typename TSIterable<value::View>::iterator it{};
    typename TSIterable<value::View>::sentinel end{};

    DictValidKeyIteratorState(DictView in_dict, TSIterable<value::View> in_keys)
        : dict(std::move(in_dict)),
          keys(std::move(in_keys)),
          it(keys.begin()),
          end(keys.end()) {
        seek();
    }

    void seek() {
        while (!(it == end)) {
            value::View key = *it;
            auto child = dict.at_key(key);
            if (child && child.valid()) {
                return;
            }
            ++it;
        }
    }

    [[nodiscard]] bool at_end() const { return it == end; }

    void next() {
        if (!(it == end)) {
            ++it;
        }
        seek();
    }

    [[nodiscard]] value::View value() const { return *it; }
};

template <typename DictView>
TSIterable<value::View> ts_dict_valid_keys(const DictView& dict, TSIterable<value::View> keys) {
    return TSIterable<value::View>::from_state(
        DictValidKeyIteratorState<DictView>{dict, std::move(keys)});
}

template <typename DictView>
struct DictValueIteratorState {
    DictView dict;
    TSIterable<value::View> keys;
    typename TSIterable<value::View>::iterator it{};
    typename TSIterable<value::View>::sentinel end{};

    DictValueIteratorState(DictView in_dict, TSIterable<value::View> in_keys)
        : dict(std::move(in_dict)),
          keys(std::move(in_keys)),
          it(keys.begin()),
          end(keys.end()) {
        seek();
    }

    void seek() {
        while (!(it == end)) {
            const value::View key = *it;
            auto child = dict.at_key(key);
            if (child) {
                return;
            }
            ++it;
        }
    }

    [[nodiscard]] bool at_end() const { return it == end; }

    void next() {
        if (!(it == end)) {
            ++it;
        }
        seek();
    }

    [[nodiscard]] dict_child_t<DictView> value() const {
        if (it == end) {
            return {};
        }
        const value::View key = *it;
        return dict.at_key(key);
    }
};

template <typename DictView>
TSIterable<dict_child_t<DictView>> ts_dict_values(const DictView& dict, TSIterable<value::View> keys) {
    return TSIterable<dict_child_t<DictView>>::from_state(
        DictValueIteratorState<DictView>{dict, std::move(keys)});
}

template <typename DictView>
struct DictItemIteratorState {
    DictView dict;
    TSIterable<value::View> keys;
    typename TSIterable<value::View>::iterator it{};
    typename TSIterable<value::View>::sentinel end{};

    DictItemIteratorState(DictView in_dict, TSIterable<value::View> in_keys)
        : dict(std::move(in_dict)),
          keys(std::move(in_keys)),
          it(keys.begin()),
          end(keys.end()) {
        seek();
    }

    void seek() {
        while (!(it == end)) {
            value::View key = *it;
            auto child = dict.at_key(key);
            if (child) {
                return;
            }
            ++it;
        }
    }

    [[nodiscard]] bool at_end() const { return it == end; }

    void next() {
        if (!(it == end)) {
            ++it;
        }
        seek();
    }

    [[nodiscard]] dict_item_t<DictView> value() const {
        if (it == end) {
            return {};
        }
        value::View key = *it;
        auto child = dict.at_key(key);
        return {std::move(key), std::move(child)};
    }
};

template <typename DictView>
TSIterable<dict_item_t<DictView>> ts_dict_items(const DictView& dict, TSIterable<value::View> keys) {
    return TSIterable<dict_item_t<DictView>>::from_state(
        DictItemIteratorState<DictView>{dict, std::move(keys)});
}

bool tsd_is_ref_valued(const TSView& view) {
    const auto* meta = view.ts_meta();
    return meta != nullptr && meta->element_ts() != nullptr && meta->element_ts()->kind == TSKind::REF;
}

bool tsd_delta_tuple_slot_set(const TSView& view, size_t tuple_index, value::SetView& out) {
    value::View delta = view.delta_payload();
    if (!delta.valid() || !delta.is_tuple()) {
        return false;
    }

    auto tuple = delta.as_tuple();
    if (tuple_index >= tuple.size()) {
        return false;
    }

    value::View slot = tuple.at(tuple_index);
    if (!slot.valid() || !slot.is_set()) {
        return false;
    }

    out = slot.as_set();
    return true;
}

bool tsd_delta_tuple_slot_map(const TSView& view, size_t tuple_index, value::MapView& out) {
    value::View delta = view.delta_payload();
    if (!delta.valid() || !delta.is_tuple()) {
        return false;
    }

    auto tuple = delta.as_tuple();
    if (tuple_index >= tuple.size()) {
        return false;
    }

    value::View slot = tuple.at(tuple_index);
    if (!slot.valid() || !slot.is_map()) {
        return false;
    }

    out = slot.as_map();
    return true;
}

bool tsd_sampled_has_removed_any(const TSView& view) {
    if (!view.view_data().sampled) {
        return false;
    }
    value::View current = view.value();
    return current.valid() && current.is_map() && current.as_map().size() > 0;
}

bool tsd_sampled_was_removed(const TSView& view, const value::View& key) {
    if (!view.view_data().sampled || !key.valid()) {
        return false;
    }
    value::View current = view.value();
    return current.valid() && current.is_map() && current.as_map().contains(key);
}

bool map_contains_key_strict(const value::MapView& map, const value::View& key) {
    if (!key.valid() || key.schema() != map.key_type()) {
        return false;
    }
    return map.contains(key);
}

std::optional<value::View> map_value_for_key_strict(const value::MapView& map, const value::View& key) {
    if (!map_contains_key_strict(map, key)) {
        return std::nullopt;
    }
    return map.at(key);
}

std::optional<value::MapView> map_view_from_view_data(const ViewData& vd) {
    if (auto value = resolve_value_slot_const(vd);
        value.has_value() && value->valid() && value->is_map()) {
        return value->as_map();
    }
    return std::nullopt;
}

const ViewData* bridge_previous_data_for_tsd_view(const TSDKeySetBridgeState& state) {
    if (state.has_previous_source) {
        return &state.previous_source;
    }
    if (state.has_bridge) {
        return &state.previous_bridge;
    }
    return nullptr;
}

const ViewData* bridge_current_data_for_tsd_view(const TSDKeySetBridgeState& state) {
    if (state.has_current_source) {
        return &state.current_source;
    }
    if (state.has_bridge) {
        return &state.current_bridge;
    }
    return nullptr;
}

struct TSDResolvedBridgeMaps {
    bool has_bridge{false};
    std::optional<value::MapView> previous;
    std::optional<value::MapView> current;
};

TSDResolvedBridgeMaps resolve_tsd_bridge_maps_for_view(const TSView& view) {
    TSDResolvedBridgeMaps out{};
    const auto state = resolve_tsd_key_set_bridge_state(view.view_data(), view.current_time());
    out.has_bridge = state.has_bridge;
    if (!out.has_bridge) {
        return out;
    }

    if (const ViewData* previous_data = bridge_previous_data_for_tsd_view(state); previous_data != nullptr) {
        out.previous = map_view_from_view_data(*previous_data);
    }
    if (const ViewData* current_data = bridge_current_data_for_tsd_view(state); current_data != nullptr) {
        out.current = map_view_from_view_data(*current_data);
    }
    return out;
}

bool tsd_bridge_has_added_for_view(const TSView& view) {
    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge || !bridge_maps.current.has_value()) {
        return false;
    }
    if (!bridge_maps.previous.has_value()) {
        return bridge_maps.current->size() > 0;
    }
    for (value::View key : bridge_maps.current->keys()) {
        if (!map_contains_key_strict(*bridge_maps.previous, key)) {
            return true;
        }
    }
    return false;
}

bool tsd_bridge_was_added_for_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }
    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge || !bridge_maps.current.has_value()) {
        return false;
    }
    if (!map_contains_key_strict(*bridge_maps.current, key)) {
        return false;
    }
    if (!bridge_maps.previous.has_value()) {
        return true;
    }
    return !map_contains_key_strict(*bridge_maps.previous, key);
}

bool tsd_bridge_has_removed_for_view(const TSView& view) {
    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge) {
        return false;
    }

    if (!bridge_maps.previous.has_value()) {
        return false;
    }

    if (!bridge_maps.current.has_value()) {
        return bridge_maps.previous->size() > 0;
    }

    const auto dict = view.as_dict();
    for (value::View key : bridge_maps.previous->keys()) {
        if (!map_contains_key_strict(*bridge_maps.current, key)) {
            return true;
        }
        auto child = dict.at_key(key);
        if (!child || !child.valid()) {
            return true;
        }
    }
    return false;
}

bool tsd_bridge_was_removed_for_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge) {
        return false;
    }

    if (!bridge_maps.previous.has_value()) {
        return false;
    }
    if (!map_contains_key_strict(*bridge_maps.previous, key)) {
        return false;
    }

    if (!bridge_maps.current.has_value() || !map_contains_key_strict(*bridge_maps.current, key)) {
        return true;
    }

    auto child = view.as_dict().at_key(key);
    return !child || !child.valid();
}

bool tsd_bridge_was_modified_non_remove_for_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge || !bridge_maps.current.has_value()) {
        return false;
    }

    if (!map_contains_key_strict(*bridge_maps.current, key)) {
        return false;
    }
    auto current_value = map_value_for_key_strict(*bridge_maps.current, key);
    if (!current_value.has_value() || !current_value->valid()) {
        return false;
    }

    if (!bridge_maps.previous.has_value() || !map_contains_key_strict(*bridge_maps.previous, key)) {
        return true;
    }

    auto previous_value = map_value_for_key_strict(*bridge_maps.previous, key);
    if (!previous_value.has_value()) {
        return true;
    }

    return previous_value->schema() != current_value->schema() || !previous_value->equals(*current_value);
}

void append_unique_key(std::vector<value::Value>& out,
                       std::unordered_set<value::View>& seen,
                       const value::View& key) {
    if (!key.valid()) {
        return;
    }
    if (!seen.insert(key).second) {
        return;
    }
    out.emplace_back(key.clone());
}

bool is_effectively_added_key(const value::SetView& added,
                              const value::SetView* removed,
                              const value::View& key) {
    if (!added.valid() || !key.valid() || key.schema() != added.element_type() || !added.contains(key)) {
        return false;
    }
    if (removed == nullptr || !removed->valid()) {
        return true;
    }
    if (key.schema() != removed->element_type()) {
        return true;
    }
    return !removed->contains(key);
}

bool has_effective_added_keys(const value::SetView& added, const value::SetView* removed) {
    if (!added.valid()) {
        return false;
    }
    if (removed == nullptr || !removed->valid()) {
        return added.size() > 0;
    }
    for (value::View key : added) {
        if (key.valid() &&
            key.schema() == added.element_type() &&
            key.schema() == removed->element_type() &&
            !removed->contains(key)) {
            return true;
        }
    }
    return false;
}

std::vector<value::Value> tsd_keys_for_view(const TSView& view, bool include_local_fallback) {
    std::vector<value::Value> out;
    std::unordered_set<value::View> seen;

    const auto append_from_map = [&](const value::View& map_view) {
        if (!map_view.valid() || !map_view.is_map()) {
            return;
        }
        for (value::View key : map_view.as_map().keys()) {
            append_unique_key(out, seen, key);
        }
    };

    append_from_map(view.value());
    if (include_local_fallback) {
        append_from_map(ts_local_navigation_value(view));
    }
    return out;
}

TSIterable<value::View> tsd_keys_for_view_iterable(const TSView& view, bool include_local_fallback) {
    if (!include_local_fallback) {
        const value::View current = view.value();
        if (current.valid() && current.is_map()) {
            return TSIterable<value::View>::from_state(MapKeyValueIteratorState{current.as_map().keys()});
        }
        return {};
    }
    return key_view_iterable_from_values(tsd_keys_for_view(view, true));
}

void stable_sort_atomic_keys(std::vector<value::Value>& keys) {
    if (keys.size() < 2) {
        return;
    }
    const value::TypeMeta* schema = keys.front().schema();
    if (schema == nullptr ||
        schema->kind != value::TypeKind::Atomic) {
        return;
    }
    for (const auto& key : keys) {
        if (key.schema() != schema) {
            return;
        }
    }
    std::stable_sort(
        keys.begin(),
        keys.end(),
        [schema](const value::Value& lhs, const value::Value& rhs) {
            return schema->ops().less_than(lhs.data(), rhs.data(), schema);
        });
}

bool tsd_key_set_delta_cache_matches(const TsdKeySetDeltaCacheEntry& cache,
                                     const ViewData& view_data,
                                     const value::TypeMeta* key_type_meta,
                                     engine_time_t evaluation_time) {
    return cache.value_data == view_data.value_data &&
           cache.delta_data == view_data.delta_data &&
           cache.observer_data == view_data.observer_data &&
           cache.link_data == view_data.link_data &&
           cache.path == view_data.path.indices &&
           cache.key_type_meta == key_type_meta &&
           cache.evaluation_time == evaluation_time;
}

void populate_tsd_key_set_delta_cache(TsdKeySetDeltaCacheEntry& cache, const TSView& view) {
    cache.added.clear();
    cache.removed.clear();
    cache.value_data = nullptr;
    cache.delta_data = nullptr;
    cache.observer_data = nullptr;
    cache.link_data = nullptr;
    cache.path.clear();
    cache.key_type_meta = nullptr;
    cache.evaluation_time = view.current_time();

    const TSMeta* meta = view.ts_meta();
    if (meta == nullptr || meta->kind != TSKind::TSD || meta->key_type() == nullptr) {
        return;
    }

    const ViewData& view_data = view.view_data();
    cache.value_data = view_data.value_data;
    cache.delta_data = view_data.delta_data;
    cache.observer_data = view_data.observer_data;
    cache.link_data = view_data.link_data;
    cache.path = view_data.path.indices;
    cache.key_type_meta = meta->key_type();

    TSView key_set_view = tsd_key_set_projection_view(view);
    if (!key_set_view) {
        return;
    }

    nb::object key_set_delta = key_set_view.delta_to_python();
    if (key_set_delta.is_none()) {
        return;
    }

    const auto parse_member = [&](const char* member_name, std::vector<value::Value>& out) {
        nb::object member = nb::getattr(key_set_delta, member_name, nb::none());
        if (member.is_none()) {
            return;
        }
        if (PyCallable_Check(member.ptr()) != 0) {
            member = member();
        }
        if (member.is_none()) {
            return;
        }

        for (auto item_h : nb::cast<nb::iterable>(member)) {
            auto key_val = tsd_key_from_python(nb::cast<nb::object>(item_h), meta);
            if (key_val.schema() == nullptr) {
                continue;
            }
            out.emplace_back(std::move(key_val));
        }
        stable_sort_atomic_keys(out);
    };

    parse_member("added", cache.added);
    parse_member("removed", cache.removed);
}

TsdKeySetDeltaCacheEntry* tsd_key_set_delta_cache_entry_for_view(const TSView& view) {
    auto* root = static_cast<PythonValueCacheNode*>(view.view_data().python_value_cache_data);
    if (root != nullptr) {
        return root->tsd_key_set_delta_cache();
    }
    return nullptr;
}

const std::vector<value::Value>* tsd_key_set_delta_keys_cached(const TSView& view, bool added) {
    TsdKeySetDeltaCacheEntry* cache = tsd_key_set_delta_cache_entry_for_view(view);
    if (cache == nullptr) {
        return nullptr;
    }
    const TSMeta* meta = view.ts_meta();
    const value::TypeMeta* key_type_meta = (meta != nullptr && meta->kind == TSKind::TSD) ? meta->key_type() : nullptr;
    if (!tsd_key_set_delta_cache_matches(*cache, view.view_data(), key_type_meta, view.current_time())) {
        populate_tsd_key_set_delta_cache(*cache, view);
    }
    return added ? &cache->added : &cache->removed;
}

std::vector<value::Value> tsd_key_set_delta_keys_uncached(const TSView& view, bool added) {
    TsdKeySetDeltaCacheEntry cache{};
    populate_tsd_key_set_delta_cache(cache, view);
    if (added) {
        return std::move(cache.added);
    }
    return std::move(cache.removed);
}

bool tsd_key_set_delta_has_any(const TSView& view, bool added) {
    if (const auto* cached = tsd_key_set_delta_keys_cached(view, added); cached != nullptr) {
        return !cached->empty();
    }
    return !tsd_key_set_delta_keys_uncached(view, added).empty();
}

bool contains_key_value(const std::vector<value::Value>& keys, const value::View& key);

bool tsd_key_set_delta_contains(const TSView& view, bool added, const value::View& key) {
    if (const auto* cached = tsd_key_set_delta_keys_cached(view, added); cached != nullptr) {
        return contains_key_value(*cached, key);
    }
    return contains_key_value(tsd_key_set_delta_keys_uncached(view, added), key);
}

template <typename Fn>
void for_each_tsd_key_set_delta_key(const TSView& view, bool added, Fn&& fn) {
    if (const auto* cached = tsd_key_set_delta_keys_cached(view, added); cached != nullptr) {
        for (const auto& key : *cached) {
            fn(key);
        }
        return;
    }
    for (const auto& key : tsd_key_set_delta_keys_uncached(view, added)) {
        fn(key);
    }
}

std::vector<value::Value> tsd_key_set_delta_keys_for_view(const TSView& view, bool added) {
    std::vector<value::Value> out;
    if (const auto* cached = tsd_key_set_delta_keys_cached(view, added); cached != nullptr) {
        out.reserve(cached->size());
        for (const auto& key : *cached) {
            out.emplace_back(key.view().clone());
        }
        return out;
    }

    auto uncached = tsd_key_set_delta_keys_uncached(view, added);
    out.reserve(uncached.size());
    for (const auto& key : uncached) {
        out.emplace_back(key.view().clone());
    }
    return out;
}

bool contains_key_value(const std::vector<value::Value>& keys, const value::View& key) {
    if (!key.valid()) {
        return false;
    }
    for (const auto& candidate : keys) {
        const value::View candidate_view = candidate.view();
        if (candidate_view.valid() &&
            candidate_view.schema() == key.schema() &&
            candidate_view.equals(key)) {
            return true;
        }
    }
    return false;
}

void append_bridge_added_keys_for_view(const TSView& view,
                                       std::vector<value::Value>& out,
                                       std::unordered_set<value::View>& seen) {
    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge || !bridge_maps.current.has_value()) {
        return;
    }
    if (!bridge_maps.previous.has_value()) {
        for (value::View key : bridge_maps.current->keys()) {
            append_unique_key(out, seen, key);
        }
        return;
    }
    for (value::View key : bridge_maps.current->keys()) {
        if (!map_contains_key_strict(*bridge_maps.previous, key)) {
            append_unique_key(out, seen, key);
        }
    }
}

void append_bridge_removed_keys_for_view(const TSView& view,
                                         std::vector<value::Value>& out,
                                         std::unordered_set<value::View>& seen) {
    const auto bridge_maps = resolve_tsd_bridge_maps_for_view(view);
    if (!bridge_maps.has_bridge || !bridge_maps.previous.has_value()) {
        return;
    }
    if (!bridge_maps.current.has_value()) {
        for (value::View key : bridge_maps.previous->keys()) {
            append_unique_key(out, seen, key);
        }
        return;
    }
    for (value::View key : bridge_maps.previous->keys()) {
        if (!map_contains_key_strict(*bridge_maps.current, key)) {
            append_unique_key(out, seen, key);
        }
    }
}

std::vector<value::Value> tsd_added_keys_for_view_native(const TSView& view) {
    if (tsd_is_ref_valued(view)) {
        return tsd_key_set_delta_keys_for_view(view, true);
    }

    std::vector<value::Value> out;
    std::unordered_set<value::View> seen;

    value::SetView tuple_added{};
    value::SetView tuple_removed{};
    const bool has_tuple_added = tsd_delta_tuple_slot_set(view, 1, tuple_added);
    const bool has_tuple_removed = tsd_delta_tuple_slot_set(view, 2, tuple_removed);
    const value::SetView* removed_ptr = has_tuple_removed ? &tuple_removed : nullptr;
    if (has_tuple_added && tuple_added.valid()) {
        for (value::View key : tuple_added) {
            if (is_effectively_added_key(tuple_added, removed_ptr, key)) {
                append_unique_key(out, seen, key);
            }
        }
    }
    append_bridge_added_keys_for_view(view, out, seen);
    return out;
}

std::vector<value::Value> tsd_removed_keys_for_view_native(const TSView& view) {
    if (tsd_is_ref_valued(view)) {
        auto out = tsd_key_set_delta_keys_for_view(view, false);
        if (out.empty() && view.view_data().sampled) {
            std::unordered_set<value::View> seen;
            for (const auto& key : out) {
                seen.insert(key.view());
            }
            value::View current = view.value();
            if (current.valid() && current.is_map()) {
                for (value::View key : current.as_map().keys()) {
                    append_unique_key(out, seen, key);
                }
            }
        }
        return out;
    }

    std::vector<value::Value> out;
    std::unordered_set<value::View> seen;

    value::SetView tuple_removed{};
    if (tsd_delta_tuple_slot_set(view, 2, tuple_removed) && tuple_removed.valid()) {
        for (value::View key : tuple_removed) {
            append_unique_key(out, seen, key);
        }
    }
    append_bridge_removed_keys_for_view(view, out, seen);
    if (out.empty() && view.view_data().sampled) {
        value::View current = view.value();
        if (current.valid() && current.is_map()) {
            for (value::View key : current.as_map().keys()) {
                append_unique_key(out, seen, key);
            }
        }
    }
    return out;
}

bool tsd_was_modified_for_output_view(const TSView& view, const value::View& key);
bool tsd_was_modified_for_input_view(const TSView& view, const value::View& key);

bool tsd_was_modified_for_view(const TSView& view, const value::View& key) {
    if (view.short_path().port_type == PortType::INPUT) {
        return tsd_was_modified_for_input_view(view, key);
    }
    return tsd_was_modified_for_output_view(view, key);
}

bool tsd_ref_input_fallback_was_modified(const TSView& view, const value::View& key) {
    auto child = view.as_dict().at_key(key);
    if (!child || !child.modified()) {
        return false;
    }

    bool include = child.last_modified_time() == view.current_time();
    if (!include && child.valid()) {
        value::View child_value = child.value();
        if (!child_value.valid()) {
            return true;
        }
        if (child_value.schema() != ts_reference_meta()) {
            return false;
        }
        const auto& ref = *static_cast<const TimeSeriesReference*>(child_value.data());
        include = !ref.is_valid();
    }

    return include;
}

std::vector<value::Value> tsd_modified_keys_for_output_view_native(const TSView& view) {
    std::vector<value::Value> out;
    auto keys = tsd_keys_for_view(view, false);
    out.reserve(keys.size());
    for (auto& key : keys) {
        if (tsd_was_modified_for_output_view(view, key.view())) {
            out.emplace_back(std::move(key));
        }
    }
    if (tsd_is_ref_valued(view)) {
        std::unordered_set<value::View> seen;
        seen.reserve(out.size());
        for (const auto& key : out) {
            seen.insert(key.view());
        }
        for_each_tsd_key_set_delta_key(view, true, [&](const value::Value& key) {
            append_unique_key(out, seen, key.view());
        });
        stable_sort_atomic_keys(out);
    }
    return out;
}

std::vector<value::Value> tsd_modified_keys_for_view_native(const TSView& view) {
    std::vector<value::Value> out;
    auto keys = tsd_keys_for_view(view, false);
    out.reserve(keys.size());
    for (auto& key : keys) {
        if (tsd_was_modified_for_view(view, key.view())) {
            out.emplace_back(std::move(key));
        }
    }
    if (tsd_is_ref_valued(view)) {
        std::unordered_set<value::View> seen;
        seen.reserve(out.size());
        for (const auto& key : out) {
            seen.insert(key.view());
        }
        for_each_tsd_key_set_delta_key(view, true, [&](const value::Value& key) {
            append_unique_key(out, seen, key.view());
        });
        stable_sort_atomic_keys(out);
    }
    return out;
}

std::vector<value::Value> tsd_modified_keys_for_input_view_native(const TSView& view) {
    std::vector<value::Value> out;
    auto keys = tsd_keys_for_view(view, true);
    out.reserve(keys.size());
    for (auto& key : keys) {
        if (tsd_was_modified_for_input_view(view, key.view())) {
            out.emplace_back(std::move(key));
        }
    }
    if (tsd_is_ref_valued(view)) {
        std::unordered_set<value::View> seen;
        seen.reserve(out.size());
        for (const auto& key : out) {
            seen.insert(key.view());
        }
        for_each_tsd_key_set_delta_key(view, true, [&](const value::Value& key) {
            append_unique_key(out, seen, key.view());
        });
        stable_sort_atomic_keys(out);
    }
    return out;
}

bool tsd_has_added_for_view(const TSView& view) {
    if (tsd_is_ref_valued(view)) {
        return tsd_key_set_delta_has_any(view, true);
    }

    value::SetView tuple_added{};
    value::SetView tuple_removed{};
    const bool has_tuple_added = tsd_delta_tuple_slot_set(view, 1, tuple_added);
    const bool has_tuple_removed = tsd_delta_tuple_slot_set(view, 2, tuple_removed);
    const value::SetView* removed_ptr = has_tuple_removed ? &tuple_removed : nullptr;
    if (has_tuple_added && has_effective_added_keys(tuple_added, removed_ptr)) {
        return true;
    }
    return tsd_bridge_has_added_for_view(view);
}

bool tsd_was_added_for_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    if (tsd_is_ref_valued(view)) {
        return tsd_key_set_delta_contains(view, true, key);
    }

    value::SetView tuple_added{};
    value::SetView tuple_removed{};
    const bool has_tuple_added = tsd_delta_tuple_slot_set(view, 1, tuple_added);
    const bool has_tuple_removed = tsd_delta_tuple_slot_set(view, 2, tuple_removed);
    const value::SetView* removed_ptr = has_tuple_removed ? &tuple_removed : nullptr;
    if (has_tuple_added && is_effectively_added_key(tuple_added, removed_ptr, key)) {
        return true;
    }
    return tsd_bridge_was_added_for_view(view, key);
}

bool tsd_has_removed_for_view(const TSView& view) {
    if (tsd_is_ref_valued(view)) {
        if (tsd_key_set_delta_has_any(view, false)) {
            return true;
        }
        return tsd_sampled_has_removed_any(view);
    }

    value::SetView tuple_removed{};
    if (tsd_delta_tuple_slot_set(view, 2, tuple_removed)) {
        return tuple_removed.size() > 0;
    }

    if (tsd_bridge_has_removed_for_view(view)) {
        return true;
    }
    return tsd_sampled_has_removed_any(view);
}

bool tsd_was_removed_for_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    if (tsd_is_ref_valued(view)) {
        if (tsd_key_set_delta_contains(view, false, key)) {
            return true;
        }
        return tsd_sampled_was_removed(view, key);
    }

    value::SetView tuple_removed{};
    if (tsd_delta_tuple_slot_set(view, 2, tuple_removed)) {
        return tuple_removed.contains(key);
    }

    if (tsd_bridge_was_removed_for_view(view, key)) {
        return true;
    }
    return tsd_sampled_was_removed(view, key);
}

bool tsd_was_modified_for_output_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    const bool ref_valued = tsd_is_ref_valued(view);
    if (ref_valued && !view.modified()) {
        return false;
    }

    value::MapView tuple_modified{};
    if (tsd_delta_tuple_slot_map(view, 0, tuple_modified) && tuple_modified.contains(key)) {
        return true;
    }

    if (tsd_bridge_was_modified_non_remove_for_view(view, key)) {
        return true;
    }

    if (!ref_valued) {
        return false;
    }

    value::SetView tuple_added{};
    if (tsd_delta_tuple_slot_set(view, 1, tuple_added) && tuple_added.contains(key)) {
        return true;
    }

    auto child = view.as_dict().at_key(key);
    return child && child.modified();
}

bool tsd_was_modified_for_input_view(const TSView& view, const value::View& key) {
    if (!key.valid()) {
        return false;
    }

    value::MapView tuple_modified{};
    if (tsd_delta_tuple_slot_map(view, 0, tuple_modified) && tuple_modified.contains(key)) {
        return true;
    }

    if (tsd_bridge_was_modified_non_remove_for_view(view, key)) {
        return true;
    }

    if (!tsd_is_ref_valued(view)) {
        return false;
    }

    value::SetView tuple_added{};
    if (tsd_delta_tuple_slot_set(view, 1, tuple_added) && tuple_added.contains(key)) {
        return true;
    }

    return tsd_ref_input_fallback_was_modified(view, key);
}

TSView child_by_key_impl(const ViewData& view_data, const value::View& key, const engine_time_t* engine_time_ptr) {
    value::View v = resolve_navigation_value(view_data);
    if (v.valid() && v.is_map()) {
        const auto slot = map_slot_for_key(v, key);
        if (!slot.has_value()) {
            // Source map is authoritative for keyed membership on resolved views.
            // Do not fall back to local wrapper slots when the key is absent.
            return {};
        }

        const bool debug_child_key = std::getenv("HGRAPH_DEBUG_CHILD_KEY") != nullptr;
        if (debug_child_key) {
            std::string key_s = nb::cast<std::string>(nb::repr(key.to_python()));
            std::fprintf(stderr,
                         "[child_by_key] path=%s key=%s map_size=%zu slot=%zu\n",
                         view_data.path.to_string().c_str(),
                         key_s.c_str(),
                         v.as_map().size(),
                         *slot);
        }
        return child_at_impl(view_data, *slot, engine_time_ptr);
    }

    if (view_data.uses_link_target) {
        if (auto local_slot = map_slot_for_key(resolve_local_navigation_value(view_data), key); local_slot.has_value()) {
            return child_at_impl(view_data, *local_slot, engine_time_ptr);
        }
    }

    return {};
}

size_t child_count_impl(const ViewData& view_data) {
    const TSMeta* current = ts_view_meta_at_path(view_data.meta, view_data.path.indices);
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

TSView::TSView(ViewData view_data, const engine_time_t* engine_time_ptr) noexcept
    : view_data_(std::move(view_data)) {
    view_data_.engine_time_ptr = engine_time_ptr;
}

TSView::TSView(const TSValue& value, const engine_time_t* engine_time_ptr, ShortPath path)
    : TSView(value.make_view_data(std::move(path), engine_time_ptr), engine_time_ptr) {}

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
    return view_data_.ops->modified(view_data_, current_time());
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

DeltaView TSView::delta_value() const {
    const engine_time_t eval_time = current_time();
    refresh_dynamic_ref_binding(view_data_, eval_time);
    return DeltaView::from_computed(view_data_, eval_time);
}

value::View TSView::delta_payload() const {
    if (view_data_.ops == nullptr) {
        return {};
    }
    const engine_time_t eval_time = current_time();
    refresh_dynamic_ref_binding(view_data_, eval_time);
    return view_data_.ops->delta_value(view_data_);
}

nb::object TSView::to_python() const {
    if (view_data_.ops == nullptr) {
        return nb::none();
    }
    return op_to_python(view_data_);
}

nb::object TSView::delta_to_python() const {
    if (view_data_.ops == nullptr) {
        return nb::none();
    }
    return op_delta_to_python(view_data_, current_time());
}

void TSView::set_value(const value::View& src) {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->set_value(view_data_, src, current_time());
}

void TSView::from_python(const nb::object& src) {
    if (view_data_.ops == nullptr) {
        return;
    }
    op_from_python(view_data_, src, current_time());
}

void TSView::apply_delta(const value::View& delta) {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->apply_delta(view_data_, delta, current_time());
}

void TSView::apply_delta(const DeltaView& delta) {
    apply_delta(delta.value());
}

void TSView::invalidate() {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->invalidate(view_data_);
}

TSView TSView::child_at(size_t index) const {
    return child_at_impl(view_data_, index, view_data_.engine_time_ptr);
}

TSView TSView::child_by_name(std::string_view name) const {
    return child_by_name_impl(view_data_, name, view_data_.engine_time_ptr);
}

TSView TSView::child_by_key(const value::View& key) const {
    if (view_data_.meta == nullptr) {
        return {};
    }
    return child_by_key_impl(view_data_, key, view_data_.engine_time_ptr);
}

std::optional<size_t> TSView::child_slot_for(const TSView& child) const {
    return child_slot_from_short_path(short_path(), child.short_path());
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

TSIterable<value::View> TSLView::keys() const {
    return ts_list_keys(*this, TSCollectionFilter::All);
}

TSIterable<value::View> TSLView::valid_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSLView::modified_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSView> TSLView::values() const {
    return ts_list_values(*this, TSCollectionFilter::All);
}

TSIterable<TSView> TSLView::valid_values() const {
    return ts_list_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSView> TSLView::modified_values() const {
    return ts_list_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSLView::item_type> TSLView::items() const {
    return ts_list_items(*this, TSCollectionFilter::All);
}

TSIterable<TSLView::item_type> TSLView::valid_items() const {
    return ts_list_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSLView::item_type> TSLView::modified_items() const {
    return ts_list_items(*this, TSCollectionFilter::Modified);
}

TSIterable<value::View> TSLView::indices() const {
    return keys();
}

TSIterable<value::View> TSLView::valid_indices() const {
    return valid_keys();
}

TSIterable<value::View> TSLView::modified_indices() const {
    return modified_keys();
}

std::optional<size_t> TSBView::index_of(std::string_view name) const {
    return bundle_index_of_impl(view_data(), name);
}

std::string_view TSBView::name_at(size_t index) const {
    return bundle_name_at_impl(view_data(), index);
}

std::optional<std::string_view> TSBView::name_for_child(const TSView& child) const {
    const auto slot = child_slot_for(child);
    if (!slot.has_value()) {
        return std::nullopt;
    }
    const std::string_view name = name_at(*slot);
    if (name.empty()) {
        return std::nullopt;
    }
    return name;
}

bool TSBView::contains(std::string_view name) const {
    return bundle_contains_impl(view_data(), name);
}

TSIterable<value::View> TSBView::keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::All);
}

TSIterable<value::View> TSBView::valid_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSBView::modified_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSView> TSBView::values() const {
    return ts_bundle_values(*this, TSCollectionFilter::All);
}

TSIterable<TSView> TSBView::valid_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSView> TSBView::modified_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSBView::item_type> TSBView::items() const {
    return ts_bundle_items(*this, TSCollectionFilter::All);
}

TSIterable<TSBView::item_type> TSBView::valid_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSBView::item_type> TSBView::modified_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Modified);
}

TSIterable<size_t> TSBView::indices() const {
    return dense_indices(count());
}

TSIterable<size_t> TSBView::valid_indices() const {
    return ts_bundle_filtered_indices(*this, TSCollectionFilter::Valid);
}

TSIterable<size_t> TSBView::modified_indices() const {
    return ts_bundle_filtered_indices(*this, TSCollectionFilter::Modified);
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

bool TSSView::contains(const value::View& elem) const {
    return tss_contains_for_view(*this, elem);
}

size_t TSSView::size() const {
    return tss_size_for_view(*this);
}

TSIterable<value::View> TSSView::values() const {
    return tss_values_for_view(*this);
}

TSIterable<value::View> TSSView::added() const {
    return tss_delta_values_for_view(*this, 0);
}

TSIterable<value::View> TSSView::removed() const {
    return tss_delta_values_for_view(*this, 1);
}

bool TSSView::was_added(const value::View& elem) const {
    return tss_was_delta_value_for_view(*this, elem, 0);
}

bool TSSView::was_removed(const value::View& elem) const {
    return tss_was_delta_value_for_view(*this, elem, 1);
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

TSSView TSDView::key_set() const {
    return TSSView(tsd_key_set_projection_view(*this));
}

TSView TSDView::get(const value::View& key) const {
    return at_key(key);
}

TSView TSDView::get(const value::View& key, TSView default_value) const {
    TSView child = at_key(key);
    return child ? child : std::move(default_value);
}

TSView TSDView::get_or_create(const value::View& key) {
    TSView child = at_key(key);
    return child ? child : create(key);
}

bool TSDView::contains(const value::View& key) const {
    return static_cast<bool>(at_key(key));
}

TSIterable<value::View> TSDView::keys() const {
    return tsd_keys_for_view_iterable(*this, false);
}

TSIterable<value::View> TSDView::valid_keys() const {
    return ts_dict_valid_keys(*this, keys());
}

TSIterable<value::View> TSDView::modified_keys() const {
    return key_view_iterable_from_values(tsd_modified_keys_for_view_native(*this));
}

TSIterable<value::View> TSDView::added_keys() const {
    return key_view_iterable_from_values(tsd_added_keys_for_view_native(*this));
}

TSIterable<value::View> TSDView::removed_keys() const {
    return key_view_iterable_from_values(tsd_removed_keys_for_view_native(*this));
}

bool TSDView::has_added() const {
    return tsd_has_added_for_view(*this);
}

bool TSDView::has_removed() const {
    return tsd_has_removed_for_view(*this);
}

bool TSDView::was_added(const value::View& key) const {
    return tsd_was_added_for_view(*this, key);
}

bool TSDView::was_removed(const value::View& key) const {
    return tsd_was_removed_for_view(*this, key);
}

bool TSDView::was_modified(const value::View& key) const {
    return tsd_was_modified_for_view(*this, key);
}

TSIterable<TSView> TSDView::values() const {
    return ts_dict_values(*this, keys());
}

TSIterable<TSView> TSDView::valid_values() const {
    return ts_dict_values(*this, valid_keys());
}

TSIterable<TSView> TSDView::modified_values() const {
    return ts_dict_values(*this, modified_keys());
}

TSIterable<TSView> TSDView::added_values() const {
    return ts_dict_values(*this, added_keys());
}

TSIterable<TSView> TSDView::removed_values() const {
    std::vector<TSView> out;
    TSView parent = *this;
    auto dict = as_dict();
    for (value::View key : removed_keys()) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            out.emplace_back(std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent, key); previous_child.has_value()) {
            TSView previous_view = parent;
            previous_view.view_data() = previous_child->view_data();
            previous_view.view_data().sampled = true;
            out.emplace_back(std::move(previous_view));
        }
    }
    return out;
}

TSIterable<TSDView::item_type> TSDView::items() const {
    return ts_dict_items(*this, keys());
}

TSIterable<TSDView::item_type> TSDView::valid_items() const {
    return ts_dict_items(*this, valid_keys());
}

TSIterable<TSDView::item_type> TSDView::modified_items() const {
    return ts_dict_items(*this, modified_keys());
}

TSIterable<TSDView::item_type> TSDView::added_items() const {
    return ts_dict_items(*this, added_keys());
}

TSIterable<TSDView::item_type> TSDView::removed_items() const {
    std::vector<value::Value> key_values;
    std::vector<TSView> child_values;
    TSView parent = *this;
    auto dict = as_dict();
    auto all_keys = removed_keys();
    for (auto key : all_keys) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            key_values.emplace_back(key.clone());
            child_values.emplace_back(std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent, key); previous_child.has_value()) {
            TSView previous_view = parent;
            previous_view.view_data() = previous_child->view_data();
            previous_view.view_data().sampled = true;
            key_values.emplace_back(key.clone());
            child_values.emplace_back(std::move(previous_view));
        }
    }
    return item_iterable_from_owned_keys(std::move(key_values), std::move(child_values));
}

std::optional<value::Value> TSDView::key_at_slot(size_t slot) const {
    const value::View current = value();
    if (!current.valid() || !current.is_map()) {
        return std::nullopt;
    }

    const value::KeySetView key_set = current.as_map().keys();
    if (!key_set.is_live(slot)) {
        return std::nullopt;
    }
    return value::Value(key_set.at(slot));
}

std::optional<value::Value> TSDView::key_for_child(const TSView& child) const {
    const auto slot = child_slot_for(child);
    if (!slot.has_value()) {
        return std::nullopt;
    }
    return key_at_slot(*slot);
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
    if (std::getenv("HGRAPH_DEBUG_TSD_CREATE_DISPATCH") != nullptr) {
        const auto* vd_ops = view_data().ops;
        const TSMeta* meta = ts_meta();
        std::fprintf(stderr,
                     "[tsd_view.create] path=%s meta_kind=%d vd_ops=%p vd_ops_kind=%d dict_ops=%p key_valid=%d\n",
                     short_path().to_string().c_str(),
                     meta != nullptr ? static_cast<int>(meta->kind) : -1,
                     static_cast<const void*>(vd_ops),
                     vd_ops != nullptr ? static_cast<int>(vd_ops->kind) : -1,
                     static_cast<const void*>(ops),
                     key.valid() ? 1 : 0);
    }
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
    view_data_.ops->bind(view_data_, target.view_data_, current_time());
}

void TSView::unbind() {
    if (view_data_.ops == nullptr) {
        return;
    }
    view_data_.ops->unbind(view_data_, current_time());
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

ShortPath TSOutputView::short_path() const {
    if (owner_ == nullptr) {
        return ts_view_.short_path();
    }
    return owner_->to_short_path(ts_view_);
}

nb::object TSOutputView::to_python() const {
    return ts_view_.to_python();
}

nb::object TSOutputView::delta_to_python() const {
    return ts_view_.delta_to_python();
}

void TSOutputView::set_value(const value::View& src) {
    ts_view_.set_value(src);
}

void TSOutputView::from_python(const nb::object& src) {
    ts_view_.from_python(src);
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

void TSOutputView::apply_delta(const value::View& delta) {
    ts_view_.apply_delta(delta);
}

void TSOutputView::apply_delta(const DeltaView& delta) {
    ts_view_.apply_delta(delta);
}

void TSOutputView::invalidate() {
    ts_view_.invalidate();
}

TSOutputView TSOutputView::field(std::string_view name) const {
    return TSOutputView(owner_, as_ts_view().child_by_name(name));
}

TSOutputView TSOutputView::at(size_t index) const {
    return TSOutputView(owner_, as_ts_view().child_at(index));
}

TSOutputView TSOutputView::at_key(const value::View& key) const {
    return TSOutputView(owner_, as_ts_view().child_by_key(key));
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

value::View TSWOutputView::removed_value() const {
    return as_ts_view().as_window().removed_value();
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

bool TSSOutputView::contains(const value::View& elem) const {
    return as_ts_view().as_set().contains(elem);
}

size_t TSSOutputView::size() const {
    return as_ts_view().as_set().size();
}

TSIterable<value::View> TSSOutputView::values() const {
    return as_ts_view().as_set().values();
}

TSIterable<value::View> TSSOutputView::added() const {
    return as_ts_view().as_set().added();
}

TSIterable<value::View> TSSOutputView::removed() const {
    return as_ts_view().as_set().removed();
}

bool TSSOutputView::was_added(const value::View& elem) const {
    return as_ts_view().as_set().was_added(elem);
}

bool TSSOutputView::was_removed(const value::View& elem) const {
    return as_ts_view().as_set().was_removed(elem);
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

TSOutputView TSSOutputView::get_contains_output(const nb::object& item, const nb::object& requester) {
    TSOutputView base = *this;
    return runtime_tss_get_contains_output(base, item, requester);
}

void TSSOutputView::release_contains_output(const nb::object& item, const nb::object& requester) {
    TSOutputView base = *this;
    runtime_tss_release_contains_output(base, item, requester);
}

TSOutputView TSSOutputView::is_empty_output() {
    TSOutputView base = *this;
    return runtime_tss_get_is_empty_output(base);
}

TSSOutputView TSDOutputView::key_set() const {
    return TSSOutputView(tsd_key_set_projection_view(*this));
}

TSOutputView TSDOutputView::get(const value::View& key) const {
    return at_key(key);
}

TSOutputView TSDOutputView::get(const value::View& key, TSOutputView default_value) const {
    TSOutputView child = at_key(key);
    return child ? child : std::move(default_value);
}

TSOutputView TSDOutputView::get_or_create(const value::View& key) {
    TSOutputView child = at_key(key);
    return child ? child : create(key);
}

bool TSDOutputView::contains(const value::View& key) const {
    return static_cast<bool>(as_ts_view().as_dict().at_key(key));
}

TSIterable<value::View> TSDOutputView::keys() const {
    return tsd_keys_for_view_iterable(as_ts_view(), false);
}

TSIterable<value::View> TSDOutputView::valid_keys() const {
    return ts_dict_valid_keys(*this, keys());
}

TSIterable<value::View> TSDOutputView::modified_keys() const {
    return key_view_iterable_from_values(tsd_modified_keys_for_output_view_native(as_ts_view()));
}

TSIterable<value::View> TSDOutputView::added_keys() const {
    return key_view_iterable_from_values(tsd_added_keys_for_view_native(as_ts_view()));
}

TSIterable<value::View> TSDOutputView::removed_keys() const {
    return key_view_iterable_from_values(tsd_removed_keys_for_view_native(as_ts_view()));
}

bool TSDOutputView::has_added() const {
    return tsd_has_added_for_view(as_ts_view());
}

bool TSDOutputView::has_removed() const {
    return tsd_has_removed_for_view(as_ts_view());
}

bool TSDOutputView::was_added(const value::View& key) const {
    return tsd_was_added_for_view(as_ts_view(), key);
}

bool TSDOutputView::was_removed(const value::View& key) const {
    return tsd_was_removed_for_view(as_ts_view(), key);
}

bool TSDOutputView::was_modified(const value::View& key) const {
    return tsd_was_modified_for_output_view(as_ts_view(), key);
}

TSIterable<TSOutputView> TSDOutputView::values() const {
    return ts_dict_values(*this, keys());
}

TSIterable<TSOutputView> TSDOutputView::valid_values() const {
    return ts_dict_values(*this, valid_keys());
}

TSIterable<TSOutputView> TSDOutputView::modified_values() const {
    return ts_dict_values(*this, modified_keys());
}

TSIterable<TSOutputView> TSDOutputView::added_values() const {
    return ts_dict_values(*this, added_keys());
}

TSIterable<TSOutputView> TSDOutputView::removed_values() const {
    std::vector<TSOutputView> out;
    TSOutputView parent = *this;
    auto dict = as_ts_view().as_dict();
    for (value::View key : removed_keys()) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            out.emplace_back(owner_, std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent.as_ts_view(), key); previous_child.has_value()) {
            TSOutputView previous_view = parent;
            previous_view.as_ts_view().view_data() = previous_child->view_data();
            previous_view.as_ts_view().view_data().sampled = true;
            out.emplace_back(std::move(previous_view));
        }
    }
    return out;
}

TSIterable<TSDOutputView::item_type> TSDOutputView::items() const {
    return ts_dict_items(*this, keys());
}

TSIterable<TSDOutputView::item_type> TSDOutputView::valid_items() const {
    return ts_dict_items(*this, valid_keys());
}

TSIterable<TSDOutputView::item_type> TSDOutputView::modified_items() const {
    return ts_dict_items(*this, modified_keys());
}

TSIterable<TSDOutputView::item_type> TSDOutputView::added_items() const {
    return ts_dict_items(*this, added_keys());
}

TSIterable<TSDOutputView::item_type> TSDOutputView::removed_items() const {
    std::vector<value::Value> key_values;
    std::vector<TSOutputView> child_values;
    TSOutputView parent = *this;
    auto dict = as_ts_view().as_dict();
    auto keys = removed_keys();
    for (auto key : keys) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            key_values.emplace_back(key.clone());
            child_values.emplace_back(owner_, std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent.as_ts_view(), key); previous_child.has_value()) {
            TSOutputView previous_view = parent;
            previous_view.as_ts_view().view_data() = previous_child->view_data();
            previous_view.as_ts_view().view_data().sampled = true;
            key_values.emplace_back(key.clone());
            child_values.emplace_back(std::move(previous_view));
        }
    }
    return item_iterable_from_owned_keys(std::move(key_values), std::move(child_values));
}

TSOutputView TSDOutputView::at_key(const value::View& key) const {
    return TSOutputView(owner_, as_ts_view().as_dict().at_key(key));
}

std::optional<value::Value> TSDOutputView::key_at_slot(size_t slot) const {
    return as_ts_view().as_dict().key_at_slot(slot);
}

std::optional<value::Value> TSDOutputView::key_for_child(const TSOutputView& child) const {
    return as_ts_view().as_dict().key_for_child(child.as_ts_view());
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

TSOutputView TSDOutputView::get_ref(const nb::object& key, const nb::object& requester) {
    TSOutputView base = *this;
    return runtime_tsd_get_ref_output(base, key, requester);
}

void TSDOutputView::release_ref(const nb::object& key, const nb::object& requester) {
    TSOutputView base = *this;
    runtime_tsd_release_ref_output(base, key, requester);
}

void TSDOutputView::clear() {
    auto dict = as_ts_view().as_dict();
    std::vector<value::Value> key_values;
    for (value::View key : dict.keys()) {
        key_values.emplace_back(key.clone());
    }
    for (const auto& key : key_values) {
        (void)dict.remove(key.view());
    }
}

TSOutputView TSDOutputView::pop(const value::View& key) {
    TSOutputView child = at_key(key);
    if (!child) {
        return {};
    }
    if (!remove(key)) {
        return {};
    }
    return child;
}

TSOutputView TSDOutputView::pop(const value::View& key, TSOutputView default_value) {
    TSOutputView child = pop(key);
    return child ? child : std::move(default_value);
}

TSOutputView TSIndexedOutputView::at(size_t index) const {
    return TSOutputView(owner_, as_ts_view().as_indexed_unchecked().at(index));
}

size_t TSIndexedOutputView::count() const {
    return as_ts_view().as_indexed_unchecked().count();
}

TSIterable<value::View> TSLOutputView::indices() const {
    return keys();
}

TSIterable<value::View> TSLOutputView::keys() const {
    return ts_list_keys(*this, TSCollectionFilter::All);
}

TSIterable<value::View> TSLOutputView::valid_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSLOutputView::modified_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSOutputView> TSLOutputView::values() const {
    return ts_list_values(*this, TSCollectionFilter::All);
}

TSIterable<TSOutputView> TSLOutputView::valid_values() const {
    return ts_list_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSOutputView> TSLOutputView::modified_values() const {
    return ts_list_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSLOutputView::item_type> TSLOutputView::items() const {
    return ts_list_items(*this, TSCollectionFilter::All);
}

TSIterable<TSLOutputView::item_type> TSLOutputView::valid_items() const {
    return ts_list_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSLOutputView::item_type> TSLOutputView::modified_items() const {
    return ts_list_items(*this, TSCollectionFilter::Modified);
}

TSIterable<value::View> TSLOutputView::valid_indices() const {
    return valid_keys();
}

TSIterable<value::View> TSLOutputView::modified_indices() const {
    return modified_keys();
}

TSOutputView TSBOutputView::field(std::string_view name) const {
    return TSOutputView(owner_, as_ts_view().as_bundle().field(name));
}

std::optional<size_t> TSBOutputView::index_of(std::string_view name) const {
    return as_ts_view().as_bundle().index_of(name);
}

std::string_view TSBOutputView::name_at(size_t index) const {
    return as_ts_view().as_bundle().name_at(index);
}

std::optional<std::string_view> TSBOutputView::name_for_child(const TSOutputView& child) const {
    return as_ts_view().as_bundle().name_for_child(child.as_ts_view());
}

bool TSBOutputView::contains(std::string_view name) const {
    return as_ts_view().as_bundle().contains(name);
}

TSIterable<value::View> TSBOutputView::keys() const {
    return as_ts_view().as_bundle().keys();
}

TSIterable<value::View> TSBOutputView::valid_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSBOutputView::modified_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSOutputView> TSBOutputView::values() const {
    return ts_bundle_values(*this, TSCollectionFilter::All);
}

TSIterable<TSOutputView> TSBOutputView::valid_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSOutputView> TSBOutputView::modified_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSBOutputView::item_type> TSBOutputView::items() const {
    return ts_bundle_items(*this, TSCollectionFilter::All);
}

TSIterable<TSBOutputView::item_type> TSBOutputView::valid_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSBOutputView::item_type> TSBOutputView::modified_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Modified);
}

TSIterable<size_t> TSBOutputView::indices() const {
    return as_ts_view().as_bundle().indices();
}

TSIterable<size_t> TSBOutputView::valid_indices() const {
    return as_ts_view().as_bundle().valid_indices();
}

TSIterable<size_t> TSBOutputView::modified_indices() const {
    return as_ts_view().as_bundle().modified_indices();
}

FQPath TSInputView::fq_path() const {
    if (owner_ == nullptr) {
        return ts_view_.fq_path();
    }
    return owner_->to_fq_path(ts_view_);
}

ShortPath TSInputView::short_path() const {
    if (owner_ == nullptr) {
        return ts_view_.short_path();
    }
    return owner_->to_short_path(ts_view_);
}

TSInputView TSInputView::field(std::string_view name) const {
    return TSInputView(owner_, as_ts_view().child_by_name(name));
}

TSInputView TSInputView::at(size_t index) const {
    return TSInputView(owner_, as_ts_view().child_at(index));
}

TSInputView TSInputView::at_key(const value::View& key) const {
    return TSInputView(owner_, as_ts_view().child_by_key(key));
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

value::View TSWInputView::removed_value() const {
    return as_ts_view().as_window().removed_value();
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

bool TSSInputView::contains(const value::View& elem) const {
    return as_ts_view().as_set().contains(elem);
}

size_t TSSInputView::size() const {
    return as_ts_view().as_set().size();
}

TSIterable<value::View> TSSInputView::values() const {
    return as_ts_view().as_set().values();
}

TSIterable<value::View> TSSInputView::added() const {
    return as_ts_view().as_set().added();
}

TSIterable<value::View> TSSInputView::removed() const {
    return as_ts_view().as_set().removed();
}

bool TSSInputView::was_added(const value::View& elem) const {
    return as_ts_view().as_set().was_added(elem);
}

bool TSSInputView::was_removed(const value::View& elem) const {
    return as_ts_view().as_set().was_removed(elem);
}

TSSInputView TSDInputView::key_set() const {
    return TSSInputView(tsd_key_set_projection_view(*this));
}

TSInputView TSDInputView::get(const value::View& key) const {
    return at_key(key);
}

TSInputView TSDInputView::get(const value::View& key, TSInputView default_value) const {
    TSInputView child = at_key(key);
    return child ? child : std::move(default_value);
}

TSInputView TSDInputView::get_or_create(const value::View& key) {
    TSInputView child = at_key(key);
    return child ? child : TSInputView(owner_, as_ts_view().as_dict().create(key));
}

bool TSDInputView::contains(const value::View& key) const {
    return static_cast<bool>(as_ts_view().as_dict().at_key(key));
}

TSIterable<value::View> TSDInputView::keys() const {
    return tsd_keys_for_view_iterable(as_ts_view(), true);
}

TSIterable<value::View> TSDInputView::valid_keys() const {
    return ts_dict_valid_keys(*this, keys());
}

TSIterable<value::View> TSDInputView::modified_keys() const {
    return key_view_iterable_from_values(tsd_modified_keys_for_input_view_native(as_ts_view()));
}

TSIterable<value::View> TSDInputView::added_keys() const {
    return key_view_iterable_from_values(tsd_added_keys_for_view_native(as_ts_view()));
}

TSIterable<value::View> TSDInputView::removed_keys() const {
    return key_view_iterable_from_values(tsd_removed_keys_for_view_native(as_ts_view()));
}

bool TSDInputView::has_added() const {
    return tsd_has_added_for_view(as_ts_view());
}

bool TSDInputView::has_removed() const {
    return tsd_has_removed_for_view(as_ts_view());
}

bool TSDInputView::was_added(const value::View& key) const {
    return tsd_was_added_for_view(as_ts_view(), key);
}

bool TSDInputView::was_removed(const value::View& key) const {
    return tsd_was_removed_for_view(as_ts_view(), key);
}

bool TSDInputView::was_modified(const value::View& key) const {
    return tsd_was_modified_for_input_view(as_ts_view(), key);
}

TSIterable<TSInputView> TSDInputView::values() const {
    return ts_dict_values(*this, keys());
}

TSIterable<TSInputView> TSDInputView::valid_values() const {
    return ts_dict_values(*this, valid_keys());
}

TSIterable<TSInputView> TSDInputView::modified_values() const {
    return ts_dict_values(*this, modified_keys());
}

TSIterable<TSInputView> TSDInputView::added_values() const {
    return ts_dict_values(*this, added_keys());
}

TSIterable<TSInputView> TSDInputView::removed_values() const {
    std::vector<TSInputView> out;
    TSInputView parent = *this;
    auto dict = as_ts_view().as_dict();
    for (value::View key : removed_keys()) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            out.emplace_back(owner_, std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent.as_ts_view(), key); previous_child.has_value()) {
            TSInputView previous_view = parent;
            previous_view.as_ts_view().view_data() = previous_child->view_data();
            previous_view.as_ts_view().view_data().sampled = true;
            out.emplace_back(std::move(previous_view));
        }
    }
    return out;
}

TSIterable<TSDInputView::item_type> TSDInputView::items() const {
    return ts_dict_items(*this, keys());
}

TSIterable<TSDInputView::item_type> TSDInputView::valid_items() const {
    return ts_dict_items(*this, valid_keys());
}

TSIterable<TSDInputView::item_type> TSDInputView::modified_items() const {
    return ts_dict_items(*this, modified_keys());
}

TSIterable<TSDInputView::item_type> TSDInputView::added_items() const {
    return ts_dict_items(*this, added_keys());
}

TSIterable<TSDInputView::item_type> TSDInputView::removed_items() const {
    std::vector<value::Value> key_values;
    std::vector<TSInputView> child_values;
    TSInputView parent = *this;
    auto dict = as_ts_view().as_dict();
    auto keys = removed_keys();
    for (auto key : keys) {
        auto child = dict.at_key(key);
        if (child && child.valid()) {
            key_values.emplace_back(key.clone());
            child_values.emplace_back(owner_, std::move(child));
            continue;
        }

        if (auto previous_child = tsd_previous_child_for_key(parent.as_ts_view(), key); previous_child.has_value()) {
            TSInputView previous_view = parent;
            previous_view.as_ts_view().view_data() = previous_child->view_data();
            previous_view.as_ts_view().view_data().sampled = true;
            key_values.emplace_back(key.clone());
            child_values.emplace_back(std::move(previous_view));
        }
    }
    return item_iterable_from_owned_keys(std::move(key_values), std::move(child_values));
}

TSInputView TSDInputView::at_key(const value::View& key) const {
    return TSInputView(owner_, as_ts_view().as_dict().at_key(key));
}

std::optional<value::Value> TSDInputView::key_at_slot(size_t slot) const {
    return as_ts_view().as_dict().key_at_slot(slot);
}

std::optional<value::Value> TSDInputView::key_for_child(const TSInputView& child) const {
    return as_ts_view().as_dict().key_for_child(child.as_ts_view());
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

TSIterable<value::View> TSLInputView::indices() const {
    return keys();
}

TSIterable<value::View> TSLInputView::keys() const {
    return ts_list_keys(*this, TSCollectionFilter::All);
}

TSIterable<value::View> TSLInputView::valid_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSLInputView::modified_keys() const {
    return ts_list_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSInputView> TSLInputView::values() const {
    return ts_list_values(*this, TSCollectionFilter::All);
}

TSIterable<TSInputView> TSLInputView::valid_values() const {
    return ts_list_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSInputView> TSLInputView::modified_values() const {
    return ts_list_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSLInputView::item_type> TSLInputView::items() const {
    return ts_list_items(*this, TSCollectionFilter::All);
}

TSIterable<TSLInputView::item_type> TSLInputView::valid_items() const {
    return ts_list_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSLInputView::item_type> TSLInputView::modified_items() const {
    return ts_list_items(*this, TSCollectionFilter::Modified);
}

TSIterable<value::View> TSLInputView::valid_indices() const {
    return valid_keys();
}

TSIterable<value::View> TSLInputView::modified_indices() const {
    return modified_keys();
}

TSInputView TSBInputView::field(std::string_view name) const {
    return TSInputView(owner_, as_ts_view().as_bundle().field(name));
}

std::optional<size_t> TSBInputView::index_of(std::string_view name) const {
    return as_ts_view().as_bundle().index_of(name);
}

std::string_view TSBInputView::name_at(size_t index) const {
    return as_ts_view().as_bundle().name_at(index);
}

std::optional<std::string_view> TSBInputView::name_for_child(const TSInputView& child) const {
    return as_ts_view().as_bundle().name_for_child(child.as_ts_view());
}

bool TSBInputView::contains(std::string_view name) const {
    return as_ts_view().as_bundle().contains(name);
}

TSIterable<value::View> TSBInputView::keys() const {
    return as_ts_view().as_bundle().keys();
}

TSIterable<value::View> TSBInputView::valid_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Valid);
}

TSIterable<value::View> TSBInputView::modified_keys() const {
    return ts_bundle_keys(*this, TSCollectionFilter::Modified);
}

TSIterable<TSInputView> TSBInputView::values() const {
    return ts_bundle_values(*this, TSCollectionFilter::All);
}

TSIterable<TSInputView> TSBInputView::valid_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Valid);
}

TSIterable<TSInputView> TSBInputView::modified_values() const {
    return ts_bundle_values(*this, TSCollectionFilter::Modified);
}

TSIterable<TSBInputView::item_type> TSBInputView::items() const {
    return ts_bundle_items(*this, TSCollectionFilter::All);
}

TSIterable<TSBInputView::item_type> TSBInputView::valid_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Valid);
}

TSIterable<TSBInputView::item_type> TSBInputView::modified_items() const {
    return ts_bundle_items(*this, TSCollectionFilter::Modified);
}

TSIterable<size_t> TSBInputView::indices() const {
    return as_ts_view().as_bundle().indices();
}

TSIterable<size_t> TSBInputView::valid_indices() const {
    return as_ts_view().as_bundle().valid_indices();
}

TSIterable<size_t> TSBInputView::modified_indices() const {
    return as_ts_view().as_bundle().modified_indices();
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

void TSInputView::bind(const TSOutputView& target) {
    ts_view_.bind(target.as_ts_view());
    if (owner_ != nullptr && owner_->active(ts_view_)) {
        // Active inputs that bind later must re-attach notifier wiring immediately.
        owner_->set_active(ts_view_, true);
    }
}

void TSInputView::unbind() {
    ts_view_.unbind();
}

bool TSInputView::is_bound() const {
    return ts_view_.is_bound();
}

}  // namespace hgraph
