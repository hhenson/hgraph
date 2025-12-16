//
// TsbOutput implementation
//

#include <hgraph/types/time_series/v2/tsb_output.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph {
namespace ts {

TsbOutput::TsbOutput(node_ptr parent, TimeSeriesSchema* schema)
    : _ctx(parent), _schema(schema) {
    // Build key-to-index mapping from schema
    const auto& keys = schema->keys();
    for (size_t i = 0; i < keys.size(); ++i) {
        _key_to_index[keys[i]] = i;
    }
}

TsbOutput::TsbOutput(TimeSeriesOutput* parent, TimeSeriesSchema* schema)
    : _ctx(parent), _schema(schema) {
    const auto& keys = schema->keys();
    for (size_t i = 0; i < keys.size(); ++i) {
        _key_to_index[keys[i]] = i;
    }
}

nb::object TsbOutput::py_value() const {
    nb::dict result;
    const auto& keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->valid()) {
            result[keys[i].c_str()] = _children[i]->py_value();
        }
    }
    return result;
}

nb::object TsbOutput::py_delta_value() const {
    nb::dict result;
    const auto& keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->modified()) {
            result[keys[i].c_str()] = _children[i]->py_delta_value();
        }
    }
    return result;
}

engine_time_t TsbOutput::last_modified_time() const {
    return _last_modified;
}

bool TsbOutput::modified() const {
    for (const auto& child : _children) {
        if (child && child->modified()) return true;
    }
    return false;
}

bool TsbOutput::valid() const {
    for (const auto& child : _children) {
        if (child && child->valid()) return true;
    }
    return false;
}

bool TsbOutput::all_valid() const {
    for (const auto& child : _children) {
        if (!child || !child->valid()) return false;
    }
    return !_children.empty();
}

void TsbOutput::builder_release_cleanup() {
    for (auto& child : _children) {
        if (child) child->builder_release_cleanup();
    }
}

bool TsbOutput::is_same_type(const TimeSeriesType* other) const {
    if (auto* tsb = dynamic_cast<const TsbOutput*>(other)) {
        // Same schema means same type
        return _schema == tsb->_schema;
    }
    return false;
}

bool TsbOutput::has_reference() const {
    for (const auto& child : _children) {
        if (child && child->has_reference()) return true;
    }
    return false;
}

TimeSeriesOutput::s_ptr TsbOutput::parent_output() const {
    return _ctx.parent_output();
}

TimeSeriesOutput::s_ptr TsbOutput::parent_output() {
    return _ctx.parent_output();
}

void TsbOutput::subscribe(Notifiable* n) {
    _subscribers.insert(n);
}

void TsbOutput::un_subscribe(Notifiable* n) {
    _subscribers.erase(n);
}

void TsbOutput::apply_result(const nb::object& value) {
    if (!value.is_none()) {
        py_set_value(value);
    }
}

void TsbOutput::py_set_value(const nb::object& value) {
    if (value.is_none()) {
        invalidate();
        return;
    }

    // Expect a dict-like object
    if (nb::isinstance<nb::dict>(value)) {
        nb::dict dict_value = nb::cast<nb::dict>(value);
        for (auto item : dict_value) {
            std::string key = nb::cast<std::string>(item.first);
            auto it = _key_to_index.find(key);
            if (it != _key_to_index.end() && _children[it->second]) {
                _children[it->second]->py_set_value(nb::cast<nb::object>(item.second));
            }
        }
    }
}

void TsbOutput::copy_from_output(const TimeSeriesOutput& output) {
    if (auto* tsb = dynamic_cast<const TsbOutput*>(&output)) {
        for (size_t i = 0; i < _children.size() && i < tsb->_children.size(); ++i) {
            if (_children[i] && tsb->_children[i]) {
                _children[i]->copy_from_output(*tsb->_children[i]);
            }
        }
    }
}

void TsbOutput::copy_from_input(const TimeSeriesInput& input) {
    // Not typically used for bundles
}

void TsbOutput::clear() {
    for (auto& child : _children) {
        if (child) child->clear();
    }
}

void TsbOutput::invalidate() {
    for (auto& child : _children) {
        if (child) child->invalidate();
    }
}

void TsbOutput::mark_invalid() {
    for (auto& child : _children) {
        if (child) child->mark_invalid();
    }
}

void TsbOutput::mark_modified() {
    _last_modified = _ctx.current_time();
    for (auto* sub : _subscribers) {
        sub->notify(_last_modified);
    }
}

void TsbOutput::mark_modified(engine_time_t modified_time) {
    _last_modified = modified_time;
    for (auto* sub : _subscribers) {
        sub->notify(modified_time);
    }
}

void TsbOutput::mark_child_modified(TimeSeriesOutput& child, engine_time_t modified_time) {
    _last_modified = modified_time;
    for (auto* sub : _subscribers) {
        sub->notify(modified_time);
    }
}

bool TsbOutput::can_apply_result(const nb::object& value) {
    return nb::isinstance<nb::dict>(value) || value.is_none();
}

TsbOutput::child_ptr& TsbOutput::operator[](size_t index) {
    return _children.at(index);
}

const TsbOutput::child_ptr& TsbOutput::operator[](size_t index) const {
    return _children.at(index);
}

TsbOutput::child_ptr& TsbOutput::operator[](const std::string& key) {
    return _children.at(_key_to_index.at(key));
}

const TsbOutput::child_ptr& TsbOutput::operator[](const std::string& key) const {
    return _children.at(_key_to_index.at(key));
}

bool TsbOutput::contains(const std::string& key) const {
    return _key_to_index.contains(key);
}

TsbOutput::key_collection_type TsbOutput::keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (const auto& key : schema_keys) {
        result.emplace_back(key);
    }
    return result;
}

TsbOutput::key_collection_type TsbOutput::valid_keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->valid()) {
            result.emplace_back(schema_keys[i]);
        }
    }
    return result;
}

TsbOutput::key_collection_type TsbOutput::modified_keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->modified()) {
            result.emplace_back(schema_keys[i]);
        }
    }
    return result;
}

void TsbOutput::set_children(collection_type children) {
    _children = std::move(children);
}

} // namespace ts
} // namespace hgraph
