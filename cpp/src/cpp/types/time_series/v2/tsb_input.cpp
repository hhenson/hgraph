//
// TsbInput implementation
//

#include <hgraph/types/time_series/v2/tsb_input.h>
#include <hgraph/types/time_series/v2/tsb_output.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
namespace ts {

TsbInput::TsbInput(node_ptr parent, TimeSeriesSchema* schema)
    : _ctx(parent), _schema(schema) {
    // Build key-to-index mapping from schema
    const auto& keys = schema->keys();
    for (size_t i = 0; i < keys.size(); ++i) {
        _key_to_index[keys[i]] = i;
    }
}

TsbInput::TsbInput(TimeSeriesInput* parent, TimeSeriesSchema* schema)
    : _ctx(parent), _schema(schema) {
    const auto& keys = schema->keys();
    for (size_t i = 0; i < keys.size(); ++i) {
        _key_to_index[keys[i]] = i;
    }
}

TsbInput::~TsbInput() {
    // Children will clean up themselves
}

nb::object TsbInput::py_value() const {
    nb::dict result;
    const auto& keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->valid()) {
            result[keys[i].c_str()] = _children[i]->py_value();
        }
    }
    return result;
}

nb::object TsbInput::py_delta_value() const {
    nb::dict result;
    const auto& keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->modified()) {
            result[keys[i].c_str()] = _children[i]->py_delta_value();
        }
    }
    return result;
}

engine_time_t TsbInput::last_modified_time() const {
    engine_time_t max_time = MIN_DT;
    for (const auto& child : _children) {
        if (child) {
            auto child_time = child->last_modified_time();
            if (child_time > max_time) max_time = child_time;
        }
    }
    return max_time;
}

bool TsbInput::modified() const {
    auto current = _ctx.current_time();
    if (_sample_time == current) return true;
    for (const auto& child : _children) {
        if (child && child->modified()) return true;
    }
    return false;
}

bool TsbInput::valid() const {
    for (const auto& child : _children) {
        if (child && child->valid()) return true;
    }
    return false;
}

bool TsbInput::all_valid() const {
    for (const auto& child : _children) {
        if (!child || !child->valid()) return false;
    }
    return !_children.empty();
}

void TsbInput::builder_release_cleanup() {
    for (auto& child : _children) {
        if (child) child->builder_release_cleanup();
    }
}

bool TsbInput::is_same_type(const TimeSeriesType* other) const {
    if (auto* tsb = dynamic_cast<const TsbInput*>(other)) {
        return _schema == tsb->_schema;
    }
    return false;
}

bool TsbInput::has_reference() const {
    for (const auto& child : _children) {
        if (child && child->has_reference()) return true;
    }
    return false;
}

TimeSeriesInput::s_ptr TsbInput::parent_input() const {
    return _ctx.parent_input();
}

void TsbInput::make_active() {
    if (!_active) {
        _active = true;
        for (auto& child : _children) {
            if (child) child->make_active();
        }
    }
}

void TsbInput::make_passive() {
    if (_active) {
        _active = false;
        for (auto& child : _children) {
            if (child) child->make_passive();
        }
    }
}

bool TsbInput::bound() const {
    for (const auto& child : _children) {
        if (child && child->bound()) return true;
    }
    return false;
}

bool TsbInput::has_peer() const {
    for (const auto& child : _children) {
        if (child && child->has_peer()) return true;
    }
    return false;
}

bool TsbInput::bind_output(time_series_output_s_ptr output_) {
    _bound_output = output_;

    // Try to bind to a TsbOutput
    auto* tsb_output = dynamic_cast<TsbOutput*>(output_.get());
    if (!tsb_output) {
        // Can't bind to non-bundle output
        return false;
    }

    // Bind children by key
    const auto& keys = _schema->keys();
    for (size_t i = 0; i < _children.size() && i < keys.size(); ++i) {
        if (_children[i] && tsb_output->contains(keys[i])) {
            _children[i]->bind_output((*tsb_output)[keys[i]]);
        }
    }

    // Check if we should notify on bind
    auto node = owning_node();
    if (node && (node->is_started() || node->is_starting()) && valid()) {
        _sample_time = _ctx.current_time();
        if (_active) notify(_sample_time);
    }

    return true;
}

void TsbInput::un_bind_output(bool unbind_refs) {
    for (auto& child : _children) {
        if (child) child->un_bind_output(unbind_refs);
    }
    _bound_output.reset();
}

TimeSeriesInput::s_ptr TsbInput::get_input(size_t index) {
    if (index < _children.size()) {
        return _children[index];
    }
    return nullptr;
}

void TsbInput::notify(engine_time_t modified_time) {
    if (has_parent_input()) {
        parent_input()->notify(modified_time);
    } else if (has_owning_node()) {
        owning_node()->notify(modified_time);
    }
}

TsbInput::child_ptr& TsbInput::operator[](size_t index) {
    return _children.at(index);
}

const TsbInput::child_ptr& TsbInput::operator[](size_t index) const {
    return _children.at(index);
}

TsbInput::child_ptr& TsbInput::operator[](const std::string& key) {
    return _children.at(_key_to_index.at(key));
}

const TsbInput::child_ptr& TsbInput::operator[](const std::string& key) const {
    return _children.at(_key_to_index.at(key));
}

bool TsbInput::contains(const std::string& key) const {
    return _key_to_index.contains(key);
}

TsbInput::key_collection_type TsbInput::keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (const auto& key : schema_keys) {
        result.emplace_back(key);
    }
    return result;
}

TsbInput::key_collection_type TsbInput::valid_keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->valid()) {
            result.emplace_back(schema_keys[i]);
        }
    }
    return result;
}

TsbInput::key_collection_type TsbInput::modified_keys() const {
    key_collection_type result;
    const auto& schema_keys = _schema->keys();
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && _children[i]->modified()) {
            result.emplace_back(schema_keys[i]);
        }
    }
    return result;
}

void TsbInput::set_children(collection_type children) {
    _children = std::move(children);
}

TsbInput::s_ptr TsbInput::copy_with(node_ptr parent, collection_type children) {
    auto copy = std::make_shared<TsbInput>(parent, _schema);
    copy->set_children(std::move(children));
    return copy;
}

} // namespace ts
} // namespace hgraph
