//
// Created by Claude on 15/12/2025.
//
// TypeMeta-based Time-Series Types Implementation
//

#include <hgraph/types/time_series/ts_v2_types.h>
#include <hgraph/types/node.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;

namespace hgraph {

// ============================================================================
// TsOutput
// ============================================================================

TsOutput::TsOutput(node_ptr parent, const TSTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta), _value(nb::none()) {}

TsOutput::TsOutput(time_series_output_ptr parent, const TSTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta), _value(nb::none()) {}

nb::object TsOutput::py_value() const {
    return _value;
}

nb::object TsOutput::py_delta_value() const {
    // For scalar TS, delta is the same as value
    return _value;
}

void TsOutput::py_set_value(const nb::object& value) {
    _value = value;
    mark_modified();
}

void TsOutput::apply_result(const nb::object& value) {
    py_set_value(value);
}

void TsOutput::mark_invalid() {
    _value = nb::none();
    BaseTimeSeriesOutput::mark_invalid();
}

void TsOutput::copy_from_output(const TimeSeriesOutput& output) {
    _value = output.py_value();
    mark_modified();
}

void TsOutput::copy_from_input(const TimeSeriesInput& input) {
    _value = input.py_value();
    mark_modified();
}

bool TsOutput::is_same_type(const TimeSeriesType* other) const {
    // Check if other is also a TsOutput with the same meta
    if (auto* ts = dynamic_cast<const TsOutput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

// ============================================================================
// TsInput
// ============================================================================

TsInput::TsInput(node_ptr parent, const TSTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {}

TsInput::TsInput(time_series_input_ptr parent, const TSTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {}

bool TsInput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TsInput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

// ============================================================================
// TssOutput
// ============================================================================

TssOutput::TssOutput(node_ptr parent, const TSSTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta),
      _value(nb::steal(PyFrozenSet_New(nullptr))),
      _added(nb::steal(PyFrozenSet_New(nullptr))),
      _removed(nb::steal(PyFrozenSet_New(nullptr))) {}

TssOutput::TssOutput(time_series_output_ptr parent, const TSSTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta),
      _value(nb::steal(PyFrozenSet_New(nullptr))),
      _added(nb::steal(PyFrozenSet_New(nullptr))),
      _removed(nb::steal(PyFrozenSet_New(nullptr))) {}

nb::object TssOutput::py_value() const {
    return _value;
}

nb::object TssOutput::py_delta_value() const {
    // Return a tuple of (added, removed) for delta
    return nb::make_tuple(_added, _removed);
}

void TssOutput::py_set_value(const nb::object& value) {
    // Compute delta: added = value - _value, removed = _value - value
    nb::object old_value = _value;
    _value = value;

    // Use Python set operations
    nb::module_ builtins = nb::module_::import_("builtins");
    nb::object frozenset_type = builtins.attr("frozenset");

    // added = frozenset(value) - frozenset(old_value)
    nb::object value_set = frozenset_type(value);
    nb::object old_set = frozenset_type(old_value);
    _added = value_set.attr("__sub__")(old_set);
    _removed = old_set.attr("__sub__")(value_set);

    mark_modified();
}

void TssOutput::apply_result(const nb::object& value) {
    py_set_value(value);
}

void TssOutput::mark_invalid() {
    _value = nb::steal(PyFrozenSet_New(nullptr));
    _added = nb::steal(PyFrozenSet_New(nullptr));
    _removed = nb::steal(PyFrozenSet_New(nullptr));
    BaseTimeSeriesOutput::mark_invalid();
}

void TssOutput::copy_from_output(const TimeSeriesOutput& output) {
    _value = output.py_value();
    if (auto* tss = dynamic_cast<const TssOutput*>(&output)) {
        _added = tss->_added;
        _removed = tss->_removed;
    }
    mark_modified();
}

void TssOutput::copy_from_input(const TimeSeriesInput& input) {
    _value = input.py_value();
    mark_modified();
}

bool TssOutput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TssOutput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

nb::object TssOutput::added() const {
    return _added;
}

nb::object TssOutput::removed() const {
    return _removed;
}

// ============================================================================
// TssInput
// ============================================================================

TssInput::TssInput(node_ptr parent, const TSSTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {}

TssInput::TssInput(time_series_input_ptr parent, const TSSTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {}

bool TssInput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TssInput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

nb::object TssInput::added() const {
    if (auto* out = dynamic_cast<const TssOutput*>(output().get())) {
        return out->added();
    }
    return nb::steal(PyFrozenSet_New(nullptr));
}

nb::object TssInput::removed() const {
    if (auto* out = dynamic_cast<const TssOutput*>(output().get())) {
        return out->removed();
    }
    return nb::steal(PyFrozenSet_New(nullptr));
}

// ============================================================================
// TslOutput
// ============================================================================

TslOutput::TslOutput(node_ptr parent, const TSLTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta) {
    // Create child elements based on size
    if (meta->size > 0) {
        _elements.reserve(meta->size);
        for (int64_t i = 0; i < meta->size; ++i) {
            auto element = meta->element_ts_type->make_output(this);
            _elements.push_back(element);
        }
    }
}

TslOutput::TslOutput(time_series_output_ptr parent, const TSLTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta) {
    if (meta->size > 0) {
        _elements.reserve(meta->size);
        for (int64_t i = 0; i < meta->size; ++i) {
            auto element = meta->element_ts_type->make_output(this);
            _elements.push_back(element);
        }
    }
}

nb::object TslOutput::py_value() const {
    nb::list result;
    for (const auto& elem : _elements) {
        result.append(elem->py_value());
    }
    return nb::tuple(result);
}

nb::object TslOutput::py_delta_value() const {
    nb::list result;
    for (const auto& elem : _elements) {
        if (elem->modified()) {
            result.append(elem->py_delta_value());
        } else {
            result.append(nb::none());
        }
    }
    return nb::tuple(result);
}

void TslOutput::py_set_value(const nb::object& value) {
    // Value should be a sequence
    nb::list items = nb::list(value);
    size_t count = std::min(static_cast<size_t>(nb::len(items)), _elements.size());
    for (size_t i = 0; i < count; ++i) {
        _elements[i]->py_set_value(items[i]);
    }
    mark_modified();
}

void TslOutput::apply_result(const nb::object& value) {
    py_set_value(value);
}

void TslOutput::mark_invalid() {
    for (auto& elem : _elements) {
        elem->mark_invalid();
    }
    BaseTimeSeriesOutput::mark_invalid();
}

void TslOutput::invalidate() {
    for (auto& elem : _elements) {
        elem->invalidate();
    }
    BaseTimeSeriesOutput::invalidate();
}

void TslOutput::copy_from_output(const TimeSeriesOutput& output) {
    if (auto* tsl = dynamic_cast<const TslOutput*>(&output)) {
        size_t count = std::min(_elements.size(), tsl->_elements.size());
        for (size_t i = 0; i < count; ++i) {
            _elements[i]->copy_from_output(*tsl->_elements[i]);
        }
    }
    mark_modified();
}

void TslOutput::copy_from_input(const TimeSeriesInput& input) {
    if (auto* tsl = dynamic_cast<const TslInput*>(&input)) {
        size_t count = std::min(_elements.size(), tsl->size());
        for (size_t i = 0; i < count; ++i) {
            _elements[i]->copy_from_input(*tsl->operator[](i));
        }
    }
    mark_modified();
}

bool TslOutput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TslOutput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

bool TslOutput::all_valid() const {
    if (empty()) return true;
    return valid() && std::ranges::all_of(_elements, [](const auto& ts) { return ts->valid(); });
}

bool TslOutput::has_reference() const {
    return std::ranges::any_of(_elements, [](const auto& ts) { return ts->has_reference(); });
}

size_t TslOutput::size() const {
    return _elements.size();
}

bool TslOutput::empty() const {
    return _elements.empty();
}

TimeSeriesOutput::s_ptr TslOutput::operator[](size_t ndx) {
    return _elements.at(ndx);
}

// ============================================================================
// TslInput
// ============================================================================

TslInput::TslInput(node_ptr parent, const TSLTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {
    if (meta->size > 0) {
        _elements.reserve(meta->size);
        for (int64_t i = 0; i < meta->size; ++i) {
            auto element = meta->element_ts_type->make_input(this);
            _elements.push_back(element);
        }
    }
}

TslInput::TslInput(time_series_input_ptr parent, const TSLTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {
    if (meta->size > 0) {
        _elements.reserve(meta->size);
        for (int64_t i = 0; i < meta->size; ++i) {
            auto element = meta->element_ts_type->make_input(this);
            _elements.push_back(element);
        }
    }
}

bool TslInput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TslInput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

bool TslInput::modified() const {
    return std::ranges::any_of(_elements, [](const auto& ts) { return ts->modified(); });
}

bool TslInput::valid() const {
    return std::ranges::any_of(_elements, [](const auto& ts) { return ts->valid(); });
}

bool TslInput::all_valid() const {
    if (empty()) return true;
    return std::ranges::all_of(_elements, [](const auto& ts) { return ts->valid(); });
}

engine_time_t TslInput::last_modified_time() const {
    engine_time_t max_time = MIN_DT;
    for (const auto& elem : _elements) {
        max_time = std::max(max_time, elem->last_modified_time());
    }
    return max_time;
}

bool TslInput::bound() const {
    return std::ranges::all_of(_elements, [](const auto& ts) { return ts->bound(); });
}

bool TslInput::active() const {
    return std::ranges::any_of(_elements, [](const auto& ts) { return ts->active(); });
}

bool TslInput::has_reference() const {
    return std::ranges::any_of(_elements, [](const auto& ts) { return ts->has_reference(); });
}

void TslInput::make_active() {
    for (auto& elem : _elements) {
        elem->make_active();
    }
}

void TslInput::make_passive() {
    for (auto& elem : _elements) {
        elem->make_passive();
    }
}

TimeSeriesInput::s_ptr TslInput::get_input(size_t index) {
    return _elements.at(index);
}

size_t TslInput::size() const {
    return _elements.size();
}

bool TslInput::empty() const {
    return _elements.empty();
}

TimeSeriesInput::s_ptr TslInput::operator[](size_t ndx) {
    return _elements.at(ndx);
}

TimeSeriesInput::s_ptr TslInput::operator[](size_t ndx) const {
    return _elements.at(ndx);
}

bool TslInput::do_bind_output(time_series_output_s_ptr value) {
    if (auto* tsl_out = dynamic_cast<TslOutput*>(value.get())) {
        if (tsl_out->size() != _elements.size()) {
            return false;
        }
        for (size_t i = 0; i < _elements.size(); ++i) {
            _elements[i]->bind_output((*tsl_out)[i]);
        }
        return true;
    }
    return false;
}

void TslInput::do_un_bind_output(bool unbind_refs) {
    for (auto& elem : _elements) {
        elem->un_bind_output(unbind_refs);
    }
}

// ============================================================================
// TsbOutput
// ============================================================================

TsbOutput::TsbOutput(node_ptr parent, const TSBTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta) {
    _fields.reserve(meta->fields.size());
    for (const auto& field : meta->fields) {
        auto element = field.type->make_output(this);
        _fields.push_back(element);
    }
}

TsbOutput::TsbOutput(time_series_output_ptr parent, const TSBTypeMeta* meta)
    : BaseTimeSeriesOutput(parent), _meta(meta) {
    _fields.reserve(meta->fields.size());
    for (const auto& field : meta->fields) {
        auto element = field.type->make_output(this);
        _fields.push_back(element);
    }
}

nb::object TsbOutput::py_value() const {
    nb::dict result;
    for (size_t i = 0; i < _fields.size(); ++i) {
        result[nb::str(_meta->fields[i].name.c_str())] = _fields[i]->py_value();
    }
    return result;
}

nb::object TsbOutput::py_delta_value() const {
    nb::dict result;
    for (size_t i = 0; i < _fields.size(); ++i) {
        if (_fields[i]->modified()) {
            result[nb::str(_meta->fields[i].name.c_str())] = _fields[i]->py_delta_value();
        }
    }
    return result;
}

void TsbOutput::py_set_value(const nb::object& value) {
    // Value should be a dict
    nb::dict d = nb::cast<nb::dict>(value);
    for (size_t i = 0; i < _fields.size(); ++i) {
        nb::str key(_meta->fields[i].name.c_str());
        if (d.contains(key)) {
            _fields[i]->py_set_value(d[key]);
        }
    }
    mark_modified();
}

void TsbOutput::apply_result(const nb::object& value) {
    py_set_value(value);
}

void TsbOutput::mark_invalid() {
    for (auto& field : _fields) {
        field->mark_invalid();
    }
    BaseTimeSeriesOutput::mark_invalid();
}

void TsbOutput::invalidate() {
    for (auto& field : _fields) {
        field->invalidate();
    }
    BaseTimeSeriesOutput::invalidate();
}

void TsbOutput::copy_from_output(const TimeSeriesOutput& output) {
    if (auto* tsb = dynamic_cast<const TsbOutput*>(&output)) {
        size_t count = std::min(_fields.size(), tsb->_fields.size());
        for (size_t i = 0; i < count; ++i) {
            _fields[i]->copy_from_output(*tsb->_fields[i]);
        }
    }
    mark_modified();
}

void TsbOutput::copy_from_input(const TimeSeriesInput& input) {
    if (auto* tsb = dynamic_cast<const TsbInput*>(&input)) {
        size_t count = std::min(_fields.size(), tsb->size());
        for (size_t i = 0; i < count; ++i) {
            _fields[i]->copy_from_input(*tsb->operator[](i));
        }
    }
    mark_modified();
}

bool TsbOutput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TsbOutput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

bool TsbOutput::all_valid() const {
    return valid() && std::ranges::all_of(_fields, [](const auto& ts) { return ts->valid(); });
}

bool TsbOutput::has_reference() const {
    return std::ranges::any_of(_fields, [](const auto& ts) { return ts->has_reference(); });
}

size_t TsbOutput::size() const {
    return _fields.size();
}

TimeSeriesOutput::s_ptr TsbOutput::operator[](size_t ndx) {
    return _fields.at(ndx);
}

TimeSeriesOutput::s_ptr TsbOutput::operator[](const std::string& name) {
    for (size_t i = 0; i < _meta->fields.size(); ++i) {
        if (_meta->fields[i].name == name) {
            return _fields[i];
        }
    }
    throw std::out_of_range("Field not found: " + name);
}

// ============================================================================
// TsbInput
// ============================================================================

TsbInput::TsbInput(node_ptr parent, const TSBTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {
    _fields.reserve(meta->fields.size());
    for (const auto& field : meta->fields) {
        auto element = field.type->make_input(this);
        _fields.push_back(element);
    }
}

TsbInput::TsbInput(time_series_input_ptr parent, const TSBTypeMeta* meta)
    : BaseTimeSeriesInput(parent), _meta(meta) {
    _fields.reserve(meta->fields.size());
    for (const auto& field : meta->fields) {
        auto element = field.type->make_input(this);
        _fields.push_back(element);
    }
}

bool TsbInput::is_same_type(const TimeSeriesType* other) const {
    if (auto* ts = dynamic_cast<const TsbInput*>(other)) {
        return _meta == ts->_meta;
    }
    return false;
}

bool TsbInput::modified() const {
    return std::ranges::any_of(_fields, [](const auto& ts) { return ts->modified(); });
}

bool TsbInput::valid() const {
    return std::ranges::any_of(_fields, [](const auto& ts) { return ts->valid(); });
}

bool TsbInput::all_valid() const {
    return std::ranges::all_of(_fields, [](const auto& ts) { return ts->valid(); });
}

engine_time_t TsbInput::last_modified_time() const {
    engine_time_t max_time = MIN_DT;
    for (const auto& field : _fields) {
        max_time = std::max(max_time, field->last_modified_time());
    }
    return max_time;
}

bool TsbInput::bound() const {
    return std::ranges::all_of(_fields, [](const auto& ts) { return ts->bound(); });
}

bool TsbInput::active() const {
    return std::ranges::any_of(_fields, [](const auto& ts) { return ts->active(); });
}

bool TsbInput::has_reference() const {
    return std::ranges::any_of(_fields, [](const auto& ts) { return ts->has_reference(); });
}

void TsbInput::make_active() {
    for (auto& field : _fields) {
        field->make_active();
    }
}

void TsbInput::make_passive() {
    for (auto& field : _fields) {
        field->make_passive();
    }
}

TimeSeriesInput::s_ptr TsbInput::get_input(size_t index) {
    return _fields.at(index);
}

size_t TsbInput::size() const {
    return _fields.size();
}

TimeSeriesInput::s_ptr TsbInput::operator[](size_t ndx) {
    return _fields.at(ndx);
}

TimeSeriesInput::s_ptr TsbInput::operator[](size_t ndx) const {
    return _fields.at(ndx);
}

TimeSeriesInput::s_ptr TsbInput::operator[](const std::string& name) {
    for (size_t i = 0; i < _meta->fields.size(); ++i) {
        if (_meta->fields[i].name == name) {
            return _fields[i];
        }
    }
    throw std::out_of_range("Field not found: " + name);
}

TimeSeriesInput::s_ptr TsbInput::operator[](const std::string& name) const {
    for (size_t i = 0; i < _meta->fields.size(); ++i) {
        if (_meta->fields[i].name == name) {
            return _fields[i];
        }
    }
    throw std::out_of_range("Field not found: " + name);
}

bool TsbInput::do_bind_output(time_series_output_s_ptr value) {
    if (auto* tsb_out = dynamic_cast<TsbOutput*>(value.get())) {
        if (tsb_out->size() != _fields.size()) {
            return false;
        }
        for (size_t i = 0; i < _fields.size(); ++i) {
            _fields[i]->bind_output((*tsb_out)[i]);
        }
        return true;
    }
    return false;
}

void TsbInput::do_un_bind_output(bool unbind_refs) {
    for (auto& field : _fields) {
        field->un_bind_output(unbind_refs);
    }
}

} // namespace hgraph
