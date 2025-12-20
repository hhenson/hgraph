//
// Created by Claude on 20/12/2025.
//
// Python wrapper for TSOutput and TSOutputView.
// Exposes TSOutput functionality to Python for testing.
//

#ifndef HGRAPH_PY_TS_OUTPUT_H
#define HGRAPH_PY_TS_OUTPUT_H

#include <nanobind/nanobind.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/util/date_time.h>
#include <memory>

namespace nb = nanobind;

namespace hgraph::ts {

/**
 * PyTSOutputView - Python wrapper for TSOutputView
 *
 * Provides fluent navigation API and value access with explicit time parameters.
 */
class PyTSOutputView {
public:
    PyTSOutputView() = default;

    explicit PyTSOutputView(TSOutputView view)
        : _view(std::move(view)) {}

    // === Validity and type queries ===
    [[nodiscard]] bool valid() const { return _view.valid(); }

    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _view.meta(); }

    [[nodiscard]] const value::TypeMeta* value_schema() const { return _view.value_schema(); }

    [[nodiscard]] value::TypeKind kind() const { return _view.kind(); }

    [[nodiscard]] TimeSeriesKind ts_kind() const { return _view.ts_kind(); }

    [[nodiscard]] std::string type_name() const {
        if (auto* m = meta()) {
            return m->type_name_str();
        }
        return "unknown";
    }

    // === Path tracking ===
    [[nodiscard]] std::string path_string() const { return _view.path_string(); }

    // === Modification tracking ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _view.modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _view.last_modified_time();
    }

    [[nodiscard]] bool has_value() const {
        return _view.has_value();
    }

    void mark_modified(engine_time_t time) {
        _view.mark_modified(time);
    }

    void mark_invalid() {
        _view.mark_invalid();
    }

    // === Value access ===
    [[nodiscard]] nb::object py_value() const {
        if (!valid()) return nb::none();
        auto const_view = _view.value_view().value_view();
        return value::value_to_python(const_view.data(), const_view.schema());
    }

    void set_value(nb::object py_obj, engine_time_t time) {
        if (!valid()) {
            throw std::runtime_error("Cannot set value on invalid view");
        }
        if (py_obj.is_none()) {
            return;
        }
        auto value_view = _view.value_view().value_view();
        value::value_from_python(value_view.data(), py_obj, value_view.schema());
        _view.mark_modified(time);
    }

    // === Bundle field navigation ===
    [[nodiscard]] PyTSOutputView field(size_t index) {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            throw std::runtime_error("field() requires a valid Bundle type");
        }
        if (index >= _view.field_count()) {
            throw std::runtime_error("Invalid field index");
        }
        return PyTSOutputView(std::move(_view).field(index));
    }

    [[nodiscard]] PyTSOutputView field_by_name(const std::string& name) {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            throw std::runtime_error("field() requires a valid Bundle type");
        }
        auto field_view = std::move(_view).field(name);
        if (!field_view.valid()) {
            throw std::runtime_error("Invalid field name: " + name);
        }
        return PyTSOutputView(std::move(field_view));
    }

    [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
        return _view.field_modified_at(index, time);
    }

    [[nodiscard]] size_t field_count() const {
        return _view.field_count();
    }

    // === List element navigation ===
    [[nodiscard]] PyTSOutputView element(size_t index) {
        if (!valid() || kind() != value::TypeKind::List) {
            throw std::runtime_error("element() requires a valid List type");
        }
        if (index >= _view.list_size()) {
            throw std::runtime_error("Invalid element index");
        }
        return PyTSOutputView(std::move(_view).element(index));
    }

    [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
        return _view.element_modified_at(index, time);
    }

    [[nodiscard]] size_t list_size() const {
        return _view.list_size();
    }

    // === Set operations ===
    [[nodiscard]] size_t set_size() const {
        return _view.set_size();
    }

    // === Dict operations ===
    [[nodiscard]] size_t dict_size() const {
        return _view.dict_size();
    }

    // === Window operations ===
    [[nodiscard]] size_t window_size() const {
        return _view.window_size();
    }

    [[nodiscard]] bool window_empty() const {
        return _view.window_empty();
    }

    [[nodiscard]] bool window_full() const {
        return _view.window_full();
    }

    [[nodiscard]] nb::object window_get(size_t index) const {
        if (!valid() || kind() != value::TypeKind::Window) {
            return nb::none();
        }
        auto elem = _view.window_get(index);
        if (!elem.valid()) return nb::none();
        return value::value_to_python(elem.data(), elem.schema());
    }

    [[nodiscard]] engine_time_t window_timestamp(size_t index) const {
        return _view.window_timestamp(index);
    }

    void window_clear(engine_time_t time) {
        _view.window_clear(time);
    }

    // === Ref operations ===
    [[nodiscard]] bool ref_is_empty() const { return _view.ref_is_empty(); }
    [[nodiscard]] bool ref_is_bound() const { return _view.ref_is_bound(); }
    [[nodiscard]] bool ref_is_valid() const { return _view.ref_is_valid(); }

    void ref_clear(engine_time_t time) {
        _view.ref_clear(time);
    }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _view.to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        return _view.to_debug_string(time);
    }

    // Access underlying view for internal use
    TSOutputView& underlying() { return _view; }
    const TSOutputView& underlying() const { return _view; }

private:
    TSOutputView _view;
};

/**
 * PyTSOutput - Python wrapper for TSOutput
 *
 * Exposes TSOutput to Python for testing without requiring a Node.
 * The owning_node can be nullptr for standalone testing.
 */
class PyTSOutput {
public:
    PyTSOutput() = default;

    /**
     * Construct from TimeSeriesTypeMeta.
     * Node is optional (nullptr for testing).
     */
    explicit PyTSOutput(const TimeSeriesTypeMeta* meta)
        : _output(std::make_unique<TSOutput>(meta, nullptr)) {}

    // === Validity and type queries ===
    [[nodiscard]] bool valid() const { return _output && _output->valid(); }

    [[nodiscard]] const TimeSeriesTypeMeta* meta() const {
        return _output ? _output->meta() : nullptr;
    }

    [[nodiscard]] const value::TypeMeta* value_schema() const {
        return _output ? _output->value_schema() : nullptr;
    }

    [[nodiscard]] value::TypeKind kind() const {
        return _output ? _output->kind() : value::TypeKind::Scalar;
    }

    [[nodiscard]] TimeSeriesKind ts_kind() const {
        return _output ? _output->ts_kind() : TimeSeriesKind::TS;
    }

    [[nodiscard]] std::string type_name() const {
        if (auto* m = meta()) {
            return m->type_name_str();
        }
        return "unknown";
    }

    // === View creation ===
    [[nodiscard]] PyTSOutputView view() {
        if (!valid()) {
            throw std::runtime_error("Cannot create view from invalid TSOutput");
        }
        return PyTSOutputView(_output->view());
    }

    // === Modification tracking ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _output && _output->modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _output ? _output->last_modified_time() : engine_time_t{};
    }

    [[nodiscard]] bool has_value() const {
        return _output && _output->has_value();
    }

    void mark_invalid() {
        if (_output) _output->mark_invalid();
    }

    // === Direct value access (convenience) ===
    [[nodiscard]] nb::object py_value() const {
        if (!valid()) return nb::none();
        auto val = _output->value();
        return value::value_to_python(val.data(), val.schema());
    }

    void set_value(nb::object py_obj, engine_time_t time) {
        if (!valid()) {
            throw std::runtime_error("Cannot set value on invalid TSOutput");
        }
        if (py_obj.is_none()) {
            return;
        }
        // Use the view to set the value
        auto v = _output->view();
        auto value_view = v.value_view().value_view();
        value::value_from_python(value_view.data(), py_obj, value_view.schema());
        v.mark_modified(time);
    }

    // === Observer/subscription support ===
    [[nodiscard]] bool has_observers() const {
        return _output && _output->has_observers();
    }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _output ? _output->to_string() : "TSOutput(invalid)";
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        return _output ? _output->to_debug_string(time) : "TSOutput(invalid)";
    }

    // Access underlying for advanced use
    TSOutput* underlying() { return _output.get(); }
    const TSOutput* underlying() const { return _output.get(); }

private:
    std::unique_ptr<TSOutput> _output;
};

// Registration function
void register_py_ts_output_with_nanobind(nb::module_& m);

} // namespace hgraph::ts

#endif // HGRAPH_PY_TS_OUTPUT_H
