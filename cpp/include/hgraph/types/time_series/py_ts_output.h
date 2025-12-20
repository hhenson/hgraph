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
#include <hgraph/types/value/observer_storage.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <unordered_map>

namespace nb = nanobind;

namespace hgraph::ts {

// Forward declaration
class PyTSOutput;
class PyTSOutputView;

/**
 * CallableNotifiableForOutput - Wrapper for Python callables as Notifiable
 * Similar to CallableNotifiable in py_time_series_value.h but for TSOutput testing.
 */
class CallableNotifiableForOutput : public hgraph::Notifiable {
public:
    explicit CallableNotifiableForOutput(nb::callable callback)
        : _callback(std::move(callback)) {}

    void notify(engine_time_t time) override {
        if (_callback) {
            nb::gil_scoped_acquire gil;
            _callback(time);
        }
    }

private:
    nb::callable _callback;
};

/**
 * SubscriptionManagerForOutput - Manages Python callable subscriptions
 *
 * Maps (observer, callback) pairs to CallableNotifiable instances.
 * Similar to SubscriptionManager in py_time_series_value.h but for TSOutput.
 */
class SubscriptionManagerForOutput {
public:
    SubscriptionManagerForOutput() = default;

    void subscribe(value::ObserverStorage* observer, nb::callable callback) {
        if (!observer || !callback) return;

        auto key = make_key(observer, callback);
        if (_subscriptions.find(key) != _subscriptions.end()) {
            return;  // Already subscribed
        }

        auto notifiable = std::make_unique<CallableNotifiableForOutput>(std::move(callback));
        observer->subscribe(notifiable.get());
        _subscriptions[key] = std::move(notifiable);
    }

    void unsubscribe(value::ObserverStorage* observer, nb::callable callback) {
        if (!observer || !callback) return;

        auto key = make_key(observer, callback);
        auto it = _subscriptions.find(key);
        if (it != _subscriptions.end()) {
            observer->unsubscribe(it->second.get());
            _subscriptions.erase(it);
        }
    }

    [[nodiscard]] bool empty() const { return _subscriptions.empty(); }
    [[nodiscard]] size_t size() const { return _subscriptions.size(); }

private:
    // Use a pair-based key to avoid XOR collisions
    using SubscriptionKey = std::pair<uintptr_t, uintptr_t>;

    struct PairHash {
        size_t operator()(const SubscriptionKey& key) const {
            size_t h1 = std::hash<uintptr_t>{}(key.first);
            size_t h2 = std::hash<uintptr_t>{}(key.second);
            return h1 ^ (h2 * 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    static SubscriptionKey make_key(value::ObserverStorage* observer, const nb::callable& callback) {
        return {reinterpret_cast<uintptr_t>(observer),
                reinterpret_cast<uintptr_t>(callback.ptr())};
    }

    std::unordered_map<SubscriptionKey, std::unique_ptr<CallableNotifiableForOutput>, PairHash> _subscriptions;
};

/**
 * PyTSOutputView - Python wrapper for TSOutputView
 *
 * Provides fluent navigation API and value access with explicit time parameters.
 * Supports hierarchical subscriptions at any navigation level.
 */
class PyTSOutputView {
public:
    PyTSOutputView() = default;

    // Full constructor with observer and subscription manager
    PyTSOutputView(TSOutputView view,
                   value::ObserverStorage* observer,
                   std::shared_ptr<SubscriptionManagerForOutput> sub_mgr)
        : _view(std::move(view)), _observer(observer), _sub_mgr(std::move(sub_mgr)) {}

    // Constructor without observer (for backward compatibility, subscriptions won't work)
    explicit PyTSOutputView(TSOutputView view)
        : _view(std::move(view)), _observer(nullptr), _sub_mgr(nullptr) {}

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
        // Use field_with_observer to ensure child observer exists for subscriptions
        auto field_view = _view.field_with_observer(index);
        auto* observer = field_view.observer();
        return PyTSOutputView(std::move(field_view), observer, _sub_mgr);
    }

    [[nodiscard]] PyTSOutputView field_by_name(const std::string& name) {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            throw std::runtime_error("field() requires a valid Bundle type");
        }
        auto field_view = _view.field_with_observer(name);
        if (!field_view.valid()) {
            throw std::runtime_error("Invalid field name: " + name);
        }
        auto* observer = field_view.observer();
        return PyTSOutputView(std::move(field_view), observer, _sub_mgr);
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
        // Use element_with_observer to ensure child observer exists for subscriptions
        auto elem_view = _view.element_with_observer(index);
        auto* observer = elem_view.observer();
        return PyTSOutputView(std::move(elem_view), observer, _sub_mgr);
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

    // === Subscription support ===
    PyTSOutputView& subscribe(nb::callable callback) {
        if (!valid()) {
            throw std::runtime_error("Cannot subscribe on invalid view");
        }
        if (!callback) {
            throw std::runtime_error("Cannot subscribe with null callback");
        }
        if (!_observer) {
            throw std::runtime_error("View has no observer for subscription");
        }
        if (!_sub_mgr) {
            throw std::runtime_error("View has no subscription manager");
        }
        _sub_mgr->subscribe(_observer, std::move(callback));
        return *this;
    }

    PyTSOutputView& unsubscribe(nb::callable callback) {
        if (!valid() || !_observer || !callback || !_sub_mgr) {
            return *this;
        }
        _sub_mgr->unsubscribe(_observer, std::move(callback));
        return *this;
    }

    [[nodiscard]] bool has_observer() const {
        return _observer != nullptr;
    }

    // === Notify observers (for testing) ===
    void notify(engine_time_t time) {
        if (_observer) {
            _observer->notify(time);
        }
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
    value::ObserverStorage* _observer{nullptr};
    std::shared_ptr<SubscriptionManagerForOutput> _sub_mgr;
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
        : _output(std::make_unique<TSOutput>(meta, nullptr))
        , _sub_mgr(std::make_shared<SubscriptionManagerForOutput>()) {}

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
        // Ensure root observer exists
        auto& underlying = _output->underlying();
        underlying.ensure_observers();
        auto ts_view = _output->view();
        auto* observer = ts_view.observer();
        return PyTSOutputView(std::move(ts_view), observer, _sub_mgr);
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

    void subscribe(nb::callable callback) {
        if (!valid()) {
            throw std::runtime_error("Cannot subscribe on invalid TSOutput");
        }
        if (!callback) {
            throw std::runtime_error("Cannot subscribe with null callback");
        }
        // Ensure observers exist and subscribe at root level
        auto& underlying = _output->underlying();
        underlying.ensure_observers();
        _sub_mgr->subscribe(underlying.observers(), std::move(callback));
    }

    void unsubscribe(nb::callable callback) {
        if (!valid() || !callback) return;
        auto& underlying = _output->underlying();
        if (underlying.observers()) {
            _sub_mgr->unsubscribe(underlying.observers(), std::move(callback));
        }
    }

    // === Notify observers (for testing) ===
    void notify(engine_time_t time) {
        if (valid()) {
            auto& underlying = _output->underlying();
            if (underlying.observers()) {
                underlying.observers()->notify(time);
            }
        }
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
    std::shared_ptr<SubscriptionManagerForOutput> _sub_mgr;
};

// Registration function
void register_py_ts_output_with_nanobind(nb::module_& m);

} // namespace hgraph::ts

#endif // HGRAPH_PY_TS_OUTPUT_H
