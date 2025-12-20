//
// Created by Claude on 20/12/2025.
//
// Python wrapper for the hgraph TimeSeriesValue class.
// Exposes TimeSeriesValue as HgTimeSeriesValue to Python for testing.
//

#ifndef HGRAPH_VALUE_PY_TIME_SERIES_VALUE_H
#define HGRAPH_VALUE_PY_TIME_SERIES_VALUE_H

#include <nanobind/nanobind.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <unordered_map>
#include <utility>

namespace nb = nanobind;

namespace hgraph::value {

    // Forward declarations
    class PyHgTimeSeriesValue;
    class PyHgTimeSeriesValueView;

    /**
     * CallableNotifiable - Internal wrapper for Python callables as Notifiable
     *
     * This is an implementation detail - not exposed to Python.
     * Users interact with subscribe/unsubscribe using Python callables directly.
     */
    class CallableNotifiable : public hgraph::Notifiable {
    public:
        explicit CallableNotifiable(nb::callable callback)
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
     * SubscriptionManager - Manages lifetime of CallableNotifiable wrappers
     *
     * Shared between PyHgTimeSeriesValue and its views to ensure proper cleanup.
     */
    class SubscriptionManager {
    public:
        SubscriptionManager() = default;

        // Non-copyable but movable
        SubscriptionManager(const SubscriptionManager&) = delete;
        SubscriptionManager& operator=(const SubscriptionManager&) = delete;
        SubscriptionManager(SubscriptionManager&&) = default;
        SubscriptionManager& operator=(SubscriptionManager&&) = default;

        // Subscribe a callback to an observer, returns the wrapper
        CallableNotifiable* subscribe(ObserverStorage* observer, nb::callable callback) {
            if (!observer || !callback) return nullptr;

            auto callback_id = reinterpret_cast<uintptr_t>(callback.ptr());
            auto observer_id = reinterpret_cast<uintptr_t>(observer);
            SubscriptionKey key{callback_id, observer_id};

            if (_subscriptions.find(key) != _subscriptions.end()) {
                throw std::runtime_error("Callback is already subscribed at this level");
            }

            auto wrapper = std::make_unique<CallableNotifiable>(callback);
            auto* wrapper_ptr = wrapper.get();
            observer->subscribe(wrapper_ptr);
            _subscriptions[key] = std::move(wrapper);
            return wrapper_ptr;
        }

        // Unsubscribe a callback from an observer
        void unsubscribe(ObserverStorage* observer, nb::callable callback) {
            if (!observer || !callback) return;

            auto callback_id = reinterpret_cast<uintptr_t>(callback.ptr());
            auto observer_id = reinterpret_cast<uintptr_t>(observer);
            SubscriptionKey key{callback_id, observer_id};

            auto it = _subscriptions.find(key);
            if (it == _subscriptions.end()) return;

            observer->unsubscribe(it->second.get());
            _subscriptions.erase(it);
        }

        [[nodiscard]] bool empty() const { return _subscriptions.empty(); }
        [[nodiscard]] size_t size() const { return _subscriptions.size(); }

    private:
        // Use a pair-based key to avoid XOR collisions
        using SubscriptionKey = std::pair<uintptr_t, uintptr_t>;

        struct PairHash {
            size_t operator()(const SubscriptionKey& key) const {
                // Combine hashes using a technique that avoids simple XOR collisions
                size_t h1 = std::hash<uintptr_t>{}(key.first);
                size_t h2 = std::hash<uintptr_t>{}(key.second);
                return h1 ^ (h2 * 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
            }
        };

        std::unordered_map<SubscriptionKey, std::unique_ptr<CallableNotifiable>, PairHash> _subscriptions;
    };

    /**
     * PyHgTimeSeriesValueView - Fluent view for navigating and subscribing to time-series values
     *
     * Provides a fluent API for hierarchical navigation and subscription:
     *   ts_value.view().field(0).subscribe(callback)
     *   ts_value.view().field("name").set_value("Alice", time=T100)
     *   ts_value.view().key("a").subscribe(callback)
     *
     * Views maintain a reference to the owning PyHgTimeSeriesValue for subscription management.
     */
    class PyHgTimeSeriesValueView {
    public:
        PyHgTimeSeriesValueView() = default;

        // Construct with view, observer, and subscription manager
        PyHgTimeSeriesValueView(TimeSeriesValueView view, ObserverStorage* observer,
                                 std::shared_ptr<SubscriptionManager> sub_mgr)
            : _view(view), _observer(observer), _sub_mgr(std::move(sub_mgr)) {}

        // =========================================================================
        // Basic properties
        // =========================================================================

        [[nodiscard]] bool valid() const { return _view.valid(); }
        [[nodiscard]] const TypeMeta* schema() const { return _view.schema(); }
        [[nodiscard]] TypeKind kind() const { return _view.kind(); }

        [[nodiscard]] std::string type_name() const {
            if (!valid()) return "<invalid>";
            return _view.schema()->type_name_str();
        }

        // =========================================================================
        // Modification tracking
        // =========================================================================

        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _view.modified_at(time);
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _view.last_modified_time();
        }

        [[nodiscard]] bool has_value() const {
            return _view.has_value();
        }

        // =========================================================================
        // Value access
        // =========================================================================

        [[nodiscard]] nb::object py_value() const {
            if (!valid()) return nb::none();
            return value_to_python(_view.value_view().data(), _view.schema());
        }

        void set_value(nb::object py_obj, engine_time_t time) {
            if (!valid()) {
                throw std::runtime_error("Cannot set value on invalid view");
            }
            if (py_obj.is_none()) {
                return;
            }
            value_from_python(_view.value_view().data(), py_obj, _view.schema());
            _view.mark_modified(time);
        }

        // =========================================================================
        // Navigation - Bundle fields (fluent API)
        // =========================================================================

        [[nodiscard]] PyHgTimeSeriesValueView field(size_t index) {
            if (!valid() || kind() != TypeKind::Bundle) {
                throw std::runtime_error("field() requires a valid Bundle type");
            }
            if (index >= _view.field_count()) {
                throw std::runtime_error("Invalid field index");
            }

            // Use C++ API that ensures child observer exists
            auto field_view = _view.field_with_observer(index);
            return {field_view, field_view.observer(), _sub_mgr};
        }

        [[nodiscard]] PyHgTimeSeriesValueView field(const std::string& name) {
            if (!valid() || kind() != TypeKind::Bundle) {
                throw std::runtime_error("field() requires a valid Bundle type");
            }

            // Use C++ API that ensures child observer exists
            auto field_view = _view.field_with_observer(name);
            if (!field_view.valid()) {
                throw std::runtime_error("Invalid field name: " + name);
            }
            return {field_view, field_view.observer(), _sub_mgr};
        }

        // =========================================================================
        // Navigation - List elements (fluent API)
        // =========================================================================

        [[nodiscard]] PyHgTimeSeriesValueView element(size_t index) {
            if (!valid() || kind() != TypeKind::List) {
                throw std::runtime_error("element() requires a valid List type");
            }
            if (index >= _view.list_size()) {
                throw std::runtime_error("Invalid element index");
            }

            // Use C++ API that ensures child observer exists
            auto elem_view = _view.element_with_observer(index);
            return {elem_view, elem_view.observer(), _sub_mgr};
        }

        // =========================================================================
        // Navigation - Dict entries (fluent API with Python key)
        // =========================================================================

        [[nodiscard]] PyHgTimeSeriesValueView key(nb::object py_key) {
            if (!valid() || kind() != TypeKind::Dict) {
                throw std::runtime_error("key() requires a valid Dict type");
            }

            // Get key type from schema
            auto* dict_meta = static_cast<const DictTypeMeta*>(schema());
            const TypeMeta* key_meta = dict_meta->key_type();

            // Convert Python key to C++ (temporary storage)
            std::vector<uint8_t> key_buffer(key_meta->size, 0);
            void* key_ptr = key_buffer.data();
            key_meta->construct_at(key_ptr);
            value_from_python(key_ptr, py_key, key_meta);

            // Create ConstValueView for type-safe key passing
            ConstValueView key_view{key_ptr, key_meta};

            // Use C++ API that ensures child observer exists
            auto entry_view = _view.entry_with_observer(key_view);

            // Clean up temporary key (safe - entry_view holds the value, not the key)
            key_meta->destruct_at(key_ptr);

            if (!entry_view.valid()) {
                throw std::runtime_error("Key not found in dict");
            }

            return {entry_view, entry_view.observer(), _sub_mgr};
        }

        // =========================================================================
        // Subscription (fluent API)
        // =========================================================================

        PyHgTimeSeriesValueView& subscribe(nb::callable callback) {
            if (!valid()) {
                throw std::runtime_error("Cannot subscribe on invalid view");
            }
            if (!callback) {
                throw std::runtime_error("Cannot subscribe with null callback");
            }
            if (!_observer) {
                throw std::runtime_error("View has no observer for subscription");
            }

            _sub_mgr->subscribe(_observer, callback);
            return *this;
        }

        PyHgTimeSeriesValueView& unsubscribe(nb::callable callback) {
            if (!valid() || !_observer || !callback) {
                return *this;
            }

            _sub_mgr->unsubscribe(_observer, callback);
            return *this;
        }

        // =========================================================================
        // Size queries
        // =========================================================================

        [[nodiscard]] size_t field_count() const {
            if (!valid() || kind() != TypeKind::Bundle) return 0;
            return _view.field_count();
        }

        [[nodiscard]] size_t list_size() const {
            if (!valid() || kind() != TypeKind::List) return 0;
            return _view.list_size();
        }

        [[nodiscard]] size_t dict_size() const {
            if (!valid() || kind() != TypeKind::Dict) return 0;
            return _view.dict_size();
        }

        [[nodiscard]] size_t set_size() const {
            if (!valid() || kind() != TypeKind::Set) return 0;
            return _view.set_size();
        }

        // =========================================================================
        // String representation
        // =========================================================================

        [[nodiscard]] std::string to_string() const {
            return _view.to_string();
        }

        [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
            return _view.to_debug_string(time);
        }

    private:
        TimeSeriesValueView _view;
        ObserverStorage* _observer{nullptr};
        std::shared_ptr<SubscriptionManager> _sub_mgr;
    };

    /**
     * PyHgTimeSeriesValue - Python wrapper for the TimeSeriesValue class
     *
     * Provides a Python-accessible wrapper around the TimeSeriesValue class,
     * which combines Value storage with modification tracking.
     *
     * Key features:
     * - Modification tracking (modified_at, last_modified_time, has_value)
     * - Time is passed as a parameter to mutating operations
     * - Fluent view API for hierarchical navigation and subscription
     *
     * Example Python usage:
     *   schema = _hgraph.get_scalar_type_meta(int)
     *   ts_value = _hgraph.HgTimeSeriesValue(schema)
     *
     *   # Set value with time
     *   ts_value.set_value(42, time=T100)
     *
     *   # Fluent navigation and subscription
     *   ts_value.view().subscribe(callback)  # Subscribe at root
     *   ts_value.view().field(0).subscribe(callback)  # Subscribe at field level
     *   ts_value.view().field("name").set_value("Alice", time=T100)
     */
    class PyHgTimeSeriesValue {
    public:
        // Default constructor - invalid value
        PyHgTimeSeriesValue() : _sub_mgr(std::make_shared<SubscriptionManager>()) {}

        // Construct from schema - allocates and default-constructs value
        explicit PyHgTimeSeriesValue(const TypeMeta* schema)
            : _ts_value(schema), _sub_mgr(std::make_shared<SubscriptionManager>()) {}

        // Move constructor
        PyHgTimeSeriesValue(PyHgTimeSeriesValue&&) = default;
        PyHgTimeSeriesValue& operator=(PyHgTimeSeriesValue&&) = default;

        // No copy - TimeSeriesValue is move-only
        PyHgTimeSeriesValue(const PyHgTimeSeriesValue&) = delete;
        PyHgTimeSeriesValue& operator=(const PyHgTimeSeriesValue&) = delete;

        // =========================================================================
        // Basic properties
        // =========================================================================

        [[nodiscard]] bool valid() const { return _ts_value.valid(); }
        [[nodiscard]] const TypeMeta* schema() const { return _ts_value.schema(); }
        [[nodiscard]] TypeKind kind() const { return _ts_value.kind(); }

        [[nodiscard]] std::string type_name() const {
            if (!valid()) return "<invalid>";
            return _ts_value.schema()->type_name_str();
        }

        // =========================================================================
        // Modification tracking
        // =========================================================================

        [[nodiscard]] bool modified_at(engine_time_t time) const {
            return _ts_value.modified_at(time);
        }

        [[nodiscard]] engine_time_t last_modified_time() const {
            return _ts_value.last_modified_time();
        }

        [[nodiscard]] bool has_value() const {
            return _ts_value.has_value();
        }

        void mark_invalid() {
            _ts_value.mark_invalid();
        }

        // =========================================================================
        // Value access (read-only py_value property)
        // =========================================================================

        [[nodiscard]] nb::object py_value() const {
            if (!valid()) return nb::none();
            return value_to_python(_ts_value.value().data(), _ts_value.schema());
        }

        // =========================================================================
        // Value mutation (with time parameter)
        // =========================================================================

        void set_value(nb::object py_obj, engine_time_t time) {
            if (!valid()) {
                throw std::runtime_error("Cannot set value on invalid HgTimeSeriesValue");
            }
            if (py_obj.is_none()) {
                return;
            }
            auto v = _ts_value.view();
            value_from_python(v.value_view().data(), py_obj, _ts_value.schema());
            v.mark_modified(time);
        }

        // =========================================================================
        // Fluent View API
        // =========================================================================

        /**
         * Get a view of this time-series value for fluent navigation and subscription.
         *
         * Example:
         *   ts_value.view().subscribe(callback)  # Root subscription
         *   ts_value.view().field(0).subscribe(callback)  # Field subscription
         *   ts_value.view().field("x").set_value(42, time=T100)
         */
        [[nodiscard]] PyHgTimeSeriesValueView view() {
            if (!valid()) {
                throw std::runtime_error("Cannot get view of invalid HgTimeSeriesValue");
            }

            // Ensure observer storage exists
            ensure_observers();

            return {_ts_value.view(), _ts_value.underlying_observers(), _sub_mgr};
        }

        // =========================================================================
        // Legacy direct access (for backwards compatibility)
        // =========================================================================

        [[nodiscard]] size_t field_count() const {
            if (!valid() || kind() != TypeKind::Bundle) return 0;
            return _ts_value.value().field_count();
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || kind() != TypeKind::Bundle) return false;
            return const_cast<TimeSeriesValue&>(_ts_value).view().field_modified_at(index, time);
        }

        [[nodiscard]] nb::object get_field(size_t index) const {
            if (!valid() || kind() != TypeKind::Bundle) return nb::none();
            auto field_view = _ts_value.value().field(index);
            if (!field_view.valid()) return nb::none();
            return value_to_python(field_view.data(), field_view.schema());
        }

        [[nodiscard]] nb::object get_field_by_name(const std::string& name) const {
            if (!valid() || kind() != TypeKind::Bundle) return nb::none();
            auto field_view = _ts_value.value().field(name);
            if (!field_view.valid()) return nb::none();
            return value_to_python(field_view.data(), field_view.schema());
        }

        void set_field(size_t index, nb::object py_obj, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Bundle) {
                throw std::runtime_error("set_field requires a Bundle type");
            }
            auto v = _ts_value.view();
            auto field_view = v.field(index);
            if (!field_view.valid()) {
                throw std::runtime_error("Invalid field index");
            }
            value_from_python(field_view.value_view().data(), py_obj, field_view.schema());
            field_view.mark_modified(time);
        }

        void set_field_by_name(const std::string& name, nb::object py_obj, engine_time_t time) {
            if (!valid() || kind() != TypeKind::Bundle) {
                throw std::runtime_error("set_field requires a Bundle type");
            }
            auto v = _ts_value.view();
            auto field_view = v.field(name);
            if (!field_view.valid()) {
                throw std::runtime_error("Invalid field name: " + name);
            }
            value_from_python(field_view.value_view().data(), py_obj, field_view.schema());
            field_view.mark_modified(time);
        }

        [[nodiscard]] size_t list_size() const {
            if (!valid() || kind() != TypeKind::List) return 0;
            return _ts_value.value().list_size();
        }

        [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || kind() != TypeKind::List) return false;
            return const_cast<TimeSeriesValue&>(_ts_value).view().element_modified_at(index, time);
        }

        [[nodiscard]] nb::object get_element(size_t index) const {
            if (!valid() || kind() != TypeKind::List) return nb::none();
            auto elem_view = _ts_value.value().element(index);
            if (!elem_view.valid()) return nb::none();
            return value_to_python(elem_view.data(), elem_view.schema());
        }

        void set_element(size_t index, nb::object py_obj, engine_time_t time) {
            if (!valid() || kind() != TypeKind::List) {
                throw std::runtime_error("set_element requires a List type");
            }
            auto v = _ts_value.view();
            auto elem_view = v.element(index);
            if (!elem_view.valid()) {
                throw std::runtime_error("Invalid element index");
            }
            value_from_python(elem_view.value_view().data(), py_obj, elem_view.schema());
            elem_view.mark_modified(time);
        }

        [[nodiscard]] size_t set_size() const {
            if (!valid() || kind() != TypeKind::Set) return 0;
            return _ts_value.value().set_size();
        }

        [[nodiscard]] size_t dict_size() const {
            if (!valid() || kind() != TypeKind::Dict) return 0;
            return _ts_value.value().dict_size();
        }

        // =========================================================================
        // String representation
        // =========================================================================

        [[nodiscard]] std::string to_string() const {
            return _ts_value.to_string();
        }

        [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
            return _ts_value.to_debug_string(time);
        }

        // =========================================================================
        // Legacy root-level subscription (for backwards compatibility)
        // =========================================================================

        void subscribe(nb::callable callback) {
            view().subscribe(callback);
        }

        void unsubscribe(nb::callable callback) {
            if (!valid()) return;
            if (!_ts_value.underlying_observers()) return;
            _sub_mgr->unsubscribe(_ts_value.underlying_observers(), callback);
        }

        [[nodiscard]] bool has_subscribers() const {
            return !_sub_mgr->empty();
        }

        [[nodiscard]] size_t subscriber_count() const {
            return _sub_mgr->size();
        }

        // =========================================================================
        // Access to underlying TimeSeriesValue
        // =========================================================================

        [[nodiscard]] TimeSeriesValue& ts_value() { return _ts_value; }
        [[nodiscard]] const TimeSeriesValue& ts_value() const { return _ts_value; }

    private:
        void ensure_observers() {
            if (!_ts_value.underlying_observers()) {
                // Subscribe with a dummy to force allocation, then immediately unsubscribe
                struct DummyNotifiable : public hgraph::Notifiable {
                    void notify(engine_time_t) override {}
                };
                DummyNotifiable dummy;
                _ts_value.subscribe(&dummy);
                _ts_value.unsubscribe(&dummy);
            }
        }

        TimeSeriesValue _ts_value;
        std::shared_ptr<SubscriptionManager> _sub_mgr;
    };

    /**
     * Register PyHgTimeSeriesValue and PyHgTimeSeriesValueView with nanobind
     */
    void register_py_time_series_value_with_nanobind(nb::module_& m);

} // namespace hgraph::value

#endif // HGRAPH_VALUE_PY_TIME_SERIES_VALUE_H
