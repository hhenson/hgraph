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
#include <hgraph/types/notifiable.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <unordered_map>

namespace nb = nanobind;

namespace hgraph::value {

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
     * PyHgTimeSeriesValue - Python wrapper for the TimeSeriesValue class
     *
     * Provides a Python-accessible wrapper around the TimeSeriesValue class,
     * which combines Value storage with modification tracking.
     *
     * Key differences from HgValue:
     * - Modification tracking (modified_at, last_modified_time, has_value)
     * - Time is passed as a parameter to mutating operations
     * - Navigation returns sub-views that propagate modifications
     *
     * Example Python usage:
     *   schema = _hgraph.get_scalar_type_meta(int)
     *   ts_value = _hgraph.HgTimeSeriesValue(schema)
     *
     *   # Set value with time
     *   ts_value.set_value(42, time=100)
     *   assert ts_value.py_value == 42
     *   assert ts_value.modified_at(100)
     *   assert not ts_value.modified_at(99)
     */
    class PyHgTimeSeriesValue {
    public:
        // Default constructor - invalid value
        PyHgTimeSeriesValue() = default;

        // Construct from schema - allocates and default-constructs value
        explicit PyHgTimeSeriesValue(const TypeMeta* schema)
            : _ts_value(schema) {}

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
            // Get mutable view and set value
            auto view = _ts_value.view();
            value_from_python(view.value_view().data(), py_obj, _ts_value.schema());
            view.mark_modified(time);
        }

        // =========================================================================
        // Bundle field access
        // =========================================================================

        [[nodiscard]] size_t field_count() const {
            if (!valid() || kind() != TypeKind::Bundle) return 0;
            return _ts_value.value().field_count();
        }

        [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
            if (!valid() || kind() != TypeKind::Bundle) return false;
            // Need to access tracker through view
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
            auto view = _ts_value.view();
            auto field_view = view.field(index);
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
            auto view = _ts_value.view();
            auto field_view = view.field(name);
            if (!field_view.valid()) {
                throw std::runtime_error("Invalid field name: " + name);
            }
            value_from_python(field_view.value_view().data(), py_obj, field_view.schema());
            field_view.mark_modified(time);
        }

        // =========================================================================
        // List element access
        // =========================================================================

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
            auto view = _ts_value.view();
            auto elem_view = view.element(index);
            if (!elem_view.valid()) {
                throw std::runtime_error("Invalid element index");
            }
            value_from_python(elem_view.value_view().data(), py_obj, elem_view.schema());
            elem_view.mark_modified(time);
        }

        // =========================================================================
        // Set operations
        // =========================================================================

        [[nodiscard]] size_t set_size() const {
            if (!valid() || kind() != TypeKind::Set) return 0;
            return _ts_value.value().set_size();
        }

        [[nodiscard]] nb::object set_py_value() const {
            // For sets, return the full set as a Python set
            return py_value();
        }

        // Note: For set add/remove, we need type-erased operations
        // The Python interface handles this through py_value get/set

        // =========================================================================
        // Dict operations
        // =========================================================================

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
        // Observer/Subscription API
        // =========================================================================

        /**
         * Subscribe a Python callable to receive notifications when this value is modified.
         *
         * The callable will be called with a single argument: the engine time (datetime)
         * at which the modification occurred.
         *
         * Example:
         *   notifications = []
         *   ts_value.subscribe(lambda t: notifications.append(t))
         *   ts_value.set_value(42, time=some_time)
         *   assert len(notifications) == 1
         */
        void subscribe(nb::callable callback) {
            if (!valid()) {
                throw std::runtime_error("Cannot subscribe to invalid HgTimeSeriesValue");
            }
            if (!callback) {
                throw std::runtime_error("Cannot subscribe with null callback");
            }

            // Use Python object id as key for tracking
            auto callback_id = reinterpret_cast<uintptr_t>(callback.ptr());

            // Check if already subscribed
            if (_subscribers.find(callback_id) != _subscribers.end()) {
                throw std::runtime_error("Callback is already subscribed");
            }

            // Create wrapper and store it
            auto wrapper = std::make_unique<CallableNotifiable>(callback);
            auto* wrapper_ptr = wrapper.get();
            _subscribers[callback_id] = std::move(wrapper);

            // Subscribe the wrapper
            _ts_value.subscribe(wrapper_ptr);
        }

        /**
         * Unsubscribe a Python callable from receiving notifications.
         *
         * The callable must have been previously subscribed via subscribe().
         */
        void unsubscribe(nb::callable callback) {
            if (!valid()) return;
            if (!callback) return;

            auto callback_id = reinterpret_cast<uintptr_t>(callback.ptr());

            auto it = _subscribers.find(callback_id);
            if (it == _subscribers.end()) {
                return;  // Not subscribed, silently ignore
            }

            // Unsubscribe and remove
            _ts_value.unsubscribe(it->second.get());
            _subscribers.erase(it);
        }

        [[nodiscard]] bool has_subscribers() const {
            return !_subscribers.empty();
        }

        [[nodiscard]] size_t subscriber_count() const {
            return _subscribers.size();
        }

        // =========================================================================
        // Access to underlying TimeSeriesValue
        // =========================================================================

        [[nodiscard]] TimeSeriesValue& ts_value() { return _ts_value; }
        [[nodiscard]] const TimeSeriesValue& ts_value() const { return _ts_value; }

    private:
        TimeSeriesValue _ts_value;
        // Map from Python callable id -> CallableNotifiable wrapper
        // We own the wrappers and manage their lifetime
        std::unordered_map<uintptr_t, std::unique_ptr<CallableNotifiable>> _subscribers;
    };

    /**
     * Register PyHgTimeSeriesValue with nanobind
     */
    void register_py_time_series_value_with_nanobind(nb::module_& m);

} // namespace hgraph::value

#endif // HGRAPH_VALUE_PY_TIME_SERIES_VALUE_H
