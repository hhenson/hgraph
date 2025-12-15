//
// Created by Howard Henson on 13/12/2025.
//

#ifndef HGRAPH_VALUE_WINDOW_TYPE_H
#define HGRAPH_VALUE_WINDOW_TYPE_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <cassert>
#include <vector>
#include <algorithm>

namespace hgraph::value {

    /**
     * WindowTypeMeta - Extended TypeMeta for window (time-series history) types
     *
     * Windows store a history of timestamped values. Two modes are supported:
     *
     * 1. Fixed-length (cyclic buffer):
     *    - max_count > 0, window_duration == 0
     *    - Stores up to max_count entries in a cyclic buffer
     *    - Oldest entry overwritten when full
     *
     * 2. Variable-length (time-based queue):
     *    - max_count == 0, window_duration > 0
     *    - Stores entries within a time window
     *    - Entries older than current_time - window_duration are expired
     */
    struct WindowTypeMeta : TypeMeta {
        const TypeMeta* element_type;
        size_t max_count;                      // For fixed-length (0 for variable)
        engine_time_delta_t window_duration;   // For variable-length (0 for fixed)

        [[nodiscard]] bool is_fixed_length() const {
            return max_count > 0;
        }

        [[nodiscard]] bool is_variable_length() const {
            return window_duration.count() > 0;
        }
    };

    // Forward declarations
    class CyclicWindowStorage;
    class QueueWindowStorage;

    /**
     * CyclicWindowStorage - Cyclic buffer implementation for fixed-length windows
     *
     * Stores up to max_count entries. When full, new entries overwrite the oldest.
     * Uses head/count tracking for O(1) push and access.
     */
    class CyclicWindowStorage {
    public:
        CyclicWindowStorage() = default;

        CyclicWindowStorage(const TypeMeta* elem_type, size_t max_count)
            : _element_type(elem_type)
            , _max_count(max_count) {
            if (_element_type && _max_count > 0) {
                _elements.resize(_max_count * _element_type->size);
                _timestamps.resize(_max_count);
            }
        }

        ~CyclicWindowStorage() {
            clear();
        }

        CyclicWindowStorage(CyclicWindowStorage&& other) noexcept
            : _element_type(other._element_type)
            , _elements(std::move(other._elements))
            , _timestamps(std::move(other._timestamps))
            , _max_count(other._max_count)
            , _head(other._head)
            , _count(other._count) {
            other._element_type = nullptr;
            other._head = 0;
            other._count = 0;
        }

        CyclicWindowStorage& operator=(CyclicWindowStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                _elements = std::move(other._elements);
                _timestamps = std::move(other._timestamps);
                _max_count = other._max_count;
                _head = other._head;
                _count = other._count;
                other._element_type = nullptr;
                other._head = 0;
                other._count = 0;
            }
            return *this;
        }

        CyclicWindowStorage(const CyclicWindowStorage&) = delete;
        CyclicWindowStorage& operator=(const CyclicWindowStorage&) = delete;

        [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }
        [[nodiscard]] size_t max_count() const { return _max_count; }
        [[nodiscard]] size_t size() const { return _count; }
        [[nodiscard]] bool empty() const { return _count == 0; }
        [[nodiscard]] bool full() const { return _count == _max_count; }

        /**
         * Push a new value with timestamp
         * If full, overwrites the oldest entry
         */
        void push(const void* value, engine_time_t timestamp) {
            if (!_element_type) return;

            if (_count < _max_count) {
                // Not full - add at next position
                size_t pos = (_head + _count) % _max_count;
                _element_type->copy_construct_at(element_ptr(pos), value);
                _timestamps[pos] = timestamp;
                ++_count;
            } else {
                // Full - overwrite oldest (at head)
                _element_type->destruct_at(element_ptr(_head));
                _element_type->copy_construct_at(element_ptr(_head), value);
                _timestamps[_head] = timestamp;
                _head = (_head + 1) % _max_count;
            }
        }

        /**
         * Get value at logical index (0 = oldest, size()-1 = newest)
         */
        [[nodiscard]] void* get(size_t index) {
            if (index >= _count) return nullptr;
            return element_ptr(physical_index(index));
        }

        [[nodiscard]] const void* get(size_t index) const {
            if (index >= _count) return nullptr;
            return element_ptr(physical_index(index));
        }

        /**
         * Get timestamp at logical index (0 = oldest)
         */
        [[nodiscard]] engine_time_t timestamp(size_t index) const {
            if (index >= _count) return MIN_DT;
            return _timestamps[physical_index(index)];
        }

        [[nodiscard]] engine_time_t oldest_timestamp() const {
            if (_count == 0) return MIN_DT;
            return timestamp(0);
        }

        [[nodiscard]] engine_time_t newest_timestamp() const {
            if (_count == 0) return MIN_DT;
            return timestamp(_count - 1);
        }

        /**
         * Compact the cyclic buffer so head is at index 0
         * Optimizes for sequential reading
         */
        void compact() {
            if (_head == 0 || _count == 0) return;

            size_t elem_size = _element_type->size;
            std::vector<char> temp_elements(_max_count * elem_size);
            std::vector<engine_time_t> temp_timestamps(_max_count);

            for (size_t i = 0; i < _count; ++i) {
                size_t src = physical_index(i);
                _element_type->move_construct_at(temp_elements.data() + i * elem_size, element_ptr(src));
                temp_timestamps[i] = _timestamps[src];
            }

            for (size_t i = 0; i < _count; ++i) {
                _element_type->move_construct_at(element_ptr(i), temp_elements.data() + i * elem_size);
                _timestamps[i] = temp_timestamps[i];
            }

            _head = 0;
        }

        void clear() {
            if (_element_type) {
                for (size_t i = 0; i < _count; ++i) {
                    _element_type->destruct_at(element_ptr(physical_index(i)));
                }
            }
            _head = 0;
            _count = 0;
        }

        /**
         * Iterator for traversing from oldest to newest
         */
        class Iterator {
        public:
            Iterator(const CyclicWindowStorage* storage, size_t logical_index)
                : _storage(storage), _logical_index(logical_index) {}

            ConstTypedPtr value() const {
                return {_storage->get(_logical_index), _storage->_element_type};
            }

            engine_time_t timestamp() const {
                return _storage->timestamp(_logical_index);
            }

            Iterator& operator++() {
                ++_logical_index;
                return *this;
            }

            bool operator!=(const Iterator& other) const {
                return _logical_index != other._logical_index;
            }

            bool operator==(const Iterator& other) const {
                return _logical_index == other._logical_index;
            }

        private:
            const CyclicWindowStorage* _storage;
            size_t _logical_index;
        };

        [[nodiscard]] Iterator begin() const { return Iterator(this, 0); }
        [[nodiscard]] Iterator end() const { return Iterator(this, _count); }

        // Buffer access for numpy compatibility
        // Note: Requires compact() to be called first if _head != 0
        [[nodiscard]] BufferInfo values_buffer_info() const {
            if (!_element_type || _count == 0) {
                return {};
            }
            // Only valid if compacted (head == 0)
            if (_head != 0) {
                return {};  // Must call compact() first
            }
            return BufferInfo{
                .ptr = const_cast<void*>(static_cast<const void*>(_elements.data())),
                .itemsize = _element_type->size,
                .count = _count,
                .readonly = true,
            };
        }

        [[nodiscard]] const engine_time_t* timestamps_data() const {
            // Only valid if compacted (head == 0)
            if (_head != 0) {
                return nullptr;  // Must call compact() first
            }
            return _timestamps.data();
        }

        [[nodiscard]] bool is_compacted() const {
            return _head == 0;
        }

    private:
        [[nodiscard]] size_t physical_index(size_t logical) const {
            return (_head + logical) % _max_count;
        }

        [[nodiscard]] void* element_ptr(size_t physical) {
            return _elements.data() + physical * _element_type->size;
        }

        [[nodiscard]] const void* element_ptr(size_t physical) const {
            return _elements.data() + physical * _element_type->size;
        }

        const TypeMeta* _element_type{nullptr};
        std::vector<char> _elements;
        std::vector<engine_time_t> _timestamps;
        size_t _max_count{0};
        size_t _head{0};
        size_t _count{0};
    };

    /**
     * QueueWindowStorage - Time-based queue implementation for variable-length windows
     *
     * Stores entries within a time duration. Entries older than
     * (newest_timestamp - window_duration) are automatically evicted on push.
     */
    class QueueWindowStorage {
    public:
        QueueWindowStorage() = default;

        QueueWindowStorage(const TypeMeta* elem_type, engine_time_delta_t duration)
            : _element_type(elem_type)
            , _window_duration(duration) {
        }

        ~QueueWindowStorage() {
            clear();
        }

        QueueWindowStorage(QueueWindowStorage&& other) noexcept
            : _element_type(other._element_type)
            , _elements(std::move(other._elements))
            , _timestamps(std::move(other._timestamps))
            , _window_duration(other._window_duration)
            , _count(other._count) {
            other._element_type = nullptr;
            other._count = 0;
        }

        QueueWindowStorage& operator=(QueueWindowStorage&& other) noexcept {
            if (this != &other) {
                clear();
                _element_type = other._element_type;
                _elements = std::move(other._elements);
                _timestamps = std::move(other._timestamps);
                _window_duration = other._window_duration;
                _count = other._count;
                other._element_type = nullptr;
                other._count = 0;
            }
            return *this;
        }

        QueueWindowStorage(const QueueWindowStorage&) = delete;
        QueueWindowStorage& operator=(const QueueWindowStorage&) = delete;

        [[nodiscard]] const TypeMeta* element_type() const { return _element_type; }
        [[nodiscard]] engine_time_delta_t window_duration() const { return _window_duration; }
        [[nodiscard]] size_t size() const { return _count; }
        [[nodiscard]] bool empty() const { return _count == 0; }

        /**
         * Push a new value with timestamp
         * Automatically evicts entries older than (timestamp - window_duration)
         */
        void push(const void* value, engine_time_t timestamp) {
            if (!_element_type) return;

            // Evict expired entries based on new timestamp
            evict_expired(timestamp);

            // Add new entry
            size_t elem_size = _element_type->size;
            _elements.resize(_elements.size() + elem_size);
            _element_type->copy_construct_at(_elements.data() + _count * elem_size, value);
            _timestamps.push_back(timestamp);
            ++_count;
        }

        [[nodiscard]] void* get(size_t index) {
            if (index >= _count) return nullptr;
            return element_ptr(index);
        }

        [[nodiscard]] const void* get(size_t index) const {
            if (index >= _count) return nullptr;
            return element_ptr(index);
        }

        [[nodiscard]] engine_time_t timestamp(size_t index) const {
            if (index >= _count) return MIN_DT;
            return _timestamps[index];
        }

        [[nodiscard]] engine_time_t oldest_timestamp() const {
            if (_count == 0) return MIN_DT;
            return _timestamps[0];
        }

        [[nodiscard]] engine_time_t newest_timestamp() const {
            if (_count == 0) return MIN_DT;
            return _timestamps[_count - 1];
        }

        /**
         * Remove entries older than (current_time - window_duration)
         */
        void evict_expired(engine_time_t current_time) {
            if (_count == 0) return;

            engine_time_t cutoff = current_time - _window_duration;

            size_t first_valid = 0;
            while (first_valid < _count && _timestamps[first_valid] < cutoff) {
                _element_type->destruct_at(element_ptr(first_valid));
                ++first_valid;
            }

            if (first_valid > 0) {
                size_t new_count = _count - first_valid;
                if (new_count > 0) {
                    size_t elem_size = _element_type->size;
                    std::memmove(_elements.data(), _elements.data() + first_valid * elem_size, new_count * elem_size);
                    std::memmove(_timestamps.data(), _timestamps.data() + first_valid, new_count * sizeof(engine_time_t));
                }
                _elements.resize(new_count * _element_type->size);
                _timestamps.resize(new_count);
                _count = new_count;
            }
        }

        void clear() {
            if (_element_type) {
                for (size_t i = 0; i < _count; ++i) {
                    _element_type->destruct_at(element_ptr(i));
                }
            }
            _elements.clear();
            _timestamps.clear();
            _count = 0;
        }

        /**
         * Iterator for traversing from oldest to newest
         */
        class Iterator {
        public:
            Iterator(const QueueWindowStorage* storage, size_t index)
                : _storage(storage), _index(index) {}

            ConstTypedPtr value() const {
                return {_storage->get(_index), _storage->_element_type};
            }

            engine_time_t timestamp() const {
                return _storage->timestamp(_index);
            }

            Iterator& operator++() {
                ++_index;
                return *this;
            }

            bool operator!=(const Iterator& other) const {
                return _index != other._index;
            }

            bool operator==(const Iterator& other) const {
                return _index == other._index;
            }

        private:
            const QueueWindowStorage* _storage;
            size_t _index;
        };

        [[nodiscard]] Iterator begin() const { return Iterator(this, 0); }
        [[nodiscard]] Iterator end() const { return Iterator(this, _count); }

        // Buffer access for numpy compatibility
        [[nodiscard]] BufferInfo values_buffer_info() const {
            if (!_element_type || _count == 0) {
                return {};
            }
            return BufferInfo{
                .ptr = const_cast<void*>(static_cast<const void*>(_elements.data())),
                .itemsize = _element_type->size,
                .count = _count,
                .readonly = true,
            };
        }

        [[nodiscard]] const engine_time_t* timestamps_data() const {
            return _timestamps.data();
        }

    private:
        [[nodiscard]] void* element_ptr(size_t index) {
            return _elements.data() + index * _element_type->size;
        }

        [[nodiscard]] const void* element_ptr(size_t index) const {
            return _elements.data() + index * _element_type->size;
        }

        const TypeMeta* _element_type{nullptr};
        std::vector<char> _elements;
        std::vector<engine_time_t> _timestamps;
        engine_time_delta_t _window_duration{0};
        size_t _count{0};
    };

    /**
     * WindowStorage - Union wrapper for fixed/variable window storage
     *
     * This allows Value to allocate a single type while the actual
     * implementation is selected based on WindowTypeMeta.
     */
    class WindowStorage {
    public:
        WindowStorage() : _is_fixed(true) {
            new (&_fixed) CyclicWindowStorage();
        }

        // Fixed-length constructor
        WindowStorage(const TypeMeta* elem_type, size_t max_count)
            : _is_fixed(true) {
            new (&_fixed) CyclicWindowStorage(elem_type, max_count);
        }

        // Variable-length constructor
        WindowStorage(const TypeMeta* elem_type, engine_time_delta_t duration)
            : _is_fixed(false) {
            new (&_variable) QueueWindowStorage(elem_type, duration);
        }

        ~WindowStorage() {
            if (_is_fixed) {
                _fixed.~CyclicWindowStorage();
            } else {
                _variable.~QueueWindowStorage();
            }
        }

        WindowStorage(WindowStorage&& other) noexcept
            : _is_fixed(other._is_fixed) {
            if (_is_fixed) {
                new (&_fixed) CyclicWindowStorage(std::move(other._fixed));
            } else {
                new (&_variable) QueueWindowStorage(std::move(other._variable));
            }
        }

        WindowStorage& operator=(WindowStorage&& other) noexcept {
            if (this != &other) {
                // Destruct current
                if (_is_fixed) {
                    _fixed.~CyclicWindowStorage();
                } else {
                    _variable.~QueueWindowStorage();
                }
                // Move construct new
                _is_fixed = other._is_fixed;
                if (_is_fixed) {
                    new (&_fixed) CyclicWindowStorage(std::move(other._fixed));
                } else {
                    new (&_variable) QueueWindowStorage(std::move(other._variable));
                }
            }
            return *this;
        }

        WindowStorage(const WindowStorage&) = delete;
        WindowStorage& operator=(const WindowStorage&) = delete;

        [[nodiscard]] bool is_fixed_length() const { return _is_fixed; }
        [[nodiscard]] bool is_variable_length() const { return !_is_fixed; }

        [[nodiscard]] CyclicWindowStorage& as_fixed() { return _fixed; }
        [[nodiscard]] const CyclicWindowStorage& as_fixed() const { return _fixed; }
        [[nodiscard]] QueueWindowStorage& as_variable() { return _variable; }
        [[nodiscard]] const QueueWindowStorage& as_variable() const { return _variable; }

        // Unified interface delegating to the appropriate implementation
        [[nodiscard]] const TypeMeta* element_type() const {
            return _is_fixed ? _fixed.element_type() : _variable.element_type();
        }

        [[nodiscard]] size_t size() const {
            return _is_fixed ? _fixed.size() : _variable.size();
        }

        [[nodiscard]] bool empty() const {
            return _is_fixed ? _fixed.empty() : _variable.empty();
        }

        [[nodiscard]] bool full() const {
            return _is_fixed && _fixed.full();
        }

        void push(const void* value, engine_time_t timestamp) {
            if (_is_fixed) {
                _fixed.push(value, timestamp);
            } else {
                _variable.push(value, timestamp);
            }
        }

        [[nodiscard]] void* get(size_t index) {
            return _is_fixed ? _fixed.get(index) : _variable.get(index);
        }

        [[nodiscard]] const void* get(size_t index) const {
            return _is_fixed ? _fixed.get(index) : _variable.get(index);
        }

        [[nodiscard]] engine_time_t timestamp(size_t index) const {
            return _is_fixed ? _fixed.timestamp(index) : _variable.timestamp(index);
        }

        [[nodiscard]] engine_time_t oldest_timestamp() const {
            return _is_fixed ? _fixed.oldest_timestamp() : _variable.oldest_timestamp();
        }

        [[nodiscard]] engine_time_t newest_timestamp() const {
            return _is_fixed ? _fixed.newest_timestamp() : _variable.newest_timestamp();
        }

        void compact(engine_time_t current_time) {
            if (_is_fixed) {
                _fixed.compact();
            } else {
                _variable.evict_expired(current_time);
            }
        }

        void evict_expired(engine_time_t current_time) {
            if (!_is_fixed) {
                _variable.evict_expired(current_time);
            }
        }

        void clear() {
            if (_is_fixed) {
                _fixed.clear();
            } else {
                _variable.clear();
            }
        }

        // Buffer access for numpy compatibility
        // For fixed windows, compact() must be called first to get valid buffer
        [[nodiscard]] BufferInfo values_buffer_info() const {
            return _is_fixed ? _fixed.values_buffer_info() : _variable.values_buffer_info();
        }

        [[nodiscard]] const engine_time_t* timestamps_data() const {
            return _is_fixed ? _fixed.timestamps_data() : _variable.timestamps_data();
        }

        [[nodiscard]] bool is_buffer_accessible() const {
            if (_is_fixed) {
                return _fixed.is_compacted();
            }
            return true;  // Variable windows are always contiguous
        }

    private:
        union {
            CyclicWindowStorage _fixed;
            QueueWindowStorage _variable;
        };
        bool _is_fixed;
    };

    /**
     * WindowTypeOps - Operations for window types
     */
    struct WindowTypeOps {
        static void construct(void* dest, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            if (window_meta->is_fixed_length()) {
                new (dest) WindowStorage(window_meta->element_type, window_meta->max_count);
            } else {
                new (dest) WindowStorage(window_meta->element_type, window_meta->window_duration);
            }
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<WindowStorage*>(dest)->~WindowStorage();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            auto* src_window = static_cast<const WindowStorage*>(src);

            if (window_meta->is_fixed_length()) {
                new (dest) WindowStorage(window_meta->element_type, window_meta->max_count);
                auto* dest_window = static_cast<WindowStorage*>(dest);
                const auto& src_fixed = src_window->as_fixed();
                for (auto it = src_fixed.begin(); it != src_fixed.end(); ++it) {
                    dest_window->push(it.value().ptr, it.timestamp());
                }
            } else {
                new (dest) WindowStorage(window_meta->element_type, window_meta->window_duration);
                auto* dest_window = static_cast<WindowStorage*>(dest);
                const auto& src_var = src_window->as_variable();
                for (auto it = src_var.begin(); it != src_var.end(); ++it) {
                    dest_window->push(it.value().ptr, it.timestamp());
                }
            }
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) WindowStorage(std::move(*static_cast<WindowStorage*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta* meta) {
            auto* dest_window = static_cast<WindowStorage*>(dest);
            auto* src_window = static_cast<const WindowStorage*>(src);
            dest_window->clear();

            if (src_window->is_fixed_length()) {
                const auto& src_fixed = src_window->as_fixed();
                for (auto it = src_fixed.begin(); it != src_fixed.end(); ++it) {
                    dest_window->push(it.value().ptr, it.timestamp());
                }
            } else {
                const auto& src_var = src_window->as_variable();
                for (auto it = src_var.begin(); it != src_var.end(); ++it) {
                    dest_window->push(it.value().ptr, it.timestamp());
                }
            }
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<WindowStorage*>(dest) = std::move(*static_cast<WindowStorage*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta* meta) {
            auto* window_a = static_cast<const WindowStorage*>(a);
            auto* window_b = static_cast<const WindowStorage*>(b);
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);

            if (window_a->size() != window_b->size()) return false;

            for (size_t i = 0; i < window_a->size(); ++i) {
                if (window_a->timestamp(i) != window_b->timestamp(i)) return false;
                if (!window_meta->element_type->equals_at(window_a->get(i), window_b->get(i))) return false;
            }
            return true;
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            auto* window_a = static_cast<const WindowStorage*>(a);
            auto* window_b = static_cast<const WindowStorage*>(b);
            return window_a->size() < window_b->size();
        }

        static size_t hash(const void* v, const TypeMeta* meta) {
            auto* window = static_cast<const WindowStorage*>(v);
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            size_t result = 0;

            for (size_t i = 0; i < window->size(); ++i) {
                result = result * 31 + std::hash<engine_time_t>{}(window->timestamp(i));
                result = result * 31 + window_meta->element_type->hash_at(window->get(i));
            }
            return result;
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* window = static_cast<const WindowStorage*>(v);
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            std::string result = "Window[size=";
            result += std::to_string(window->size());
            if (window->size() > 0) {
                result += ", newest=";
                result += window_meta->element_type->to_string_at(window->get(window->size() - 1));
            }
            result += "]";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            // Format: Window[element_type, Size[count]] or Window[element_type, timedelta]
            std::string result = "Window[" + window_meta->element_type->type_name_str();
            if (window_meta->is_fixed_length()) {
                result += ", Size[" + std::to_string(window_meta->max_count) + "]";
            } else {
                // Time-based window - show duration in appropriate units
                auto ns = window_meta->window_duration.count();
                if (ns >= 1000000000LL * 60 * 60) {
                    result += ", timedelta[hours=" + std::to_string(ns / (1000000000LL * 60 * 60)) + "]";
                } else if (ns >= 1000000000LL * 60) {
                    result += ", timedelta[minutes=" + std::to_string(ns / (1000000000LL * 60)) + "]";
                } else if (ns >= 1000000000LL) {
                    result += ", timedelta[seconds=" + std::to_string(ns / 1000000000LL) + "]";
                } else {
                    result += ", timedelta[ns=" + std::to_string(ns) + "]";
                }
            }
            result += "]";
            return result;
        }

        static const TypeOps ops;
    };

    inline const TypeOps WindowTypeOps::ops = {
        .construct = WindowTypeOps::construct,
        .destruct = WindowTypeOps::destruct,
        .copy_construct = WindowTypeOps::copy_construct,
        .move_construct = WindowTypeOps::move_construct,
        .copy_assign = WindowTypeOps::copy_assign,
        .move_assign = WindowTypeOps::move_assign,
        .equals = WindowTypeOps::equals,
        .less_than = WindowTypeOps::less_than,
        .hash = WindowTypeOps::hash,
        .to_string = WindowTypeOps::to_string,
        .type_name = WindowTypeOps::type_name,
        .to_python = nullptr,
        .from_python = nullptr,
    };

    /**
     * WindowTypeBuilder - Builds WindowTypeMeta
     *
     * Usage for fixed-length window:
     *   auto meta = WindowTypeBuilder()
     *       .element<int>()
     *       .fixed_count(100)
     *       .build("IntWindow100");
     *
     * Usage for variable-length window:
     *   auto meta = WindowTypeBuilder()
     *       .element<double>()
     *       .time_duration(std::chrono::minutes(5))
     *       .build("DoubleWindow5min");
     */
    class WindowTypeBuilder {
    public:
        WindowTypeBuilder& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        WindowTypeBuilder& element() {
            return element_type(scalar_type_meta<T>());
        }

        WindowTypeBuilder& fixed_count(size_t count) {
            _max_count = count;
            _window_duration = engine_time_delta_t{0};
            return *this;
        }

        WindowTypeBuilder& time_duration(engine_time_delta_t duration) {
            _window_duration = duration;
            _max_count = 0;
            return *this;
        }

        template<typename Duration>
        WindowTypeBuilder& time_duration(Duration duration) {
            _window_duration = std::chrono::duration_cast<engine_time_delta_t>(duration);
            _max_count = 0;
            return *this;
        }

        std::unique_ptr<WindowTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);
            assert(_max_count > 0 || _window_duration.count() > 0);
            assert(!(_max_count > 0 && _window_duration.count() > 0)); // Not both

            auto meta = std::make_unique<WindowTypeMeta>();

            meta->size = sizeof(WindowStorage);
            meta->alignment = alignof(WindowStorage);

            TypeFlags flags = TypeFlags::None;
            if (has_flag(_element_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }
            if (has_flag(_element_type->flags, TypeFlags::Equatable)) {
                flags = flags | TypeFlags::Equatable;
            }
            meta->flags = flags;

            meta->kind = TypeKind::Window;
            meta->ops = &WindowTypeOps::ops;
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Windows are not directly numpy-compatible
            meta->element_type = _element_type;
            meta->max_count = _max_count;
            meta->window_duration = _window_duration;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
        size_t _max_count{0};
        engine_time_delta_t _window_duration{0};
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_WINDOW_TYPE_H
