//
// Created by Howard Henson on 12/12/2025.
//

#ifndef HGRAPH_VALUE_TYPE_REGISTRY_H
#define HGRAPH_VALUE_TYPE_REGISTRY_H

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <string>
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <vector>
#include <mutex>
#include <functional>

namespace hgraph::value {

    /**
     * TypeRegistry - Central registry for type metadata
     *
     * Provides:
     * - Registration of types by name
     * - Retrieval of types by name
     * - Ownership of dynamically created type metadata
     * - Built-in scalar types automatically registered
     *
     * Usage:
     *   TypeRegistry registry;
     *   registry.register_type("Point", point_meta);
     *   const TypeMeta* type = registry.get("Point");
     */
    class TypeRegistry {
    public:
        TypeRegistry() {
            register_builtin_scalars();
        }

        // Register a type with a name (takes ownership)
        template<typename MetaT>
        const MetaT* register_type(const std::string& name, std::unique_ptr<MetaT> meta) {
            if (_types.contains(name)) {
                throw std::runtime_error("Type already registered: " + name);
            }
            auto* ptr = meta.get();
            _owned_types.push_back(std::move(meta));
            _types[name] = ptr;
            return ptr;
        }

        // Register a type with external ownership (e.g., static scalar types)
        void register_type(const std::string& name, const TypeMeta* meta) {
            if (_types.contains(name)) {
                throw std::runtime_error("Type already registered: " + name);
            }
            _types[name] = meta;
        }

        // Get a type by name (returns nullptr if not found)
        [[nodiscard]] const TypeMeta* get(const std::string& name) const {
            auto it = _types.find(name);
            return it != _types.end() ? it->second : nullptr;
        }

        // Get a type by name, throwing if not found
        [[nodiscard]] const TypeMeta* require(const std::string& name) const {
            auto* type = get(name);
            if (!type) {
                throw std::runtime_error("Type not found: " + name);
            }
            return type;
        }

        // Check if a type is registered
        [[nodiscard]] bool contains(const std::string& name) const {
            return _types.contains(name);
        }

        // Get all registered type names
        [[nodiscard]] std::vector<std::string> type_names() const {
            std::vector<std::string> names;
            names.reserve(_types.size());
            for (const auto& [name, _] : _types) {
                names.push_back(name);
            }
            return names;
        }

        // Get typed meta (with runtime check)
        template<typename MetaT>
        [[nodiscard]] const MetaT* get_as(const std::string& name) const {
            auto* meta = get(name);
            if (!meta) return nullptr;
            // Runtime kind check instead of dynamic_cast
            return static_cast<const MetaT*>(meta);
        }

        // Singleton access (optional - can also use local instances)
        static TypeRegistry& global() {
            static TypeRegistry instance;
            return instance;
        }

        // ============================================================
        // Hash-key based caching for composite types
        // ============================================================

        // Register a type by hash key (for composite type caching)
        // Thread-safe, returns existing if key already registered
        const TypeMeta* register_by_key(size_t key, std::unique_ptr<TypeMeta> meta) {
            std::lock_guard<std::mutex> lock(_key_mutex);
            auto it = _key_types.find(key);
            if (it != _key_types.end()) {
                return it->second.get();
            }
            auto* ptr = meta.get();
            _key_types[key] = std::move(meta);
            return ptr;
        }

        // Lookup by hash key (returns nullptr if not found)
        [[nodiscard]] const TypeMeta* lookup_by_key(size_t key) const {
            std::lock_guard<std::mutex> lock(_key_mutex);
            auto it = _key_types.find(key);
            return it != _key_types.end() ? it->second.get() : nullptr;
        }

        // Check if hash key exists
        [[nodiscard]] bool contains_key(size_t key) const {
            std::lock_guard<std::mutex> lock(_key_mutex);
            return _key_types.contains(key);
        }

        // Get number of key-cached types
        [[nodiscard]] size_t key_cache_size() const {
            std::lock_guard<std::mutex> lock(_key_mutex);
            return _key_types.size();
        }

    private:
        void register_builtin_scalars() {
            // Register common scalar types
            register_type("bool", scalar_type_meta<bool>());
            register_type("int8", scalar_type_meta<int8_t>());
            register_type("int16", scalar_type_meta<int16_t>());
            register_type("int32", scalar_type_meta<int32_t>());
            register_type("int64", scalar_type_meta<int64_t>());
            register_type("uint8", scalar_type_meta<uint8_t>());
            register_type("uint16", scalar_type_meta<uint16_t>());
            register_type("uint32", scalar_type_meta<uint32_t>());
            register_type("uint64", scalar_type_meta<uint64_t>());
            register_type("float32", scalar_type_meta<float>());
            register_type("float64", scalar_type_meta<double>());

            // Common aliases
            register_type("int", scalar_type_meta<int>());
            register_type("long", scalar_type_meta<long>());
            register_type("float", scalar_type_meta<float>());
            register_type("double", scalar_type_meta<double>());
            register_type("size_t", scalar_type_meta<size_t>());
        }

        std::unordered_map<std::string, const TypeMeta*> _types;
        std::vector<std::unique_ptr<TypeMeta>> _owned_types;

        // Hash-key based cache for composite types (thread-safe)
        mutable std::mutex _key_mutex;
        std::unordered_map<size_t, std::unique_ptr<TypeMeta>> _key_types;
    };

    /**
     * Hash combining utilities for building composite type keys
     */
    inline size_t hash_combine(size_t h1, size_t h2) {
        // Boost-style hash combine
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }

    inline size_t hash_string(const std::string& s) {
        return std::hash<std::string>{}(s);
    }

    inline size_t hash_cstr(const char* s) {
        if (!s) return 0;
        return std::hash<std::string>{}(s);
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TYPE_REGISTRY_H
