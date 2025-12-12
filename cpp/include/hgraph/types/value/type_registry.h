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
    };

} // namespace hgraph::value

#endif // HGRAPH_VALUE_TYPE_REGISTRY_H
