#ifndef HGRAPH_TYPE_REGISTRY_H
#define HGRAPH_TYPE_REGISTRY_H

#include <hgraph/hgraph_base.h>

namespace hgraph {
    /**
     * TypeRegistry - Registry for types in the type system
     *
     * This class manages the registration and retrieval of types
     * within the type system. It provides methods to register new
     * types and look them up by name.
     */
    class TypeRegistry {
    public:
        TypeRegistry() = default;

        Type* lookup(const std::string &name, const std::function<Type*>& type);
        Type* get_type_by_name(const std::string &name) const;

    private:
        std::unordered_map<std::string, Type*> _type_map;
    };
} // namespace hgraph

#endif // HGRAPH_TYPE_REGISTRY_H