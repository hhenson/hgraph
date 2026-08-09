#include <hgraph/runtime/context_node.h>

#include <hgraph/runtime/shared_output_node.h>

#include <stdexcept>

namespace hgraph
{
    std::string context_output_key(std::string_view scope, std::string_view path)
    {
        if (scope.empty()) { throw std::invalid_argument("context output scope must not be empty"); }
        if (path.empty()) { throw std::invalid_argument("context output path must not be empty"); }

        std::string key;
        key.reserve(std::string_view{"context-"}.size() + scope.size() + 1U + path.size());
        key.append("context-");
        key.append(scope);
        key.push_back('-');
        key.append(path);
        return key;
    }

    NodeBuilder make_context_source_node(std::string key, const TSValueTypeMetaData &target_schema)
    {
        if (key.empty()) { throw std::invalid_argument("context node key must not be empty"); }
        const std::string label_key = key;
        NodeBuilder       builder   = make_shared_output_source_node(std::move(key), target_schema);
        builder.label(std::string{"context_source:"} + label_key);
        return builder;
    }

    NodeBuilder make_context_capture_node(std::string key, const TSValueTypeMetaData &target_schema)
    {
        if (key.empty()) { throw std::invalid_argument("context node key must not be empty"); }
        const std::string label_key = key;
        NodeBuilder       builder   = make_shared_output_capture_node(std::move(key), target_schema);
        builder.label(std::string{"context_capture:"} + label_key);
        return builder;
    }
}  // namespace hgraph
