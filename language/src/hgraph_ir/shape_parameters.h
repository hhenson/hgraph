#ifndef HGL_HGRAPH_IR_SHAPE_PARAMETERS_H
#define HGL_HGRAPH_IR_SHAPE_PARAMETERS_H

#include "hgraph_ir/ir.h"

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace hgl::hgraph_ir
{
    // A delta activates the type occurrences below it. A named application's
    // shape formal activates its corresponding argument, which can in turn
    // activate another declaration formal. Solve this monotone dependency graph
    // once: recursive declarations and repeated children need no provisional
    // recursion result, and each node/edge is processed at most once.
    inline std::unordered_set<std::uint32_t> temporal_struct_parameters(const Module &module) {
        using Node = std::uint64_t;
        const auto type_node = [](TypeId id) { return static_cast<Node>(id.value) << 1U; };
        const auto formal_node = [](BindingId id) { return (static_cast<Node>(id.value) << 1U) | 1U; };
        std::unordered_map<std::string_view, const StructContract *> structures;
        std::unordered_set<std::uint32_t> formals;
        std::vector<TypeId> pending;
        for (const auto &structure : module.structures) {
            structures.emplace(structure.identity, &structure);
            for (const auto &generic : structure.generics) { if (!generic.is_const) { formals.insert(generic.binding.value); } }
            for (const auto &field : structure.fields) { pending.push_back(field.type); }
            pending.insert(pending.end(), structure.parents.begin(), structure.parents.end());
        }
        std::unordered_map<Node, std::vector<Node>> edges;
        std::unordered_set<std::uint32_t> visited;
        std::vector<Node> active;
        while (!pending.empty()) {
            const TypeId id = pending.back();
            pending.pop_back();
            if (!id.valid() || !visited.insert(id.value).second) { continue; }
            const auto &type = module.types.at(id.value);
            const Node node = type_node(id);
            if (type.kind == ir::hir::TypeKind::Delta) { active.push_back(node); }
            if (type.binding.valid() && formals.contains(type.binding.value)) {
                edges[node].push_back(formal_node(type.binding));
            }
            for (const auto child : type.children) {
                edges[node].push_back(type_node(child));
                pending.push_back(child);
            }
            const auto nested = structures.find(type.nominal_identity);
            for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                const auto &argument = type.arguments[index];
                if (!argument.type) { continue; }
                const Node argument_node = type_node(*argument.type);
                edges[node].push_back(argument_node);
                pending.push_back(*argument.type);
                if (nested != structures.end() && index < nested->second->generics.size()) {
                    edges[formal_node(nested->second->generics[index].binding)].push_back(argument_node);
                }
            }
        }
        std::unordered_set<Node> reached;
        std::unordered_set<std::uint32_t> result;
        while (!active.empty()) {
            const Node node = active.back();
            active.pop_back();
            if (!reached.insert(node).second) { continue; }
            if ((node & 1U) != 0U) { result.insert(static_cast<std::uint32_t>(node >> 1U)); }
            if (const auto found = edges.find(node); found != edges.end()) {
                active.insert(active.end(), found->second.begin(), found->second.end());
            }
        }
        return result;
    }
}

#endif
