#include "semantics/resolve.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

// The first-pass resolver (developer guide, "Frontend components"). It
// collects the module's declarations, applies the `use` rules of the
// interim kernel table, then walks every function and test body with a
// scope stack that follows the lookup order of the syntax guide ("Scopes
// and name lookup"). Classification (syntax guide, "Function classification
// boundary") is a separate walk over the same body so the two rules cannot
// drift apart.
namespace hgl::semantics
{
    namespace
    {
        using syntax::Category;
        using syntax::SourceRange;

        /// Lets a `std::string`-keyed map be looked up by `std::string_view`
        /// without materialising a key.
        struct TransparentStringHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
                return std::hash<std::string_view>{}(value);
            }
        };

        constexpr std::string_view kernel_std       = "hgraph.std";
        constexpr std::string_view kernel_analytics = "hgraph.analytics";

        constexpr std::string_view intrinsics[] = {
            "const",  "valid",    "modified",   "all_valid", "last_modified", "delta",  "key_set", "keys",
            "values", "elements", "items",      "added",     "removed",       "insert", "update",  "upsert",
            "remove", "discard",  "invalidate", "clear",     "push",          "pop",    "schemas", "contains",
            "at",     "time_at",  "front",      "back",      "removed_value", "scheduled", "passivate", "activate",
        };

        /// Tarjan's strongly connected components over an adjacency list,
        /// without recursion; returns the component of every node.
        [[nodiscard]] std::vector<std::uint32_t>
        strongly_connected_components(const std::vector<std::vector<std::uint32_t>> &edges) {
            constexpr std::uint32_t                            unvisited = std::numeric_limits<std::uint32_t>::max();
            const std::size_t                                  count     = edges.size();
            std::vector<std::uint32_t>                         order(count, unvisited);
            std::vector<std::uint32_t>                         low(count, 0);
            std::vector<std::uint32_t>                         component(count, unvisited);
            std::vector<bool>                                  on_stack(count, false);
            std::vector<std::uint32_t>                         stack;
            std::vector<std::pair<std::uint32_t, std::size_t>> frames;  ///< node, next edge
            std::uint32_t                                      next_order     = 0;
            std::uint32_t                                      next_component = 0;
            const auto                                         visit          = [&](std::uint32_t node) {
                order[node] = low[node] = next_order++;
                stack.push_back(node);
                on_stack[node] = true;
                frames.emplace_back(node, 0U);
            };
            for (std::uint32_t root = 0; root < count; ++root) {
                if (order[root] != unvisited) { continue; }
                visit(root);
                while (!frames.empty()) {
                    const std::uint32_t node = frames.back().first;
                    if (const std::size_t next = frames.back().second++; next < edges[node].size()) {
                        const std::uint32_t target = edges[node][next];
                        if (order[target] == unvisited) {
                            visit(target);
                        } else if (on_stack[target]) {
                            low[node] = std::min(low[node], order[target]);
                        }
                        continue;
                    }
                    frames.pop_back();
                    if (!frames.empty()) { low[frames.back().first] = std::min(low[frames.back().first], low[node]); }
                    if (low[node] != order[node]) { continue; }
                    std::uint32_t member = unvisited;
                    while (member != node) {
                        member = stack.back();
                        stack.pop_back();
                        on_stack[member]  = false;
                        component[member] = next_component;
                    }
                    ++next_component;
                }
            }
            return component;
        }

        [[nodiscard]] std::string join_path(const std::vector<ast::Name> &path) {
            std::string result;
            for (const ast::Name &segment : path) {
                if (!result.empty()) { result += '.'; }
                result += segment.text;
            }
            return result;
        }

        class Resolver
        {
          public:
            Resolver(const syntax::SourceFile &file, const ast::Module &module, const ModuleCatalog &catalog,
                     const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics)
                : file_{file}, module_{module}, catalog_{catalog}, has_operator_{has_operator}, diagnostics_{diagnostics} {
                result_.bindings.resize(module.exprs.size());
                result_.type_bindings.resize(module.types.size());
                argument_bindings_.resize(module.types.size());
                result_.constraint_bindings.resize(module.constraints.size());
                result_.implementation_bindings.resize(module.decls.size());
                result_.instantiation_bindings.resize(module.decls.size());
                result_.kinds.resize(module.decls.size(), FunctionKind::Composition);
                result_.struct_info.resize(module.decls.size());
            }

            ResolvedModule run() {
                collect_declarations();
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl       = module_.decl(id);
                    const bool       test_scope = decl.test_only || std::holds_alternative<ast::TestDecl>(decl.node);
                    test_scope_active_          = test_scope;
                    if (const auto *structure = std::get_if<ast::StructDecl>(&decl.node)) {
                        resolve_struct(id, *structure);
                    } else if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node)) {
                        resolve_function(id, *fn);
                    } else if (const auto *native = std::get_if<ast::NativeFunctionDecl>(&decl.node)) {
                        resolve_native_function(id, *native);
                    } else if (const auto *op = std::get_if<ast::OperatorDecl>(&decl.node)) {
                        resolve_operator(id, *op);
                    } else if (const auto *instantiate = std::get_if<ast::InstantiateDecl>(&decl.node)) {
                        resolve_instantiation(id, *instantiate);
                    } else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node)) {
                        resolve_test(id, *test);
                    }
                    test_scope_active_ = false;
                }
                validate_structs();
                validate_constructors();
                return std::move(result_);
            }

          private:
            /// One lexical scope. A name is declared at most once per scope, so a
            /// hash lookup finds the only candidate without scanning.
            struct Scope
            { std::unordered_map<std::string_view, Binding> names; };

            struct Context
            {
                ast::DeclId fn{ast::no_node};
                bool        in_test{false};
                bool        in_sequence{false};  ///< inside a harness sequence literal
            };

            // ------------------------------------------------------ module level

            void collect_declarations() {
                scopes_.push_back(Scope{});
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *mod = std::get_if<ast::ModuleDecl>(&decl.node)) {
                        if (!result_.module_path.empty()) {
                            report(Category::Module, decl.range, "a compilation unit has one module declaration");
                        }
                        result_.module_path = join_path(mod->path);
                    }
                }
                if (result_.module_path.empty()) {
                    const SourceRange at =
                        module_.declarations.empty() ? SourceRange{} : module_.decl(module_.declarations.front()).range;
                    report(Category::Module, SourceRange{at.begin, at.begin},
                           "a compilation unit begins with a module declaration");
                }
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *use = std::get_if<ast::UseDecl>(&decl.node)) { resolve_use(*use, decl.range); }
                }
                // Operators first so a plain `fn` of an operator's name is a conflict.
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *structure = std::get_if<ast::StructDecl>(&decl.node)) {
                        result_.structs.push_back(id);
                        Binding binding;
                        binding.kind = BindingKind::Struct;
                        binding.decl = id;
                        declare(structure->name, binding, "in the module");
                    }
                }
                // Operators share the value namespace with exact functions
                // and structs, and are collected before functions so a plain
                // `fn` of an operator's name is diagnosed as a conflict.
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *op = std::get_if<ast::OperatorDecl>(&decl.node)) {
                        result_.operators.push_back(id);
                        Binding binding;
                        binding.kind              = BindingKind::LocalOperator;
                        binding.decl              = id;
                        binding.operator_identity = result_.module_path + "." + std::string{op->name.text};
                        declare(op->name, binding, "in the module");
                    }
                }
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node)) {
                        if (decl.test_only) { continue; }
                        result_.functions.push_back(id);
                        declare_function(id, *fn);
                    } else if (const auto *native = std::get_if<ast::NativeFunctionDecl>(&decl.node)) {
                        result_.native_functions.push_back(id);
                        declare_native_function(id, *native);
                    } else if (std::holds_alternative<ast::TestDecl>(decl.node)) {
                        result_.tests.push_back(id);
                    }
                }
                // All source parts contribute to a single test overlay. Resolve
                // production declarations without it so test names cannot leak.
                push_scope();
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node); fn && decl.test_only) {
                        result_.functions.push_back(id);
                        declare_function(id, *fn);
                    } else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node)) {
                        Binding binding;
                        binding.kind = BindingKind::Test;
                        binding.decl = id;
                        declare(test->name, binding, "in the module");
                    }
                }
                test_scope_ = std::move(scopes_.back());
                pop_scope();
            }

            void declare_function(ast::DeclId id, const ast::FunctionDecl &fn) {
                const std::optional<Binding> existing = lookup(fn.name.text);
                if (existing && existing->kind == BindingKind::Function && fn.visibility != ast::FunctionVisibility::Impl) {
                    const auto &other = std::get<ast::FunctionDecl>(module_.decl(existing->decl).node);
                    if (module_.decl(existing->decl).test_only == module_.decl(id).test_only && other.is_const != fn.is_const) {
                        // Execution roles share a spelling, not a declaration identity.
                        // Typed call resolution chooses the role after argument checking.
                        const auto duplicate = std::ranges::count_if(result_.functions, [&](ast::DeclId candidate) {
                            const auto &value = std::get<ast::FunctionDecl>(module_.decl(candidate).node);
                            return module_.decl(candidate).test_only == module_.decl(id).test_only &&
                                   value.name.text == fn.name.text && value.is_const == fn.is_const;
                        });
                        if (duplicate == 1) { return; }
                    }
                }
                const bool operator_in_scope =
                    existing && (existing->kind == BindingKind::Operator || existing->kind == BindingKind::LocalOperator);
                if (fn.visibility == ast::FunctionVisibility::Impl) {
                    if (!operator_in_scope) {
                        report(Category::Module, fn.name.range,
                               "'impl fn " + std::string{fn.name.text} + "' has no operator named '" + std::string{fn.name.text} +
                                   "' in scope");
                    } else {
                        result_.implementation_bindings[id] = *existing;
                    }
                    // An implementation is reached through its operator's identity,
                    // never as an unqualified value of its own.
                    return;
                }
                if (operator_in_scope) {
                    report(Category::Name, fn.name.range,
                           "'fn " + std::string{fn.name.text} + "' conflicts with operator " + operator_identity(*existing) +
                               "; declare 'impl fn " + std::string{fn.name.text} + "' or rename it");
                    return;
                }
                Binding binding;
                binding.kind = BindingKind::Function;
                binding.decl = id;
                declare(fn.name, binding, "in the module");
            }

            void declare_native_function(ast::DeclId id, const ast::NativeFunctionDecl &fn) {
                const auto family = native_family_indices_.find(std::string{fn.name.text});
                if (family != native_family_indices_.end()) {
                    result_.native_families[family->second].push_back(id);
                    return;
                }
                const std::uint32_t index = static_cast<std::uint32_t>(result_.native_families.size());
                result_.native_families.push_back({id});
                native_family_indices_.emplace(std::string{fn.name.text}, index);
                Binding binding;
                binding.kind  = BindingKind::NativeFunction;
                binding.index = index;
                declare(fn.name, binding, "in the module");
            }

            [[nodiscard]] std::string operator_identity(const Binding &binding) const {
                if (!binding.operator_identity.empty()) {
                    std::string identity = binding.operator_identity;
                    if (const std::size_t split = identity.rfind('.'); split != std::string::npos) {
                        identity.replace(split, 1, "::");
                    }
                    return identity;
                }
                return binding.registry_name;
            }

            void resolve_use(const ast::UseDecl &use, SourceRange range) {
                const std::string path   = join_path(use.path);
                const bool        kernel = path == kernel_std || path == kernel_analytics;
                if (!kernel && catalog_.find(path) == nullptr) {
                    report(Category::Module, range, "module '" + path + "' is not available in the supplied package target");
                    return;
                }
                if (!use.alias.empty()) {
                    for (const ModuleAlias &alias : result_.aliases) {
                        if (alias.alias == use.alias.text) {
                            report(Category::Name, use.alias.range,
                                   "module alias '" + std::string{use.alias.text} + "' is declared twice");
                            return;
                        }
                    }
                    result_.aliases.push_back(ModuleAlias{std::string{use.alias.text}, path});
                    return;
                }
                for (const ast::Name &name : use.names) {
                    if (const auto *contract = catalog_.find_operator(path, name.text)) {
                        const auto binding = imported_operator(*contract, name.range);
                        if (binding) {
                            if (const auto existing = lookup(name.text); existing && existing->kind == BindingKind::Operator) {
                                report(Category::Module, name.range,
                                       "operator '" + std::string{name.text} + "' is imported unqualified more than once");
                            } else {
                                declare(name, *binding, "in the module");
                            }
                        }
                        continue;
                    }
                    if (!kernel) {
                        // A struct imports exactly as a function does (ADR
                        // 0013): the name binds here, and its identity stays
                        // the exporting module's.
                        if (const ImportedStruct *structure = catalog_.find_struct(path, name.text)) {
                            if (const auto binding = imported_struct_binding(*structure, name.range)) {
                                declare(name, *binding, "in the module");
                            }
                            continue;
                        }
                        const std::span<const ImportedFunction> functions = catalog_.find_functions(path, name.text);
                        if (functions.empty()) {
                            report(Category::Module, name.range, path + " does not export '" + std::string{name.text} + "'");
                            continue;
                        }
                        const std::optional<Binding> binding = imported_function(functions, name.range);
                        if (binding) { declare(name, *binding, "in the module"); }
                        continue;
                    }
                    const std::optional<std::string> registry_name = kernel_registry_name(path, name.text);
                    if (!registry_name) {
                        report(Category::Module, name.range, path + " does not export '" + std::string{name.text} + "'");
                        continue;
                    }
                    if (const std::optional<Binding> existing = lookup(name.text);
                        existing && existing->kind == BindingKind::Operator) {
                        std::string other = operator_identity(*existing);
                        report(Category::Module, name.range,
                               "operator '" + std::string{name.text} + "' is imported unqualified from both " +
                                   other.substr(0, other.find("::")) + " and " + path);
                        continue;
                    }
                    result_.imports.push_back(ImportedOperator{std::string{name.text}, path, *registry_name, name.range});
                    Binding binding;
                    binding.kind              = BindingKind::Operator;
                    binding.registry_name     = *registry_name;
                    binding.operator_identity = path + "." + std::string{name.text};
                    declare(name, binding, "in the module");
                }
            }

            [[nodiscard]] std::optional<Binding> imported_operator(const ImportedOperatorContract &contract, SourceRange range) {
                if (!contract.support_error.empty()) {
                    report(Category::Module, range,
                           "operator '" + contract.identity + "' is unavailable: " + contract.support_error);
                    return std::nullopt;
                }
                if (std::ranges::none_of(result_.imported_contracts,
                                         [&](const auto &entry) { return entry.identity == contract.identity; })) {
                    result_.imported_contracts.push_back(contract);
                }
                Binding binding;
                binding.kind              = BindingKind::Operator;
                binding.registry_name     = contract.registry_name;
                binding.operator_identity = contract.identity;
                return binding;
            }

            /// Binds a struct another module exports (ADR 0013). The identity
            /// stays the owner's, so repeated mentions share one binding and
            /// nothing is copied into this module's namespace.
            [[nodiscard]] std::optional<Binding> imported_struct_binding(const ImportedStruct &structure, SourceRange range) {
                if (!structure.support_error.empty()) {
                    report(Category::Module, range,
                           "struct '" + structure.identity + "' is unavailable: " + structure.support_error);
                    return std::nullopt;
                }
                if (const auto found = imported_struct_bindings_.find(structure.identity);
                    found != imported_struct_bindings_.end()) {
                    return found->second;
                }
                Binding binding;
                binding.kind  = BindingKind::ImportedStruct;
                binding.index = static_cast<std::uint32_t>(result_.imported_structs.size());
                // By value: binding the closure below appends to this very
                // vector, and `structure` may be a reference into it.
                const ImportedStruct record = structure;
                result_.imported_structs.push_back(record);
                // Memoized BEFORE the closure, so a struct that reaches itself
                // terminates here rather than recurring.
                imported_struct_bindings_.emplace(record.identity, binding);
                // **The whole closure travels with the struct.** A parent is
                // never spelled in this module, and neither is a struct only a
                // FIELD reaches, so binding just the named struct leaves a
                // backend with no layout for part of the shape it has to
                // register -- it reports an unknown nominal type, at a name the
                // source never mentions. Reachability is over parents and
                // field types alike, which is the same closure the exporting
                // module's export check walks.
                //
                // Driven by a worklist rather than by recursing through this
                // function: a descriptor is an input, and a valid acyclic
                // chain `A0` holding `A1` holding `A2` ... is as deep as the
                // supplying module chose. Each hop is shallow, so the
                // per-type depth budget never fires; only the number of hops
                // grows, and that would be the compiler's stack.
                std::vector<ImportedStruct> work{record};
                std::vector<ImportedStruct> closure{record};
                while (!work.empty()) {
                    const ImportedStruct current = std::move(work.back());
                    work.pop_back();
                    std::vector<ImportedStruct> reached;
                    for (const ImportedType &parent : current.parents) {
                        reached_structs(parent, current.identity, range, reached);
                    }
                    for (const ImportedStructField &field : current.fields) {
                        reached_structs(field.type, current.identity, range, reached);
                    }
                    for (const ImportedStruct &next : reached) {
                        if (!next.support_error.empty()) {
                            report(Category::Module, range,
                                   "struct '" + next.identity + "' is unavailable: " + next.support_error);
                            continue;
                        }
                        if (imported_struct_bindings_.contains(next.identity)) { continue; }
                        Binding reached_binding;
                        reached_binding.kind  = BindingKind::ImportedStruct;
                        reached_binding.index = static_cast<std::uint32_t>(result_.imported_structs.size());
                        result_.imported_structs.push_back(next);
                        imported_struct_bindings_.emplace(next.identity, reached_binding);
                        work.push_back(next);
                        closure.push_back(next);
                    }
                }
                // A cycle through ORDINARY fields or parents is not a layout,
                // it is an infinite value. The local rule rejects one
                // (`check_recursive_fields`, ADR 0012 rule 2: an edge must be
                // an optional `atomic`), and an imported layout is not exempt
                // just because another module wrote it -- a backend realizing
                // it recurses `register_value(A) -> value(B) -> register_value(A)`
                // and takes the process with it. Two records are enough.
                if (const std::optional<std::string> cycle = imported_layout_cycle(closure)) {
                    report(Category::Type, range,
                           "imported struct '" + *cycle +
                               "' is part of a layout cycle through fields that are not recursive edges, so it "
                               "describes a value of unbounded size");
                }
                return binding;
            }

            /// The nominal identities `type` names directly, not through a
            /// recursive edge -- an edge is an owner, so it bounds the value.
            static void layout_references(const ImportedType &type, std::vector<std::string> &out) {
                if (!type.nominal_identity.empty()) { out.push_back(type.nominal_identity); }
                for (const ImportedType &child : type.children) { layout_references(child, out); }
            }

            /// One reference from a struct's layout to another struct, and
            /// whether it is an ADR 0012 owned edge (which bounds the value)
            /// or an ordinary link (which does not).
            struct LayoutLink
            {
                std::string target{};
                bool        owned{false};
            };

            /// An identity in a component the layout cannot bound, if the
            /// closure has one.
            ///
            /// Judged per strongly connected component, not per back edge. An
            /// owned edge (ADR 0012) bounds a cycle, so a component whose
            /// internal links are all owned is the recursive-struct shape; a
            /// component that is cyclic and contains ANY ordinary internal
            /// link describes a value of unbounded size. Walking back edges
            /// instead made the answer depend on field order -- with
            /// `A -owned-> C`, `A -> B`, `C -owned-> B`, `B -owned-> A`, the
            /// all-owned path completes `B` first and the ordinary `A -> B`
            /// then looks at a finished node and says nothing.
            ///
            /// Tarjan, iteratively: a descriptor is an input, so neither the
            /// component search nor the walk that feeds it may put the
            /// closure's size on the stack. Adjacency is built once per
            /// member (CLAUDE.md guardrail iv).
            [[nodiscard]] static std::optional<std::string> imported_layout_cycle(
                const std::vector<ImportedStruct> &closure) {
                std::unordered_map<std::string_view, std::vector<LayoutLink>> adjacency;
                adjacency.reserve(closure.size());
                for (const ImportedStruct &member : closure) {
                    std::vector<LayoutLink>  links;
                    std::vector<std::string> names;
                    for (const ImportedType &parent : member.parents) {
                        names.clear();
                        layout_references(parent, names);
                        for (std::string &name : names) { links.push_back(LayoutLink{std::move(name), false}); }
                    }
                    for (const ImportedStructField &field : member.fields) {
                        names.clear();
                        layout_references(field.type, names);
                        for (std::string &name : names) { links.push_back(LayoutLink{std::move(name), field.recursive}); }
                    }
                    adjacency.emplace(member.identity, std::move(links));
                }

                struct Node
                {
                    std::size_t index{0};
                    std::size_t low{0};
                    bool        on_stack{false};
                    bool        visited{false};
                };
                std::unordered_map<std::string_view, Node> nodes;
                nodes.reserve(adjacency.size());
                std::vector<std::string_view>              component_stack;
                std::size_t                                next_index = 0;

                struct Frame
                {
                    std::string_view identity{};
                    std::size_t      edge{0};
                };
                for (const auto &[root, _] : adjacency) {
                    if (nodes[root].visited) { continue; }
                    std::vector<Frame> stack{Frame{root, 0}};
                    nodes[root] = Node{next_index, next_index, true, true};
                    ++next_index;
                    component_stack.push_back(root);
                    while (!stack.empty()) {
                        const std::string_view identity = stack.back().identity;
                        const std::vector<LayoutLink> &links = adjacency.at(identity);
                        if (stack.back().edge < links.size()) {
                            const std::string_view target = links[stack.back().edge++].target;
                            const auto             known  = adjacency.find(target);
                            if (known == adjacency.end()) { continue; }
                            Node &node = nodes[known->first];
                            if (!node.visited) {
                                node = Node{next_index, next_index, true, true};
                                ++next_index;
                                component_stack.push_back(known->first);
                                stack.push_back(Frame{known->first, 0});
                            } else if (node.on_stack) {
                                nodes[identity].low = std::min(nodes[identity].low, node.index);
                            }
                            continue;
                        }
                        // Finished: close the component, or fold into the parent.
                        const Node finished = nodes[identity];
                        stack.pop_back();
                        if (!stack.empty()) {
                            Node &parent = nodes[stack.back().identity];
                            parent.low   = std::min(parent.low, finished.low);
                        }
                        if (finished.low != finished.index) { continue; }
                        std::unordered_set<std::string_view> component;
                        while (!component_stack.empty()) {
                            const std::string_view member = component_stack.back();
                            component_stack.pop_back();
                            nodes[member].on_stack = false;
                            component.insert(member);
                            if (member == identity) { break; }
                        }
                        // Cyclic when it has more than one member, or one with
                        // a link back to itself.
                        bool cyclic = component.size() > 1U;
                        for (const std::string_view member : component) {
                            for (const LayoutLink &link : adjacency.at(member)) {
                                if (!component.contains(link.target)) { continue; }
                                if (link.target == member) { cyclic = true; }
                            }
                        }
                        if (!cyclic) { continue; }
                        for (const std::string_view member : component) {
                            for (const LayoutLink &link : adjacency.at(member)) {
                                if (!link.owned && component.contains(link.target)) { return std::string{member}; }
                            }
                        }
                    }
                }
                return std::nullopt;
            }

            /// Collects every struct `type` reaches, at any depth (ADR 0013),
            /// onto `found` rather than binding it here -- the inter-struct
            /// walk is driven by a worklist in `imported_struct_binding`, so
            /// the two recursions do not compound.
            ///
            /// A name it cannot find is REPORTED, not skipped: the module that
            /// declares it is missing from the supplied package target, so this
            /// module's layout cannot be rebuilt whole. Accepting the gap here
            /// let `hgl check` finish against a field whose type nothing
            /// describes, and left direct wiring to fail later at an unknown
            /// nominal type -- a name the source never mentions.
            void reached_structs(const ImportedType &type, std::string_view owner, SourceRange range,
                                 std::vector<ImportedStruct> &found) {
                if (!type.nominal_identity.empty()) {
                    const ImportedStruct *reached = catalog_.find_struct_by_identity(type.nominal_identity);
                    if (reached == nullptr) {
                        report(Category::Module, range,
                               "imported struct '" + std::string{owner} + "' reaches '" + type.nominal_identity +
                                   "', whose module is not in the supplied package target, so its layout cannot be "
                                   "rebuilt");
                        return;
                    }
                    found.push_back(*reached);
                    // NOT a return: `Box<Leaf>` names `Box` at the head and
                    // `Leaf` only as an argument, and `Box`'s own record
                    // mentions nothing but its parameter. Stopping here left
                    // `Leaf` undescribed and a backend reporting an unknown
                    // nominal type.
                }
                for (const ImportedType &child : type.children) { reached_structs(child, owner, range, found); }
            }

            [[nodiscard]] std::optional<Binding> imported_function(std::span<const ImportedFunction> functions, SourceRange range) {
                if (functions.empty()) { return std::nullopt; }
                std::vector<const ImportedFunction *> supported;
                for (const ImportedFunction &function : functions) {
                    if (function.support_error.empty()) { supported.push_back(&function); }
                }
                if (supported.empty()) {
                    report(Category::Module, range,
                           "native function '" + functions.front().identity +
                               "' is unavailable: " + functions.front().support_error);
                    return std::nullopt;
                }
                const std::string family = functions.front().module_identity + "::" + functions.front().name;
                if (const auto found = imported_function_bindings_.find(family); found != imported_function_bindings_.end()) {
                    return found->second;
                }
                Binding binding;
                binding.kind  = BindingKind::ImportedFunction;
                binding.index = static_cast<std::uint32_t>(result_.imported_functions.size());
                binding.count = static_cast<std::uint32_t>(supported.size());
                for (const ImportedFunction *function : supported) { result_.imported_functions.push_back(*function); }
                imported_function_bindings_.emplace(family, binding);
                return binding;
            }

            /// The interim kernel table (developer guide, "Interim kernel table").
            [[nodiscard]] std::optional<std::string> kernel_registry_name(std::string_view module, std::string_view name) const {
                if (module == kernel_analytics) { return std::string{kernel_analytics} + "." + std::string{name}; }
                if (has_operator_(name)) { return std::string{name}; }
                const std::string underscored = std::string{name} + "_";
                if (has_operator_(underscored)) { return underscored; }
                return std::nullopt;
            }

            // -------------------------------------------------------- functions

            void resolve_function(ast::DeclId id, const ast::FunctionDecl &fn) {
                result_.kinds[id] = classify(fn);
                if (fn.is_const && fn.visibility != ast::FunctionVisibility::Internal) {
                    report(Category::FunctionKind, fn.name.range,
                           "const fn is currently module-internal; const/export/impl combinations require a separate contract");
                }
                if (fn.is_const && (!fn.generics.empty() || std::ranges::any_of(fn.signature.parameters, [](const auto &p) {
                        return p.pack != ast::ParameterPack::None;
                    }))) {
                    report(Category::FunctionKind, fn.name.range,
                           "generic and parameter-pack const fn lowering is not supported yet");
                }
                if (fn.is_const && result_.kinds[id] == FunctionKind::Runtime) {
                    report(Category::FunctionKind, fn.name.range,
                           "a const fn cannot declare when, state, inject, start, or stop; put temporal policy in a fn wrapper");
                }
                if (result_.kinds[id] == FunctionKind::Runtime) {
                    const bool positional = std::ranges::any_of(fn.signature.parameters, [](const ast::Parameter &parameter) {
                        return parameter.pack == ast::ParameterPack::Positional;
                    });
                    const bool keyword    = std::ranges::any_of(fn.signature.parameters, [](const ast::Parameter &parameter) {
                        return parameter.pack == ast::ParameterPack::Keyword;
                    });
                    if (positional && keyword) {
                        report(Category::Type, fn.name.range,
                               "a runtime function currently supports one aggregate parameter pack, not both positional and named "
                               "packs");
                    }
                }
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, fn.generics, context);
                resolve_signature(id, fn.signature, context);
                resolve_constraint(fn.requirements, context);
                if (fn.concise_body != ast::no_node) { resolve_expr(fn.concise_body, context); }
                if (fn.block_body != ast::no_node) { resolve_block(fn.block_body, context); }
                pop_scope();
            }

            void resolve_native_function(ast::DeclId id, const ast::NativeFunctionDecl &fn) {
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, fn.generics, context);
                resolve_signature(id, fn.signature, context, true);
                for (const ast::Parameter &parameter : fn.signature.parameters) {
                    if (parameter.pack != ast::ParameterPack::None) {
                        report(Category::Type, parameter.name.range,
                               "a source-native function cannot declare a parameter pack; use an ordinary HGL wrapper");
                    }
                    if (parameter.default_value != ast::no_node) {
                        report(Category::Type, module_.expr(parameter.default_value).range,
                               "a native function parameter cannot have a default value");
                    }
                }
                if (fn.requirements != ast::no_node) {
                    report(Category::Type, module_.constraint(fn.requirements).range,
                           "a native function cannot have a requires clause until descriptor constraints are importable");
                }
                pop_scope();
            }

            void resolve_operator(ast::DeclId id, const ast::OperatorDecl &op) {
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, op.generics, context);
                resolve_signature(id, op.signature, context);
                resolve_constraint(op.requirements, context);
                for (const ast::OperatorProperties &properties : op.properties) {
                    for (ast::TypeId domain : properties.domain) { resolve_type(domain, context); }
                    for (const ast::OperatorProperty &property : properties.entries) {
                        if (property.value != ast::no_node) { resolve_expr(property.value, context); }
                    }
                }
                pop_scope();
            }

            void resolve_instantiation(ast::DeclId id, const ast::InstantiateDecl &declaration) {
                Context context;
                context.fn = id;
                push_scope();
                auto &bindings = result_.instantiation_bindings[id];
                bindings.reserve(declaration.entries.size());
                for (const ast::Instantiation &entry : declaration.entries) {
                    Binding                      binding;
                    const std::optional<Binding> found = lookup(entry.name.text);
                    if (found && found->kind == BindingKind::Operator) {
                        if (std::ranges::any_of(result_.imported_contracts, [&](const auto &contract) {
                                return contract.identity == found->operator_identity;
                            })) {
                            binding = *found;
                        } else {
                            report(Category::Module, entry.name.range,
                                   "'instantiate " + std::string{entry.name.text} +
                                       "<...>' of an imported operator requires external contract metadata");
                        }
                    } else if (!found || found->kind != BindingKind::LocalOperator) {
                        report(Category::Module, entry.name.range,
                               "'instantiate " + std::string{entry.name.text} + "<...>' names no operator declared in this module");
                    } else {
                        binding = *found;
                    }
                    bindings.push_back(std::move(binding));
                    for (const ast::GenericArgument &argument : entry.arguments) {
                        if (argument.type != ast::no_node) {
                            resolve_type(argument.type, context);
                        } else if (argument.value != ast::no_node) {
                            resolve_expr(argument.value, context);
                        }
                    }
                }
                pop_scope();
            }

            void resolve_struct(ast::DeclId id, const ast::StructDecl &structure) {
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, structure.generics, context);
                for (const ast::TypeId parent : structure.parents) { resolve_type(parent, context); }
                for (const ast::StructMember &member : structure.members) {
                    std::visit(
                        [&](const auto &item) {
                            using T = std::decay_t<decltype(item)>;
                            if constexpr (std::is_same_v<T, ast::StructField>) {
                                resolve_type(item.type, context);
                                if (item.default_value != ast::no_node) { resolve_expr(item.default_value, context); }
                            } else {
                                resolve_expr(item.value, context);
                            }
                        },
                        member);
                }
                resolve_constraint(structure.requirements, context);
                pop_scope();
            }

            void resolve_test(ast::DeclId id, const ast::TestDecl &test) {
                Context context;
                context.fn      = id;
                context.in_test = true;
                push_scope();
                resolve_block(test.block, context);
                pop_scope();
            }

            void declare_generics(ast::DeclId fn, const std::vector<ast::GenericParameter> &generics, Context &context) {
                for (std::size_t i = 0; i < generics.size(); ++i) {
                    if (generics[i].type != ast::no_node) { resolve_type(generics[i].type, context); }
                    Binding binding;
                    binding.kind  = BindingKind::Generic;
                    binding.decl  = fn;
                    binding.index = static_cast<std::uint32_t>(i);
                    declare(generics[i].name, binding, "among the generic parameters");
                }
            }

            [[nodiscard]] const ast::GenericParameter *generic_parameter(ast::DeclId owner, const ast::Type &type) const noexcept {
                if (type.kind != ast::TypeKind::Named || !type.qualifier.empty()) { return nullptr; }
                const auto find = [&](const auto &declaration) -> const ast::GenericParameter * {
                    for (const ast::GenericParameter &generic : declaration.generics) {
                        if (generic.name.text == type.name.text) { return &generic; }
                    }
                    return nullptr;
                };
                const ast::Decl &declaration = module_.decl(owner);
                if (const auto *node = std::get_if<ast::FunctionDecl>(&declaration.node)) { return find(*node); }
                if (const auto *node = std::get_if<ast::NativeFunctionDecl>(&declaration.node)) { return find(*node); }
                if (const auto *node = std::get_if<ast::OperatorDecl>(&declaration.node)) { return find(*node); }
                return nullptr;
            }

            void resolve_signature(ast::DeclId fn, const ast::Signature &signature, Context &context, bool native = false) {
                bool seen_positional_pack = false;
                bool seen_keyword_pack    = false;
                for (std::size_t i = 0; i < signature.parameters.size(); ++i) {
                    const ast::Parameter &parameter = signature.parameters[i];
                    if (parameter.type != ast::no_node) {
                        resolve_type(parameter.type, context, !parameter.is_const, native && !parameter.is_const);
                        if (module_.type(parameter.type).kind == ast::TypeKind::Signal && parameter.default_value != ast::no_node) {
                            report(Category::Type, module_.expr(parameter.default_value).range,
                                   "a 'signal' input cannot have a default value");
                        }
                    }
                    if (parameter.default_value != ast::no_node) { resolve_expr(parameter.default_value, context); }

                    const ast::Type             &parameter_type = module_.type(parameter.type);
                    const ast::GenericParameter *generic        = generic_parameter(fn, parameter_type);
                    if (parameter.pack == ast::ParameterPack::None) {
                        if (seen_positional_pack || seen_keyword_pack) {
                            report(Category::Type, parameter.name.range, "a fixed parameter cannot follow a parameter pack");
                        }
                        if (generic != nullptr && generic->is_pack) {
                            report(Category::Type, parameter_type.range,
                                   "a type pack is used through a positional '...Ts' or named '...{Fields}' parameter");
                        }
                    } else {
                        if (parameter.cardinality.maximum && *parameter.cardinality.maximum < parameter.cardinality.minimum) {
                            report(Category::Type, parameter.cardinality.range,
                                   "a parameter pack maximum cannot be less than its minimum");
                        }
                        if (parameter.is_const || parameter.default_value != ast::no_node) {
                            report(Category::Type, parameter.name.range, "a parameter pack cannot be const or have a default");
                        }
                        if (parameter.pack == ast::ParameterPack::Positional) {
                            if (seen_positional_pack || seen_keyword_pack) {
                                report(Category::Type, parameter.name.range,
                                       "a signature has at most one positional pack, before its named pack");
                            }
                            seen_positional_pack = true;
                        } else {
                            if (seen_keyword_pack) {
                                report(Category::Type, parameter.name.range, "a signature has at most one named parameter pack");
                            }
                            seen_keyword_pack = true;
                            if (generic == nullptr || !generic->is_pack) {
                                report(Category::Type, parameter_type.range,
                                       "a named pack uses a heterogeneous type pack, for example '<...Fields>(args: ...{Fields})'");
                            }
                        }
                    }
                }
                if (signature.result != ast::no_node) { resolve_type(signature.result, context); }
                // Parameters become visible together, after their defaults.
                for (std::size_t i = 0; i < signature.parameters.size(); ++i) {
                    Binding binding;
                    binding.kind  = BindingKind::Parameter;
                    binding.decl  = fn;
                    binding.index = static_cast<std::uint32_t>(i);
                    declare(signature.parameters[i].name, binding, "among the parameters");
                }
            }

            [[nodiscard]] FunctionKind classify(const ast::FunctionDecl &fn) const {
                if (fn.block_body != ast::no_node && block_has_runtime_form(fn.block_body)) { return FunctionKind::Runtime; }
                return FunctionKind::Composition;
            }

            [[nodiscard]] bool block_has_runtime_form(ast::BlockId id) const {
                for (const ast::StmtId stmt_id : module_.block(id).statements) {
                    if (stmt_has_runtime_form(stmt_id)) { return true; }
                }
                return false;
            }

            [[nodiscard]] bool stmt_has_runtime_form(ast::StmtId id) const {
                const ast::Stmt &stmt = module_.stmt(id);
                return std::visit(
                    [&](const auto &node) -> bool {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::StateDecl> || std::is_same_v<T, ast::InjectDecl> ||
                                      std::is_same_v<T, ast::LifecycleBlock> || std::is_same_v<T, ast::WhenStmt>) {
                            return true;
                        } else if constexpr (std::is_same_v<T, ast::ForStmt>) {
                            // Iteration follows the phase established by its
                            // containing function. A node-only construct in
                            // the body still classifies the whole function as
                            // runtime, but `for` itself is phase-neutral.
                            return block_has_runtime_form(node.block);
                        } else if constexpr (std::is_same_v<T, ast::ExprStmt>) {
                            return expr_has_runtime_form(node.expr);
                        } else if constexpr (std::is_same_v<T, ast::LocalDecl>) {
                            return expr_has_runtime_form(node.init);
                        } else if constexpr (std::is_same_v<T, ast::AssignStmt>) {
                            return expr_has_runtime_form(node.value);
                        } else if constexpr (std::is_same_v<T, ast::ReturnStmt>) {
                            return expr_has_runtime_form(node.value);
                        } else {
                            return false;
                        }
                    },
                    stmt.node);
            }

            [[nodiscard]] bool expr_has_runtime_form(ast::ExprId id) const {
                if (id == ast::no_node) { return false; }
                const ast::Expr &expr = module_.expr(id);
                if (const auto *if_ = std::get_if<ast::If>(&expr.node)) {
                    return block_has_runtime_form(if_->then_block) || expr_has_runtime_form(if_->otherwise);
                }
                if (const auto *block = std::get_if<ast::BlockExpr>(&expr.node)) { return block_has_runtime_form(block->block); }
                return false;
            }

            // ----------------------------------------------------------- bodies

            void resolve_block(ast::BlockId id, Context &context) {
                push_scope();
                for (const ast::StmtId stmt_id : module_.block(id).statements) { resolve_stmt(stmt_id, context); }
                pop_scope();
            }

            void resolve_stmt(ast::StmtId id, Context &context) {
                const ast::Stmt &stmt = module_.stmt(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::LocalDecl>) {
                            if (node.type != ast::no_node) { resolve_type(node.type, context); }
                            if (node.init != ast::no_node) {
                                resolve_expr(node.init, context);
                            } else {
                                if (!node.mutable_) { report(Category::Type, stmt.range, "'let' requires an initializer"); }
                                if (node.type == ast::no_node) {
                                    report(Category::Type, stmt.range, "an uninitialized 'var' requires an explicit type");
                                }
                            }
                            Binding binding;
                            binding.kind = BindingKind::Local;
                            binding.stmt = id;
                            declare(node.name, binding, "in the block");
                        } else if constexpr (std::is_same_v<T, ast::StateDecl>) {
                            reject_in_test(context, stmt.range, node.cache ? "cache" : "state");
                            if (node.type != ast::no_node) { resolve_type(node.type, context); }
                            resolve_expr(node.init, context);
                            Binding binding;
                            binding.kind = BindingKind::Local;
                            binding.stmt = id;
                            declare(node.name, binding, "in the block");
                        } else if constexpr (std::is_same_v<T, ast::InjectDecl>) {
                            reject_in_test(context, stmt.range, "inject");
                            for (std::size_t index = 0; index < node.names.size(); ++index) {
                                Binding binding;
                                binding.kind  = BindingKind::Local;
                                binding.stmt  = id;
                                binding.index = static_cast<std::uint32_t>(index);
                                declare(node.names[index], binding, "in the block");
                            }
                        } else if constexpr (std::is_same_v<T, ast::LifecycleBlock>) {
                            reject_in_test(context, stmt.range, node.is_stop ? "stop" : "start");
                            resolve_block(node.block, context);
                        } else if constexpr (std::is_same_v<T, ast::WhenStmt>) {
                            reject_in_test(context, stmt.range, "when");
                            if (node.condition != ast::no_node) { resolve_expr(node.condition, context); }
                            resolve_block(node.block, context);
                        } else if constexpr (std::is_same_v<T, ast::ForStmt>) {
                            reject_in_test(context, stmt.range, "for");
                            resolve_expr(node.iterable, context);
                            push_scope();
                            Binding first;
                            first.kind = BindingKind::Local;
                            first.stmt = id;
                            declare(node.first, first, "in the iteration pattern");
                            if (!node.second.empty()) {
                                Binding second = first;
                                second.second  = true;
                                second.index   = 1;
                                declare(node.second, second, "in the iteration pattern");
                            }
                            resolve_block(node.block, context);
                            pop_scope();
                        } else if constexpr (std::is_same_v<T, ast::AssignStmt>) {
                            resolve_expr(node.place, context);
                            resolve_expr(node.value, context);
                        } else if constexpr (std::is_same_v<T, ast::ReturnStmt>) {
                            if (node.value != ast::no_node) { resolve_expr(node.value, context); }
                        } else if constexpr (std::is_same_v<T, ast::AssertStmt>) {
                            if (!context.in_test) {
                                report(Category::Phase, stmt.range, "'assert' is only valid inside a test body");
                            }
                            resolve_expr(node.condition, context);
                        } else if constexpr (std::is_same_v<T, ast::ExprStmt>) {
                            resolve_expr(node.expr, context);
                        }
                    },
                    stmt.node);
            }

            void reject_in_test(const Context &context, SourceRange range, std::string_view form) {
                if (context.in_test) {
                    report(Category::Phase, range, "'" + std::string{form} + "' is not available in a test body");
                }
            }

            void resolve_expr(ast::ExprId id, Context &context) {
                if (id == ast::no_node) { return; }
                const ast::Expr &expr = module_.expr(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::Placeholder>) {
                            if (!context.in_sequence) {
                                report(Category::Type, expr.range, "'_' is only valid in a harness sequence");
                            }
                        } else if constexpr (std::is_same_v<T, ast::NameRef>) {
                            resolve_name(id, node.name);
                        } else if constexpr (std::is_same_v<T, ast::QualifiedRef>) {
                            resolve_qualified(id, node);
                        } else if constexpr (std::is_same_v<T, ast::Unary>) {
                            resolve_expr(node.operand, context);
                        } else if constexpr (std::is_same_v<T, ast::Binary>) {
                            resolve_expr(node.lhs, context);
                            resolve_expr(node.rhs, context);
                        } else if constexpr (std::is_same_v<T, ast::Call>) {
                            resolve_expr(node.callee, context);
                            if (result_.bindings[node.callee].kind == BindingKind::Struct) {
                                for (const ast::Argument &argument : node.arguments) {
                                    if (argument.name.empty()) {
                                        report(Category::Type, module_.expr(argument.value).range,
                                               "struct construction uses named arguments");
                                    }
                                }
                            }
                            for (const ast::Argument &argument : node.arguments) { resolve_expr(argument.value, context); }
                        } else if constexpr (std::is_same_v<T, ast::Index>) {
                            resolve_expr(node.target, context);
                            resolve_expr(node.index, context);
                        } else if constexpr (std::is_same_v<T, ast::Field>) {
                            resolve_expr(node.target, context);
                        } else if constexpr (std::is_same_v<T, ast::SequenceLiteral>) {
                            const bool saved    = context.in_sequence;
                            context.in_sequence = context.in_test;
                            for (const ast::SequenceElement &element : node.elements) {
                                resolve_expr(element.key, context);
                                resolve_expr(element.value, context);
                            }
                            context.in_sequence = saved;
                        } else if constexpr (std::is_same_v<T, ast::TupleLiteral>) {
                            for (const ast::ExprId element : node.elements) { resolve_expr(element, context); }
                        } else if constexpr (std::is_same_v<T, ast::AnonymousFn>) {
                            push_scope();
                            for (std::size_t i = 0; i < node.parameters.size(); ++i) {
                                if (node.parameters[i].type != ast::no_node) { resolve_type(node.parameters[i].type, context); }
                                // Anonymous parameters bind by the expression that declares them.
                                Binding binding;
                                binding.kind  = BindingKind::Parameter;
                                binding.decl  = ast::no_node;
                                binding.stmt  = id;
                                binding.index = static_cast<std::uint32_t>(i);
                                declare(node.parameters[i].name, binding, "among the parameters");
                            }
                            if (node.result != ast::no_node) { resolve_type(node.result, context); }
                            resolve_expr(node.body, context);
                            pop_scope();
                        } else if constexpr (std::is_same_v<T, ast::If>) {
                            resolve_expr(node.condition, context);
                            resolve_block(node.then_block, context);
                            resolve_expr(node.otherwise, context);
                        } else if constexpr (std::is_same_v<T, ast::BlockExpr>) {
                            resolve_block(node.block, context);
                        } else if constexpr (std::is_same_v<T, ast::Eval>) {
                            if (!context.in_test) { report(Category::Phase, expr.range, "eval is only valid inside a test body"); }
                            resolve_expr(node.callee, context);
                            for (const ast::Argument &argument : node.arguments) { resolve_expr(argument.value, context); }
                        } else if constexpr (std::is_same_v<T, ast::Construct>) {
                            resolve_type(node.type, context);
                            const Binding &target = result_.type_bindings[node.type];
                            if (target.kind != BindingKind::Struct) {
                                report(Category::Type, module_.type(node.type).range,
                                       "a struct constructor target is a concrete struct type");
                            }
                            for (const ast::Argument &argument : node.arguments) {
                                if (argument.name.empty()) {
                                    report(Category::Type, module_.expr(argument.value).range,
                                           "struct construction uses named arguments");
                                }
                                resolve_expr(argument.value, context);
                            }
                        }
                        // Literals bind nothing.
                    },
                    expr.node);
            }

            void resolve_name(ast::ExprId id, const ast::Name &name) {
                const std::optional<Binding> binding = lookup(name.text);
                if (!binding) {
                    if (is_intrinsic(name.text)) {
                        Binding intrinsic;
                        intrinsic.kind          = BindingKind::Intrinsic;
                        intrinsic.registry_name = std::string{name.text};
                        result_.bindings[id]    = std::move(intrinsic);
                        return;
                    }
                    report(Category::Name, name.range, "unknown name '" + std::string{name.text} + "'");
                    return;
                }
                if (binding->kind == BindingKind::Test) {
                    report(Category::Name, name.range, "'" + std::string{name.text} + "' is a test, not a value");
                    return;
                }
                result_.bindings[id] = *binding;
            }

            void resolve_qualified(ast::ExprId id, const ast::QualifiedRef &ref) {
                for (const ModuleAlias &alias : result_.aliases) {
                    if (alias.alias != ref.qualifier.text) { continue; }
                    if (const auto *contract = catalog_.find_operator(alias.module, ref.name.text)) {
                        if (const auto binding = imported_operator(*contract, ref.name.range)) { result_.bindings[id] = *binding; }
                        return;
                    }
                    // `m::Quote(...)` constructs a struct the module exports
                    // (ADR 0013). The alias form and `use m::{Quote}` share the
                    // binding, so they cannot drift.
                    if (const ImportedStruct *structure = catalog_.find_struct(alias.module, ref.name.text)) {
                        const ImportedStruct record = *structure;
                        if (const auto binding = imported_struct_binding(record, ref.name.range)) {
                            result_.bindings[id] = *binding;
                        }
                        return;
                    }
                    if (alias.module != kernel_std && alias.module != kernel_analytics) {
                        const std::span<const ImportedFunction> functions = catalog_.find_functions(alias.module, ref.name.text);
                        if (functions.empty()) {
                            report(Category::Module, ref.name.range,
                                   alias.module + " does not export '" + std::string{ref.name.text} + "'");
                            return;
                        }
                        const std::optional<Binding> binding = imported_function(functions, ref.name.range);
                        if (binding) { result_.bindings[id] = *binding; }
                        return;
                    }
                    const std::optional<std::string> registry_name = kernel_registry_name(alias.module, ref.name.text);
                    if (!registry_name) {
                        report(Category::Module, ref.name.range,
                               alias.module + " does not export '" + std::string{ref.name.text} + "'");
                        return;
                    }
                    Binding binding;
                    binding.kind              = BindingKind::Operator;
                    binding.registry_name     = *registry_name;
                    binding.operator_identity = alias.module + "." + std::string{ref.name.text};
                    result_.bindings[id]      = binding;
                    return;
                }
                report(Category::Name, ref.qualifier.range, "unknown module alias '" + std::string{ref.qualifier.text} + "'");
            }

            [[nodiscard]] const std::vector<ast::GenericParameter> &generics_of(ast::DeclId id) const {
                const ast::DeclNode &node = module_.decl(id).node;
                if (const auto *structure = std::get_if<ast::StructDecl>(&node)) { return structure->generics; }
                if (const auto *fn = std::get_if<ast::FunctionDecl>(&node)) { return fn->generics; }
                if (const auto *fn = std::get_if<ast::NativeFunctionDecl>(&node)) { return fn->generics; }
                return std::get<ast::OperatorDecl>(node).generics;
            }

            [[nodiscard]] bool is_const_generic(const Binding &binding) const {
                return binding.kind == BindingKind::Generic && binding.index < generics_of(binding.decl).size() &&
                       generics_of(binding.decl)[binding.index].is_const;
            }

            /// Resolves one argument of an applied struct type and returns what a
            /// bare-name argument names: that argument has no type node to carry
            /// the binding, and the struct recursion check must see through it.
            std::optional<Binding> resolve_generic_argument(const ast::GenericArgument  &argument,
                                                            const ast::GenericParameter &parameter, Context &context) {
                if (argument.type != ast::no_node) {
                    resolve_type(argument.type, context);
                    if (parameter.is_const) {
                        report(Category::Type, argument.range,
                               "const generic '" + std::string{parameter.name.text} + "' takes a value argument");
                    } else {
                        const ast::TypeKind kind = module_.type(argument.type).kind;
                        if (kind == ast::TypeKind::Atomic || kind == ast::TypeKind::Rolling || kind == ast::TypeKind::Reference) {
                            report(Category::Type, argument.range,
                                   "generic struct type arguments are canonical value types; put "
                                   "the temporal shape in the field declaration");
                        }
                    }
                    return std::nullopt;
                }
                if (argument.value != ast::no_node) {
                    resolve_expr(argument.value, context);
                    if (!parameter.is_const) {
                        report(Category::Type, argument.range,
                               "type generic '" + std::string{parameter.name.text} + "' takes a type argument");
                    }
                    return std::nullopt;
                }

                const std::optional<Binding> binding = lookup(argument.name.text);
                if (!binding) {
                    report(Category::Type, argument.name.range,
                           "unknown generic argument '" + std::string{argument.name.text} + "'");
                    return std::nullopt;
                }
                if (parameter.is_const) {
                    if (!is_const_generic(*binding)) {
                        report(Category::Type, argument.name.range,
                               "const generic '" + std::string{parameter.name.text} + "' takes a const value argument");
                    }
                    return binding;
                }
                if (binding->kind == BindingKind::Generic && is_const_generic(*binding)) {
                    report(Category::Type, argument.name.range,
                           "type generic '" + std::string{parameter.name.text} + "' takes a type argument");
                } else if (binding->kind != BindingKind::Generic && binding->kind != BindingKind::Struct) {
                    report(Category::Type, argument.name.range, "'" + std::string{argument.name.text} + "' is not a type");
                } else if (binding->kind == BindingKind::Struct && !generics_of(binding->decl).empty()) {
                    report(Category::Type, argument.name.range,
                           "generic struct '" + std::string{argument.name.text} + "' must be fully applied");
                }
                return binding;
            }

            /// One argument of an imported generic application (ADR 0013).
            /// Mirrors the local resolver's role checks against the exporting
            /// module's parameter, which is an ImportedGeneric rather than a
            /// declaration of this module.
            void resolve_imported_generic_argument(const ast::GenericArgument &argument, const ImportedGeneric &parameter,
                                                   Context &context) {
                if (argument.type != ast::no_node) {
                    resolve_type(argument.type, context);
                    if (parameter.is_const) {
                        report(Category::Type, argument.range,
                               "const generic '" + parameter.name + "' takes a value argument");
                    }
                    return;
                }
                if (argument.value != ast::no_node) {
                    resolve_expr(argument.value, context);
                    if (!parameter.is_const) {
                        report(Category::Type, argument.range, "type generic '" + parameter.name + "' takes a type argument");
                    }
                    return;
                }
                if (argument.name.empty()) { return; }
                const std::optional<Binding> binding = lookup(argument.name.text);
                if (!binding) {
                    report(Category::Type, argument.name.range,
                           "unknown generic argument '" + std::string{argument.name.text} + "'");
                    return;
                }
                if (parameter.is_const) {
                    if (!is_const_generic(*binding)) {
                        report(Category::Type, argument.name.range,
                               "const generic '" + parameter.name + "' takes a const value argument");
                    }
                } else if (binding->kind != BindingKind::Generic && binding->kind != BindingKind::Struct &&
                           binding->kind != BindingKind::ImportedStruct) {
                    // A value name is not a type: the local path refuses it, so
                    // an imported application must too, or it binds an invalid
                    // specialization as a type.
                    report(Category::Type, argument.name.range,
                           "type generic '" + parameter.name + "' takes a type argument");
                }
            }

            /// Seeds a child's effective fields from an imported parent, its own
            /// parent, and so on. The catalog records only the fields a struct
            /// DECLARES, recording what it inherits through its parents, so
            /// reading `parent.fields` alone loses a grandparent's fields --
            /// supplying one would be rejected as unknown and omitting a
            /// required one accepted. Ancestors come first, so a field keeps the
            /// position it has in the exporting family, and each entry keeps the
            /// struct that declares it as its source.
            /// Taken BY VALUE, not by reference: binding an ancestor appends to
            /// `result_.imported_structs`, and a reference into that vector
            /// would dangle the moment it reallocates -- which silently drops
            /// the fields this struct declares.
            [[nodiscard]] bool seed_imported_fields(ast::DeclId id, StructInfo &info, const ImportedStruct structure,
                                                    const StructSource &source, SourceRange range,
                                                    std::vector<std::string> visiting = {}) {
                if (std::ranges::find(visiting, structure.identity) != visiting.end()) { return true; }
                visiting.push_back(structure.identity);
                for (const ImportedType &parent : structure.parents) {
                    if (parent.nominal_identity.empty()) { continue; }
                    const ImportedStruct *ancestor = catalog_.find_struct_by_identity(parent.nominal_identity);
                    if (ancestor == nullptr) {
                        // Catalog records omit inherited fields, so continuing
                        // without this ancestor would validate the child
                        // against a SHORT layout -- construction would accept
                        // omitting the ancestor's required fields. Whatever
                        // crosses a module boundary crosses whole (ADR 0013).
                        return false;
                    }
                    // An ancestor is referenced through the same imported record
                    // the child came from; this module gains no declaration for
                    // it either.
                    // Each catalog record holds only the fields it DECLARES, so a
                    // field seeded from an ancestor must name that ancestor as
                    // its source -- naming the immediate parent would make its
                    // type unfindable, and leave the field typeless in the IRs.
                    const ImportedStruct          ancestor_copy   = *ancestor;
                    const std::optional<Binding>  ancestor_binding = imported_struct_binding(ancestor_copy, range);
                    if (!ancestor_binding) { return false; }
                    const StructSource ancestor_source{.imported = ancestor_binding->index};
                    if (!seed_imported_fields(id, info, ancestor_copy, ancestor_source, range, visiting)) { return false; }
                }
                for (const ImportedStructField &field : structure.fields) {
                    if (field_indices_[id].contains(field.name)) { continue; }
                    field_indices_[id].emplace(field.name, info.fields.size());
                    info.fields.push_back(StructField{.name          = field.name,
                                                      .type          = ast::no_node,
                                                      .default_value = ast::no_node,
                                                      .origin        = source,
                                                      .optional      = field.optional,
                                                      .recursive     = field.recursive});
                }
                return true;
            }

            /// A qualified source type names a struct another module exports
            /// (ADR 0013). Its identity stays the owner's, so this binds the
            /// name and copies nothing into the importing module. The generic
            /// arguments are resolved either way, so a spelling error inside
            /// them is reported even when the head does not resolve.
            void resolve_imported_named_type(ast::TypeId id, const ast::Type &type, Context &context) {
                const ImportedStruct *structure = nullptr;
                const ModuleAlias    *alias     = nullptr;
                for (const ModuleAlias &candidate : result_.aliases) {
                    if (candidate.alias == type.qualifier.text) { alias = &candidate; }
                }
                if (alias != nullptr) { structure = catalog_.find_struct(alias->module, type.name.text); }
                // Arguments resolve whether or not the head does, so a spelling
                // error inside them is still reported. Where the application's
                // arity matches, each argument is checked against its parameter
                // -- including a bare name, which the parser leaves in `name`
                // with neither `type` nor `value` set.
                const bool paired = structure != nullptr && structure->generics.size() == type.arguments.size();
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    const ast::GenericArgument &argument = type.arguments[index];
                    if (paired) {
                        resolve_imported_generic_argument(argument, structure->generics[index], context);
                    } else if (argument.type != ast::no_node) {
                        resolve_type(argument.type, context);
                    } else if (argument.value != ast::no_node) {
                        resolve_expr(argument.value, context);
                    } else if (!argument.name.empty() && !lookup(argument.name.text)) {
                        report(Category::Type, argument.name.range,
                               "unknown generic argument '" + std::string{argument.name.text} + "'");
                    }
                }
                if (alias == nullptr) {
                    report(Category::Name, type.qualifier.range,
                           "unknown module alias '" + std::string{type.qualifier.text} + "'");
                    return;
                }
                if (structure == nullptr) {
                    report(Category::Module, type.name.range,
                           alias->module + " does not export struct '" + std::string{type.name.text} + "'");
                    return;
                }
                if (structure->generics.size() != type.arguments.size()) {
                    report(Category::Type, type.range,
                           "imported generic struct '" + structure->identity + "' expects " +
                               std::to_string(structure->generics.size()) + " arguments, got " +
                               std::to_string(type.arguments.size()));
                    return;
                }
                if (const std::optional<Binding> imported = imported_struct_binding(*structure, type.name.range)) {
                    result_.type_bindings[id] = *imported;
                }
            }

            void resolve_type(ast::TypeId id, Context &context, bool allow_signal = false, bool allow_schema = false) {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Signal && !allow_signal) {
                    report(Category::Type, type.range,
                           "'signal' is an input-only type marker and is only valid as a non-const parameter type");
                }
                if (type.kind == ast::TypeKind::Schema && !allow_schema) {
                    report(Category::Type, type.range,
                           "'schema' is borrowed runtime metadata and is only valid as a non-const native parameter type");
                }
                if (type.value_position && (type.kind == ast::TypeKind::Atomic || type.kind == ast::TypeKind::Rolling ||
                                            type.kind == ast::TypeKind::Reference)) {
                    const std::string_view spelling = type.kind == ast::TypeKind::Atomic    ? "atomic"
                                                      : type.kind == ast::TypeKind::Rolling ? "rolling"
                                                                                            : "ref";
                    report(Category::Type, type.range,
                           "'" + std::string{spelling} + "' is a temporal shape, not a canonical value type");
                }
                if (type.kind == ast::TypeKind::Reference && type.children.size() == 1U &&
                    module_.type(type.children.front()).kind == ast::TypeKind::Reference) {
                    report(Category::Type, type.range, "nested 'ref' boundaries are not supported");
                }
                if (type.kind == ast::TypeKind::Map && type.children.size() == 2U &&
                    module_.type(type.children[1]).kind == ast::TypeKind::Reference) {
                    report(Category::Type, type.range,
                           "map values wrapped in 'ref' require the collection-reference mapping to be resolved");
                }
                if (type.kind == ast::TypeKind::Named) {
                    // A qualified type names a struct another module exports
                    // (ADR 0013). The identity stays the owner's; this module
                    // binds the name and nothing is copied into its namespace.
                    if (!type.qualifier.empty()) {
                        resolve_imported_named_type(id, type, context);
                    } else {
                    const std::optional<Binding> binding = lookup(type.name.text);
                    if (binding && binding->kind == BindingKind::ImportedStruct) {
                        // `use m::{Quote}` brought the name in; it names the
                        // exporting module's type, exactly as the qualified
                        // spelling does.
                        const ImportedStruct &imported = result_.imported_structs[binding->index];
                        if (imported.generics.size() != type.arguments.size()) {
                            report(Category::Type, type.range,
                                   "imported generic struct '" + imported.identity + "' expects " +
                                       std::to_string(imported.generics.size()) + " arguments, got " +
                                       std::to_string(type.arguments.size()));
                        } else {
                            for (std::size_t i = 0; i < type.arguments.size(); ++i) {
                                resolve_imported_generic_argument(type.arguments[i], imported.generics[i], context);
                            }
                            result_.type_bindings[id] = *binding;
                        }
                    } else if (!binding ||
                               (binding->kind != BindingKind::Generic && binding->kind != BindingKind::Struct)) {
                        report(Category::Type, type.name.range, "unknown type '" + std::string{type.name.text} + "'");
                    } else {
                        result_.type_bindings[id] = *binding;
                        if (binding->kind == BindingKind::Generic) {
                            if (!type.arguments.empty()) {
                                report(Category::Type, type.range, "a type parameter cannot be applied as a generic struct");
                            }
                        } else {
                            const auto &parameters = generics_of(binding->decl);
                            if (parameters.size() != type.arguments.size()) {
                                report(Category::Type, type.range,
                                       "generic struct '" + std::string{type.name.text} + "' expects " +
                                           std::to_string(parameters.size()) + " arguments, got " +
                                           std::to_string(type.arguments.size()));
                            }
                            const std::size_t count = std::min(parameters.size(), type.arguments.size());
                            for (std::size_t i = 0; i < count; ++i) {
                                if (std::optional<Binding> named =
                                        resolve_generic_argument(type.arguments[i], parameters[i], context)) {
                                    std::vector<Binding> &slots = argument_bindings_[id];
                                    slots.resize(type.arguments.size());
                                    slots[i] = std::move(*named);
                                }
                            }
                            for (std::size_t i = count; i < type.arguments.size(); ++i) {
                                const ast::GenericArgument &argument = type.arguments[i];
                                if (argument.type != ast::no_node) {
                                    resolve_type(argument.type, context);
                                } else if (argument.value != ast::no_node) {
                                    resolve_expr(argument.value, context);
                                }
                            }
                        }
                    }
                    }
                }
                for (const ast::TypeId child : type.children) { resolve_type(child, context); }
                if (type.size != ast::no_node) { resolve_expr(type.size, context); }
                if (type.min_size != ast::no_node) { resolve_expr(type.min_size, context); }
            }

            void resolve_constraint(ast::ConstraintId id, Context &context) {
                if (id == ast::no_node) { return; }
                const ast::Constraint &constraint = module_.constraint(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::ConstraintName>) {
                            const std::optional<Binding> binding = lookup(node.name.text);
                            // A struct another module exports names a type in
                            // a constraint exactly as a local one does
                            // (ADR 0013) -- `U == Quote`, `fields(Quote)`.
                            if (!binding ||
                                (binding->kind != BindingKind::Generic && binding->kind != BindingKind::ConstraintLocal &&
                                 binding->kind != BindingKind::Parameter && binding->kind != BindingKind::Struct &&
                                 binding->kind != BindingKind::ImportedStruct)) {
                                report(Category::Name, node.name.range,
                                       "unknown constraint name '" + std::string{node.name.text} + "'");
                            } else {
                                result_.constraint_bindings[id] = *binding;
                            }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintType>) {
                            resolve_type(node.type, context);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintValue>) {
                            resolve_expr(node.value, context);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintSet>) {
                            for (const ast::ConstraintId element : node.elements) { resolve_constraint(element, context); }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintCall>) {
                            if (!node.qualifier.empty() ||
                                (node.name.text != "fields" && node.name.text != "has_fields" && node.name.text != "field_type" &&
                                 node.name.text != "schema" && node.name.text != "keys" && node.name.text != "len" &&
                                 node.name.text != "types" && node.name.text != "type_at")) {
                                report(Category::Type, constraint.range,
                                       "'" + std::string{node.name.text} + "' is not a compile-time reflection function");
                            } else {
                                Binding binding;
                                binding.kind                    = BindingKind::Intrinsic;
                                binding.registry_name           = std::string{node.name.text};
                                result_.constraint_bindings[id] = std::move(binding);
                            }
                            for (const ast::ConstraintId argument : node.arguments) { resolve_constraint(argument, context); }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintEach>) {
                            resolve_constraint(node.source, context);
                            Binding binding;
                            binding.kind                    = BindingKind::ConstraintLocal;
                            binding.decl                    = context.fn;
                            binding.constraint              = id;
                            result_.constraint_bindings[id] = binding;
                            push_scope();
                            declare(node.binding, binding, "in the each constraint");
                            resolve_constraint(node.body, context);
                            pop_scope();
                        } else if constexpr (std::is_same_v<T, ast::OperatorRequirement>) {
                            Binding binding;
                            if (node.qualifier.empty()) {
                                const std::optional<Binding> found = lookup(node.name.text);
                                if (found && (found->kind == BindingKind::Operator || found->kind == BindingKind::LocalOperator)) {
                                    binding = *found;
                                } else {
                                    report(Category::Name, node.name.range,
                                           "operator requirement names no operator '" + std::string{node.name.text} + "'");
                                }
                            } else {
                                bool found_alias = false;
                                for (const ModuleAlias &alias : result_.aliases) {
                                    if (alias.alias != node.qualifier.text) { continue; }
                                    found_alias = true;
                                    if (const auto *contract = catalog_.find_operator(alias.module, node.name.text)) {
                                        if (const auto imported = imported_operator(*contract, node.name.range)) {
                                            binding = *imported;
                                        }
                                        break;
                                    }
                                    if (alias.module != kernel_std && alias.module != kernel_analytics) {
                                        report(Category::Module, node.name.range,
                                               alias.module + " does not export '" + std::string{node.name.text} + "'");
                                        break;
                                    }
                                    const std::optional<std::string> name = kernel_registry_name(alias.module, node.name.text);
                                    if (!name) {
                                        report(Category::Module, node.name.range,
                                               alias.module + " does not export '" + std::string{node.name.text} + "'");
                                    } else {
                                        binding.kind              = BindingKind::Operator;
                                        binding.registry_name     = *name;
                                        binding.operator_identity = alias.module + "." + std::string{node.name.text};
                                    }
                                    break;
                                }
                                if (!found_alias) {
                                    report(Category::Name, node.qualifier.range,
                                           "unknown module alias '" + std::string{node.qualifier.text} + "'");
                                }
                            }
                            result_.constraint_bindings[id] = std::move(binding);
                            for (const ast::ConstraintId argument : node.arguments) { resolve_constraint(argument, context); }
                            if (node.result != ast::no_node) { resolve_type(node.result, context); }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintRelation>) {
                            resolve_constraint(node.lhs, context);
                            if (node.op == ast::ConstraintRelationOp::Is) {
                                if (node.category.text != "struct") {
                                    report(Category::Type, node.category.range,
                                           "unknown type category '" + std::string{node.category.text} + "'");
                                }
                            } else {
                                resolve_constraint(node.rhs, context);
                            }
                        } else if constexpr (std::is_same_v<T, ast::ConstraintNot>) {
                            resolve_constraint(node.operand, context);
                        } else if constexpr (std::is_same_v<T, ast::ConstraintLogic>) {
                            resolve_constraint(node.lhs, context);
                            resolve_constraint(node.rhs, context);
                        }
                    },
                    constraint.node);
            }

            // ---------------------------------------------------- structures

            [[nodiscard]] bool is_null(ast::ExprId id) const noexcept {
                return id != ast::no_node && std::holds_alternative<ast::NullLiteral>(module_.expr(id).node);
            }

            bool validate_struct(ast::DeclId id) {
                if (struct_states_[id] == 2) { return result_.struct_info[id].valid; }
                const auto &structure = std::get<ast::StructDecl>(module_.decl(id).node);
                if (struct_states_[id] == 1) {
                    report(Category::Type, structure.name.range,
                           "struct inheritance cycle reaches '" + std::string{structure.name.text} + "'");
                    return false;
                }
                struct_states_[id] = 1;
                bool        valid  = true;
                StructInfo &info   = result_.struct_info[id];

                if (structure.parents.size() > 1) {
                    report(Category::Type, structure.name.range,
                           "multiple abstract parents are parsed, but lowering awaits the "
                           "stable field-order rule");
                    valid = false;
                }
                for (const ast::TypeId parent_type : structure.parents) {
                    const Binding &binding = result_.type_bindings[parent_type];
                    // A struct another module exports may be inherited (ADR
                    // 0013). It is referenced, never absorbed: this module
                    // gains no declaration for it, and each field it declares
                    // keeps that struct as its source, so a diagnostic names
                    // the module the field really comes from.
                    if (binding.kind == BindingKind::ImportedStruct) {
                        // By value: seeding may bind ancestors, which appends to
                        // this very vector and would invalidate a reference.
                        const ImportedStruct parent = result_.imported_structs[binding.index];
                        if (!parent.abstract) {
                            report(Category::Type, module_.type(parent_type).range,
                                   "only an abstract struct may be inherited; '" + parent.identity +
                                       "' is concrete and implicitly final");
                            valid = false;
                            continue;
                        }
                        const StructSource source{.imported = binding.index};
                        info.parents.push_back(source);
                        if (structure.parents.size() == 1 &&
                            !seed_imported_fields(id, info, parent, source, module_.type(parent_type).range)) {
                            report(Category::Module, module_.type(parent_type).range,
                                   "imported struct '" + parent.identity +
                                       "' inherits a struct whose module is not in the supplied package target, so its "
                                       "layout cannot be rebuilt");
                            valid = false;
                        }
                        continue;
                    }
                    if (binding.kind != BindingKind::Struct) {
                        valid = false;
                        continue;
                    }
                    const auto &parent = std::get<ast::StructDecl>(module_.decl(binding.decl).node);
                    if (!parent.abstract) {
                        report(Category::Type, module_.type(parent_type).range,
                               "only an abstract struct may be inherited; '" + std::string{parent.name.text} +
                                   "' is concrete and implicitly final");
                        valid = false;
                        continue;
                    }
                    if (!validate_struct(binding.decl)) { valid = false; }
                    info.parents.push_back(StructSource{.decl = binding.decl});
                    if (structure.parents.size() == 1) {
                        info.fields        = result_.struct_info[binding.decl].fields;
                        field_indices_[id] = field_indices_[binding.decl];
                    }
                }

                auto &field_index = field_indices_[id];
                std::unordered_set<std::string_view>               local_names;
                std::unordered_set<std::string_view>               overridden;
                for (const ast::StructMember &member : structure.members) {
                    std::visit(
                        [&](const auto &item) {
                            using T = std::decay_t<decltype(item)>;
                            if constexpr (std::is_same_v<T, ast::StructField>) {
                                if (!local_names.insert(item.name.text).second) {
                                    report(Category::Name, item.name.range,
                                           "struct field '" + std::string{item.name.text} + "' is declared twice");
                                    valid = false;
                                    return;
                                }
                                if (field_index.contains(item.name.text)) {
                                    report(Category::Type, item.name.range,
                                           "inherited field '" + std::string{item.name.text} +
                                               "' cannot be redeclared with a type; override only "
                                               "its default");
                                    valid = false;
                                    return;
                                }
                                field_index.emplace(item.name.text, info.fields.size());
                                info.fields.push_back(StructField{std::string{item.name.text}, item.type, item.default_value,
                                                                  StructSource{.decl = id},
                                                                  is_null(item.default_value)});
                            } else {
                                if (!overridden.insert(item.name.text).second) {
                                    report(Category::Name, item.name.range,
                                           "inherited default '" + std::string{item.name.text} + "' is set twice");
                                    valid = false;
                                    return;
                                }
                                const auto found = field_index.find(item.name.text);
                                if (found == field_index.end()) {
                                    report(Category::Type, item.name.range,
                                           "default override names no inherited field '" + std::string{item.name.text} + "'");
                                    valid = false;
                                    return;
                                }
                                StructField &inherited = info.fields[found->second];
                                if (is_null(item.value) && !inherited.optional) {
                                    report(Category::Type, module_.expr(item.value).range,
                                           "only an optional inherited field may have a null default");
                                    valid = false;
                                    return;
                                }
                                inherited.default_value = item.value;
                            }
                        },
                        member);
                }
                info.valid         = valid;
                struct_states_[id] = 2;
                return valid;
            }

            void validate_structs() {
                struct_states_.assign(module_.decls.size(), 0);
                field_indices_.assign(module_.decls.size(), {});
                for (const ast::DeclId id : result_.structs) { (void)validate_struct(id); }
                check_recursive_fields();
                check_export_closure();
            }

            /// An importer rebuilds an exported struct from its layout, so every
            /// struct that layout reaches has to be exported too (ADR 0013,
            /// "Exports are closed under reachability"). A field reaches each
            /// struct its type names, through collection elements, generic
            /// arguments and an `atomic` edge; a parent is reached by
            /// inheritance. The rule runs from exported roots only, so an
            /// unexported leaf or chain may reference other internal structs.
            /// The check is on the exporting module, so the error lands on
            /// whoever broke the contract rather than on a consumer.
            void check_export_closure() {
                const auto exported = [&](ast::DeclId id) {
                    return std::get<ast::StructDecl>(module_.decl(id).node).exported;
                };
                std::vector<StructReference> references;
                for (const ast::DeclId owner : result_.structs) {
                    if (!exported(owner)) { continue; }
                    const auto       &structure = std::get<ast::StructDecl>(module_.decl(owner).node);
                    const StructInfo &info      = result_.struct_info[owner];
                    for (const ast::TypeId parent : structure.parents) {
                        // The actual parent edge, never the family expansion
                        // struct_references performs: two local structs may
                        // inherit one imported family without either reaching
                        // the other by inheritance, and an imported parent is
                        // by definition exported by its own module.
                        const Binding &binding = result_.type_bindings[parent];
                        if (binding.kind != BindingKind::Struct || exported(binding.decl)) { continue; }
                        report(Category::Type, module_.type(parent).range,
                               "exported struct '" + struct_name(owner) + "' inherits module-internal struct '" +
                                   struct_name(binding.decl) + "'; everything an exported struct reaches must be " +
                                   "exported (ADR 0013)");
                    }
                    for (const StructField &field : info.fields) {
                        // Inherited fields are reported against the struct that
                        // declares them, so each is named once.
                        if (field.origin != StructSource{.decl = owner} || field.type == ast::no_node) { continue; }
                        references.clear();
                        field_references(field.type, references);
                        for (const StructReference &reference : references) {
                            if (exported(reference.decl)) { continue; }
                            report(Category::Type, module_.type(field.type).range,
                                   "exported struct '" + struct_name(owner) + "' reaches module-internal struct '" +
                                       struct_name(reference.decl) + "' through field '" + field.name +
                                       "'; everything an exported struct reaches must be exported (ADR 0013)");
                        }
                    }
                }
            }

            // ------------------------------------------- recursive struct fields

            static constexpr std::uint32_t no_position = std::numeric_limits<std::uint32_t>::max();

            /// How a field type reaches a struct it names: as the field's own type
            /// (the `T` of `atomic<T>`, or a bare `T`), through a `ref`, or nested
            /// in a collection element or a generic argument.
            enum class Reach : std::uint8_t {
                Direct,
                Reference,
                Container,
                Argument,
            };

            /// A struct named in a field type, the applied type naming it (no_node
            /// for a bare generic argument, which applies nothing), and its reach.
            struct StructReference
            {
                ast::DeclId decl{ast::no_node};
                ast::TypeId applied{ast::no_node};
                Reach       reach{Reach::Direct};
            };

            [[nodiscard]] const Binding *argument_binding(ast::TypeId id, std::size_t index) const noexcept {
                const std::vector<Binding> &slots = argument_bindings_[id];
                return index < slots.size() && slots[index].kind != BindingKind::Unbound ? &slots[index] : nullptr;
            }

            /// Every struct of this module that inherits the imported struct at
            /// `index`, directly or through another local struct. Naming an
            /// imported family reaches them: a value of that family can be any
            /// of its members, and the members declared here are this module's.
            [[nodiscard]] std::vector<ast::DeclId> local_members_of(std::uint32_t index) const {
                std::vector<ast::DeclId> members;
                bool                     grew = true;
                while (grew) {
                    grew = false;
                    for (const ast::DeclId decl : result_.structs) {
                        if (std::ranges::find(members, decl) != members.end()) { continue; }
                        for (const StructSource &parent : result_.struct_info[decl].parents) {
                            const bool inherits = parent.is_imported()
                                                      ? parent.imported == index
                                                      : std::ranges::find(members, parent.decl) != members.end();
                            if (inherits) {
                                members.push_back(decl);
                                grew = true;
                                break;
                            }
                        }
                    }
                }
                return members;
            }

            void struct_references(ast::TypeId id, Reach reach, std::vector<StructReference> &out) const {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Named && result_.type_bindings[id].kind == BindingKind::Struct) {
                    out.push_back(StructReference{result_.type_bindings[id].decl, id, reach});
                }
                // A local struct that inherits an imported abstract family is a
                // MEMBER of it (ADR 0013), so a field typed by that family can
                // hold this module's struct and the cycle is local -- it never
                // crosses a module, which is what rule 5 actually says. Naming
                // the family therefore reaches every local member of it.
                if (type.kind == ast::TypeKind::Named && result_.type_bindings[id].kind == BindingKind::ImportedStruct) {
                    for (const ast::DeclId member : local_members_of(result_.type_bindings[id].index)) {
                        out.push_back(StructReference{member, id, reach});
                    }
                }
                const Reach nested = reach == Reach::Direct && type.kind == ast::TypeKind::Reference ? Reach::Reference
                                     : reach == Reach::Direct                                        ? Reach::Container
                                                                                                     : reach;
                for (const ast::TypeId child : type.children) { struct_references(child, nested, out); }
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    if (type.arguments[index].type != ast::no_node) {
                        struct_references(type.arguments[index].type, Reach::Argument, out);
                    } else if (const Binding *binding = argument_binding(id, index);
                               binding != nullptr && binding->kind == BindingKind::Struct) {
                        out.push_back(StructReference{binding->decl, ast::no_node, Reach::Argument});
                    }
                }
            }

            /// The struct references of a field type. An outer `atomic<...>` is the
            /// declared boundary; the type inside it is the field's direct type.
            void field_references(ast::TypeId id, std::vector<StructReference> &out) const {
                const ast::Type &type = module_.type(id);
                struct_references(type.kind == ast::TypeKind::Atomic && type.children.size() == 1U ? type.children.front() : id,
                                  Reach::Direct, out);
            }

            [[nodiscard]] static std::string struct_key(ast::DeclId decl, std::string_view arguments) {
                return std::to_string(static_cast<unsigned>(ast::TypeKind::Named)) + ':' + std::to_string(decl) +
                       std::string{arguments};
            }

            /// A structural key for a concrete type, or nothing when the type
            /// mentions a generic parameter, a constant or an unresolved name
            /// and so may denote more than one specialization.
            [[nodiscard]] std::optional<std::string> type_key(ast::TypeId id) const {
                const ast::Type &type = module_.type(id);
                if (type.size != ast::no_node || type.min_size != ast::no_node) { return std::nullopt; }
                if (type.kind == ast::TypeKind::Named) {
                    const std::optional<std::string> arguments = arguments_key(id);
                    if (result_.type_bindings[id].kind != BindingKind::Struct || !arguments) { return std::nullopt; }
                    return struct_key(result_.type_bindings[id].decl, *arguments);
                }
                std::string key = std::to_string(static_cast<unsigned>(type.kind));
                if (type.kind == ast::TypeKind::Scalar) { key += ':' + std::to_string(static_cast<unsigned>(type.scalar)); }
                key += type.unbounded ? "*(" : "(";
                for (const ast::TypeId child : type.children) {
                    const std::optional<std::string> element = type_key(child);
                    if (!element) { return std::nullopt; }
                    key += *element + ',';
                }
                return key + ')';
            }

            /// The key of argument `index` of `id`, or nothing when it may denote
            /// more than one type.
            [[nodiscard]] std::optional<std::string> argument_key(ast::TypeId id, std::size_t index) const {
                const ast::GenericArgument &argument = module_.type(id).arguments[index];
                if (argument.type != ast::no_node) { return type_key(argument.type); }
                if (const Binding *binding = argument_binding(id, index);
                    binding != nullptr && binding->kind == BindingKind::Struct) {
                    return struct_key(binding->decl, "<>");
                }
                return std::nullopt;
            }

            /// The key of each of `id`'s arguments, or nothing when one may
            /// denote more than one type.
            [[nodiscard]] std::optional<std::vector<std::string>> argument_keys(ast::TypeId id) const {
                const std::size_t        count = module_.type(id).arguments.size();
                std::vector<std::string> keys;
                keys.reserve(count);
                for (std::size_t index = 0; index < count; ++index) {
                    std::optional<std::string> key = argument_key(id, index);
                    if (!key) { return std::nullopt; }
                    keys.push_back(std::move(*key));
                }
                return keys;
            }

            [[nodiscard]] static std::string joined_key(const std::vector<std::string> &keys) {
                std::string key{"<"};
                for (const std::string &element : keys) { key += element + ','; }
                return key + '>';
            }

            [[nodiscard]] std::optional<std::string> arguments_key(ast::TypeId id) const {
                const std::optional<std::vector<std::string>> keys = argument_keys(id);
                if (!keys) { return std::nullopt; }
                return joined_key(*keys);
            }

            /// When `child`'s parent application `applied` is exactly `child`'s
            /// generic parameters, each once and in any order, the parameter at
            /// each argument: every specialization of `child` is then the parent
            /// at the same arguments, permuted. Otherwise nothing.
            [[nodiscard]] std::optional<std::vector<std::uint32_t>> parameter_permutation(ast::TypeId applied,
                                                                                          ast::DeclId child) const {
                const ast::Type  &type       = module_.type(applied);
                const std::size_t parameters = std::get<ast::StructDecl>(module_.decl(child).node).generics.size();
                if (type.arguments.size() != parameters) { return std::nullopt; }
                std::vector<std::uint32_t> result;
                std::vector<bool>          seen(parameters, false);
                result.reserve(parameters);
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    const ast::GenericArgument &argument = type.arguments[index];
                    const Binding              *binding  = argument_binding(applied, index);
                    if (argument.type != ast::no_node) {
                        const ast::Type &named = module_.type(argument.type);
                        if (named.kind != ast::TypeKind::Named || !named.arguments.empty()) { return std::nullopt; }
                        binding = &result_.type_bindings[argument.type];
                    }
                    if (binding == nullptr || binding->kind != BindingKind::Generic || binding->decl != child ||
                        binding->index >= parameters || seen[binding->index]) {
                        return std::nullopt;
                    }
                    seen[binding->index] = true;
                    result.push_back(binding->index);
                }
                return result;
            }

            [[nodiscard]] bool literal(ast::ExprId id) const {
                const ast::ExprNode &node = module_.expr(id).node;
                return std::holds_alternative<ast::IntLiteral>(node) || std::holds_alternative<ast::FloatLiteral>(node) ||
                       std::holds_alternative<ast::StringLiteral>(node) || std::holds_alternative<ast::BoolLiteral>(node) ||
                       std::holds_alternative<ast::TemporalLiteral>(node);
            }

            /// Whether a constant expression can mention a generic parameter. A
            /// literal cannot; a name does when it binds to one; an operator does
            /// when an operand does. Any other form is taken to mention one,
            /// which only ever keeps the older, stricter answer (ADR 0012,
            /// rule 4) rather than admitting a cycle it should not.
            [[nodiscard]] bool expression_mentions_parameter(ast::ExprId id) const {
                if (id == ast::no_node) { return false; }
                const ast::ExprNode &node = module_.expr(id).node;
                if (literal(id) || std::holds_alternative<ast::NullLiteral>(node)) { return false; }
                if (std::holds_alternative<ast::NameRef>(node) || std::holds_alternative<ast::QualifiedRef>(node)) {
                    return result_.binding(id).kind == BindingKind::Generic;
                }
                if (const auto *unary = std::get_if<ast::Unary>(&node)) {
                    return expression_mentions_parameter(unary->operand);
                }
                if (const auto *binary = std::get_if<ast::Binary>(&node)) {
                    return expression_mentions_parameter(binary->lhs) || expression_mentions_parameter(binary->rhs);
                }
                return true;
            }

            /// Whether a type may mention a generic parameter: a parameter by
            /// name, or a size whose expression mentions one.
            [[nodiscard]] bool mentions_parameter(ast::TypeId id) const {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Named && result_.type_bindings[id].kind != BindingKind::Struct) { return true; }
                if (expression_mentions_parameter(type.size) || expression_mentions_parameter(type.min_size)) {
                    return true;
                }
                for (const ast::TypeId child : type.children) {
                    if (mentions_parameter(child)) { return true; }
                }
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    if (argument_mentions_parameter(id, index)) { return true; }
                }
                return false;
            }

            [[nodiscard]] bool argument_mentions_parameter(ast::TypeId applied, std::size_t index) const {
                const ast::GenericArgument &argument = module_.type(applied).arguments[index];
                if (argument.type != ast::no_node) { return mentions_parameter(argument.type); }
                if (argument.value != ast::no_node) { return expression_mentions_parameter(argument.value); }
                const Binding *binding = argument_binding(applied, index);
                return binding == nullptr || binding->kind != BindingKind::Struct;
            }

            /// Whether argument `index` of `applied` is exactly a generic parameter
            /// of `decl`.
            [[nodiscard]] bool parameter_argument(ast::TypeId applied, std::size_t index, ast::DeclId decl) const {
                const ast::GenericArgument &argument = module_.type(applied).arguments[index];
                const Binding              *binding  = argument_binding(applied, index);
                if (argument.type != ast::no_node) {
                    if (module_.type(argument.type).kind != ast::TypeKind::Named ||
                        !module_.type(argument.type).arguments.empty()) {
                        return false;
                    }
                    binding = &result_.type_bindings[argument.type];
                }
                return binding != nullptr && binding->kind == BindingKind::Generic && binding->decl == decl;
            }

            /// A struct another module exports is named by its identity; it
            /// has no declaration here to take a name from (ADR 0013).
            [[nodiscard]] std::string struct_name(const StructSource &source) const {
                if (source.is_imported()) { return result_.imported_structs[source.imported].identity; }
                return struct_name(source.decl);
            }

            [[nodiscard]] std::string struct_name(ast::DeclId decl) const {
                return std::string{std::get<ast::StructDecl>(module_.decl(decl).node).name.text};
            }

            /// Finds every recursive edge of the module's structs and admits it
            /// only under ADR 0012 (syntax guide, "Compilation-unit grammar").
            ///
            /// A value of a struct contains another value of the same struct
            /// through a field whose type reaches a struct of its own strongly
            /// connected component. A field reaches each struct its type names,
            /// through collection elements and generic arguments. A field typed by
            /// a struct with descendants also reaches that family: every
            /// descendant of a non-generic struct, and of a generic one only the
            /// descendants that can be the field's specialization. Such a
            /// field is a recursive edge; it is admitted when it is an optional
            /// `atomic<T>` whose cycle runs through `T` itself (rules 2, 3 and 8)
            /// and whose generic arguments are parameters of the declaring struct
            /// or mention none (rule 4). A cycle that also runs through a parent is
            /// rejected: hgraph declares a parent before its children, so it
            /// cannot register one. Every step is linear in the declared fields,
            /// times the depth of generic inheritance for the specialized groups.
            void check_recursive_fields() {
                const std::size_t          count = result_.structs.size();
                std::vector<std::uint32_t> position(module_.decls.size(), no_position);
                for (std::size_t index = 0; index < count; ++index) {
                    position[result_.structs[index]] = static_cast<std::uint32_t>(index);
                }
                // Node 2i is a value of struct i; node 2i+1 is a value of struct i
                // or of any of its descendants. Family group nodes follow them.
                std::vector<std::vector<std::uint32_t>> edges(2 * count);
                const auto                              value_node  = [](std::uint32_t index) { return 2 * index; };
                const auto                              family_node = [](std::uint32_t index) { return 2 * index + 1; };
                const auto                              add_node    = [&] {
                    edges.emplace_back();
                    return static_cast<std::uint32_t>(edges.size() - 1);
                };

                struct ExactGroup
                {
                    std::vector<std::string> keys{};  ///< the parent application's argument keys
                    std::uint32_t            node{no_position};
                };
                struct Family
                {
                    std::uint32_t                               any{no_position};   ///< every child group
                    std::uint32_t                               open{no_position};  ///< non-concrete parent applications
                    std::unordered_map<std::string, ExactGroup> exact{};            ///< by concrete parent application
                };
                // A child whose parent application is exactly its own parameters
                // is that parent at each of its specializations, arguments
                // permuted; any other open child is kept whole.
                struct Permuted
                {
                    std::uint32_t              child{};
                    std::vector<std::uint32_t> parameters{};  ///< the child's parameter at each parent argument
                };
                std::vector<Family>                     families(count);
                std::vector<std::vector<Permuted>>      permuted(count);
                std::vector<std::vector<std::uint32_t>> unmatched(count);
                for (std::uint32_t child = 0; child < count; ++child) {
                    edges[family_node(child)].push_back(value_node(child));
                    const auto &structure = std::get<ast::StructDecl>(module_.decl(result_.structs[child]).node);
                    for (const ast::TypeId parent_type : structure.parents) {
                        const Binding &binding = result_.type_bindings[parent_type];
                        if (binding.kind != BindingKind::Struct || position[binding.decl] == no_position) { continue; }
                        const std::uint32_t parent = position[binding.decl];
                        Family             &family = families[parent];
                        if (family.any == no_position) { family.any = add_node(); }
                        std::uint32_t *group = &family.open;
                        if (std::optional<std::vector<std::string>> keys = argument_keys(parent_type)) {
                            std::string joined = joined_key(*keys);
                            group = &family.exact.try_emplace(std::move(joined), ExactGroup{std::move(*keys)}).first->second.node;
                        } else if (std::optional<std::vector<std::uint32_t>> parameters =
                                       parameter_permutation(parent_type, result_.structs[child])) {
                            permuted[parent].push_back(Permuted{child, std::move(*parameters)});
                        } else {
                            unmatched[parent].push_back(child);
                        }
                        if (*group == no_position) {
                            *group = add_node();
                            edges[family.any].push_back(*group);
                        }
                        edges[*group].push_back(family_node(child));
                    }
                }
                for (std::uint32_t index = 0; index < count; ++index) {
                    if (families[index].any != no_position) { edges[family_node(index)].push_back(families[index].any); }
                }

                // A value of struct i at a specialization nothing below it fixes:
                // struct i and its permuted children at the same freedom, and,
                // over-approximating, every other open child's whole family.
                std::vector<std::uint32_t> free(count);
                for (std::uint32_t index = 0; index < count; ++index) { free[index] = add_node(); }
                for (std::uint32_t index = 0; index < count; ++index) {
                    edges[free[index]].push_back(value_node(index));
                    for (const Permuted &link : permuted[index]) { edges[free[index]].push_back(free[link.child]); }
                    for (const std::uint32_t child : unmatched[index]) { edges[free[index]].push_back(family_node(child)); }
                }

                // The values of struct i's family that exist only at one of its
                // specializations: the children whose parent application is that
                // specialization, and the same below each permuted child, keyed by
                // struct i's argument keys. A permuted child's groups are complete
                // before its parents', so each is built once.
                struct Specialized
                {
                    std::vector<std::string> keys{};
                    std::uint32_t            node{};
                };
                std::vector<std::unordered_map<std::string, Specialized>> specialized(count);
                const auto specialized_node = [&](std::uint32_t index, std::vector<std::string> keys) {
                    std::string joined     = joined_key(keys);
                    auto [found, inserted] = specialized[index].try_emplace(std::move(joined));
                    if (inserted) {
                        found->second.keys = std::move(keys);
                        found->second.node = add_node();
                    }
                    return found->second.node;
                };
                std::vector<std::uint32_t> children_first;
                {
                    std::vector<std::uint8_t>                          state(count, 0);  // 0 new, 1 open, 2 done
                    std::vector<std::pair<std::uint32_t, std::size_t>> stack;
                    for (std::uint32_t root = 0; root < count; ++root) {
                        if (state[root] != 0) { continue; }
                        state[root] = 1;
                        stack.emplace_back(root, 0);
                        while (!stack.empty()) {
                            const std::uint32_t index = stack.back().first;
                            if (stack.back().second < permuted[index].size()) {
                                const std::uint32_t child = permuted[index][stack.back().second++].child;
                                if (state[child] == 0) {
                                    state[child] = 1;
                                    stack.emplace_back(child, 0);
                                }
                                continue;
                            }
                            state[index] = 2;
                            children_first.push_back(index);
                            stack.pop_back();
                        }
                    }
                }
                for (const std::uint32_t index : children_first) {
                    for (const auto &[joined, group] : families[index].exact) {
                        const std::uint32_t node = specialized_node(index, group.keys);
                        edges[node].push_back(group.node);
                    }
                    for (const Permuted &link : permuted[index]) {
                        if (link.child == index) { continue; }
                        for (const auto &[joined, entry] : specialized[link.child]) {
                            std::vector<std::string> keys;
                            keys.reserve(link.parameters.size());
                            for (const std::uint32_t parameter : link.parameters) { keys.push_back(entry.keys[parameter]); }
                            const std::uint32_t node = specialized_node(index, std::move(keys));
                            edges[node].push_back(entry.node);
                        }
                    }
                }

                // Every node an effective field reaches, with the reach and the
                // struct reference that produced it; fields index into `targets`.
                struct Target
                {
                    std::uint32_t   node{};
                    StructReference reference{};
                };
                struct FieldTargets
                {
                    std::uint32_t owner{};
                    std::size_t   field{};
                    std::size_t   begin{};
                    std::size_t   end{};
                };
                std::vector<Target>          targets;
                std::vector<FieldTargets>    field_targets;
                std::vector<StructReference> references;
                for (std::uint32_t owner = 0; owner < count; ++owner) {
                    const StructInfo &info = result_.struct_info[result_.structs[owner]];
                    for (std::size_t field = 0; field < info.fields.size(); ++field) {
                        if (info.fields[field].type == ast::no_node) { continue; }
                        const std::size_t begin = targets.size();
                        references.clear();
                        field_references(info.fields[field].type, references);
                        for (const StructReference &reference : references) {
                            const std::uint32_t target = position[reference.decl];
                            if (target == no_position) { continue; }
                            targets.push_back(Target{value_node(target), reference});
                            const Family &family = families[target];
                            if (family.any == no_position) { continue; }
                            const std::optional<std::string> key = reference.applied == ast::no_node
                                                                       ? std::optional<std::string>{"<>"}
                                                                       : arguments_key(reference.applied);
                            if (!key) {
                                targets.push_back(Target{family.any, reference});
                                continue;
                            }
                            targets.push_back(Target{free[target], reference});
                            if (const auto found = specialized[target].find(*key); found != specialized[target].end()) {
                                targets.push_back(Target{found->second.node, reference});
                            }
                        }
                        for (std::size_t index = begin; index < targets.size(); ++index) {
                            edges[value_node(owner)].push_back(targets[index].node);
                        }
                        field_targets.push_back(FieldTargets{owner, field, begin, targets.size()});
                    }
                }
                const std::vector<std::uint32_t> component = strongly_connected_components(edges);

                // Registration order: a struct needs its parents and every struct
                // its own fields name. hgraph declares a recursive batch whose
                // members reach each other only through owned edges, and requires
                // each member's parents to exist first, so a registration cycle
                // through a parent cannot be declared.
                std::vector<std::vector<std::uint32_t>> registration(count);
                for (std::uint32_t index = 0; index < count; ++index) {
                    for (const StructSource &parent : result_.struct_info[result_.structs[index]].parents) {
                        // A cycle never crosses a module (ADR 0012 rule 5), so an
                        // imported parent is no part of this module's analysis.
                        if (parent.is_imported()) { continue; }
                        if (position[parent.decl] != no_position) { registration[index].push_back(position[parent.decl]); }
                    }
                }
                for (const FieldTargets &item : field_targets) {
                    const StructInfo &info = result_.struct_info[result_.structs[item.owner]];
                    if (info.fields[item.field].origin != StructSource{.decl = result_.structs[item.owner]}) { continue; }
                    for (std::size_t index = item.begin; index < item.end; ++index) {
                        const std::uint32_t target = position[targets[index].reference.decl];
                        if (registration[item.owner].empty() || registration[item.owner].back() != target) {
                            registration[item.owner].push_back(target);
                        }
                    }
                }
                const std::vector<std::uint32_t> registered = strongly_connected_components(registration);
                // A parent edge inside a registration component, by component.
                std::unordered_map<std::uint32_t, std::pair<std::uint32_t, std::uint32_t>> inheritance_cycles;
                for (std::uint32_t index = 0; index < count; ++index) {
                    for (const StructSource &parent : result_.struct_info[result_.structs[index]].parents) {
                        if (parent.is_imported()) { continue; }
                        if (position[parent.decl] != no_position && registered[position[parent.decl]] == registered[index]) {
                            inheritance_cycles.try_emplace(registered[index], index, position[parent.decl]);
                        }
                    }
                }

                // One verdict per field declaration, keyed by its type node.
                enum class Verdict : std::uint8_t {
                    Unchecked,
                    Admitted,
                    Rejected,
                };
                std::vector<Verdict> verdicts(module_.types.size(), Verdict::Unchecked);
                std::vector<bool>    recursive(field_targets.size(), false);
                // A descendant may replace an inherited default, so rule 2's null
                // default is checked on every struct that has the edge.
                std::vector<bool> replaced(field_targets.size(), false);
                std::vector<bool> reported_defaults(module_.exprs.size(), false);
                for (std::size_t item_index = 0; item_index < field_targets.size(); ++item_index) {
                    const FieldTargets            &item  = field_targets[item_index];
                    const std::uint32_t            self  = component[value_node(item.owner)];
                    const ast::DeclId              owner = result_.structs[item.owner];
                    const StructField             &field = result_.struct_info[owner].fields[item.field];
                    std::optional<StructReference> direct;
                    std::optional<StructReference> nested;
                    for (std::size_t index = item.begin; index < item.end; ++index) {
                        if (component[targets[index].node] != self) { continue; }
                        const StructReference &reference = targets[index].reference;
                        if (reference.reach == Reach::Direct) {
                            direct = reference;
                        } else if (!nested) {
                            nested = reference;
                        }
                    }
                    if (!direct && !nested) { continue; }
                    recursive[item_index] = true;
                    if (field.optional && field.default_value != ast::no_node && !is_null(field.default_value)) {
                        replaced[item_index] = true;
                        if (!reported_defaults[field.default_value]) {
                            reported_defaults[field.default_value] = true;
                            report(Category::Type, module_.expr(field.default_value).range,
                                   "recursive edge '" + field.name + "' of '" + struct_name(field.origin) +
                                       "' keeps a null default (ADR 0012, rule 2)");
                        }
                    }
                    if (verdicts[field.type] != Verdict::Unchecked) { continue; }
                    verdicts[field.type] =
                        check_recursive_edge(field, owner, direct, nested) ? Verdict::Admitted : Verdict::Rejected;
                    if (verdicts[field.type] == Verdict::Rejected) { continue; }
                    // The edge obeys the language rules; it must also be registrable.
                    // Registration follows the declaring struct's own fields.
                    const std::uint32_t declared = registered[position[field.origin.decl]];
                    const auto          cycle    = inheritance_cycles.find(declared);
                    if (cycle == inheritance_cycles.end() ||
                        std::none_of(targets.begin() + static_cast<std::ptrdiff_t>(item.begin),
                                     targets.begin() + static_cast<std::ptrdiff_t>(item.end), [&](const Target &target) {
                                         return registered[position[target.reference.decl]] == declared;
                                     })) {
                        continue;
                    }
                    verdicts[field.type] = Verdict::Rejected;
                    report(Category::Type, module_.type(field.type).range,
                           "recursive edge '" + field.name + "' of '" + struct_name(field.origin) +
                               "' closes a cycle through inheritance: '" + struct_name(result_.structs[cycle->second.first]) +
                               "' inherits from '" + struct_name(result_.structs[cycle->second.second]) +
                               "', and hgraph declares a parent before its children, so the cycle cannot be registered");
                }
                for (std::size_t item_index = 0; item_index < field_targets.size(); ++item_index) {
                    if (!recursive[item_index]) { continue; }
                    const FieldTargets &item  = field_targets[item_index];
                    StructInfo         &info  = result_.struct_info[result_.structs[item.owner]];
                    StructField        &field = info.fields[item.field];
                    if (verdicts[field.type] == Verdict::Admitted && !replaced[item_index]) {
                        field.recursive = true;
                    } else {
                        info.valid = false;
                    }
                }
            }

            /// Applies rules 2, 3, 4 and 8 of ADR 0012 to one recursive edge of
            /// `owner` and reports each rule it breaks. `direct` is the reference
            /// inside the edge's `atomic<...>` when the cycle runs through it;
            /// `nested` is the first other reference by which the field stays in
            /// its cycle. At least one of them is set.
            bool check_recursive_edge(const StructField &field, ast::DeclId owner, const std::optional<StructReference> &direct,
                                      const std::optional<StructReference> &nested) {
                const std::string edge  = "recursive edge '" + field.name + "' of '" + struct_name(field.origin) + "'";
                const ast::Type  &type  = module_.type(field.type);
                const SourceRange range = type.range;
                if (nested && (nested->reach == Reach::Container || nested->reach == Reach::Argument)) {
                    report(Category::Type, range,
                           edge + " reaches '" + struct_name(owner) + "' again through " +
                               (nested->reach == Reach::Container ? "a collection element" : "a generic argument") +
                               "; recursion through a container or a generic argument is not supported (ADR 0012, rule 8)");
                    return false;
                }
                bool valid = true;
                if (type.kind != ast::TypeKind::Atomic || nested) {
                    const StructReference &reached = direct ? *direct : *nested;
                    const std::string      target  = reached.applied == ast::no_node
                                                         ? struct_name(reached.decl)
                                                         : std::string{file_.slice(module_.type(reached.applied).range)};
                    report(Category::Type, range,
                           edge + " must be an atomic boundary (ADR 0012, rule 3): declare it 'atomic<" + target + ">'");
                    valid = false;
                }
                if (!field.optional) {
                    report(Category::Type, range, edge + " must be optional (ADR 0012, rule 2): declare it '= null'");
                    valid = false;
                }
                if (!valid) { return false; }
                // Rule 4: a cycle reaches finitely many specializations, so each
                // generic argument on an edge is a parameter of the declaring
                // struct or mentions none. A parameter wrapped in a type, as in
                // `Tree<list<T>>` inside `Tree<T>`, would denote an unbounded family.
                if (direct->applied != ast::no_node) {
                    const std::vector<ast::GenericArgument> &arguments = module_.type(direct->applied).arguments;
                    for (std::size_t index = 0; index < arguments.size(); ++index) {
                        if (parameter_argument(direct->applied, index, field.origin.decl) ||
                            !argument_mentions_parameter(direct->applied, index)) {
                            continue;
                        }
                        report(Category::Type, range,
                               edge + " passes '" + std::string{file_.slice(arguments[index].range)} + "' to '" +
                                   struct_name(direct->decl) + "'; in a cycle a generic argument is a parameter of '" +
                                   struct_name(field.origin) +
                                   "' or mentions none, since a wrapped parameter denotes an unbounded family of "
                                   "specializations (ADR 0012, rule 4)");
                        return false;
                    }
                }
                return true;
            }

            void validate_constructor(ast::DeclId decl, const std::vector<ast::Argument> &arguments, bool delta,
                                      SourceRange range) {
                const auto       &structure = std::get<ast::StructDecl>(module_.decl(decl).node);
                const StructInfo &info      = result_.struct_info[decl];
                if (!info.valid) { return; }
                if (structure.abstract) {
                    report(Category::Type, range,
                           "abstract struct '" + std::string{structure.name.text} + "' is not constructible");
                    return;
                }
                const auto &field_index = field_indices_[decl];
                std::vector<bool>                                        supplied(info.fields.size(), false);
                for (const ast::Argument &argument : arguments) {
                    if (argument.name.empty()) { continue; }
                    const auto found = field_index.find(argument.name.text);
                    if (found == field_index.end()) {
                        report(Category::Name, argument.name.range,
                               "struct '" + std::string{structure.name.text} + "' has no field named '" +
                                   std::string{argument.name.text} + "'");
                        continue;
                    }
                    const std::size_t index = found->second;
                    if (supplied[index]) {
                        report(Category::Name, argument.name.range,
                               "field '" + std::string{argument.name.text} + "' is given twice");
                    }
                    supplied[index] = true;
                    if (is_null(argument.value) && !info.fields[index].optional) {
                        report(Category::Type, module_.expr(argument.value).range,
                               "required field '" + info.fields[index].name + "' cannot be null");
                    }
                }
                if (delta) { return; }
                for (std::size_t i = 0; i < info.fields.size(); ++i) {
                    if (!supplied[i] && info.fields[i].default_value == ast::no_node && !info.fields[i].optional) {
                        report(Category::Type, range,
                               "struct '" + std::string{structure.name.text} + "' needs field '" + info.fields[i].name + "'");
                    }
                }
            }

            void validate_constructors() {
                for (ast::ExprId id = 0; id < module_.exprs.size(); ++id) {
                    const ast::Expr &expr = module_.expr(id);
                    if (const auto *construct = std::get_if<ast::Construct>(&expr.node)) {
                        const Binding &binding = result_.type_bindings[construct->type];
                        if (binding.kind == BindingKind::Struct) {
                            validate_constructor(binding.decl, construct->arguments, construct->delta, expr.range);
                        }
                    } else if (const auto *call = std::get_if<ast::Call>(&expr.node)) {
                        const Binding &binding = result_.bindings[call->callee];
                        if (binding.kind == BindingKind::Struct) {
                            validate_constructor(binding.decl, call->arguments, false, expr.range);
                        }
                    }
                }
            }

            // ----------------------------------------------------------- scopes

            void push_scope() { scopes_.push_back(Scope{}); }
            void pop_scope() { scopes_.pop_back(); }

            void declare(const ast::Name &name, Binding binding, std::string_view where) {
                if (name.empty()) { return; }
                if (!scopes_.back().names.try_emplace(name.text, std::move(binding)).second) {
                    report(Category::Name, name.range, "'" + std::string{name.text} + "' is declared twice " + std::string{where});
                }
            }

            /// Innermost scope first. While a test-only declaration resolves, the
            /// test overlay sits between the module scope and its own scopes.
            [[nodiscard]] std::optional<Binding> lookup(std::string_view name) const {
                for (std::size_t depth = scopes_.size(); depth-- > 0;) {
                    if (depth == 0 && test_scope_active_) {
                        if (const auto found = test_scope_.names.find(name); found != test_scope_.names.end()) {
                            return found->second;
                        }
                    }
                    if (const auto found = scopes_[depth].names.find(name); found != scopes_[depth].names.end()) {
                        return found->second;
                    }
                }
                return std::nullopt;
            }

            void report(Category category, SourceRange range, std::string message) {
                diagnostics_.report(category, range, std::move(message));
            }

            const syntax::SourceFile                      &file_;
            const ast::Module                             &module_;
            const ModuleCatalog                           &catalog_;
            const OperatorLookup                          &has_operator_;
            syntax::DiagnosticSink                        &diagnostics_;
            ResolvedModule                                 result_{};
            std::vector<Scope>                             scopes_{};
            Scope                                          test_scope_{};
            bool                                           test_scope_active_{false};
            std::unordered_map<std::string, Binding>       imported_function_bindings_{};
            /// One binding per imported struct identity (ADR 0013).
            std::unordered_map<std::string, Binding>       imported_struct_bindings_{};
            std::unordered_map<std::string, std::uint32_t> native_family_indices_{};
            std::vector<std::uint8_t>                      struct_states_{};
            /// What each bare-name argument of an applied type names, indexed
            /// by TypeId and argument position; empty for other types.
            std::vector<std::vector<Binding>> argument_bindings_{};
            /// Each struct's effective field names to their position in
            /// StructInfo::fields; by DeclId.
            ///
            /// The keys OWN their spelling. A locally declared field's name is
            /// a view into the source text and would outlive anything, but a
            /// field seeded from an imported parent (ADR 0013) is named by a
            /// catalog record this resolver holds by value -- a view into that
            /// dangles the moment the seeding call returns, and the lookup then
            /// reads freed memory and reports a field the struct plainly has.
            std::vector<std::unordered_map<std::string, std::size_t, TransparentStringHash, std::equal_to<>>> field_indices_{};
        };
    }  // namespace

    bool is_intrinsic(std::string_view name) noexcept {
        for (const std::string_view intrinsic : intrinsics) {
            if (intrinsic == name) { return true; }
        }
        return false;
    }

    ResolvedModule resolve(const syntax::SourceFile &file, const ast::Module &module, const ModuleCatalog &catalog,
                           const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics) {
        return Resolver{file, module, catalog, has_operator, diagnostics}.run();
    }

    ResolvedModule resolve(const syntax::SourceFile &file, const ast::Module &module, const OperatorLookup &has_operator,
                           syntax::DiagnosticSink &diagnostics) {
        return resolve(file, module, ModuleCatalog{}, has_operator, diagnostics);
    }
}  // namespace hgl::semantics
