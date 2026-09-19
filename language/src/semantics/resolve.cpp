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
            Resolver(const ast::Module &module, const ModuleCatalog &catalog, const OperatorLookup &has_operator,
                     syntax::DiagnosticSink &diagnostics)
                : module_{module}, catalog_{catalog}, has_operator_{has_operator}, diagnostics_{diagnostics} {
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
                    if (test_scope) { scopes_.push_back(test_scope_); }
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
                    if (test_scope) { pop_scope(); }
                }
                validate_structs();
                validate_constructors();
                return std::move(result_);
            }

          private:
            struct Scope
            { std::vector<std::pair<std::string_view, Binding>> names; };

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
                    if (!type.qualifier.empty()) {
                        report(Category::Type, type.qualifier.range,
                               "qualified source types require a module descriptor; only local "
                               "struct types are available in this prototype");
                    }
                    const std::optional<Binding> binding = lookup(type.name.text);
                    if (!binding || (binding->kind != BindingKind::Generic && binding->kind != BindingKind::Struct)) {
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
                            if (!binding ||
                                (binding->kind != BindingKind::Generic && binding->kind != BindingKind::ConstraintLocal &&
                                 binding->kind != BindingKind::Parameter && binding->kind != BindingKind::Struct)) {
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
                    info.parents.push_back(binding.decl);
                    if (structure.parents.size() == 1) {
                        info.fields        = result_.struct_info[binding.decl].fields;
                        field_indices_[id] = field_indices_[binding.decl];
                    }
                }

                std::unordered_map<std::string_view, std::size_t> &field_index = field_indices_[id];
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
                                info.fields.push_back(StructField{std::string{item.name.text}, item.type, item.default_value, id,
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
                reject_recursive_fields();
            }

            // ------------------------------------------- recursive struct fields

            static constexpr std::uint32_t no_position = std::numeric_limits<std::uint32_t>::max();

            /// A struct named in a field type, and the applied type naming it;
            /// no_node for a bare generic argument, which applies nothing.
            struct StructReference
            {
                ast::DeclId decl{ast::no_node};
                ast::TypeId applied{ast::no_node};
            };

            [[nodiscard]] const Binding *argument_binding(ast::TypeId id, std::size_t index) const noexcept {
                const std::vector<Binding> &slots = argument_bindings_[id];
                return index < slots.size() && slots[index].kind != BindingKind::Unbound ? &slots[index] : nullptr;
            }

            void struct_references(ast::TypeId id, std::vector<StructReference> &out) const {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Named && result_.type_bindings[id].kind == BindingKind::Struct) {
                    out.push_back(StructReference{result_.type_bindings[id].decl, id});
                }
                for (const ast::TypeId child : type.children) { struct_references(child, out); }
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    if (type.arguments[index].type != ast::no_node) {
                        struct_references(type.arguments[index].type, out);
                    } else if (const Binding *binding = argument_binding(id, index);
                               binding != nullptr && binding->kind == BindingKind::Struct) {
                        out.push_back(StructReference{binding->decl, ast::no_node});
                    }
                }
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

            [[nodiscard]] std::optional<std::string> arguments_key(ast::TypeId id) const {
                const ast::Type &type = module_.type(id);
                std::string      key{"<"};
                for (std::size_t index = 0; index < type.arguments.size(); ++index) {
                    std::optional<std::string> element;
                    if (type.arguments[index].type != ast::no_node) {
                        element = type_key(type.arguments[index].type);
                    } else if (const Binding *binding = argument_binding(id, index);
                               binding != nullptr && binding->kind == BindingKind::Struct) {
                        element = struct_key(binding->decl, "<>");
                    }
                    if (!element) { return std::nullopt; }
                    key += *element + ',';
                }
                return key + '>';
            }

            /// Rejects every field through which a value of a struct could
            /// contain another value of the same struct (syntax guide,
            /// "Compilation-unit grammar"). A field reaches each struct its type
            /// names, through collection elements and generic arguments. A field
            /// typed by a struct with descendants also reaches that family: every
            /// descendant of a non-generic struct, and of a generic one only the
            /// children whose parent application can equal the field's.
            /// Strongly connected components find every such field in one pass
            /// that is linear in the declared fields.
            void reject_recursive_fields() {
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

                struct Family
                {
                    std::uint32_t                                  any{no_position};   ///< every child group
                    std::uint32_t                                  open{no_position};  ///< non-concrete parent applications
                    std::unordered_map<std::string, std::uint32_t> exact{};            ///< by concrete parent application
                };
                std::vector<Family> families(count);
                for (std::uint32_t child = 0; child < count; ++child) {
                    edges[family_node(child)].push_back(value_node(child));
                    const auto &structure = std::get<ast::StructDecl>(module_.decl(result_.structs[child]).node);
                    for (const ast::TypeId parent_type : structure.parents) {
                        const Binding &binding = result_.type_bindings[parent_type];
                        if (binding.kind != BindingKind::Struct || position[binding.decl] == no_position) { continue; }
                        Family &family = families[position[binding.decl]];
                        if (family.any == no_position) { family.any = add_node(); }
                        std::uint32_t *group = &family.open;
                        if (const std::optional<std::string> key = arguments_key(parent_type)) {
                            group = &family.exact.try_emplace(*key, no_position).first->second;
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

                struct FieldEdges
                {
                    std::uint32_t owner{};
                    std::size_t   field{};
                    std::size_t   begin{};
                    std::size_t   end{};
                };
                std::vector<FieldEdges>      field_edges;
                std::vector<StructReference> references;
                for (std::uint32_t owner = 0; owner < count; ++owner) {
                    const StructInfo &info = result_.struct_info[result_.structs[owner]];
                    for (std::size_t field = 0; field < info.fields.size(); ++field) {
                        if (info.fields[field].type == ast::no_node) { continue; }
                        std::vector<std::uint32_t> &out   = edges[value_node(owner)];
                        const std::size_t           begin = out.size();
                        references.clear();
                        struct_references(info.fields[field].type, references);
                        for (const StructReference &reference : references) {
                            const std::uint32_t target = position[reference.decl];
                            if (target == no_position) { continue; }
                            out.push_back(value_node(target));
                            const Family &family = families[target];
                            if (family.any == no_position) { continue; }
                            const std::optional<std::string> key = reference.applied == ast::no_node
                                                                       ? std::optional<std::string>{"<>"}
                                                                       : arguments_key(reference.applied);
                            if (!key) {
                                out.push_back(family.any);
                                continue;
                            }
                            if (const auto exact = family.exact.find(*key); exact != family.exact.end()) {
                                out.push_back(exact->second);
                            }
                            if (family.open != no_position) { out.push_back(family.open); }
                        }
                        field_edges.push_back(FieldEdges{owner, field, begin, out.size()});
                    }
                }

                const std::vector<std::uint32_t> component = strongly_connected_components(edges);
                std::vector<bool>                reported(module_.types.size(), false);
                for (const FieldEdges &item : field_edges) {
                    const std::vector<std::uint32_t> &out   = edges[value_node(item.owner)];
                    const std::uint32_t               self  = component[value_node(item.owner)];
                    const auto                        begin = out.begin() + static_cast<std::ptrdiff_t>(item.begin);
                    const auto                        end   = out.begin() + static_cast<std::ptrdiff_t>(item.end);
                    if (std::none_of(begin, end, [&](std::uint32_t target) { return component[target] == self; })) { continue; }
                    const ast::DeclId  decl  = result_.structs[item.owner];
                    StructInfo        &info  = result_.struct_info[decl];
                    const StructField &field = info.fields[item.field];
                    info.valid               = false;
                    if (reported[field.type]) { continue; }
                    reported[field.type] = true;
                    const std::string name{std::get<ast::StructDecl>(module_.decl(decl).node).name.text};
                    report(Category::Type, module_.type(field.type).range,
                           "recursive struct fields are not supported in this prototype: through field '" + field.name +
                               "', a value of '" + name + "' can contain another '" + name + "'");
                }
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
                const std::unordered_map<std::string_view, std::size_t> &field_index = field_indices_[decl];
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
                Scope &scope = scopes_.back();
                for (const auto &[existing, _] : scope.names) {
                    if (existing == name.text) {
                        report(Category::Name, name.range,
                               "'" + std::string{name.text} + "' is declared twice " + std::string{where});
                        return;
                    }
                }
                scope.names.emplace_back(name.text, std::move(binding));
            }

            [[nodiscard]] std::optional<Binding> lookup(std::string_view name) const {
                for (auto scope = scopes_.rbegin(); scope != scopes_.rend(); ++scope) {
                    for (auto entry = scope->names.rbegin(); entry != scope->names.rend(); ++entry) {
                        if (entry->first == name) { return entry->second; }
                    }
                }
                return std::nullopt;
            }

            void report(Category category, SourceRange range, std::string message) {
                diagnostics_.report(category, range, std::move(message));
            }

            const ast::Module                             &module_;
            const ModuleCatalog                           &catalog_;
            const OperatorLookup                          &has_operator_;
            syntax::DiagnosticSink                        &diagnostics_;
            ResolvedModule                                 result_{};
            std::vector<Scope>                             scopes_{};
            Scope                                          test_scope_{};
            std::unordered_map<std::string, Binding>       imported_function_bindings_{};
            std::unordered_map<std::string, std::uint32_t> native_family_indices_{};
            std::vector<std::uint8_t>                      struct_states_{};
            /// What each bare-name argument of an applied type names, indexed
            /// by TypeId and argument position; empty for other types.
            std::vector<std::vector<Binding>> argument_bindings_{};
            /// Each struct's effective field names, keyed by the declaring source
            /// spelling, to their position in StructInfo::fields; by DeclId.
            std::vector<std::unordered_map<std::string_view, std::size_t>> field_indices_{};
        };
    }  // namespace

    bool is_intrinsic(std::string_view name) noexcept {
        for (const std::string_view intrinsic : intrinsics) {
            if (intrinsic == name) { return true; }
        }
        return false;
    }

    ResolvedModule resolve(const syntax::SourceFile &, const ast::Module &module, const ModuleCatalog &catalog,
                           const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics) {
        return Resolver{module, catalog, has_operator, diagnostics}.run();
    }

    ResolvedModule resolve(const syntax::SourceFile &file, const ast::Module &module, const OperatorLookup &has_operator,
                           syntax::DiagnosticSink &diagnostics) {
        return resolve(file, module, ModuleCatalog{}, has_operator, diagnostics);
    }
}  // namespace hgl::semantics
