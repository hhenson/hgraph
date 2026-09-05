#include "semantics/resolve.h"

#include <algorithm>
#include <optional>
#include <string>
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

        constexpr std::string_view intrinsics[] = {"valid", "modified", "all_valid", "last_modified", "delta",  "key_set",
                                                   "keys",  "values",   "items",     "added",         "removed"};

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
            Resolver(const ast::Module &module, const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics)
                : module_{module}, has_operator_{has_operator}, diagnostics_{diagnostics} {
                result_.bindings.resize(module.exprs.size());
                result_.type_bindings.resize(module.types.size());
                result_.constraint_bindings.resize(module.constraints.size());
                result_.implementation_bindings.resize(module.decls.size());
                result_.kinds.resize(module.decls.size(), FunctionKind::Composition);
                result_.struct_info.resize(module.decls.size());
            }

            ResolvedModule run() {
                collect_declarations();
                for (const ast::DeclId id : module_.declarations) {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *structure = std::get_if<ast::StructDecl>(&decl.node)) {
                        resolve_struct(id, *structure);
                    } else if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node)) {
                        resolve_function(id, *fn);
                    } else if (const auto *op = std::get_if<ast::OperatorDecl>(&decl.node)) {
                        resolve_operator(id, *op);
                    } else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node)) {
                        resolve_test(id, *test);
                    }
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
                        result_.functions.push_back(id);
                        declare_function(id, *fn);
                    } else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node)) {
                        result_.tests.push_back(id);
                        Binding binding;
                        binding.kind = BindingKind::Test;
                        binding.decl = id;
                        declare(test->name, binding, "in the module");
                    }
                }
            }

            void declare_function(ast::DeclId id, const ast::FunctionDecl &fn) {
                const std::optional<Binding> existing = lookup(fn.name.text);
                const bool                   operator_in_scope =
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
                const std::string path = join_path(use.path);
                if (path != kernel_std && path != kernel_analytics) {
                    report(Category::Module, range,
                           "module '" + path + "' is not available: the first pass links the kernel modules " +
                               std::string{kernel_std} + " and " + std::string{kernel_analytics} + " only");
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

            void resolve_operator(ast::DeclId id, const ast::OperatorDecl &op) {
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, op.generics, context);
                resolve_signature(id, op.signature, context);
                resolve_constraint(op.requirements, context);
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

            void resolve_signature(ast::DeclId fn, const ast::Signature &signature, Context &context) {
                for (std::size_t i = 0; i < signature.parameters.size(); ++i) {
                    const ast::Parameter &parameter = signature.parameters[i];
                    if (parameter.type != ast::no_node) { resolve_type(parameter.type, context); }
                    if (parameter.default_value != ast::no_node) { resolve_expr(parameter.default_value, context); }
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
                                      std::is_same_v<T, ast::LifecycleBlock> || std::is_same_v<T, ast::WhenStmt> ||
                                      std::is_same_v<T, ast::ForStmt>) {
                            return true;
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
                            resolve_expr(node.init, context);
                            Binding binding;
                            binding.kind = BindingKind::Local;
                            binding.stmt = id;
                            declare(node.name, binding, "in the block");
                        } else if constexpr (std::is_same_v<T, ast::StateDecl>) {
                            reject_in_test(context, stmt.range, "state");
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
                            resolve_expr(node.condition, context);
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
                return std::get<ast::OperatorDecl>(node).generics;
            }

            [[nodiscard]] bool is_const_generic(const Binding &binding) const {
                return binding.kind == BindingKind::Generic && binding.index < generics_of(binding.decl).size() &&
                       generics_of(binding.decl)[binding.index].is_const;
            }

            void resolve_generic_argument(const ast::GenericArgument &argument, const ast::GenericParameter &parameter,
                                          Context &context) {
                if (argument.type != ast::no_node) {
                    resolve_type(argument.type, context);
                    if (parameter.is_const) {
                        report(Category::Type, argument.range,
                               "const generic '" + std::string{parameter.name.text} + "' takes a value argument");
                    } else {
                        const ast::TypeKind kind = module_.type(argument.type).kind;
                        if (kind == ast::TypeKind::Atomic || kind == ast::TypeKind::Rolling) {
                            report(Category::Type, argument.range,
                                   "generic struct type arguments are canonical value types; put "
                                   "'atomic' or 'rolling' in the field declaration");
                        }
                    }
                    return;
                }
                if (argument.value != ast::no_node) {
                    resolve_expr(argument.value, context);
                    if (!parameter.is_const) {
                        report(Category::Type, argument.range,
                               "type generic '" + std::string{parameter.name.text} + "' takes a type argument");
                    }
                    return;
                }

                const std::optional<Binding> binding = lookup(argument.name.text);
                if (!binding) {
                    report(Category::Type, argument.name.range,
                           "unknown generic argument '" + std::string{argument.name.text} + "'");
                    return;
                }
                if (parameter.is_const) {
                    if (!is_const_generic(*binding)) {
                        report(Category::Type, argument.name.range,
                               "const generic '" + std::string{parameter.name.text} + "' takes a const value argument");
                    }
                    return;
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
            }

            void resolve_type(ast::TypeId id, Context &context) {
                const ast::Type &type = module_.type(id);
                if (type.value_position && (type.kind == ast::TypeKind::Atomic || type.kind == ast::TypeKind::Rolling)) {
                    report(Category::Type, type.range,
                           std::string{"'"} + (type.kind == ast::TypeKind::Atomic ? "atomic" : "rolling") +
                               "' is a temporal shape, not a canonical value type");
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
                                resolve_generic_argument(type.arguments[i], parameters[i], context);
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
                            if (!binding || (binding->kind != BindingKind::Generic && binding->kind != BindingKind::Parameter &&
                                             binding->kind != BindingKind::Struct)) {
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
                                 node.name.text != "schema" && node.name.text != "keys")) {
                                report(Category::Type, constraint.range,
                                       "'" + std::string{node.name.text} + "' is not a compile-time reflection function");
                            } else {
                                Binding binding;
                                binding.kind                    = BindingKind::Intrinsic;
                                binding.registry_name           = std::string{node.name.text};
                                result_.constraint_bindings[id] = std::move(binding);
                            }
                            for (const ast::ConstraintId argument : node.arguments) { resolve_constraint(argument, context); }
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
                                    found_alias                           = true;
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

            [[nodiscard]] bool contains_struct(ast::TypeId id, ast::DeclId target) const {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Named && result_.type_bindings[id].kind == BindingKind::Struct &&
                    result_.type_bindings[id].decl == target) {
                    return true;
                }
                for (const ast::TypeId child : type.children) {
                    if (contains_struct(child, target)) { return true; }
                }
                for (const ast::GenericArgument &argument : type.arguments) {
                    if (argument.type != ast::no_node && contains_struct(argument.type, target)) { return true; }
                }
                return false;
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
                    if (structure.parents.size() == 1) { info.fields = result_.struct_info[binding.decl].fields; }
                }

                std::vector<std::string_view> local_names;
                std::vector<std::string_view> overridden;
                for (const ast::StructMember &member : structure.members) {
                    std::visit(
                        [&](const auto &item) {
                            using T = std::decay_t<decltype(item)>;
                            if constexpr (std::is_same_v<T, ast::StructField>) {
                                if (std::find(local_names.begin(), local_names.end(), item.name.text) != local_names.end()) {
                                    report(Category::Name, item.name.range,
                                           "struct field '" + std::string{item.name.text} + "' is declared twice");
                                    valid = false;
                                    return;
                                }
                                local_names.push_back(item.name.text);
                                const auto inherited =
                                    std::find_if(info.fields.begin(), info.fields.end(),
                                                 [&](const StructField &field) { return field.name == item.name.text; });
                                if (inherited != info.fields.end()) {
                                    report(Category::Type, item.name.range,
                                           "inherited field '" + std::string{item.name.text} +
                                               "' cannot be redeclared with a type; override only "
                                               "its default");
                                    valid = false;
                                    return;
                                }
                                if (contains_struct(item.type, id)) {
                                    report(Category::Type, module_.type(item.type).range,
                                           "self-recursive struct fields are not supported in this "
                                           "prototype");
                                    valid = false;
                                }
                                info.fields.push_back(StructField{std::string{item.name.text}, item.type, item.default_value, id,
                                                                  is_null(item.default_value)});
                            } else {
                                if (std::find(overridden.begin(), overridden.end(), item.name.text) != overridden.end()) {
                                    report(Category::Name, item.name.range,
                                           "inherited default '" + std::string{item.name.text} + "' is set twice");
                                    valid = false;
                                    return;
                                }
                                overridden.push_back(item.name.text);
                                const auto inherited =
                                    std::find_if(info.fields.begin(), info.fields.end(),
                                                 [&](const StructField &field) { return field.name == item.name.text; });
                                if (inherited == info.fields.end()) {
                                    report(Category::Type, item.name.range,
                                           "default override names no inherited field '" + std::string{item.name.text} + "'");
                                    valid = false;
                                    return;
                                }
                                if (is_null(item.value) && !inherited->optional) {
                                    report(Category::Type, module_.expr(item.value).range,
                                           "only an optional inherited field may have a null default");
                                    valid = false;
                                    return;
                                }
                                inherited->default_value = item.value;
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
                for (const ast::DeclId id : result_.structs) { (void)validate_struct(id); }
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
                std::vector<bool> supplied(info.fields.size(), false);
                for (const ast::Argument &argument : arguments) {
                    if (argument.name.empty()) { continue; }
                    const auto found = std::find_if(info.fields.begin(), info.fields.end(),
                                                    [&](const StructField &field) { return field.name == argument.name.text; });
                    if (found == info.fields.end()) {
                        report(Category::Name, argument.name.range,
                               "struct '" + std::string{structure.name.text} + "' has no field named '" +
                                   std::string{argument.name.text} + "'");
                        continue;
                    }
                    const auto index = static_cast<std::size_t>(found - info.fields.begin());
                    if (supplied[index]) {
                        report(Category::Name, argument.name.range,
                               "field '" + std::string{argument.name.text} + "' is given twice");
                    }
                    supplied[index] = true;
                    if (is_null(argument.value) && !found->optional) {
                        report(Category::Type, module_.expr(argument.value).range,
                               "required field '" + found->name + "' cannot be null");
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

            const ast::Module        &module_;
            const OperatorLookup     &has_operator_;
            syntax::DiagnosticSink   &diagnostics_;
            ResolvedModule            result_{};
            std::vector<Scope>        scopes_{};
            std::vector<std::uint8_t> struct_states_{};
        };
    }  // namespace

    bool is_intrinsic(std::string_view name) noexcept {
        for (const std::string_view intrinsic : intrinsics) {
            if (intrinsic == name) { return true; }
        }
        return false;
    }

    ResolvedModule resolve(const syntax::SourceFile &, const ast::Module &module, const OperatorLookup &has_operator,
                           syntax::DiagnosticSink &diagnostics) {
        return Resolver{module, has_operator, diagnostics}.run();
    }
}  // namespace hgl::semantics
