#include "semantics/resolve.h"

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

        constexpr std::string_view intrinsics[] = {"valid",   "modified", "all_valid", "last_modified", "delta", "key_set",
                                                   "keys",    "values",   "items",     "added",         "removed"};

        [[nodiscard]] std::string join_path(const std::vector<ast::Name> &path)
        {
            std::string result;
            for (const ast::Name &segment : path)
            {
                if (!result.empty()) { result += '.'; }
                result += segment.text;
            }
            return result;
        }

        class Resolver
        {
          public:
            Resolver(const ast::Module &module, const OperatorLookup &has_operator, syntax::DiagnosticSink &diagnostics)
                : module_{module}, has_operator_{has_operator}, diagnostics_{diagnostics}
            {
                result_.bindings.resize(module.exprs.size());
                result_.kinds.resize(module.decls.size(), FunctionKind::Composition);
            }

            ResolvedModule run()
            {
                collect_declarations();
                for (const ast::DeclId id : module_.declarations)
                {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node)) { resolve_function(id, *fn); }
                    else if (const auto *op = std::get_if<ast::OperatorDecl>(&decl.node)) { resolve_operator(id, *op); }
                    else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node)) { resolve_test(id, *test); }
                }
                return std::move(result_);
            }

          private:
            struct Scope
            {
                std::vector<std::pair<std::string_view, Binding>> names;
            };

            struct Context
            {
                ast::DeclId fn{ast::no_node};
                bool        in_test{false};
                bool        in_sequence{false};  ///< inside a harness sequence literal
            };

            // ------------------------------------------------------ module level

            void collect_declarations()
            {
                scopes_.push_back(Scope{});
                for (const ast::DeclId id : module_.declarations)
                {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *mod = std::get_if<ast::ModuleDecl>(&decl.node))
                    {
                        if (!result_.module_path.empty())
                        {
                            report(Category::Module, decl.range, "a compilation unit has one module declaration");
                        }
                        result_.module_path = join_path(mod->path);
                    }
                }
                if (result_.module_path.empty())
                {
                    const SourceRange at = module_.declarations.empty() ? SourceRange{} : module_.decl(module_.declarations.front()).range;
                    report(Category::Module, SourceRange{at.begin, at.begin}, "a compilation unit begins with a module declaration");
                }
                for (const ast::DeclId id : module_.declarations)
                {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *use = std::get_if<ast::UseDecl>(&decl.node)) { resolve_use(*use, decl.range); }
                }
                // Operators first so a plain `fn` of an operator's name is a conflict.
                for (const ast::DeclId id : module_.declarations)
                {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *op = std::get_if<ast::OperatorDecl>(&decl.node))
                    {
                        result_.operators.push_back(id);
                        Binding binding;
                        binding.kind = BindingKind::LocalOperator;
                        binding.decl = id;
                        declare(op->name, binding, "in the module");
                    }
                }
                for (const ast::DeclId id : module_.declarations)
                {
                    const ast::Decl &decl = module_.decl(id);
                    if (const auto *fn = std::get_if<ast::FunctionDecl>(&decl.node))
                    {
                        result_.functions.push_back(id);
                        declare_function(id, *fn);
                    }
                    else if (const auto *test = std::get_if<ast::TestDecl>(&decl.node))
                    {
                        result_.tests.push_back(id);
                        Binding binding;
                        binding.kind = BindingKind::Test;
                        binding.decl = id;
                        declare(test->name, binding, "in the module");
                    }
                }
            }

            void declare_function(ast::DeclId id, const ast::FunctionDecl &fn)
            {
                const std::optional<Binding> existing = lookup(fn.name.text);
                const bool operator_in_scope =
                    existing && (existing->kind == BindingKind::Operator || existing->kind == BindingKind::LocalOperator);
                if (fn.visibility == ast::FunctionVisibility::Impl)
                {
                    if (!operator_in_scope)
                    {
                        report(Category::Module, fn.name.range,
                               "'impl fn " + std::string{fn.name.text} + "' has no operator named '" +
                                   std::string{fn.name.text} + "' in scope");
                    }
                    // An implementation is reached through its operator's identity,
                    // never as an unqualified value of its own.
                    return;
                }
                if (operator_in_scope)
                {
                    report(Category::Name, fn.name.range,
                           "'fn " + std::string{fn.name.text} + "' conflicts with operator " +
                               operator_identity(*existing) + "; declare 'impl fn " + std::string{fn.name.text} +
                               "' or rename it");
                    return;
                }
                Binding binding;
                binding.kind = BindingKind::Function;
                binding.decl = id;
                declare(fn.name, binding, "in the module");
            }

            [[nodiscard]] std::string operator_identity(const Binding &binding) const
            {
                if (binding.kind == BindingKind::Operator)
                {
                    for (const ImportedOperator &import : result_.imports)
                    {
                        if (import.registry_name == binding.registry_name) { return import.module + "::" + import.name; }
                    }
                    return binding.registry_name;
                }
                const auto &op = std::get<ast::OperatorDecl>(module_.decl(binding.decl).node);
                return result_.module_path + "::" + std::string{op.name.text};
            }

            void resolve_use(const ast::UseDecl &use, SourceRange range)
            {
                const std::string path = join_path(use.path);
                if (path != kernel_std && path != kernel_analytics)
                {
                    report(Category::Module, range,
                           "module '" + path + "' is not available: the first pass links the kernel modules " +
                               std::string{kernel_std} + " and " + std::string{kernel_analytics} + " only");
                    return;
                }
                if (!use.alias.empty())
                {
                    for (const ModuleAlias &alias : result_.aliases)
                    {
                        if (alias.alias == use.alias.text)
                        {
                            report(Category::Name, use.alias.range,
                                   "module alias '" + std::string{use.alias.text} + "' is declared twice");
                            return;
                        }
                    }
                    result_.aliases.push_back(ModuleAlias{std::string{use.alias.text}, path});
                    return;
                }
                for (const ast::Name &name : use.names)
                {
                    const std::optional<std::string> registry_name = kernel_registry_name(path, name.text);
                    if (!registry_name)
                    {
                        report(Category::Module, name.range, path + " does not export '" + std::string{name.text} + "'");
                        continue;
                    }
                    if (const std::optional<Binding> existing = lookup(name.text);
                        existing && existing->kind == BindingKind::Operator)
                    {
                        std::string other = operator_identity(*existing);
                        report(Category::Module, name.range,
                               "operator '" + std::string{name.text} + "' is imported unqualified from both " +
                                   other.substr(0, other.find("::")) + " and " + path);
                        continue;
                    }
                    result_.imports.push_back(ImportedOperator{std::string{name.text}, path, *registry_name, name.range});
                    Binding binding;
                    binding.kind          = BindingKind::Operator;
                    binding.registry_name = *registry_name;
                    declare(name, binding, "in the module");
                }
            }

            /// The interim kernel table (developer guide, "Interim kernel table").
            [[nodiscard]] std::optional<std::string> kernel_registry_name(std::string_view module,
                                                                          std::string_view name) const
            {
                if (module == kernel_analytics) { return std::string{kernel_analytics} + "." + std::string{name}; }
                if (has_operator_(name)) { return std::string{name}; }
                const std::string underscored = std::string{name} + "_";
                if (has_operator_(underscored)) { return underscored; }
                return std::nullopt;
            }

            // -------------------------------------------------------- functions

            void resolve_function(ast::DeclId id, const ast::FunctionDecl &fn)
            {
                result_.kinds[id] = classify(fn);
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, fn.generics);
                resolve_signature(id, fn.signature, context);
                if (fn.concise_body != ast::no_node) { resolve_expr(fn.concise_body, context); }
                if (fn.block_body != ast::no_node) { resolve_block(fn.block_body, context); }
                pop_scope();
            }

            void resolve_operator(ast::DeclId id, const ast::OperatorDecl &op)
            {
                Context context;
                context.fn = id;
                push_scope();
                declare_generics(id, op.generics);
                resolve_signature(id, op.signature, context);
                pop_scope();
            }

            void resolve_test(ast::DeclId id, const ast::TestDecl &test)
            {
                Context context;
                context.fn      = id;
                context.in_test = true;
                push_scope();
                resolve_block(test.block, context);
                pop_scope();
            }

            void declare_generics(ast::DeclId fn, const std::vector<ast::GenericParameter> &generics)
            {
                for (std::size_t i = 0; i < generics.size(); ++i)
                {
                    Binding binding;
                    binding.kind  = BindingKind::Generic;
                    binding.decl  = fn;
                    binding.index = static_cast<std::uint32_t>(i);
                    declare(generics[i].name, binding, "among the generic parameters");
                }
            }

            void resolve_signature(ast::DeclId fn, const ast::Signature &signature, Context &context)
            {
                for (std::size_t i = 0; i < signature.parameters.size(); ++i)
                {
                    const ast::Parameter &parameter = signature.parameters[i];
                    if (parameter.type != ast::no_node) { resolve_type(parameter.type, context); }
                    if (parameter.default_value != ast::no_node) { resolve_expr(parameter.default_value, context); }
                }
                if (signature.result != ast::no_node) { resolve_type(signature.result, context); }
                // Parameters become visible together, after their defaults.
                for (std::size_t i = 0; i < signature.parameters.size(); ++i)
                {
                    Binding binding;
                    binding.kind  = BindingKind::Parameter;
                    binding.decl  = fn;
                    binding.index = static_cast<std::uint32_t>(i);
                    declare(signature.parameters[i].name, binding, "among the parameters");
                }
            }

            [[nodiscard]] FunctionKind classify(const ast::FunctionDecl &fn) const
            {
                if (fn.block_body != ast::no_node && block_has_runtime_form(fn.block_body)) { return FunctionKind::Runtime; }
                return FunctionKind::Composition;
            }

            [[nodiscard]] bool block_has_runtime_form(ast::BlockId id) const
            {
                for (const ast::StmtId stmt_id : module_.block(id).statements)
                {
                    if (stmt_has_runtime_form(stmt_id)) { return true; }
                }
                return false;
            }

            [[nodiscard]] bool stmt_has_runtime_form(ast::StmtId id) const
            {
                const ast::Stmt &stmt = module_.stmt(id);
                return std::visit(
                    [&](const auto &node) -> bool {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::StateDecl> || std::is_same_v<T, ast::InjectDecl> ||
                                      std::is_same_v<T, ast::LifecycleBlock> || std::is_same_v<T, ast::WhenStmt> ||
                                      std::is_same_v<T, ast::ForStmt>)
                        {
                            return true;
                        }
                        else if constexpr (std::is_same_v<T, ast::ExprStmt>) { return expr_has_runtime_form(node.expr); }
                        else if constexpr (std::is_same_v<T, ast::LocalDecl>) { return expr_has_runtime_form(node.init); }
                        else if constexpr (std::is_same_v<T, ast::AssignStmt>) { return expr_has_runtime_form(node.value); }
                        else if constexpr (std::is_same_v<T, ast::ReturnStmt>) { return expr_has_runtime_form(node.value); }
                        else { return false; }
                    },
                    stmt.node);
            }

            [[nodiscard]] bool expr_has_runtime_form(ast::ExprId id) const
            {
                if (id == ast::no_node) { return false; }
                const ast::Expr &expr = module_.expr(id);
                if (const auto *if_ = std::get_if<ast::If>(&expr.node))
                {
                    return block_has_runtime_form(if_->then_block) || expr_has_runtime_form(if_->otherwise);
                }
                if (const auto *block = std::get_if<ast::BlockExpr>(&expr.node)) { return block_has_runtime_form(block->block); }
                return false;
            }

            // ----------------------------------------------------------- bodies

            void resolve_block(ast::BlockId id, Context &context)
            {
                push_scope();
                for (const ast::StmtId stmt_id : module_.block(id).statements) { resolve_stmt(stmt_id, context); }
                pop_scope();
            }

            void resolve_stmt(ast::StmtId id, Context &context)
            {
                const ast::Stmt &stmt = module_.stmt(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::LocalDecl>)
                        {
                            if (node.type != ast::no_node) { resolve_type(node.type, context); }
                            resolve_expr(node.init, context);
                            Binding binding;
                            binding.kind = BindingKind::Local;
                            binding.stmt = id;
                            declare(node.name, binding, "in the block");
                        }
                        else if constexpr (std::is_same_v<T, ast::StateDecl>)
                        {
                            reject_in_test(context, stmt.range, "state");
                            if (node.type != ast::no_node) { resolve_type(node.type, context); }
                            resolve_expr(node.init, context);
                            Binding binding;
                            binding.kind = BindingKind::Local;
                            binding.stmt = id;
                            declare(node.name, binding, "in the block");
                        }
                        else if constexpr (std::is_same_v<T, ast::InjectDecl>)
                        {
                            reject_in_test(context, stmt.range, "inject");
                            for (const ast::Name &name : node.names)
                            {
                                Binding binding;
                                binding.kind = BindingKind::Local;
                                binding.stmt = id;
                                declare(name, binding, "in the block");
                            }
                        }
                        else if constexpr (std::is_same_v<T, ast::LifecycleBlock>)
                        {
                            reject_in_test(context, stmt.range, node.is_stop ? "stop" : "start");
                            resolve_block(node.block, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::WhenStmt>)
                        {
                            reject_in_test(context, stmt.range, "when");
                            resolve_expr(node.condition, context);
                            resolve_block(node.block, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::ForStmt>)
                        {
                            reject_in_test(context, stmt.range, "for");
                            resolve_expr(node.iterable, context);
                            push_scope();
                            Binding first;
                            first.kind = BindingKind::Local;
                            first.stmt = id;
                            declare(node.first, first, "in the iteration pattern");
                            if (!node.second.empty())
                            {
                                Binding second = first;
                                second.second  = true;
                                declare(node.second, second, "in the iteration pattern");
                            }
                            resolve_block(node.block, context);
                            pop_scope();
                        }
                        else if constexpr (std::is_same_v<T, ast::AssignStmt>)
                        {
                            resolve_expr(node.place, context);
                            resolve_expr(node.value, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::ReturnStmt>)
                        {
                            if (node.value != ast::no_node) { resolve_expr(node.value, context); }
                        }
                        else if constexpr (std::is_same_v<T, ast::AssertStmt>)
                        {
                            if (!context.in_test)
                            {
                                report(Category::Phase, stmt.range, "'assert' is only valid inside a test body");
                            }
                            resolve_expr(node.condition, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::ExprStmt>) { resolve_expr(node.expr, context); }
                    },
                    stmt.node);
            }

            void reject_in_test(const Context &context, SourceRange range, std::string_view form)
            {
                if (context.in_test)
                {
                    report(Category::Phase, range, "'" + std::string{form} + "' is not available in a test body");
                }
            }

            void resolve_expr(ast::ExprId id, Context &context)
            {
                if (id == ast::no_node) { return; }
                const ast::Expr &expr = module_.expr(id);
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, ast::Placeholder>)
                        {
                            if (!context.in_sequence)
                            {
                                report(Category::Type, expr.range, "'_' is only valid in a harness sequence");
                            }
                        }
                        else if constexpr (std::is_same_v<T, ast::NameRef>) { resolve_name(id, node.name); }
                        else if constexpr (std::is_same_v<T, ast::QualifiedRef>) { resolve_qualified(id, node); }
                        else if constexpr (std::is_same_v<T, ast::Unary>) { resolve_expr(node.operand, context); }
                        else if constexpr (std::is_same_v<T, ast::Binary>)
                        {
                            resolve_expr(node.lhs, context);
                            resolve_expr(node.rhs, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::Call>)
                        {
                            resolve_expr(node.callee, context);
                            for (const ast::Argument &argument : node.arguments) { resolve_expr(argument.value, context); }
                        }
                        else if constexpr (std::is_same_v<T, ast::Index>)
                        {
                            resolve_expr(node.target, context);
                            resolve_expr(node.index, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::Field>) { resolve_expr(node.target, context); }
                        else if constexpr (std::is_same_v<T, ast::SequenceLiteral>)
                        {
                            const bool saved   = context.in_sequence;
                            context.in_sequence = context.in_test;
                            for (const ast::SequenceElement &element : node.elements)
                            {
                                resolve_expr(element.key, context);
                                resolve_expr(element.value, context);
                            }
                            context.in_sequence = saved;
                        }
                        else if constexpr (std::is_same_v<T, ast::TupleLiteral>)
                        {
                            for (const ast::ExprId element : node.elements) { resolve_expr(element, context); }
                        }
                        else if constexpr (std::is_same_v<T, ast::AnonymousFn>)
                        {
                            push_scope();
                            for (std::size_t i = 0; i < node.parameters.size(); ++i)
                            {
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
                        }
                        else if constexpr (std::is_same_v<T, ast::If>)
                        {
                            resolve_expr(node.condition, context);
                            resolve_block(node.then_block, context);
                            resolve_expr(node.otherwise, context);
                        }
                        else if constexpr (std::is_same_v<T, ast::BlockExpr>) { resolve_block(node.block, context); }
                        else if constexpr (std::is_same_v<T, ast::Eval>)
                        {
                            if (!context.in_test) { report(Category::Phase, expr.range, "eval is only valid inside a test body"); }
                            resolve_expr(node.callee, context);
                            for (const ast::Argument &argument : node.arguments) { resolve_expr(argument.value, context); }
                        }
                        // Literals bind nothing.
                    },
                    expr.node);
            }

            void resolve_name(ast::ExprId id, const ast::Name &name)
            {
                const std::optional<Binding> binding = lookup(name.text);
                if (!binding)
                {
                    if (is_intrinsic(name.text))
                    {
                        Binding intrinsic;
                        intrinsic.kind          = BindingKind::Intrinsic;
                        intrinsic.registry_name = std::string{name.text};
                        result_.bindings[id]    = std::move(intrinsic);
                        return;
                    }
                    report(Category::Name, name.range, "unknown name '" + std::string{name.text} + "'");
                    return;
                }
                if (binding->kind == BindingKind::Test)
                {
                    report(Category::Name, name.range, "'" + std::string{name.text} + "' is a test, not a value");
                    return;
                }
                result_.bindings[id] = *binding;
            }

            void resolve_qualified(ast::ExprId id, const ast::QualifiedRef &ref)
            {
                for (const ModuleAlias &alias : result_.aliases)
                {
                    if (alias.alias != ref.qualifier.text) { continue; }
                    const std::optional<std::string> registry_name = kernel_registry_name(alias.module, ref.name.text);
                    if (!registry_name)
                    {
                        report(Category::Module, ref.name.range,
                               alias.module + " does not export '" + std::string{ref.name.text} + "'");
                        return;
                    }
                    Binding binding;
                    binding.kind          = BindingKind::Operator;
                    binding.registry_name = *registry_name;
                    result_.bindings[id]  = binding;
                    return;
                }
                report(Category::Name, ref.qualifier.range, "unknown module alias '" + std::string{ref.qualifier.text} + "'");
            }

            void resolve_type(ast::TypeId id, Context &context)
            {
                const ast::Type &type = module_.type(id);
                if (type.kind == ast::TypeKind::Named)
                {
                    const std::optional<Binding> binding = lookup(type.name.text);
                    if (!binding || binding->kind != BindingKind::Generic)
                    {
                        report(Category::Type, type.name.range, "unknown type '" + std::string{type.name.text} + "'");
                    }
                }
                for (const ast::TypeId child : type.children) { resolve_type(child, context); }
                if (type.size != ast::no_node) { resolve_expr(type.size, context); }
                if (type.min_size != ast::no_node) { resolve_expr(type.min_size, context); }
            }

            // ----------------------------------------------------------- scopes

            void push_scope() { scopes_.push_back(Scope{}); }
            void pop_scope() { scopes_.pop_back(); }

            void declare(const ast::Name &name, Binding binding, std::string_view where)
            {
                if (name.empty()) { return; }
                Scope &scope = scopes_.back();
                for (const auto &[existing, _] : scope.names)
                {
                    if (existing == name.text)
                    {
                        report(Category::Name, name.range,
                               "'" + std::string{name.text} + "' is declared twice " + std::string{where});
                        return;
                    }
                }
                scope.names.emplace_back(name.text, std::move(binding));
            }

            [[nodiscard]] std::optional<Binding> lookup(std::string_view name) const
            {
                for (auto scope = scopes_.rbegin(); scope != scopes_.rend(); ++scope)
                {
                    for (auto entry = scope->names.rbegin(); entry != scope->names.rend(); ++entry)
                    {
                        if (entry->first == name) { return entry->second; }
                    }
                }
                return std::nullopt;
            }

            void report(Category category, SourceRange range, std::string message)
            {
                diagnostics_.report(category, range, std::move(message));
            }

            const ast::Module     &module_;
            const OperatorLookup  &has_operator_;
            syntax::DiagnosticSink &diagnostics_;
            ResolvedModule         result_{};
            std::vector<Scope>     scopes_{};
        };
    }  // namespace

    bool is_intrinsic(std::string_view name) noexcept
    {
        for (const std::string_view intrinsic : intrinsics)
        {
            if (intrinsic == name) { return true; }
        }
        return false;
    }

    ResolvedModule resolve(const syntax::SourceFile &file, const ast::Module &module, const OperatorLookup &has_operator,
                           syntax::DiagnosticSink &diagnostics)
    {
        static_cast<void>(file);
        return Resolver{module, has_operator, diagnostics}.run();
    }
}  // namespace hgl::semantics
