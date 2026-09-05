#include "ir/generic_substitution.h"

#include <utility>

namespace hgl::ir::detail
{
    using namespace hir;

    GenericSubstitution::GenericSubstitution(Module &module, CanonicalTypes &types) : module_{module}, types_{types} {}

    bool GenericSubstitution::bind_type(SymbolId parameter, TypeId value) {
        value = types_.canonical(value);
        if (!parameter.valid() || !value.valid()) { return false; }
        const auto [found, inserted] = type_bindings_.emplace(parameter.value, value);
        return inserted || types_.same(found->second, value);
    }

    bool GenericSubstitution::bind_value(SymbolId parameter, ExprId value) {
        if (!parameter.valid() || !value.valid()) { return false; }
        const auto [found, inserted] = value_bindings_.emplace(parameter.value, value);
        return inserted || types_.same_value(found->second, value);
    }

    bool GenericSubstitution::has_type(SymbolId parameter) const noexcept {
        return parameter.valid() && type_bindings_.contains(parameter.value);
    }

    bool GenericSubstitution::has_value(SymbolId parameter) const noexcept {
        return parameter.valid() && value_bindings_.contains(parameter.value);
    }

    std::optional<TypeId> GenericSubstitution::type_binding(SymbolId parameter) const noexcept {
        if (!parameter.valid()) { return std::nullopt; }
        const auto found = type_bindings_.find(parameter.value);
        return found == type_bindings_.end() ? std::nullopt : std::optional<TypeId>{found->second};
    }

    std::optional<ExprId> GenericSubstitution::value_binding(SymbolId parameter) const noexcept {
        if (!parameter.valid()) { return std::nullopt; }
        const auto found = value_bindings_.find(parameter.value);
        return found == value_bindings_.end() ? std::nullopt : std::optional<ExprId>{found->second};
    }

    bool GenericSubstitution::unify_value(ExprId pattern, ExprId actual) {
        if (!pattern.valid() || !actual.valid()) { return pattern == actual; }
        const Expr &pattern_expr = module_.expr(pattern);
        if (const auto *reference = std::get_if<SymbolRef>(&pattern_expr.node);
            reference && reference->symbol.valid() && module_.symbol(reference->symbol).kind == SymbolKind::ConstParameter) {
            return bind_value(reference->symbol, actual);
        }
        return types_.same_value(pattern, actual);
    }

    bool GenericSubstitution::unify(TypeId pattern, TypeId actual) {
        pattern = types_.canonical(pattern);
        actual  = types_.canonical(actual);
        if (!pattern.valid() || !actual.valid()) { return false; }
        const Type &lhs = module_.type(pattern);
        const Type &rhs = module_.type(actual);
        if (lhs.kind == TypeKind::Symbol && lhs.symbol.valid() && module_.symbol(lhs.symbol).kind == SymbolKind::TypeParameter) {
            return bind_type(lhs.symbol, actual);
        }
        if (lhs.kind != rhs.kind || lhs.scalar != rhs.scalar || lhs.symbol != rhs.symbol ||
            lhs.children.size() != rhs.children.size() || lhs.arguments.size() != rhs.arguments.size() ||
            lhs.unbounded != rhs.unbounded) {
            return types_.assignable(pattern, actual);
        }
        for (std::size_t index = 0; index < lhs.children.size(); ++index) {
            if (!unify(lhs.children[index], rhs.children[index])) { return false; }
        }
        for (std::size_t index = 0; index < lhs.arguments.size(); ++index) {
            const TypeArgument &a = lhs.arguments[index];
            const TypeArgument &b = rhs.arguments[index];
            if (a.kind != b.kind) { return false; }
            if (a.kind == TypeArgumentKind::Type) {
                if (!unify(a.type, b.type)) { return false; }
            } else if (!unify_value(a.value, b.value)) {
                return false;
            }
        }
        return unify_value(lhs.size, rhs.size) && unify_value(lhs.min_size, rhs.min_size);
    }

    ExprId GenericSubstitution::apply_value(ExprId input) const noexcept {
        if (!input.valid()) { return {}; }
        if (const auto *reference = std::get_if<SymbolRef>(&module_.expr(input).node); reference && reference->symbol.valid()) {
            if (const auto found = value_bindings_.find(reference->symbol.value); found != value_bindings_.end()) {
                return found->second;
            }
        }
        return input;
    }

    TypeId GenericSubstitution::apply(TypeId input) {
        input = types_.canonical(input);
        if (!input.valid()) { return {}; }
        const Type &source = module_.type(input);
        if (source.kind == TypeKind::Symbol && source.symbol.valid()) {
            if (const auto found = type_bindings_.find(source.symbol.value); found != type_bindings_.end()) {
                return found->second;
            }
        }
        Type value = source;
        for (TypeId &child : value.children) { child = apply(child); }
        for (TypeArgument &argument : value.arguments) {
            if (argument.kind == TypeArgumentKind::Type) {
                argument.type = apply(argument.type);
            } else {
                argument.value = apply_value(argument.value);
            }
        }
        value.size     = apply_value(value.size);
        value.min_size = apply_value(value.min_size);
        return types_.intern(std::move(value));
    }

    std::vector<Substitution> GenericSubstitution::materialize(const std::vector<GenericParameter> &generics) const {
        std::vector<Substitution> result;
        result.reserve(generics.size());
        for (const GenericParameter &generic : generics) {
            Substitution value;
            value.parameter = generic.symbol;
            if (generic.is_const) {
                if (const auto found = value_bindings_.find(generic.symbol.value); found != value_bindings_.end()) {
                    value.value = found->second;
                }
            } else if (const auto found = type_bindings_.find(generic.symbol.value); found != type_bindings_.end()) {
                value.type = found->second;
            }
            result.push_back(value);
        }
        return result;
    }
}  // namespace hgl::ir::detail
