#include "ir/canonical_types.h"

#include "syntax/temporal.h"

#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

namespace hgl::ir::detail
{
    using namespace hir;

    CanonicalTypes::CanonicalTypes(Module &module, syntax::DiagnosticSink &diagnostics)
        : module_{module}, diagnostics_{diagnostics} {}

    void CanonicalTypes::initialize() {
        const std::size_t source_count = module_.types.size();
        source_canonical_.resize(source_count);
        source_visiting_.resize(source_count);
        for (std::uint32_t index = 0; index < source_count; ++index) { (void)canonicalize(TypeId{index}); }
        void_type_ = make(TypeKind::Void);
        rewrite_type_references();
    }

    TypeId CanonicalTypes::canonical(TypeId id) const noexcept {
        if (!id.valid()) { return {}; }
        const TypeId result = module_.type(id).canonical;
        return result.valid() ? result : id;
    }

    std::string CanonicalTypes::value_key(ExprId id) const {
        if (!id.valid()) { return "_"; }
        const Expr &expression = module_.expr(id);
        if (expression.constant) {
            return std::visit(
                [](const auto &value) -> std::string {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, NullValue>) {
                        return "null";
                    } else if constexpr (std::is_same_v<T, PlaceholderValue>) {
                        return "placeholder";
                    } else if constexpr (std::is_same_v<T, bool>) {
                        return value ? "b:1" : "b:0";
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        return "s:" + std::to_string(value.size()) + ':' + value;
                    } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                        return "t:" + syntax::canonical_spelling(value);
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        return "i:" + std::to_string(value);
                    } else {
                        std::ostringstream out;
                        out << "f:" << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
                        return std::move(out).str();
                    }
                },
                *expression.constant);
        }
        if (const auto *reference = std::get_if<SymbolRef>(&expression.node)) {
            return "s" + std::to_string(reference->symbol.value);
        }
        return "e" + std::to_string(id.value);
    }

    std::string CanonicalTypes::type_key(const Type &value) const {
        std::ostringstream out;
        out << static_cast<unsigned>(value.kind);
        if (value.kind == TypeKind::Scalar) { out << ':' << static_cast<unsigned>(value.scalar); }
        if (value.symbol.valid()) { out << ":s" << value.symbol.value; }
        out << '<';
        for (const TypeArgument &argument : value.arguments) {
            if (argument.kind == TypeArgumentKind::Type) {
                out << 't' << canonical(argument.type).value;
            } else {
                out << 'v' << value_key(argument.value);
            }
            out << ',';
        }
        out << ">{";
        for (TypeId child : value.children) { out << canonical(child).value << ','; }
        out << "}:" << value_key(value.size) << ':' << value_key(value.min_size) << ':' << value.unbounded;
        return out.str();
    }

    TypeId CanonicalTypes::intern(Type value) {
        for (TypeId &child : value.children) { child = canonical(child); }
        for (TypeArgument &argument : value.arguments) {
            if (argument.kind == TypeArgumentKind::Type) { argument.type = canonical(argument.type); }
        }
        value.range           = {};
        value.value_position  = false;
        value.canonical       = {};
        const std::string key = type_key(value);
        if (const auto found = type_intern_.find(key); found != type_intern_.end()) { return found->second; }
        const TypeId result{static_cast<std::uint32_t>(module_.types.size())};
        value.canonical = result;
        module_.types.push_back(std::move(value));
        type_intern_.emplace(key, result);
        return result;
    }

    TypeId CanonicalTypes::make(TypeKind kind, std::vector<TypeId> children, SymbolId symbol) {
        Type value;
        value.kind     = kind;
        value.children = std::move(children);
        value.symbol   = symbol;
        return intern(std::move(value));
    }

    TypeId CanonicalTypes::scalar(ScalarType value) {
        Type result;
        result.kind   = TypeKind::Scalar;
        result.scalar = value;
        return intern(std::move(result));
    }

    TypeId CanonicalTypes::canonicalize(TypeId id) {
        if (!id.valid()) { return {}; }
        if (id.value >= source_canonical_.size()) { return canonical(id); }
        if (source_canonical_[id.value].valid()) { return source_canonical_[id.value]; }
        if (source_visiting_[id.value]) {
            diagnostics_.report(syntax::Category::Type, module_.type(id).range, "recursive structural type alias");
            return {};
        }
        source_visiting_[id.value] = true;
        Type value                 = module_.type(id);
        // A rolling window defaults its minimum to its maximum. Normalize
        // before interning so generic max/min patterns see the same shape.
        if (value.kind == TypeKind::Rolling && value.size.valid() && !value.min_size.valid()) { value.min_size = value.size; }
        for (TypeId &child : value.children) { child = canonicalize(child); }
        for (TypeArgument &argument : value.arguments) {
            if (argument.kind == TypeArgumentKind::Type) { argument.type = canonicalize(argument.type); }
        }
        const TypeId result               = intern(std::move(value));
        source_canonical_[id.value]       = result;
        source_visiting_[id.value]        = false;
        module_.types[id.value].canonical = result;
        return result;
    }

    void CanonicalTypes::rewrite_signature(Signature &signature) {
        for (Parameter &parameter : signature.parameters) { parameter.type = canonical(parameter.type); }
        signature.result = signature.result.valid() ? canonical(signature.result) : void_type_;
    }

    void CanonicalTypes::rewrite_type_references() {
        for (Symbol &symbol : module_.symbols) {
            if (symbol.type.valid()) { symbol.type = canonical(symbol.type); }
        }
        for (Expr &expression : module_.exprs) {
            if (expression.type.valid()) { expression.type = canonical(expression.type); }
            if (auto *lambda = std::get_if<Lambda>(&expression.node); lambda && lambda->result.valid()) {
                lambda->result = canonical(lambda->result);
            } else if (auto *construct = std::get_if<Construct>(&expression.node)) {
                construct->type = canonical(construct->type);
            }
        }
        for (Stmt &statement : module_.stmts) {
            if (auto *local = std::get_if<LocalDecl>(&statement.node); local && local->type.valid()) {
                local->type = canonical(local->type);
            } else if (auto *state = std::get_if<StateDecl>(&statement.node); state && state->type.valid()) {
                state->type = canonical(state->type);
            }
        }
        for (Constraint &constraint : module_.constraints) {
            if (auto *constraint_type = std::get_if<ConstraintType>(&constraint.node)) {
                constraint_type->type = canonical(constraint_type->type);
            } else if (auto *requirement = std::get_if<OperatorRequirement>(&constraint.node);
                       requirement && requirement->result.valid()) {
                requirement->result = canonical(requirement->result);
            }
        }
        for (Declaration &declaration : module_.declarations) {
            std::visit(
                [&](auto &node) {
                    using T = std::decay_t<decltype(node)>;
                    if constexpr (std::is_same_v<T, StructDecl>) {
                        for (GenericParameter &generic : node.generics) {
                            if (generic.type.valid()) { generic.type = canonical(generic.type); }
                        }
                        for (TypeId &parent : node.parents) { parent = canonical(parent); }
                        for (StructField &field : node.fields) { field.type = canonical(field.type); }
                    } else if constexpr (std::is_same_v<T, OperatorDecl> || std::is_same_v<T, FunctionDecl>) {
                        for (GenericParameter &generic : node.generics) {
                            if (generic.type.valid()) { generic.type = canonical(generic.type); }
                        }
                        rewrite_signature(node.signature);
                    }
                },
                declaration.node);
        }
    }

    bool CanonicalTypes::same(TypeId lhs, TypeId rhs) const noexcept {
        return lhs.valid() && rhs.valid() && canonical(lhs) == canonical(rhs);
    }

    bool CanonicalTypes::numeric(TypeId id) const noexcept {
        if (!id.valid()) { return false; }
        const Type &value = module_.type(canonical(id));
        return value.kind == TypeKind::Scalar && (value.scalar == ScalarType::I64 || value.scalar == ScalarType::F64);
    }

    bool CanonicalTypes::boolean(TypeId id) const noexcept {
        if (!id.valid()) { return false; }
        const Type &value = module_.type(canonical(id));
        return value.kind == TypeKind::Scalar && value.scalar == ScalarType::Bool;
    }

    bool CanonicalTypes::assignable(TypeId expected, TypeId actual) const noexcept {
        expected = canonical(expected);
        actual   = canonical(actual);
        if (same(expected, actual)) { return true; }
        if (!expected.valid() || !actual.valid()) { return false; }
        const Type &to   = module_.type(expected);
        const Type &from = module_.type(actual);
        if (to.kind == TypeKind::Scalar && from.kind == TypeKind::Scalar && to.scalar == ScalarType::F64 &&
            from.scalar == ScalarType::I64) {
            return true;
        }
        if (to.kind == TypeKind::Atomic && !to.children.empty() && same(to.children.front(), actual)) { return true; }
        if (from.kind == TypeKind::Atomic && !from.children.empty() && same(expected, from.children.front())) { return true; }
        return false;
    }

    bool CanonicalTypes::same_value(ExprId lhs, ExprId rhs) const { return value_key(lhs) == value_key(rhs); }

    std::string CanonicalTypes::name(TypeId id) const {
        if (!id.valid()) { return "<unresolved>"; }
        const Type &value = module_.type(canonical(id));
        switch (value.kind) {
            case TypeKind::Void: return "void";
            case TypeKind::Scalar: return std::string{scalar_type_name(value.scalar)};
            case TypeKind::Symbol: return value.symbol.valid() ? module_.symbol(value.symbol).name : "<type>";
            case TypeKind::Tuple: return "tuple";
            case TypeKind::List: return "list";
            case TypeKind::Set: return "set";
            case TypeKind::Map: return "map";
            case TypeKind::Rolling: return "rolling";
            case TypeKind::Atomic: return "atomic";
            case TypeKind::Iterator: return "iterator";
            case TypeKind::Callable: return "fn";
            case TypeKind::Capability: return "capability";
            case TypeKind::HarnessSequence: return "harness sequence";
            case TypeKind::Deferred: return "<deferred>";
        }
        std::unreachable();
    }
}  // namespace hgl::ir::detail
