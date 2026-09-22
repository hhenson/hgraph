#include "wiring/type_bridge.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgl::wiring
{
    /// Counts the nominal structs currently being realized, so the chain's
    /// length is answered by a diagnostic rather than by the stack. It is a
    /// member of the bridge, not a thread-local: two bridges realize
    /// independently, and nothing here is per-thread.
    struct TypeBridge::NominalDepth
    {
        explicit NominalDepth(TypeBridge &bridge) : bridge_{bridge} { ++bridge_.nominal_depth_; }
        NominalDepth(const NominalDepth &)            = delete;
        NominalDepth &operator=(const NominalDepth &) = delete;
        ~NominalDepth() { --bridge_.nominal_depth_; }

        [[nodiscard]] bool within_bound() const { return bridge_.nominal_depth_ <= TypeBridge::max_nominal_depth; }

      private:
        TypeBridge &bridge_;
    };

    namespace
    {
        namespace hir = ir::hir;

        const hgraph::ValueTypeMetaData *scalar_meta(hir::ScalarType                                type,
                                                     const hgraph::stdlib::RegisteredStandardTypes &types) noexcept {
            switch (type) {
                case hir::ScalarType::Bool: return types.bool_type;
                case hir::ScalarType::I64: return types.int_type;
                case hir::ScalarType::F64: return types.float_type;
                case hir::ScalarType::Str: return types.str_type;
                case hir::ScalarType::Date: return types.date_type;
                case hir::ScalarType::Time: return types.time_type;
                case hir::ScalarType::DateTime: return types.datetime_type;
                case hir::ScalarType::Duration: return types.timedelta_type;
                case hir::ScalarType::CivilDateTime: return types.civil_datetime_type;
                case hir::ScalarType::ZonedDateTime: return types.zoned_datetime_type;
                case hir::ScalarType::TimeZone: return types.zone_id_type;
                case hir::ScalarType::ZonedTime: return nullptr;
            }
            std::unreachable();
        }

        std::pair<std::string, std::string> split_identity(std::string_view identity) {
            const std::size_t separator = identity.rfind('.');
            if (separator == std::string_view::npos) { return {{}, std::string{identity}}; }
            return {std::string{identity.substr(0, separator)}, std::string{identity.substr(separator + 1U)}};
        }
    }  // namespace

    TypeBridge::TypeBridge(const hgraph_ir::Module &module, syntax::DiagnosticSink &diagnostics)
        : module_{module}, diagnostics_{diagnostics}, registry_{hgraph::TypeRegistry::instance()},
          types_{hgraph::stdlib::register_standard_types(registry_)}, generation_{registry_.reset_generation()} {
        structures_.reserve(module_.structures.size());
        for (const hgraph_ir::StructContract &contract : module_.structures) { structures_.emplace(contract.identity, &contract); }
    }

    void TypeBridge::refresh_registry() {
        const std::uint64_t current = registry_.reset_generation();
        if (generation_ == current) { return; }
        values_.clear();
        schemas_.clear();
        types_      = hgraph::stdlib::register_standard_types(registry_);
        generation_ = registry_.reset_generation();
    }

    void TypeBridge::report(syntax::SourceRange range, std::string message) {
        diagnostics_.report(syntax::Category::Backend, range, std::move(message));
    }

    const hgraph_ir::StructContract *TypeBridge::structure(std::string_view identity) const noexcept {
        const auto found = structures_.find(identity);
        return found == structures_.end() ? nullptr : found->second;
    }

    hgraph_ir::TypeId TypeBridge::resolved(hgraph_ir::TypeId type, const Bindings &bindings) const {
        if (!type.valid() || type.value >= module_.types.size()) { return type; }
        const hgraph_ir::Type &value = module_.types[type.value];
        if (value.kind != hir::TypeKind::Symbol || !value.binding.valid()) { return type; }
        const auto found = bindings.types.find(value.binding.value);
        return found == bindings.types.end() ? type : found->second;
    }

    std::optional<TypeBridge::Bindings> TypeBridge::bind(const hgraph_ir::Type &type, const hgraph_ir::StructContract &contract,
                                                         const Bindings &outer) {
        if (type.arguments.size() != contract.generics.size()) {
            report(type.range, "nominal type '" + type.nominal_identity + "' has an incomplete generic application");
            return std::nullopt;
        }
        Bindings result = outer;
        for (std::size_t index = 0; index < contract.generics.size(); ++index) {
            const hgraph_ir::GenericParameter &generic  = contract.generics[index];
            const hgraph_ir::TypeArgument     &argument = type.arguments[index];
            if (generic.is_const) {
                if (!generic.binding.valid() || !argument.value) {
                    report(type.range, "const generic '" + generic.name + "' requires a value argument");
                    return std::nullopt;
                }
                result.values[generic.binding.value] = *argument.value;
            } else {
                if (!generic.binding.valid() || !argument.type) {
                    report(type.range, "type generic '" + generic.name + "' requires a type argument");
                    return std::nullopt;
                }
                // An argument is read in the scope that applies it: `Tree<T>` inside
                // `Tree<T>` names the outer `T`, not the parameter it binds.
                result.types[generic.binding.value] = resolved(*argument.type, outer);
            }
        }
        return result;
    }

    std::optional<hgraph::Value> TypeBridge::literal(hgraph_ir::ConstExprId expression) {
        refresh_registry();
        if (!expression.valid() || expression.value >= module_.const_exprs.size()) { return std::nullopt; }
        const hgraph_ir::ConstExpr &source = module_.const_exprs[expression.value];
        if (source.kind != hgraph_ir::ConstExprKind::Literal || !source.literal) {
            report(source.range, "the hgraph type bridge requires a folded constant literal");
            return std::nullopt;
        }
        return std::visit(
            [&](const auto &item) -> std::optional<hgraph::Value> {
                using T = std::decay_t<decltype(item)>;
                if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
                              std::is_same_v<T, std::string>) {
                    return hgraph::Value{item};
                } else if constexpr (std::is_same_v<T, syntax::TemporalValue>) {
                    using syntax::TemporalKind;
                    switch (item.kind) {
                        case TemporalKind::Date:
                            return hgraph::Value{hgraph::Date{std::chrono::sys_days{std::chrono::days{item.micros}}}};
                        case TemporalKind::Time: return hgraph::Value{hgraph::Time{item.micros}};
                        case TemporalKind::DateTime: return hgraph::Value{hgraph::DateTime{std::chrono::microseconds{item.micros}}};
                        case TemporalKind::Duration: return hgraph::Value{hgraph::TimeDelta{item.micros}};
                        case TemporalKind::CivilDateTime:
                        case TemporalKind::ZonedDateTime:
                        case TemporalKind::ZonedTime:
                        case TemporalKind::TimeZone:
                            report(source.range, "zoned and civil constants are not supported by the direct backend yet");
                            return std::nullopt;
                    }
                    std::unreachable();
                } else {
                    return std::nullopt;
                }
            },
            *source.literal);
    }

    std::optional<std::int64_t> TypeBridge::integer(hgraph_ir::ConstExprId expression, syntax::SourceRange range,
                                                    std::string_view role) {
        const std::optional<hgraph::Value> value = literal(expression);
        if (value && value->schema() == types_.int_type) {
            const std::int64_t result = value->view().checked_as<hgraph::Int>();
            if (result >= 0) { return result; }
        }
        report(range, std::string{role} + " must be a non-negative i64 constant");
        return std::nullopt;
    }

    std::optional<TypeBridge::Specialization> TypeBridge::specialize(const hgraph_ir::Type &type, const Bindings &outer) {
        const hgraph_ir::StructContract *contract = structure(type.nominal_identity);
        if (contract == nullptr) {
            report(type.range, "unknown nominal type '" + type.nominal_identity + "'");
            return std::nullopt;
        }
        std::optional<Bindings> applied = bind(type, *contract, outer);
        if (!applied) { return std::nullopt; }

        Specialization result{.contract = contract, .applied = std::move(*applied)};
        result.local_name = split_identity(contract->identity).second;
        if (!contract->generics.empty()) { result.local_name += '['; }
        for (std::size_t index = 0; index < contract->generics.size(); ++index) {
            // The static schema's spelling (`Pair[int, str]`), so both backends
            // register one specialization under one name.
            if (index != 0) { result.local_name += ", "; }
            const hgraph_ir::GenericParameter &generic = contract->generics[index];
            if (generic.is_const) {
                report(type.range, "const generic struct arguments require typed constant Bundle metadata in hgraph");
                return std::nullopt;
            }
            const hgraph::ValueTypeMetaData *argument = value(result.applied.types.at(generic.binding.value), result.applied);
            if (argument == nullptr) { return std::nullopt; }
            result.generic_types.push_back(argument);
            result.local_name += argument->name();
        }
        if (!contract->generics.empty()) { result.local_name += ']'; }
        result.module_name = split_identity(contract->identity).first;
        return result;
    }

    std::optional<TypeBridge::Specialization> TypeBridge::recursive_target(const hgraph_ir::StructField &field,
                                                                           const Bindings               &applied) {
        const hgraph_ir::Type &boundary = module_.types[field.type.value];
        if (boundary.kind != hir::TypeKind::Atomic || boundary.children.size() != 1U) {
            report(field.range, "hgraph IR recursive edge '" + field.name + "' is not an atomic boundary");
            return std::nullopt;
        }
        return specialize(module_.types[resolved(boundary.children.front(), applied).value], applied);
    }

    const hgraph::ValueTypeMetaData *TypeBridge::field_value(const hgraph_ir::StructField &field, const Bindings &applied) {
        if (!field.recursive) { return value(field.type, applied); }
        // A recursive edge holds its target through one owner pointer, so a
        // value is a finite tree and the target's fields are never inlined.
        const hgraph_ir::Type           &boundary = module_.types[field.type.value];
        const hgraph::ValueTypeMetaData *target =
            boundary.children.size() == 1U ? value(boundary.children.front(), applied) : nullptr;
        return target == nullptr ? nullptr : registry_.owned(target);
    }

    /// The recursive path's preflight (ADR 0013, acceptance 4).
    ///
    /// It cannot rely on the registry refusing a disagreement the way the
    /// plain path does: `recursive_bundle_closure` answers from the registered
    /// type as soon as the name is known and never calls the describer, so an
    /// importer built against a changed layout would silently receive the
    /// other one's. Comparing names and arity is not enough either -- renaming
    /// a field's TYPE is ordinary version skew and leaves both unchanged -- so
    /// the description is compared field type by field type, plus the parents,
    /// abstractness and generic arguments that make two same-shaped bundles
    /// different schemas.
    ///
    /// A RECURSIVE field is compared structurally rather than by realizing it:
    /// realizing the edge would need the very type being checked. The edge
    /// must be an owner, and of a bundle of the declared schema.
    const hgraph::ValueTypeMetaData *TypeBridge::registered(const Specialization &specialization, syntax::SourceRange range) {
        const hgraph::ValueTypeMetaData *existing = registry_.value_type(specialization.qualified());
        if (existing == nullptr) { return nullptr; }
        const std::vector<hgraph_ir::StructField> &fields = specialization.contract->fields;
        // Reporting is not enough: `value()` caches whatever this returns and
        // the backend aborts only on a NULL result, so handing back the
        // incompatible metadata would let a run continue against the wrong
        // field layout and merely print a diagnostic afterwards.
        const auto disagrees = [&](std::string_view what) -> const hgraph::ValueTypeMetaData * {
            report(range, "cannot register struct '" + specialization.local_name + "': a different schema is already " +
                              "registered under that name (" + std::string{what} + ")");
            return nullptr;
        };
        if (!existing->is_named_bundle() || existing->field_count != fields.size()) { return disagrees("field count"); }
        if (existing->is_abstract_bundle() != specialization.contract->abstract) { return disagrees("abstract"); }

        const auto *hierarchy = existing->bundle_hierarchy;
        if (hierarchy == nullptr) { return disagrees("hierarchy"); }
        if (hierarchy->parents.size() != specialization.contract->parents.size()) { return disagrees("parents"); }
        for (std::size_t index = 0; index < specialization.contract->parents.size(); ++index) {
            const hgraph::ValueTypeMetaData *parent = value(specialization.contract->parents[index], specialization.applied);
            if (parent == nullptr) { return nullptr; }
            if (parent != hierarchy->parents[index]) { return disagrees("parent '" + std::string{parent->name()} + "'"); }
        }
        // The bridge never sets these, so a registered schema that carries
        // anything but the defaults is a different schema: it tags its
        // polymorphic alternatives differently, which the non-recursive
        // `bundle()` path already refuses.
        if (existing->bundle_discriminator() != std::string_view{"__type__"}) { return disagrees("discriminator"); }
        if (hierarchy->discriminator_value != nullptr &&
            std::string_view{hierarchy->discriminator_value} != std::string_view{existing->name()}) {
            return disagrees("discriminator value");
        }
        if (hierarchy->generic_arguments.size() != specialization.generic_types.size()) {
            return disagrees("generic arguments");
        }
        for (std::size_t index = 0; index < specialization.generic_types.size(); ++index) {
            if (hierarchy->generic_arguments[index] != specialization.generic_types[index]) {
                return disagrees("generic arguments");
            }
        }

        for (std::size_t index = 0; index < fields.size(); ++index) {
            const hgraph_ir::StructField    &field    = fields[index];
            const hgraph::ValueTypeMetaData *declared = existing->fields[index].type;
            if (existing->fields[index].name == nullptr || field.name != existing->fields[index].name) {
                return disagrees("field '" + field.name + "'");
            }
            if (field.recursive) {
                // The edge is compared by the TARGET IT NAMES rather than
                // realized: realizing it would need the very type being
                // checked. "An owner of some named bundle" is not enough --
                // an edge that owns a different struct is exactly the skew
                // this preflight exists to catch, and the closure never asks
                // the describer once the name is registered.
                if (declared == nullptr || !declared->is_owned() || declared->element_type == nullptr ||
                    !declared->element_type->is_named_bundle()) {
                    return disagrees("recursive field '" + field.name + "'");
                }
                const std::optional<Specialization> target = recursive_target(field, specialization.applied);
                if (!target) { return nullptr; }
                if (std::string{declared->element_type->name()} != target->qualified()) {
                    return disagrees("recursive field '" + field.name + "' targets '" +
                                     std::string{declared->element_type->name()} + "'");
                }
                continue;
            }
            const hgraph::ValueTypeMetaData *described = field_value(field, specialization.applied);
            if (described == nullptr) { return nullptr; }
            if (described != declared) { return disagrees("field '" + field.name + "'"); }
        }
        return existing;
    }

    const hgraph::ValueTypeMetaData *TypeBridge::register_value(const Specialization &specialization, syntax::SourceRange range) {
        std::vector<std::pair<std::string, const hgraph::ValueTypeMetaData *>> fields;
        fields.reserve(specialization.contract->fields.size());
        for (const hgraph_ir::StructField &field : specialization.contract->fields) {
            const hgraph::ValueTypeMetaData *field_type = field_value(field, specialization.applied);
            if (field_type == nullptr) { return nullptr; }
            fields.emplace_back(field.name, field_type);
        }

        std::vector<const hgraph::ValueTypeMetaData *> parents;
        parents.reserve(specialization.contract->parents.size());
        for (hgraph_ir::TypeId parent : specialization.contract->parents) {
            const hgraph::ValueTypeMetaData *parent_type = value(parent, specialization.applied);
            if (parent_type == nullptr) { return nullptr; }
            parents.push_back(parent_type);
        }

        try {
            return registry_.bundle(specialization.module_name, specialization.local_name, fields, parents,
                                    specialization.contract->abstract, "__type__", specialization.generic_types);
        } catch (const std::exception &error) {
            report(range, "cannot register struct '" + specialization.local_name + "': " + error.what());
            return nullptr;
        }
    }

    const hgraph::ValueTypeMetaData *TypeBridge::nominal_value(const hgraph_ir::Type &type, const Bindings &outer) {
        const NominalDepth depth{*this};
        if (!depth.within_bound()) {
            report(type.range, "nominal type '" + type.nominal_identity + "' nests more than " +
                                   std::to_string(max_nominal_depth) + " structs deep, which this bridge cannot realize");
            return nullptr;
        }
        std::optional<Specialization> specialization = specialize(type, outer);
        if (!specialization) { return nullptr; }
        if (std::ranges::any_of(specialization->contract->fields,
                                [](const hgraph_ir::StructField &field) { return field.recursive; })) {
            return recursive_value(std::move(*specialization), type.range);
        }
        return register_value(*specialization, type.range);
    }

    /// Realizes a struct with recursive edges (ADR 0012) through hgraph's
    /// recursive Bundle closure (RFC 0041): the registry walks the
    /// specializations the edges reach and registers each strongly connected
    /// component as one batch, the same rule the static schema's `Edge` uses for
    /// generated C++, so both backends register identical schemas. This bridge
    /// only describes each specialization when the registry asks for it.
    const hgraph::ValueTypeMetaData *TypeBridge::recursive_value(Specialization root, syntax::SourceRange range) {
        // A realization failure is already reported; this unwinds the closure.
        struct Reported
        {};
        const std::string root_name = root.qualified();

        // The WHOLE closure, collected before anything is decided: the root and
        // every specialization its edges reach. The registry describes only
        // what is not yet registered, so a member that is already there would
        // never be compared -- and registering `A { next: atomic<B> }` against
        // somebody else's `B` is as wrong as registering somebody else's `A`.
        // Comparing an edge by the target it NAMES is only sufficient because
        // the target is checked as a member in its own right.
        std::unordered_map<std::string, Specialization> pending;
        {
            std::vector<Specialization> work;
            work.push_back(std::move(root));
            while (!work.empty()) {
                Specialization    current = std::move(work.back());
                const std::string name    = current.qualified();
                work.pop_back();
                if (pending.contains(name)) { continue; }
                const Specialization &member = pending.emplace(name, std::move(current)).first->second;
                for (const hgraph_ir::StructField &field : member.contract->fields) {
                    if (!field.recursive) { continue; }
                    std::optional<Specialization> target = recursive_target(field, member.applied);
                    if (!target) { return nullptr; }
                    work.push_back(std::move(*target));
                }
            }
        }

        // Every member that is already registered has to agree with this
        // module's description of it, whether or not the root is one of them.
        const auto members_agree = [&]() {
            return std::ranges::all_of(pending, [&](const auto &entry) {
                return registry_.value_type(entry.first) == nullptr || registered(entry.second, range) != nullptr;
            });
        };
        if (!members_agree()) { return nullptr; }
        if (const hgraph::ValueTypeMetaData *existing = registry_.value_type(root_name)) {
            // Another bridge may have registered the closure between the
            // comparison above and this lookup, so the root being there is not
            // evidence that it agrees. Every return goes through the same
            // comparison; none is a shortcut past it.
            return members_agree() ? existing : nullptr;
        }

        const auto describe = [&](std::string_view name) -> hgraph::RecursiveBundleRequest {
            const auto found = pending.find(std::string{name});
            if (found == pending.end()) { throw std::logic_error("undescribed recursive struct '" + std::string{name} + "'"); }
            const Specialization          &specialization = found->second;
            hgraph::RecursiveBundleRequest request;
            request.definition.bundle_namespace  = specialization.module_name;
            request.definition.local_name        = specialization.local_name;
            request.definition.is_abstract       = specialization.contract->abstract;
            request.definition.generic_arguments = specialization.generic_types;
            for (const hgraph_ir::StructField &field : specialization.contract->fields) {
                if (field.recursive) {
                    std::optional<Specialization> target = recursive_target(field, specialization.applied);
                    if (!target) { throw Reported{}; }
                    std::string target_name = target->qualified();
                    request.edges.emplace_back(request.definition.fields.size(), target_name);
                    pending.try_emplace(std::move(target_name), std::move(*target));
                    request.definition.fields.push_back({.name = field.name});
                    continue;
                }
                const hgraph::ValueTypeMetaData *field_type = value(field.type, specialization.applied);
                if (field_type == nullptr) { throw Reported{}; }
                request.definition.fields.push_back({.name = field.name, .type = field_type});
            }
            for (hgraph_ir::TypeId parent : specialization.contract->parents) {
                const hgraph::ValueTypeMetaData *parent_type = value(parent, specialization.applied);
                if (parent_type == nullptr) { throw Reported{}; }
                request.definition.parents.push_back(parent_type);
            }
            return request;
        };
        try {
            const hgraph::ValueTypeMetaData *closed = registry_.recursive_bundle_closure(root_name, describe);
            if (closed == nullptr) { return nullptr; }
            // Describing a closure is not the same as agreeing with what got
            // registered. The closure accepts whichever batch closed first,
            // under its own lock, without comparing descriptions -- so a
            // bridge that loses that race would cache the other's layout even
            // though nothing was registered to compare against on the way in.
            // Re-running the same comparison over every member makes the check
            // total: first or not, what is registered has to be what was
            // described.
            return members_agree() ? closed : nullptr;
        } catch (const Reported &) { return nullptr; } catch (const std::exception &error) {
            report(range, "cannot register recursive struct '" + root_name + "': " + error.what());
            return nullptr;
        }
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::nominal_schema(const hgraph_ir::Type &type, const Bindings &outer) {
        // The temporal side descends the same chain independently -- it asks
        // `nominal_value` first, but that guard has unwound by the time this
        // recurses into a field's schema -- so it shares the counter.
        const NominalDepth depth{*this};
        if (!depth.within_bound()) {
            report(type.range, "nominal type '" + type.nominal_identity + "' nests more than " +
                                   std::to_string(max_nominal_depth) + " structs deep, which this bridge cannot realize");
            return nullptr;
        }
        const hgraph_ir::StructContract *contract = structure(type.nominal_identity);
        if (contract == nullptr) {
            report(type.range, "unknown nominal type '" + type.nominal_identity + "'");
            return nullptr;
        }
        const std::optional<Bindings> applied = bind(type, *contract, outer);
        if (!applied) { return nullptr; }
        const hgraph::ValueTypeMetaData *value_type = nominal_value(type, outer);
        if (value_type == nullptr) { return nullptr; }

        std::vector<std::pair<std::string, const hgraph::TSValueTypeMetaData *>> fields;
        fields.reserve(contract->fields.size());
        for (const hgraph_ir::StructField &field : contract->fields) {
            // A recursive edge is one endpoint carrying a complete target or
            // nothing (ADR 0012, rule 3). Its value is the owner the struct
            // stores, so the bundle's value schema is the struct itself; hgraph
            // treats the owner as storage, equivalent to TS[target] at binding.
            const hgraph::ValueTypeMetaData   *owner = field.recursive ? field_value(field, *applied) : nullptr;
            const hgraph::TSValueTypeMetaData *field_type =
                field.recursive ? (owner == nullptr ? nullptr : registry_.ts(owner)) : schema(field.type, *applied);
            if (field_type == nullptr) { return nullptr; }
            fields.emplace_back(field.name, field_type);
        }
        try {
            return registry_.tsb(value_type->name(), fields);
        } catch (const std::exception &error) {
            report(type.range, "cannot register temporal struct '" + std::string{value_type->name()} + "': " + error.what());
            return nullptr;
        }
    }

    const hgraph::ValueTypeMetaData *TypeBridge::value(hgraph_ir::TypeId id) {
        refresh_registry();
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        if (const auto found = values_.find(id.value); found != values_.end()) { return found->second; }
        const hgraph::ValueTypeMetaData *result = value(id, Bindings{});
        if (result != nullptr) { values_.emplace(id.value, result); }
        return result;
    }

    const hgraph::ValueTypeMetaData *TypeBridge::value(hgraph_ir::TypeId id, const Bindings &bindings) {
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Scalar:
                {
                    const hgraph::ValueTypeMetaData *result = scalar_meta(type.scalar, types_);
                    if (result == nullptr) { report(type.range, "unsupported scalar value type"); }
                    return result;
                }
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    if (const auto generic = bindings.types.find(type.binding.value); generic != bindings.types.end()) {
                        if (generic->second == id) {
                            report(type.range, "generic type '" + type.nominal_identity + "' is not concretely bound");
                            return nullptr;
                        }
                        return value(generic->second, bindings);
                    }
                }
                return nominal_value(type, bindings);
            case hir::TypeKind::Tuple:
                {
                    std::vector<const hgraph::ValueTypeMetaData *> elements;
                    elements.reserve(type.children.size());
                    for (hgraph_ir::TypeId child : type.children) {
                        const hgraph::ValueTypeMetaData *element = value(child, bindings);
                        if (element == nullptr) { return nullptr; }
                        elements.push_back(element);
                    }
                    return registry_.tuple(elements);
                }
            case hir::TypeKind::List:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    if (!type.unbounded && type.size.valid()) {
                        const std::optional<std::int64_t> count = integer(type.size, type.range, "a list size");
                        if (!count) { return nullptr; }
                        return registry_.fixed_list(element, static_cast<std::size_t>(*count));
                    }
                    return registry_.list(element);
                }
            case hir::TypeKind::Set:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings)) {
                        return registry_.set(element);
                    }
                }
                return nullptr;
            case hir::TypeKind::Map:
                if (type.children.size() == 2U) {
                    const hgraph::ValueTypeMetaData *key    = value(type.children[0], bindings);
                    const hgraph::ValueTypeMetaData *mapped = value(type.children[1], bindings);
                    if (key != nullptr && mapped != nullptr) { return registry_.map(key, mapped); }
                }
                return nullptr;
            case hir::TypeKind::Atomic:
                if (!type.children.empty()) { return value(type.children.front(), bindings); }
                return nullptr;
            case hir::TypeKind::Reference:
                report(type.range, "'ref' has no value type; it is an opaque time-series reference");
                return nullptr;
            case hir::TypeKind::Signal:
                report(type.range, "'signal' has no value type; it is an input-only observation marker");
                return nullptr;
            case hir::TypeKind::Schema:
            case hir::TypeKind::SchemaView: report(type.range, "runtime schema metadata has no hgraph value type"); return nullptr;
            case hir::TypeKind::Rolling:
                report(type.range, "'rolling' has no value type; it is a time-series window");
                return nullptr;
            case hir::TypeKind::Void:
            case hir::TypeKind::Iterator:
            case hir::TypeKind::Callable:
            case hir::TypeKind::Capability:
            case hir::TypeKind::HarnessSequence:
            case hir::TypeKind::Deferred: break;
        }
        report(type.range, "unsupported hgraph value type");
        return nullptr;
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::schema(hgraph_ir::TypeId id) {
        refresh_registry();
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        if (const auto found = schemas_.find(id.value); found != schemas_.end()) { return found->second; }
        const hgraph::TSValueTypeMetaData *result = schema(id, Bindings{});
        if (result != nullptr) { schemas_.emplace(id.value, result); }
        return result;
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::schema(hgraph_ir::TypeId id, const Bindings &bindings) {
        if (!id.valid() || id.value >= module_.types.size()) { return nullptr; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Scalar:
                {
                    const hgraph::ValueTypeMetaData *meta = scalar_meta(type.scalar, types_);
                    if (meta != nullptr) { return registry_.ts(meta); }
                    report(type.range, "unsupported scalar time-series type");
                    return nullptr;
                }
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    if (const auto generic = bindings.types.find(type.binding.value); generic != bindings.types.end()) {
                        if (generic->second == id) {
                            report(type.range, "generic type '" + type.nominal_identity + "' is not concretely bound");
                            return nullptr;
                        }
                        return schema(generic->second, bindings);
                    }
                }
                return nominal_schema(type, bindings);
            case hir::TypeKind::Tuple:
                report(type.range, "a structural tuple has no time-series schema; use atomic<tuple<...>> for one value");
                return nullptr;
            case hir::TypeKind::List:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::TSValueTypeMetaData *element = schema(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    std::size_t size = hgraph::unbounded_tsl_size;
                    if (!type.unbounded && type.size.valid()) {
                        const std::optional<std::int64_t> count = integer(type.size, type.range, "a list size");
                        if (!count) { return nullptr; }
                        size = static_cast<std::size_t>(*count);
                    }
                    return registry_.tsl(element, size);
                }
            case hir::TypeKind::Set:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings)) {
                        return registry_.tss(element);
                    }
                }
                return nullptr;
            case hir::TypeKind::Map:
                if (type.children.size() == 2U) {
                    const hgraph::ValueTypeMetaData   *key    = value(type.children[0], bindings);
                    const hgraph::TSValueTypeMetaData *mapped = schema(type.children[1], bindings);
                    if (key != nullptr && mapped != nullptr) { return registry_.tsd(key, mapped); }
                }
                return nullptr;
            case hir::TypeKind::Rolling:
                {
                    if (type.children.empty()) { break; }
                    const hgraph::ValueTypeMetaData *element = value(type.children.front(), bindings);
                    if (element == nullptr) { return nullptr; }
                    const std::optional<hgraph::Value> period_value  = literal(type.size);
                    const std::optional<hgraph::Value> minimum_value = literal(type.min_size);
                    if (!period_value || !minimum_value) { return nullptr; }
                    // The checker owns the size rules (type_check.cpp, check_type_shape);
                    // what remains here guards the materialization against a
                    // typed HIR that did not enforce them.
                    if (period_value->schema() == types_.timedelta_type) {
                        if (minimum_value->schema() != types_.timedelta_type) {
                            report(type.range, "typed HIR admitted a rolling duration with a non-duration minimum");
                            return nullptr;
                        }
                        const hgraph::TimeDelta period  = period_value->view().checked_as<hgraph::TimeDelta>();
                        const hgraph::TimeDelta minimum = minimum_value->view().checked_as<hgraph::TimeDelta>();
                        if (period <= hgraph::TimeDelta{0} || minimum < hgraph::TimeDelta{0} || minimum > period) {
                            report(type.range, "typed HIR admitted an invalid rolling duration");
                            return nullptr;
                        }
                        return registry_.tsw_duration(element, period, minimum);
                    }
                    if (period_value->schema() != types_.int_type || minimum_value->schema() != types_.int_type) {
                        report(type.range, "typed HIR admitted a rolling size that is neither i64 nor duration");
                        return nullptr;
                    }
                    const std::int64_t period  = period_value->view().checked_as<hgraph::Int>();
                    const std::int64_t minimum = minimum_value->view().checked_as<hgraph::Int>();
                    if (period <= 0 || minimum <= 0 || minimum > period) {
                        report(type.range, "typed HIR admitted an invalid rolling minimum size");
                        return nullptr;
                    }
                    return registry_.tsw(element, static_cast<std::size_t>(period), static_cast<std::size_t>(minimum));
                }
            case hir::TypeKind::Atomic:
                if (!type.children.empty()) {
                    if (const hgraph::ValueTypeMetaData *meta = value(type.children.front(), bindings)) {
                        return registry_.ts(meta);
                    }
                }
                return nullptr;
            case hir::TypeKind::Reference:
                if (!type.children.empty()) {
                    if (const hgraph::TSValueTypeMetaData *target = schema(type.children.front(), bindings)) {
                        return registry_.ref(target);
                    }
                }
                return nullptr;
            case hir::TypeKind::Signal: return registry_.signal();
            case hir::TypeKind::Schema:
            case hir::TypeKind::SchemaView:
            case hir::TypeKind::Void:
            case hir::TypeKind::Iterator:
            case hir::TypeKind::Callable:
            case hir::TypeKind::Capability:
            case hir::TypeKind::HarnessSequence:
            case hir::TypeKind::Deferred: break;
        }
        report(type.range, "unsupported hgraph time-series type");
        return nullptr;
    }
}  // namespace hgl::wiring
