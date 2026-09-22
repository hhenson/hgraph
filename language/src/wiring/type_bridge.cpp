#include "wiring/type_bridge.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgl::wiring
{
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
        // The memos hold registry pointers, so a reset invalidates them along
        // with everything else this bridge cached.
        realized_.clear();
        realized_schemas_.clear();
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

    void TypeBridge::value_edges(hgraph_ir::TypeId id, const Bindings &bindings, std::vector<hgraph_ir::TypeId> &out,
                                 std::size_t depth) const {
        if (!id.valid() || id.value >= module_.types.size()) { return; }
        // Guards the walk over ONE type expression, which is all this
        // descends. It is not the chain limit: a chain is hops between
        // expressions, and the driver takes those on the heap.
        if (depth >= 256U) { return; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    const auto generic = bindings.types.find(type.binding.value);
                    if (generic != bindings.types.end() && generic->second != id) {
                        value_edges(generic->second, bindings, out, depth + 1U);
                        return;
                    }
                }
                if (type.nominal_identity.empty()) { return; }
                // ARGUMENTS FIRST: specializing `Box<A1>` realizes `A1` to name
                // it, so `A1` has to be done before `Box<A1>` is reached, or
                // specializing nests one realization per link.
                for (const hgraph_ir::TypeArgument &argument : type.arguments) {
                    if (argument.type) { value_edges(*argument.type, bindings, out, depth + 1U); }
                }
                out.push_back(id);
                return;
            // Exactly the kinds `value()` recurses into with `value()`.
            case hir::TypeKind::Tuple:
            case hir::TypeKind::List:
            case hir::TypeKind::Set:
            case hir::TypeKind::Map:
            case hir::TypeKind::Atomic:
                for (hgraph_ir::TypeId child : type.children) { value_edges(child, bindings, out, depth + 1U); }
                return;
            default: return;
        }
    }

    void TypeBridge::schema_edges(hgraph_ir::TypeId id, const Bindings &bindings, std::vector<hgraph_ir::TypeId> &out,
                                  std::size_t depth) const {
        if (!id.valid() || id.value >= module_.types.size() || depth >= 256U) { return; }
        const hgraph_ir::Type &type = module_.types[id.value];
        switch (type.kind) {
            case hir::TypeKind::Symbol:
                if (type.binding.valid()) {
                    const auto generic = bindings.types.find(type.binding.value);
                    if (generic != bindings.types.end() && generic->second != id) {
                        schema_edges(generic->second, bindings, out, depth + 1U);
                        return;
                    }
                }
                if (!type.nominal_identity.empty()) { out.push_back(id); }
                return;
            // Exactly the positions `schema()` recurses into with `schema()`.
            // An `atomic<T>`, a set element, a map KEY and a rolling element
            // take `value()` instead, so a struct there needs its value type
            // only -- building its temporal bundle as well is not merely
            // wasted, it fails for any struct holding a tuple.
            case hir::TypeKind::List:
            case hir::TypeKind::Reference:
                if (!type.children.empty()) { schema_edges(type.children.front(), bindings, out, depth + 1U); }
                return;
            case hir::TypeKind::Map:
                if (type.children.size() == 2U) { schema_edges(type.children[1], bindings, out, depth + 1U); }
                return;
            default: return;
        }
    }

    const hgraph::ValueTypeMetaData *TypeBridge::realize_value_closure(Specialization root, syntax::SourceRange range) {
        // THE INVARIANT: a struct is realized only once everything it can
        // reach outside its own cycle is realized. Then describing it --
        // `register_value`, or the batch describer inside `recursive_value` --
        // finds every nominal it asks for in the memo, and no realization ever
        // nests inside another. Reverse topological order over strongly
        // connected components is exactly that order, so this is Tarjan's
        // algorithm on an explicit stack, over every edge `value()` follows.
        //
        // A component with more than one member is a cycle, and only owned
        // edges may form one (ADR 0012 rule 2 locally, ADR 0013's layout-cycle
        // check for imports), so it is registered as one recursive batch. The
        // earlier shapes of this driver approximated the invariant -- plain
        // structs only, then whole reachability batches -- and each left a
        // chain that nested one frame per link.
        constexpr std::uint32_t unvisited = std::numeric_limits<std::uint32_t>::max();
        struct Node
        {
            Specialization                 specialization{};
            std::vector<hgraph_ir::TypeId> edges{};
            std::uint32_t                  order{unvisited};
            std::uint32_t                  low{unvisited};
            bool                           on_stack{false};
        };
        struct Frame
        {
            std::uint32_t node{0};
            std::size_t   next{0};
        };
        // Names this call has opened, released on every exit. Reaching one of
        // them from a NESTED call means a nominal was needed before its own
        // component closed -- only a cycle through a generic argument does
        // that, which the checker refuses -- so it fails here, by name, instead
        // of recursing without end.
        struct Opened
        {
            std::unordered_set<std::string> &in_progress;
            std::vector<std::string>         names{};
            explicit Opened(std::unordered_set<std::string> &set) : in_progress{set} {}
            Opened(const Opened &)            = delete;
            Opened &operator=(const Opened &) = delete;
            ~Opened() {
                for (const std::string &name : names) { in_progress.erase(name); }
            }
        } opened{in_progress_};

        std::vector<Node>                              nodes;
        std::unordered_map<std::string, std::uint32_t> index;
        std::vector<std::uint32_t>                     stack;
        std::vector<Frame>                             frames;
        std::uint32_t                                  next_order = 0;

        const auto enter = [&](Specialization specialization) {
            const auto id   = static_cast<std::uint32_t>(nodes.size());
            std::string name = specialization.qualified();
            Node        node{.specialization = std::move(specialization), .order = next_order, .low = next_order, .on_stack = true};
            for (const hgraph_ir::StructField &field : node.specialization.contract->fields) {
                value_edges(field.type, node.specialization.applied, node.edges);
            }
            for (hgraph_ir::TypeId parent : node.specialization.contract->parents) {
                value_edges(parent, node.specialization.applied, node.edges);
            }
            index.emplace(name, id);
            in_progress_.insert(name);
            opened.names.push_back(std::move(name));
            nodes.push_back(std::move(node));
            ++next_order;
            stack.push_back(id);
            frames.push_back(Frame{id, 0});
        };
        // Registers one closed component. Everything it reaches outside
        // itself is already in the memo.
        const auto close = [&](const std::vector<std::uint32_t> &members) -> bool {
            const Specialization &first     = nodes[members.front()].specialization;
            const bool            recursive = members.size() > 1U || std::ranges::any_of(first.contract->fields, [](const hgraph_ir::StructField &field) {
                                       return field.recursive;
                                   });
            if (!recursive) {
                const hgraph::ValueTypeMetaData *meta = register_value(first, range);
                if (meta == nullptr) { return false; }
                realized_.insert_or_assign(first.qualified(), meta);
                return true;
            }
            if (recursive_value(first, range) == nullptr) { return false; }
            // `recursive_value` compared every member against the registry
            // before and after the close, so none of these skips that check.
            for (const std::uint32_t member : members) {
                std::string                      name = nodes[member].specialization.qualified();
                const hgraph::ValueTypeMetaData *meta = registry_.value_type(name);
                if (meta == nullptr) {
                    report(range, "recursive struct '" + name + "' was not registered with its batch");
                    return false;
                }
                realized_.insert_or_assign(std::move(name), meta);
            }
            return true;
        };

        const std::string root_name = root.qualified();
        enter(std::move(root));
        while (!frames.empty()) {
            const std::uint32_t current = frames.back().node;
            if (const std::size_t next = frames.back().next++; next < nodes[current].edges.size()) {
                const hgraph_ir::Type &type = module_.types[nodes[current].edges[next].value];
                // Arguments come before their application in the edge list, so
                // specializing here finds them in the memo.
                std::optional<Specialization> target = specialize(type, nodes[current].specialization.applied);
                if (!target) { return nullptr; }
                const std::string name = target->qualified();
                if (realized_.contains(name)) { continue; }
                if (const auto found = index.find(name); found != index.end()) {
                    // On the stack: a back edge into the open component. Off
                    // it: a component this call already closed.
                    if (nodes[found->second].on_stack) {
                        nodes[current].low = std::min(nodes[current].low, nodes[found->second].order);
                    }
                    continue;
                }
                if (in_progress_.contains(name)) {
                    report(type.range, "struct '" + name + "' is needed before its own layout is complete");
                    return nullptr;
                }
                enter(std::move(*target));
                continue;
            }
            frames.pop_back();
            if (!frames.empty()) {
                const std::uint32_t parent = frames.back().node;
                nodes[parent].low          = std::min(nodes[parent].low, nodes[current].low);
            }
            if (nodes[current].low != nodes[current].order) { continue; }
            std::vector<std::uint32_t> members;
            std::uint32_t              member = unvisited;
            do {
                member = stack.back();
                stack.pop_back();
                nodes[member].on_stack = false;
                members.push_back(member);
            } while (member != current);
            if (!close(members)) { return nullptr; }
        }
        const auto found = realized_.find(root_name);
        return found == realized_.end() ? nullptr : found->second;
    }

    const hgraph::ValueTypeMetaData *TypeBridge::nominal_value(const hgraph_ir::Type &type, const Bindings &outer) {
        std::optional<Specialization> specialization = specialize(type, outer);
        if (!specialization) { return nullptr; }
        if (const auto found = realized_.find(specialization->qualified()); found != realized_.end()) {
            return found->second;
        }
        // Plain or recursive alike: both go through the driver, so neither
        // kind of struct can take the chain's length onto the stack.
        return realize_value_closure(std::move(*specialization), type.range);
    }

    /// The members of a recursive batch: `root` and every specialization its
    /// recursive edges reach, root first. ONE walk, shared by the driver that
    /// orders the batch's dependencies and by `recursive_value` that closes it,
    /// so the two cannot disagree about what the batch is.
    std::optional<std::vector<TypeBridge::Specialization>> TypeBridge::recursive_members(Specialization root) {
        std::vector<Specialization>     members;
        std::unordered_set<std::string> seen;
        std::vector<Specialization>     work;
        work.push_back(std::move(root));
        while (!work.empty()) {
            Specialization current = std::move(work.back());
            work.pop_back();
            if (!seen.insert(current.qualified()).second) { continue; }
            // Realized already, by this bridge and against the registry: the
            // driver closes components in reverse topological order, so what
            // lies downstream of this batch is done. Collecting it again would
            // make each batch pay for everything after it -- quadratic along a
            // chain of batches.
            if (!members.empty() && realized_.contains(current.qualified())) { continue; }
            for (const hgraph_ir::StructField &field : current.contract->fields) {
                if (!field.recursive) { continue; }
                std::optional<Specialization> target = recursive_target(field, current.applied);
                if (!target) { return std::nullopt; }
                work.push_back(std::move(*target));
            }
            members.push_back(std::move(current));
        }
        return members;
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
        std::optional<std::vector<Specialization>> members = recursive_members(std::move(root));
        if (!members) { return nullptr; }
        std::unordered_map<std::string, Specialization> pending;
        pending.reserve(members->size());
        for (Specialization &member : *members) {
            std::string name = member.qualified();
            pending.emplace(std::move(name), std::move(member));
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

    const hgraph::TSValueTypeMetaData *TypeBridge::realize_schema_closure(const hgraph_ir::Type &type, const Bindings &outer) {
        // The temporal side descends a chain independently: it asks for the
        // value type first, but that has finished by the time it reaches a
        // field's schema, so it needs its own worklist. It follows only the
        // edges `schema()` follows with `schema()`; everything else it touches
        // is a value type, which realizing the root's value closure already did.
        //
        // No cycle can reach here -- a recursive field is an owner endpoint and
        // is skipped, and an ordinary cycle is refused upstream -- so a
        // post-order walk is enough; the value side needs components, this
        // does not.
        struct Frame
        {
            const hgraph_ir::Type         *type{nullptr};
            Bindings                       outer{};
            Bindings                       applied{};
            std::string                    name{};
            std::vector<hgraph_ir::TypeId> edges{};
            std::size_t                    next{0};
        };
        const auto open = [&](const hgraph_ir::Type &source, const Bindings &bindings) -> std::optional<Frame> {
            const hgraph::ValueTypeMetaData *meta = nominal_value(source, bindings);
            if (meta == nullptr) { return std::nullopt; }
            const hgraph_ir::StructContract *contract = structure(source.nominal_identity);
            if (contract == nullptr) {
                report(source.range, "unknown nominal type '" + source.nominal_identity + "'");
                return std::nullopt;
            }
            std::optional<Bindings> applied = bind(source, *contract, bindings);
            if (!applied) { return std::nullopt; }
            Frame frame{.type = &source, .outer = bindings, .applied = std::move(*applied), .name = std::string{meta->name()}};
            for (const hgraph_ir::StructField &field : contract->fields) {
                // A recursive edge is an owner endpoint, not a nested schema.
                if (field.recursive) { continue; }
                schema_edges(field.type, frame.applied, frame.edges);
            }
            return frame;
        };
        std::optional<Frame> root = open(type, outer);
        if (!root) { return nullptr; }
        const std::string               root_name = root->name;
        std::unordered_set<std::string> visiting{root_name};
        std::vector<Frame>              stack;
        stack.push_back(std::move(*root));
        while (!stack.empty()) {
            if (const std::size_t next = stack.back().next++; next < stack.back().edges.size()) {
                const hgraph_ir::Type &edge = module_.types[stack.back().edges[next].value];
                std::optional<Frame>   opened = open(edge, stack.back().applied);
                if (!opened) { return nullptr; }
                if (realized_schemas_.contains(opened->name) || !visiting.insert(opened->name).second) { continue; }
                stack.push_back(std::move(*opened));
                continue;
            }
            const Frame done = std::move(stack.back());
            stack.pop_back();
            const hgraph::TSValueTypeMetaData *built = register_schema(*done.type, done.outer);
            if (built == nullptr) { return nullptr; }
            realized_schemas_.insert_or_assign(done.name, built);
        }
        const auto found = realized_schemas_.find(root_name);
        return found == realized_schemas_.end() ? nullptr : found->second;
    }

    const hgraph::TSValueTypeMetaData *TypeBridge::nominal_schema(const hgraph_ir::Type &type, const Bindings &outer) {
        const hgraph::ValueTypeMetaData *value_type = nominal_value(type, outer);
        if (value_type == nullptr) { return nullptr; }
        if (const auto found = realized_schemas_.find(std::string{value_type->name()}); found != realized_schemas_.end()) {
            return found->second;
        }
        return realize_schema_closure(type, outer);
    }

    /// One struct's temporal schema, with every nominal its fields name already
    /// realized -- so the `schema(field.type, ...)` calls below answer from the
    /// memo instead of descending the chain again.
    const hgraph::TSValueTypeMetaData *TypeBridge::register_schema(const hgraph_ir::Type &type, const Bindings &outer) {
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
