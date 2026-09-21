#include "descriptor/import_catalog.h"

#include "descriptor/module_descriptor_reader.h"

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgl::descriptor
{
    namespace
    {
        [[nodiscard]] std::optional<semantics::ImportedScalarType> scalar_type(const ModuleDescriptor &descriptor,
                                                                               SchemaId                id) noexcept {
            if (id == no_schema_id || id >= descriptor.types.size()) { return std::nullopt; }
            const TypeRecord &type = descriptor.types[id];
            if (type.category != TypeCategory::Scalar) { return std::nullopt; }
            using semantics::ImportedScalarType;
            if (type.scalar_name == "bool") { return ImportedScalarType::Bool; }
            if (type.scalar_name == "i64") { return ImportedScalarType::I64; }
            if (type.scalar_name == "f64") { return ImportedScalarType::F64; }
            if (type.scalar_name == "str") { return ImportedScalarType::Str; }
            if (type.scalar_name == "date") { return ImportedScalarType::Date; }
            if (type.scalar_name == "time") { return ImportedScalarType::Time; }
            if (type.scalar_name == "datetime") { return ImportedScalarType::DateTime; }
            if (type.scalar_name == "duration") { return ImportedScalarType::Duration; }
            if (type.scalar_name == "civil_datetime") { return ImportedScalarType::CivilDateTime; }
            if (type.scalar_name == "zoned_datetime") { return ImportedScalarType::ZonedDateTime; }
            if (type.scalar_name == "zoned_time") { return ImportedScalarType::ZonedTime; }
            if (type.scalar_name == "timezone") { return ImportedScalarType::TimeZone; }
            return std::nullopt;
        }

        [[nodiscard]] std::optional<semantics::ImportedConstant> imported_constant(const ModuleDescriptor &descriptor,
                                                                                   SchemaId                id) noexcept {
            using semantics::ImportedConstant;
            using semantics::ImportedConstantKind;
            if (id == no_schema_id) { return ImportedConstant{}; }
            if (id >= descriptor.constant_expressions.size()) { return std::nullopt; }
            const ConstantExpressionRecord &source = descriptor.constant_expressions[id];
            if (source.category == ConstantExpressionCategory::Parameter) {
                return ImportedConstant{.kind = ImportedConstantKind::Parameter, .binding_identity = source.parameter_identity};
            }
            if (source.category == ConstantExpressionCategory::Literal && source.literal) {
                if (const auto *value = std::get_if<std::int64_t>(&*source.literal)) {
                    return ImportedConstant{.kind = ImportedConstantKind::I64, .i64 = *value};
                }
            }
            return std::nullopt;
        }

        /// `atomic<T>` is a struct field's shape, never a signature's: the
        /// resolver rejects it in value position, and ADR 0012 admits an edge
        /// only at a field's top level, never through a container. So it
        /// converts only where a layout asks for it, and never for a child.
        [[nodiscard]] std::optional<semantics::ImportedType> imported_type(const ModuleDescriptor &descriptor, SchemaId id,
                                                                           std::vector<SchemaId> path         = {},
                                                                           bool                  allow_layout = false,
                                                                           bool                  allow_atomic = false) noexcept {
            using semantics::ImportedType;
            using semantics::ImportedTypeKind;
            if (id == no_schema_id || id >= descriptor.types.size()) { return std::nullopt; }
            // The descriptor validator checks reference bounds, not whether
            // every structural type can be expanded into a value tree. Reject
            // the first cycle, independently of unrelated arena entries. Bound
            // acyclic nesting as well so untrusted descriptors cannot exhaust
            // the compiler stack. Siblings get separate paths: DAGs are valid.
            if (path.size() >= 256U || std::ranges::find(path, id) != path.end()) { return std::nullopt; }
            path.push_back(id);
            const TypeRecord &source = descriptor.types[id];
            if (!source.arguments.empty()) { return std::nullopt; }
            ImportedType result;
            if (const auto scalar = scalar_type(descriptor, id)) {
                result.scalar = *scalar;
                return result;
            }
            switch (source.category) {
                case TypeCategory::Symbol:
                    // A signature's Symbol is a generic parameter, which
                    // lowering resolves by binding identity. A nominal struct
                    // (ADR 0013) is a type to register, and only a layout can
                    // carry one: advertising it in a signature would report
                    // support for a function lowering then rejects.
                    if (source.binding_identity.empty() && !(allow_layout && !source.nominal_identity.empty())) {
                        return std::nullopt;
                    }
                    result.kind             = ImportedTypeKind::Symbol;
                    result.binding_identity = source.binding_identity;
                    result.nominal_identity = source.nominal_identity;
                    break;
                case TypeCategory::List: result.kind = ImportedTypeKind::List; break;
                case TypeCategory::Set: result.kind = ImportedTypeKind::Set; break;
                case TypeCategory::Map: result.kind = ImportedTypeKind::Map; break;
                case TypeCategory::Rolling: result.kind = ImportedTypeKind::Rolling; break;
                case TypeCategory::Signal: result.kind = ImportedTypeKind::Signal; break;
                case TypeCategory::Schema: result.kind = ImportedTypeKind::Schema; break;
                case TypeCategory::Atomic:
                    if (!allow_atomic) { return std::nullopt; }
                    result.kind = ImportedTypeKind::Atomic;
                    break;
                default: return std::nullopt;
            }
            for (SchemaId child : source.children) {
                // A layout's nested types may still name a struct; only the
                // atomic boundary is confined to the field's top level.
                std::optional<ImportedType> lowered = imported_type(descriptor, child, path, allow_layout);
                if (!lowered) { return std::nullopt; }
                result.children.push_back(std::move(*lowered));
            }
            const auto size     = imported_constant(descriptor, source.size);
            const auto min_size = imported_constant(descriptor, source.min_size);
            if (!size || !min_size) { return std::nullopt; }
            result.size      = *size;
            result.min_size  = *min_size;
            result.unbounded = source.unbounded;
            return result;
        }

        [[nodiscard]] std::string short_name(std::string_view module, std::string_view identity) {
            const std::string prefix = std::string{module} + "::";
            if (!identity.starts_with(prefix)) { return {}; }
            identity.remove_prefix(prefix.size());
            if (identity.empty() || identity.find("::") != std::string_view::npos) { return {}; }
            return std::string{identity};
        }

        [[nodiscard]] semantics::NativeCallPhase phase(NativePhase value) noexcept {
            using semantics::NativeCallPhase;
            switch (value) {
                case NativePhase::Wiring: return NativeCallPhase::Wiring;
                case NativePhase::Start: return NativeCallPhase::Start;
                case NativePhase::Evaluation: return NativeCallPhase::Evaluation;
                case NativePhase::Stop: return NativeCallPhase::Stop;
            }
            std::unreachable();
        }

        [[nodiscard]] semantics::NativeParameterAccess access(NativeParameterAccess value) noexcept {
            switch (value) {
                case NativeParameterAccess::Value: return semantics::NativeParameterAccess::Value;
                case NativeParameterAccess::InputView: return semantics::NativeParameterAccess::InputView;
            }
            std::unreachable();
        }

        /// Rebuilds the constraint graph reachable from `root` into the
        /// catalog's arena, so an imported family can be checked by the
        /// existing solver when it is applied (ADR 0013). Returns false when
        /// any node cannot cross, since a partial requirement would be weaker
        /// than the one the exporting module declared.
        [[nodiscard]] bool imported_constraints(const ModuleDescriptor &descriptor, SchemaId root,
                                                std::vector<semantics::ImportedConstraint> &arena,
                                                std::unordered_map<SchemaId, std::uint32_t> &seen,
                                                std::uint32_t &out, std::uint32_t depth = 0) {
            // Validation checks reference bounds, not graph depth, so an
            // arbitrarily deep acyclic chain would exhaust the stack. The same
            // budget imported_type uses.
            if (depth >= 256U) { return false; }
            if (root == no_schema_id || root >= descriptor.constraints.size()) { return false; }
            if (const auto found = seen.find(root); found != seen.end()) {
                out = found->second;
                return true;
            }
            const ConstraintRecord &source = descriptor.constraints[root];
            const auto              index  = static_cast<std::uint32_t>(arena.size());
            seen.emplace(root, index);
            arena.emplace_back();
            semantics::ImportedConstraint node;
            switch (source.category) {
                case ConstraintCategory::Symbol: node.kind = semantics::ImportedConstraintKind::Symbol; break;
                case ConstraintCategory::Type: node.kind = semantics::ImportedConstraintKind::Type; break;
                case ConstraintCategory::Value: node.kind = semantics::ImportedConstraintKind::Value; break;
                case ConstraintCategory::Set: node.kind = semantics::ImportedConstraintKind::Set; break;
                case ConstraintCategory::Call: node.kind = semantics::ImportedConstraintKind::Call; break;
                case ConstraintCategory::Each: node.kind = semantics::ImportedConstraintKind::Each; break;
                case ConstraintCategory::Operator: node.kind = semantics::ImportedConstraintKind::Operator; break;
                case ConstraintCategory::Relation: node.kind = semantics::ImportedConstraintKind::Relation; break;
                case ConstraintCategory::Not: node.kind = semantics::ImportedConstraintKind::Not; break;
                case ConstraintCategory::Logic: node.kind = semantics::ImportedConstraintKind::Logic; break;
            }
            node.identity          = source.identity;
            node.registry_name     = source.registry_name;
            node.operator_spelling = source.operator_spelling;
            node.relation_category = source.relation_category;
            // An Operator requirement stores the return type it demands in
            // `result`, not `type`; reading only `type` would drop it and leave
            // a requirement weaker than the exporting module declared.
            const SchemaId type_ref = source.category == ConstraintCategory::Operator ? source.result : source.type;
            if (type_ref != no_schema_id) {
                node.type = imported_type(descriptor, type_ref, {}, /*allow_layout=*/true);
                if (!node.type) { return false; }
            }
            if (source.value != no_schema_id) {
                const auto constant = imported_constant(descriptor, source.value);
                if (!constant) { return false; }
                node.value = *constant;
            }
            const auto child = [&](SchemaId id, std::uint32_t &slot) {
                if (id == no_schema_id) { return true; }
                return imported_constraints(descriptor, id, arena, seen, slot, depth + 1U);
            };
            if (!child(source.lhs, node.lhs) || !child(source.rhs, node.rhs) || !child(source.operand, node.operand) ||
                !child(source.source, node.source) || !child(source.body, node.body)) {
                return false;
            }
            for (const SchemaId element : source.elements) {
                std::uint32_t slot = semantics::no_imported_constraint;
                if (!imported_constraints(descriptor, element, arena, seen, slot, depth + 1U)) { return false; }
                node.elements.push_back(slot);
            }
            for (const SchemaId argument : source.arguments) {
                std::uint32_t slot = semantics::no_imported_constraint;
                if (!imported_constraints(descriptor, argument, arena, seen, slot, depth + 1U)) { return false; }
                node.arguments.push_back(slot);
            }
            arena[index] = std::move(node);
            out          = index;
            return true;
        }

        /// One exported struct's layout (ADR 0013). The importer rebuilds the
        /// type from this and registers it under the owner's identity, so every
        /// field type, parent and generic has to survive the crossing; whatever
        /// does not is recorded as a support error rather than silently dropped,
        /// the same discipline imported operators follow.
        [[nodiscard]] semantics::ImportedStruct imported_struct(const ModuleDescriptor     &descriptor,
                                                                const InterfaceDeclaration &declaration) {
            semantics::ImportedStruct result;
            result.module_identity        = descriptor.module_identity;
            result.identity               = declaration.identity;
            result.abstract               = declaration.abstract;
            result.descriptor_fingerprint = descriptor.descriptor_fingerprint;
            // Generated C++ refers to the exporter's type rather than
            // re-declaring it (ADR 0013), so a consumer needs the header that
            // declares it -- the same reason an imported native function
            // carries them.
            result.public_headers         = descriptor.build.public_headers;
            const std::string prefix      = descriptor.module_identity + ".";
            if (declaration.identity.starts_with(prefix)) {
                // The local name is spelled straight into generated C++, so it
                // has to BE an identifier -- "contains no dot or colon" leaves
                // every other character through.
                const std::string name = declaration.identity.substr(prefix.size());
                if (is_identifier(name)) { result.name = name; }
            }
            const auto unsupported = [&](std::string message) {
                if (result.support_error.empty()) { result.support_error = std::move(message); }
            };
            if (declaration.signature.requirements != no_schema_id) {
                // A `where` requirement crosses whole or not at all: a partial
                // one would be weaker than the exporting module declared, and
                // would admit specializations it rejects.
                std::unordered_map<SchemaId, std::uint32_t> seen;
                if (!imported_constraints(descriptor, declaration.signature.requirements, result.constraints, seen,
                                          result.requirements)) {
                    result.constraints.clear();
                    result.requirements = semantics::no_imported_constraint;
                    unsupported("imported struct requirements are not supported by the catalog");
                }
            }
            for (const GenericParameter &generic : declaration.signature.generics) {
                semantics::ImportedGeneric lowered{
                    .name = generic.name, .binding_identity = generic.binding_identity, .is_const = generic.is_const};
                if (generic.is_pack) { unsupported("imported struct type packs require catalog pack reconstruction"); }
                if (generic.type != no_schema_id) {
                    lowered.type = imported_type(descriptor, generic.type);
                    if (!lowered.type) { unsupported("imported struct generic type is not supported by the catalog"); }
                }
                result.generics.push_back(std::move(lowered));
            }
            for (const SchemaId parent : declaration.parents) {
                const auto type = imported_type(descriptor, parent, {}, /*allow_layout=*/true);
                if (!type) {
                    unsupported("imported struct parent type is not supported by the catalog");
                    continue;
                }
                result.parents.push_back(*type);
            }
            for (const StructField &field : declaration.fields) {
                // An inherited field arrives with the parent, which the importer
                // rebuilds first; carrying it twice would duplicate it. A child
                // that overrides the inherited default is a different matter:
                // the override lives only here, so dropping the field silently
                // would rebuild the parent's default instead of the child's.
                if (!field.origin_identity.empty() && field.origin_identity != declaration.identity) {
                    if (field.default_value != no_schema_id) {
                        unsupported("imported struct inherited field defaults require catalog constant reconstruction");
                    }
                    continue;
                }
                if (field.default_value != no_schema_id) {
                    unsupported("imported struct field defaults require catalog constant reconstruction");
                }
                const auto type = imported_type(descriptor, field.type, {}, /*allow_layout=*/true, /*allow_atomic=*/true);
                if (!type) {
                    unsupported("imported struct field type is not supported by the catalog");
                    continue;
                }
                result.fields.push_back(
                    {.name = field.name, .type = *type, .optional = field.optional, .recursive = field.recursive});
            }
            return result;
        }

        [[nodiscard]] semantics::ImportedOperatorContract imported_operator(const ModuleDescriptor     &descriptor,
                                                                            const InterfaceDeclaration &declaration) {
            semantics::ImportedOperatorContract result;
            result.module_identity        = descriptor.module_identity;
            result.identity               = declaration.identity;
            result.registry_name          = declaration.registry_name;
            result.descriptor_fingerprint = descriptor.descriptor_fingerprint;
            // Public HGL declarations use module.name; native functions use
            // module::name. Do not conflate either spelling with the dispatch key.
            const std::string prefix = descriptor.module_identity + ".";
            if (declaration.identity.starts_with(prefix)) {
                const std::string name = declaration.identity.substr(prefix.size());
                if (!name.empty() && name.find_first_of(".:") == std::string::npos) { result.name = name; }
            }
            const auto unsupported = [&](std::string message) {
                if (result.support_error.empty()) { result.support_error = std::move(message); }
            };
            if (result.registry_name.empty()) { unsupported("imported operator requires an explicit registry identity"); }
            if (declaration.signature.requirements != no_schema_id) {
                unsupported("imported operator constraints require catalog constraint reconstruction");
            }
            if (!declaration.properties.empty()) {
                unsupported("imported operator properties require catalog property reconstruction");
            }
            for (const GenericParameter &generic : declaration.signature.generics) {
                semantics::ImportedGeneric lowered{
                    .name = generic.name, .binding_identity = generic.binding_identity, .is_const = generic.is_const};
                if (generic.is_pack) { unsupported("imported operator type packs require catalog pack reconstruction"); }
                if (generic.type != no_schema_id) {
                    lowered.type = imported_type(descriptor, generic.type);
                    if (!lowered.type) { unsupported("imported operator generic type is not supported by the catalog"); }
                }
                result.generics.push_back(std::move(lowered));
            }
            for (const Parameter &parameter : declaration.signature.parameters) {
                if (parameter.pack != ParameterPack::None) {
                    unsupported("imported operator parameter packs require catalog pack reconstruction");
                }
                if (parameter.default_value != no_schema_id) {
                    unsupported("imported operator defaults require catalog constant reconstruction");
                }
                if (parameter.runtime_value) {
                    unsupported("imported operator runtime-value parameters are not supported by the catalog");
                }
                const auto type = imported_type(descriptor, parameter.type);
                if (!type) {
                    unsupported("imported operator parameter type is not supported by the catalog");
                    continue;
                }
                result.parameters.push_back({parameter.name, parameter.binding_identity, *type, parameter.is_const});
            }
            if (declaration.signature.result != no_schema_id &&
                descriptor.types[declaration.signature.result].category != TypeCategory::Void) {
                result.result = imported_type(descriptor, declaration.signature.result);
                if (!result.result) { unsupported("imported operator result type is not supported by the catalog"); }
            }
            return result;
        }
    }  // namespace

    std::optional<ReadError> add_to_catalog(const ModuleDescriptor &descriptor, semantics::ModuleCatalog &catalog) {
        if (const std::optional<ReadError> invalid = validate(descriptor)) { return invalid; }

        semantics::ImportableModule module;
        module.identity               = descriptor.module_identity;
        module.descriptor_fingerprint = descriptor.descriptor_fingerprint;
        for (std::size_t index = 0; index < descriptor.interface.size(); ++index) {
            const InterfaceDeclaration &declaration = descriptor.interface[index];
            if (declaration.category != DeclarationCategory::Operator) { continue; }
            auto contract = imported_operator(descriptor, declaration);
            if (contract.name.empty()) {
                return ReadError{"$.interface[" + std::to_string(index) + "].identity",
                                 "operator identity must be '" + descriptor.module_identity + ".<name>'"};
            }
            module.operators.push_back(std::move(contract));
        }
        for (std::size_t index = 0; index < descriptor.interface.size(); ++index) {
            const InterfaceDeclaration &declaration = descriptor.interface[index];
            if (declaration.category != DeclarationCategory::Structure) { continue; }
            auto structure = imported_struct(descriptor, declaration);
            if (structure.name.empty()) {
                return ReadError{"$.interface[" + std::to_string(index) + "].identity",
                                 "struct identity must be '" + descriptor.module_identity + ".<name>'"};
            }
            module.structs.push_back(std::move(structure));
        }
        // A descriptor may name a struct of its own module that it does not
        // declare: validation resolves a recursive edge's target but not an
        // ordinary parent or field. An importer would have no layout to
        // rebuild it from, so the layout is unsupported rather than merely
        // incomplete (ADR 0013).
        {
            std::unordered_set<std::string_view> declared;
            declared.reserve(module.structs.size());
            for (const semantics::ImportedStruct &structure : module.structs) { declared.emplace(structure.identity); }
            const std::string prefix = descriptor.module_identity + ".";
            const auto        unresolved =
                [&](const semantics::ImportedType &type, auto &&self) -> const std::string * {
                if (type.kind == semantics::ImportedTypeKind::Symbol && !type.nominal_identity.empty() &&
                    type.nominal_identity.starts_with(prefix) && !declared.contains(type.nominal_identity)) {
                    return &type.nominal_identity;
                }
                for (const semantics::ImportedType &child : type.children) {
                    if (const std::string *found = self(child, self)) { return found; }
                }
                return nullptr;
            };
            for (semantics::ImportedStruct &structure : module.structs) {
                if (!structure.support_error.empty()) { continue; }
                for (const semantics::ImportedType &parent : structure.parents) {
                    if (const std::string *missing = unresolved(parent, unresolved)) {
                        structure.support_error = "imported struct parent '" + *missing + "' is not declared by " +
                                                  descriptor.module_identity;
                        break;
                    }
                }
                if (!structure.support_error.empty()) { continue; }
                for (const semantics::ImportedStructField &field : structure.fields) {
                    if (const std::string *missing = unresolved(field.type, unresolved)) {
                        structure.support_error = "imported struct field '" + field.name + "' names '" + *missing +
                                                  "', which is not declared by " + descriptor.module_identity;
                        break;
                    }
                }
            }
        }
        for (std::size_t declaration_index = 0; declaration_index < descriptor.native_declarations.size(); ++declaration_index) {
            const NativeDeclaration &declaration = descriptor.native_declarations[declaration_index];
            if (declaration.category != NativeDeclarationCategory::Function) { continue; }

            semantics::ImportedFunction function;
            function.module_identity        = descriptor.module_identity;
            function.name                   = short_name(descriptor.module_identity, declaration.identity);
            function.identity               = declaration.identity;
            function.candidate_identity     = declaration.identity + "#" + std::to_string(declaration_index);
            function.cpp_symbol             = declaration.cpp_symbol;
            function.public_headers         = descriptor.build.public_headers;
            function.cmake_packages         = descriptor.build.cmake_packages;
            function.imported_targets       = descriptor.build.imported_targets;
            function.runtime_images         = descriptor.build.runtime_images;
            function.descriptor_fingerprint = descriptor.descriptor_fingerprint;
            function.throws                 = declaration.exception_policy == NativeExceptionPolicy::Translated;
            if (function.name.empty()) {
                return ReadError{"$.native.declarations[" + std::to_string(declaration_index) + "].identity",
                                 "native function identity must be '" + descriptor.module_identity + "::<name>'"};
            }
            if (!declaration.effects.empty()) {
                function.support_error = "native value calls with declared effects are not supported yet";
            }
            // Node hooks only: a value call may be admitted in start, evaluation
            // and stop; wiring-time native calls are outside the first interface.
            if (declaration.phases.empty() || std::ranges::any_of(declaration.phases, [](NativePhase phase) {
                    return phase != NativePhase::Start && phase != NativePhase::Evaluation && phase != NativePhase::Stop;
                })) {
                function.support_error = "native value calls currently require node hook phases (start, evaluation, stop)";
            }
            if (declaration.thread_safety == NativeThreadSafety::Serialized) {
                function.support_error = "serialized native value calls are not supported yet";
            }
            if (declaration.result.ownership != NativeOwnership::Value || declaration.result.mutable_value ||
                !declaration.result.dependent_on.empty()) {
                function.support_error = "native value call results must use value ownership";
            }
            for (const GenericParameter &generic : declaration.signature.generics) {
                semantics::ImportedGeneric lowered{
                    .name             = generic.name,
                    .binding_identity = generic.binding_identity,
                    .is_const         = generic.is_const,
                };
                if (generic.type != no_schema_id) { lowered.type = imported_type(descriptor, generic.type); }
                if (generic.type != no_schema_id && !lowered.type) {
                    function.support_error = "native generic constraints must use supported value types";
                }
                function.generics.push_back(std::move(lowered));
            }
            for (std::size_t index = 0; index < declaration.signature.parameters.size(); ++index) {
                const Parameter &parameter = declaration.signature.parameters[index];
                const auto       type      = imported_type(descriptor, parameter.type);
                if (!type) {
                    function.support_error =
                        "native calls require supported scalar, collection-view, or signal input-view parameter types";
                    continue;
                }
                const NativeValuePolicy &policy = declaration.parameters[index].value;
                const bool               schema = type && type->kind == semantics::ImportedTypeKind::Schema;
                if ((schema && policy.ownership != NativeOwnership::Borrowed) ||
                    (!schema && policy.ownership != NativeOwnership::Value) || policy.mutable_value ||
                    !policy.dependent_on.empty()) {
                    function.support_error = schema ? "native schema parameters must use borrowed immutable ownership"
                                                    : "native value call parameters must use value ownership";
                }
                function.parameters.push_back(semantics::ImportedParameter{parameter.name, *type, parameter.is_const,
                                                                           access(declaration.parameters[index].access)});
            }
            if (declaration.signature.result != no_schema_id) {
                function.result = imported_type(descriptor, declaration.signature.result);
                if (!function.result) { function.support_error = "native calls require a supported value result"; }
            }
            for (NativePhase allowed : declaration.phases) { function.phases.push_back(phase(allowed)); }
            module.functions.push_back(std::move(function));
        }

        if (const std::optional<semantics::CatalogError> error = catalog.add(std::move(module))) {
            return ReadError{error->path, error->message};
        }
        return std::nullopt;
    }
}  // namespace hgl::descriptor
