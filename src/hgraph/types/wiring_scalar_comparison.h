#ifndef HGRAPH_WIRING_SCALAR_COMPARISON_H
#define HGRAPH_WIRING_SCALAR_COMPARISON_H

#include <hgraph/runtime/canonical_error.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_hash.h>

#include <cmath>
#include <unordered_map>
#include <unordered_set>

#include "wiring_scalar_key.h"

namespace hgraph::wiring_detail {

/** Compare diagnostic provenance and floating-zero signs in value-equal scalar arguments.
 *  The cache belongs to one wiring session. Ordinary argument schemas need
 *  no second traversal of their values. */
class ScalarComparison {
public:
    bool equal(const Value &left, const Value &right) const {
        const auto *schema = left.type().schema();
        if (schema != last_schema_) {
            last_details_ = details(schema);
            last_schema_ = schema;
        }
        return last_details_ == None || equal_details(left.view(), right.view(), last_details_);
    }

private:
    enum Detail : unsigned { None = 0, Error = 1, Zero = 2, Both = Error | Zero };

    static bool is_floating(const ValueTypeMetaData *schema) {
        return schema == scalar_descriptor<Float>::value_meta() || schema == scalar_descriptor<float>::value_meta();
    }

    unsigned details(const ValueTypeMetaData *schema) const {
        if (!schema) { return None; }
        const auto [entry, fresh] = schemas_.try_emplace(schema, Both);
        if (!fresh) { return entry->second; }
        // A recursive or abstract schema can contain either kind. Element
        // references remain valid when a recursive insertion rehashes the map.
        unsigned &result = entry->second;
        const auto kind = schema->try_value_kind();
        if (kind == ValueTypeKind::Atomic) {
            result = schema->name() == "hgl.error" ? Error : (is_floating(schema) ? Zero : None);
        } else if (kind == ValueTypeKind::Any) {
            result = schema->is_opaque_python() ? None : Both;
        } else if (!schema->is_abstract_bundle()) {
            result = details(schema->element_type) | details(schema->key_type);
            for (std::size_t i = 0; result != Both && i < schema->field_count; ++i) {
                result |= details(schema->fields[i].type);
            }
        }
        if (schema->is_named_bundle()) {
            const auto *provider = ValuePlanFactory::python_storage_provider();
            if (provider && provider->python_bundle_schema && provider->python_bundle_schema(schema)) {
                // A storage provider can compare the retained backing objects without invoking field getters.
                if (detail::stored_zero_signs_comparison().load(std::memory_order_acquire)) { result |= Zero; }
                else { result &= ~Zero; }
            } else {
                // A realized native base can contain a derived floating field.
                result |= Zero;
            }
        }
        return result;
    }

    static bool negative_zero(const ValueView &atom) {
        if (const auto *number = atom.try_as<Float>()) { return *number == 0.0 && std::signbit(*number); }
        if (const auto *number = atom.try_as<float>()) { return *number == 0.0f && std::signbit(*number); }
        return false;
    }

    bool equal_details(const ValueView &left, const ValueView &right, unsigned selected) const {
        if (!left.has_value() || !right.has_value()) { return left.has_value() == right.has_value(); }
        selected &= details(left.schema());
        if (left.data() == right.data() || selected == None) { return true; }
        if ((selected & Zero) && left.schema()->is_named_bundle()) {
            if (const auto compare = detail::stored_zero_signs_comparison().load(std::memory_order_acquire)) {
                if (const auto same = compare(left, right)) {
                    if (!*same) { return false; }
                    selected &= ~Zero;
                    if (selected == None) { return true; }
                }
            }
        }
        if (left.is_atomic()) {
            if (const auto *error = left.try_as<CanonicalError>()) {
                return error->context == right.checked_as<CanonicalError>().context;
            }
            return negative_zero(left) == negative_zero(right);
        }
        if (left.type().ops()->concrete_type_impl || right.type().ops()->concrete_type_impl) {
            const auto a = left.concrete(), b = right.concrete();
            if (a.binding() != left.binding() || b.binding() != right.binding()) {
                if (a.binding() != b.binding()) {
                    return ((details(a.schema()) | details(b.schema())) & selected) == None;
                }
                return equal_details(a, b, selected);
            }
        }
        if (left.is_any()) { return equal_details(left.as_any().get(), right.as_any().get(), selected); }
        if (left.is_optional()) { return equal_details(left.as_optional().value(), right.as_optional().value(), selected); }
        if (left.is_map()) {
            const auto a = left.as_map(), b = right.as_map();
            if ((details(a.key_schema()) & selected) && !equal_details(a.key_set(), b.key_set(), selected)) { return false; }
            if (details(a.value_schema()) & selected) {
                for (const auto &[key, member] : a) {
                    if (!equal_details(member, b.at(key), selected)) { return false; }
                }
            }
        } else if (left.is_set()) {
            const auto source = left.as_set(), other = right.as_set();
            if (is_floating(source.element_schema())) {
                // Each set can contain only one value equal to zero.
                const auto has_negative_zero = [](const SetView &set) {
                    for (const auto member : set) {
                        if (negative_zero(member)) { return true; }
                    }
                    return false;
                };
                return has_negative_zero(source) == has_negative_zero(other);
            }
            std::unordered_set<ValueView, ValueHash, ValueEqual> members;
            for (auto member : other) { members.insert(std::move(member)); }
            for (const auto member : source) {
                const auto match = members.find(member);
                if (match == members.end() || !equal_details(member, *match, selected)) { return false; }
            }
        } else if (auto a = left.try_as_indexed()) {
            const auto b = right.as_indexed();
            const auto *schema = left.schema();
            for (std::size_t i = 0; i < a->size(); ++i) {
                if (schema->fields && !(details(schema->fields[i].type) & selected)) { continue; }
                if (!equal_details(a->at(i), b.at(i), selected)) { return false; }
            }
        }
        return true;
    }

    mutable std::unordered_map<const ValueTypeMetaData *, unsigned> schemas_;
    mutable const ValueTypeMetaData *last_schema_{};
    mutable unsigned last_details_{None};
};

}  // namespace hgraph::wiring_detail

#endif
