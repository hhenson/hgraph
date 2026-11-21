#ifndef HGRAPH_WIRING_ERROR_CONTEXT_H
#define HGRAPH_WIRING_ERROR_CONTEXT_H

#include <hgraph/runtime/canonical_error.h>
#include <hgraph/types/value/value_hash.h>

#include <unordered_map>
#include <unordered_set>

namespace hgraph::wiring_detail {

/** Compare the observable provenance of value-equal scalar arguments (ERROR-1/2).
 *  The cache belongs to one wiring session. Ordinary argument schemas need
 *  no second traversal of their values. */
class ErrorContextComparison {
public:
    bool equal(const Value &left, const Value &right) const {
        const auto *schema = left.type().schema();
        if (schema != last_schema_) {
            last_may_hold_error_ = may_hold_error(schema);
            last_schema_ = schema;
        }
        return !last_may_hold_error_ || equal_contexts(left.view(), right.view());
    }

private:
    bool may_hold_error(const ValueTypeMetaData *schema) const {
        if (!schema) { return false; }
        const auto [entry, fresh] = schemas_.try_emplace(schema, true);
        if (!fresh) { return entry->second; }
        // A recursive schema encountered again can contain a diagnostic.
        // References to unordered_map elements survive a rehash.
        bool &result = entry->second;
        const auto kind = schema->try_value_kind();
        if (kind == ValueTypeKind::Atomic) {
            result = schema->name() == "hgl.error";
        } else if (kind == ValueTypeKind::Any) {
            // Opaque host objects do not expose a native diagnostic member.
            result = !schema->is_opaque_python();
        } else if (!schema->is_abstract_bundle()) {
            result = may_hold_error(schema->element_type) || may_hold_error(schema->key_type);
            for (std::size_t i = 0; !result && i < schema->field_count; ++i) {
                result = may_hold_error(schema->fields[i].type);
            }
        }
        return result;
    }

    bool equal_contexts(const ValueView &left, const ValueView &right) const {
        if (!left.has_value() || !right.has_value()) { return left.has_value() == right.has_value(); }
        if (left.data() == right.data() || !may_hold_error(left.schema())) {
            return true;
        }
        if (const auto *error = left.try_as<CanonicalError>()) {
            return error->context == right.checked_as<CanonicalError>().context;
        }
        if (left.type().ops()->concrete_type_impl || right.type().ops()->concrete_type_impl) {
            const auto a = left.concrete(), b = right.concrete();
            if (a.binding() != left.binding() || b.binding() != right.binding()) {
                if (a.binding() != b.binding()) {
                    return !may_hold_error(a.schema()) && !may_hold_error(b.schema());
                }
                return equal_contexts(a, b);
            }
        }
        if (left.is_any()) { return equal_contexts(left.as_any().get(), right.as_any().get()); }
        if (left.is_optional()) { return equal_contexts(left.as_optional().value(), right.as_optional().value()); }
        if (left.is_map()) {
            const auto a = left.as_map(), b = right.as_map();
            if (may_hold_error(a.key_schema()) && !equal_contexts(a.key_set(), b.key_set())) { return false; }
            if (may_hold_error(a.value_schema())) {
                for (const auto &[key, member] : a) {
                    if (!equal_contexts(member, b.at(key))) { return false; }
                }
            }
        } else if (left.is_set()) {
            const auto other = right.as_set();
            std::unordered_set<ValueView, ValueHash, ValueEqual> members;
            for (auto member : other) { members.insert(std::move(member)); }
            for (const auto member : left.as_set()) {
                const auto match = members.find(member);
                if (match == members.end() || !equal_contexts(member, *match)) { return false; }
            }
        } else if (auto a = left.try_as_indexed()) {
            const auto b = right.as_indexed();
            const auto *schema = left.schema();
            for (std::size_t i = 0; i < a->size(); ++i) {
                if (schema->fields && !may_hold_error(schema->fields[i].type)) { continue; }
                if (!equal_contexts(a->at(i), b.at(i))) { return false; }
            }
        }
        return true;
    }

    mutable std::unordered_map<const ValueTypeMetaData *, bool> schemas_;
    mutable const ValueTypeMetaData *last_schema_{};
    mutable bool last_may_hold_error_{};
};

}  // namespace hgraph::wiring_detail

#endif
