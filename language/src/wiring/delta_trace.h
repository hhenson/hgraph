#ifndef HGL_WIRING_DELTA_TRACE_H
#define HGL_WIRING_DELTA_TRACE_H

#include <hgl/ordinary_values.h>
#include <hgraph/types/value/value_hash.h>
#include <unordered_map>
#include <unordered_set>

namespace hgl::wiring {
    // Construction-time admission state. Payload values are deliberately not
    // retained: only owning exact keys and recursively unchanged child state matter.
    class DeltaTrace {
      public:
        void accept(const hgraph::TSValueTypeMetaData *shape, const hgraph::ValueView &native,
                    const ordinary::OptionalField &optional = {}) {
            using namespace hgraph;
            switch (shape->kind) {
                case TSTypeKind::TS:
                case TSTypeKind::TSW: {
                    if (shape->delta_value_schema->is_abstract_bundle()) {
                        const auto family = value_type_for_wiring(shape->delta_value_schema);
                        const auto concrete = native.concrete();
                        require(concrete.valid() && family.ops_ref().accepts_source(family, concrete.binding()),
                                "publication is not a concrete member of its declared family");
                    }
                    ordinary::validate_complete_value(native, optional);
                    return;
                }
                case TSTypeKind::TSS: {
                    const auto parts = native.as_bundle();
                    const auto added = parts.at(0).as_set();
                    const auto removed = parts.at(1).as_set();
                    require(!added.empty() || !removed.empty(), "empty set publication");
                    for (const auto key : added) { ordinary::validate_scalar_key(key); }
                    for (const auto key : removed) { ordinary::validate_scalar_key(key); }
                    for (const auto value : added) {
                        require(!members_.contains(value), "addition of a present set member");
                        require(!removed.contains(value), "overlapping set membership changes");
                    }
                    for (const auto value : removed) { require(members_.contains(value), "removal of an absent set member"); }
                    for (const auto value : added) { members_.insert(Value{value}); }
                    for (const auto value : removed) { members_.erase(members_.find(value)); }
                    return;
                }
                case TSTypeKind::TSL: {
                    if (shape->is_unbounded_tsl()) {
                        const auto parts = native.as_bundle();
                        const auto removed = parts.at(0).as_set();
                        const auto modified = parts.at(1).as_map();
                        require(!removed.empty() || !modified.empty(), "empty growing-list publication");
                        std::size_t retained = length_;
                        for (const auto item : removed) {
                            const auto index = item.checked_as<Int>();
                            require(index >= 0 && static_cast<std::uint64_t>(index) < length_, "growing-list removal outside its live length");
                            retained = std::min(retained, static_cast<std::size_t>(index));
                        }
                        require(removed.size() == length_ - retained, "growing-list removals must form a complete tail");
                        std::size_t appended = 0;
                        for (const auto [key, child] : modified) {
                            const auto index = key.checked_as<Int>();
                            require(index >= 0, "negative growing-list index");
                            if (!removed.empty()) { require(static_cast<std::uint64_t>(index) < retained, "removed and modified growing-list overlap"); }
                            else if (static_cast<std::uint64_t>(index) >= length_) { ++appended; }
                        }
                        const auto next_length = retained + appended;
                        for (const auto [key, child] : modified) {
                            require(static_cast<std::uint64_t>(key.checked_as<Int>()) < next_length, "gap in growing-list append");
                        }
                        for (auto it = children_.begin(); it != children_.end();) {
                            if (static_cast<std::uint64_t>(it->first) >= retained) { it = children_.erase(it); }
                            else { ++it; }
                        }
                        for (const auto [key, child] : modified) { children_[key.checked_as<Int>()].accept(shape->element_ts(), child, optional); }
                        length_ = next_length;
                        return;
                    }
                    const auto entries = native.as_map();
                    require(!entries.empty(), "empty fixed-list publication");
                    for (const auto [key, child] : entries) {
                        const auto index = key.checked_as<Int>();
                        require(index >= 0 && static_cast<std::uint64_t>(index) < shape->value_schema->fixed_size,
                                "fixed-list index outside its extent");
                        children_[index].accept(shape->element_ts(), child, optional);
                    }
                    return;
                }
                case TSTypeKind::TSB: {
                    const auto entries = native.as_bundle();
                    bool any = false;
                    for (std::size_t i = 0; i < shape->field_count(); ++i) {
                        if (!entries.element_valid(i)) { continue; }
                        any = true;
                        children_[static_cast<Int>(i)].accept(shape->fields()[i].type, entries.at(i), optional);
                    }
                    require(any, "empty bundle publication");
                    return;
                }
                case TSTypeKind::TSD: {
                    const auto parts = native.as_bundle();
                    const auto removed = parts.at(0).as_set();
                    const auto modified = parts.at(1).as_map();
                    require(!removed.empty() || !modified.empty(), "empty map publication");
                    for (const auto key : removed) { ordinary::validate_scalar_key(key); }
                    for (const auto [key, child] : modified) { ordinary::validate_scalar_key(key); }
                    for (const auto key : removed) {
                        require(keyed_children_.contains(key), "removal of an absent map key");
                        require(!modified.contains(key), "overlapping map key changes");
                    }
                    for (const auto key : removed) { keyed_children_.erase(keyed_children_.find(key)); }
                    for (const auto [key, child] : modified) { keyed_children_[Value{key}].accept(shape->element_ts(), child, optional); }
                    return;
                }
                default: throw std::invalid_argument("unsupported publication shape");
            }
        }
      private:
        static void require(bool condition, const char *message) {
            if (!condition) { throw std::invalid_argument(message); }
        }
        std::unordered_set<hgraph::Value, hgraph::ValueHash, hgraph::ValueEqual> members_;
        std::unordered_map<hgraph::Value, DeltaTrace, hgraph::ValueHash, hgraph::ValueEqual> keyed_children_;
        std::unordered_map<hgraph::Int, DeltaTrace> children_;
        std::size_t length_{};
    };
}
#endif
