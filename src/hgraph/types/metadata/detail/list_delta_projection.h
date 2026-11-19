#pragma once
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

namespace hgraph::ts_data_plan_factory_detail
{
    // Both list layouts expose the same borrowed three-field bundle. The child
    // modification map keeps its layout-specific projection. Only detaching a
    // delta (e.g. for recording) copies the child payloads.
    template <class Layout> struct ListDeltaProjection
    {
        static const Layout &layout(const void *context) { return *static_cast<const Layout *>(context); }
        static bool          removed_candidate(const void *c, const void *m, std::size_t i) {
            return layout(c).is_removed(m, layout(c).candidate(m, i));
        }
        static std::size_t        index(const void *c, const void *m, std::size_t i) { return layout(c).candidate(m, i); }
        static Range<std::size_t> removed_indices(const void *c, const void *m) {
            return {c, m, layout(c).candidate_count(m), &removed_candidate, &index};
        }
        static ValueView key(const void *c, const void *m, std::size_t i) {
            const auto &l = layout(c);
            return {l.ordinal_key_binding, &l.index_key(m, l.candidate(m, i))};
        }
        static Range<ValueView> keys(const void *c, const void *m) {
            return {c, m, layout(c).candidate_count(m), &removed_candidate, &key};
        }
        static std::size_t removed_count(const void *c, const void *m) noexcept {
            auto range = removed_indices(c, m);
            return static_cast<std::size_t>(std::distance(range.begin(), range.end()));
        }
        static const void *removed_at(const void *c, const void *m, std::size_t ordinal) {
            for (auto i : removed_indices(c, m))
                if (ordinal-- == 0) return &layout(c).index_key(m, i);
            throw std::out_of_range("list removal ordinal out of range");
        }
        static ValueTypeRef key_type(const void *c, const void *, std::size_t) noexcept { return layout(c).ordinal_key_binding; }
        static bool         contains(const void *c, const void *m, const void *raw) {
            const auto i = *static_cast<const std::int64_t *>(raw);
            return i >= 0 && layout(c).is_removed(m, static_cast<std::size_t>(i));
        }
        static ValueTypeRef field_type(const void *c, const void *, std::size_t i) noexcept {
            const auto &l = layout(c);
            return i == 0 ? l.removed_set_binding : i == 1 ? l.modified_map_binding : l.ordinal_key_binding;
        }
        static const void *field_data(const void *c, const void *m, std::size_t i) {
            if (i < 2) return m;
            if (i == 2) return layout(c).changed_size(m);
            throw std::out_of_range("TSL delta field out of range");
        }
        static ValueView field(const void *c, const void *m, std::size_t i) { return {field_type(c, m, i), field_data(c, m, i)}; }
        static Range<ValueView> fields(const void *c, const void *m) { return {c, m, 3, nullptr, &field}; }
        static std::size_t      field_count(const void *, const void *) noexcept { return 3; }

        template <bool Bundle> static Value own(const void *c, const void *m) {
            const auto &l = layout(c);
            if constexpr (Bundle) {
                BundleBuilder out{l.canonical_delta()};
                out.set(0, own<false>(c, m));
                out.set(1, Value{field(c, m, 1)});
                if (const auto size = field(c, m, 2); size.has_value()) out.set(2, size);
                return out.build();
            } else {
                SetBuilder out{l.ordinal_key_binding};
                for (auto i : removed_indices(c, m)) static_cast<void>(out.insert_copy(&l.index_key(m, i)));
                return out.build();
            }
        }
        template <bool Bundle> static bool equals(const void *context, const void *left, const void *right) {
            if (left == right) return true;
            if (!left || !right) return false;
            if constexpr (Bundle) {
                for (std::size_t i = 0; i < 3; ++i)
                    if (!field(context, left, i).equals(field(context, right, i))) return false;
                return true;
            } else {
                if (removed_count(context, left) != removed_count(context, right)) return false;
                for (auto i : removed_indices(context, left))
                    if (!layout(context).is_removed(right, i)) return false;
                return true;
            }
        }

        template <bool Bundle> static std::size_t hash(const void *context, const void *memory) {
            std::size_t seed = 0;
            if constexpr (Bundle) {
                for (const auto &part : fields(context, memory)) {
                    constexpr std::size_t unset = 0x9e3779b97f4a7c15ULL;
                    const auto            value = part.has_value() ? part.hash() : unset;
                    seed ^= value + unset + (seed << 6U) + (seed >> 2U);
                }
            } else {
                for (const auto &k : keys(context, memory)) seed ^= k.hash();
            }
            return seed;
        }

        template <bool Bundle> static void value_ops(ValueOps &ops, const Layout *l) {
            ops.context      = l;
            ops.hash_impl    = &hash<Bundle>;
            ops.equals_impl  = &equals<Bundle>;
            ops.compare_impl = [](const void *c, const void *a, const void *b) noexcept {
                return fallback_on_exception(std::partial_ordering::unordered, [&] {
                    return equals<Bundle>(c, a, b) ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
                });
            };
            ops.to_string_impl = [](const void *c, const void *m) { return own<Bundle>(c, m).to_string(); };
            ops.to_python_impl = [](const void *c, const void *m) {
                const auto  v = own<Bundle>(c, m);
                const auto &o = v.binding().ops_ref();
                return o.to_python_impl(o.context, v.view().data());
            };
            ops.owning_type_impl = [](const void *c, ValueTypeRef) {
                if constexpr (Bundle) return layout(c).canonical_delta();
                else
                    return layout(c).removed_owned_binding;
            };
            ops.copy_construct_view_impl = [](const void *c, const ValueTypeRef &dest, void *dst, const void *m) {
                auto v = own<Bundle>(c, m);
                dest.checked_plan().copy_construct(dst, v.view().data());
            };
            ops.copy_assign_view_impl = [](const void *c, const ValueTypeRef &dest, void *dst, const void *m) {
                auto v = own<Bundle>(c, m);
                dest.checked_plan().copy_assign(dst, v.view().data());
            };
        }
        static void configure(Layout &l) {
            l.removed_set_ops =
                SetValueOps{{{ValueOpsKind::Set}, &removed_count, &removed_at, &key_type, &keys, nullptr}, &contains};
            value_ops<false>(l.removed_set_ops, &l);
            l.list_delta_bundle_ops =
                IndexedValueOps{{ValueOpsKind::Indexed}, &field_count, &field_data, &field_type, &fields, nullptr};
            value_ops<true>(l.list_delta_bundle_ops, &l);
        }
    };
}  // namespace hgraph::ts_data_plan_factory_detail
