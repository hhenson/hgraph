#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H

/**
 * Collection-operator overloads (catalogue: ``operators/collection.h``).
 *
 * This header holds the templated/static-node bodies for collection views,
 * scalar map transforms, TSS/TSD/TSL aggregates, set algebra, partitioning,
 * and the combine-to-TSD family. Registration lives in
 * ``src/hgraph/lib/std/operators/collection_impl.cpp``.
 */

#include <functional>
#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/impl/collection_input_semantics.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/static_node.h>

#include <ankerl/unordered_dense.h>

#include <compare>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    namespace collection_impl_detail
    {
        struct ValueKeyHash
        {
            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
        };

        inline void apply_tss_delta(TSSOutputView &out, const std::vector<Value> &removed,
                                    const std::vector<Value> &added)
        {
            if (removed.empty() && added.empty())
            {
                // An evaluated set expression has a current value even when
                // that value is empty. Do not emit repeated empty ticks once
                // the identity has been published.
                if (!out.valid())
                {
                    auto mutation = out.begin_mutation(out.evaluation_time());
                    mutation.touch();
                }
                return;
            }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const Value &member : removed) { (void)mutation.remove(member.view()); }
            for (const Value &member : added) { (void)mutation.add(member.view()); }
        }

        /**
         * ``keys_(tsd) -> TSS[K]`` — the dictionary's key set as a ZERO-COPY
         * projection over the same output (``TSDOutputView::key_set``): no
         * node is wired; the returned port addresses the dict's key-set view
         * via the ``ts_key_set_path_component`` path sentinel. This is the
         * C++ analogue of Python's ``keys_tsd_as_tss`` REF.
         */
        /** keys_[OUT: TS[Set[K]]](tsd): the key set as a VALUE snapshot
            (hgraph's keys_ TS[Set] overload) - the full set on every key
            change, not the TSS projection. */
        struct keys_tsd_as_set
        {
            static constexpr auto name = "keys_tsd_as_set";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                // The output pattern TS<ScalarVar<"S">> binds S from the
                // REQUESTED output (keys_[OUT: TS[Set[K]]]).
                const auto *element = resolution.find_scalar("S");
                if (element == nullptr || element->value_kind() != ValueTypeKind::Set) { return false; }
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                return schema != nullptr && schema->key_type() == element->element_type;
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"S">>> out)
            {
                const TSDInputView &dict   = ts;
                const auto         &erased = static_cast<const TSOutputView &>(out);
                const auto *out_meta = erased.schema()->value_schema;
                SetBuilder builder{value_type_for_active_realization(out_meta->element_type)};
                for (const auto key : dict.keys()) { (void)builder.insert(key); }
                Value set = builder.build();
                if (erased.data_view().has_current_value() && erased.value().equals(set.view())) { return; }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(set)));
            }
        };

        /** values_[TSS[V]](tsd): the dictionary's VALUES as a TSS (hgraph's
            values_tsd_to_tss - own-output reconciliation; expensive on a
            large dict by design). */
        struct values_tsd_as_tss
        {
            static constexpr auto name = "values_tsd_as_tss";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *element = resolution.find_scalar("E");
                if (element == nullptr) { return false; }
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                const auto *value  = schema != nullptr ? ts_value_schema(schema->element_ts()) : nullptr;
                return value == element;
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts, Out<TSS<ScalarVar<"E">>> out)
            {
                const TSDInputView  &dict    = ts;
                TSSOutputView       &erased  = out;
                // TSS value schema = Set[element]; the element binding builds it.
                const auto *element_meta = erased.schema()->value_schema->element_type;
                SetBuilder union_builder{value_type_for_active_realization(element_meta)};
                for (auto &&[key, child] : dict.items())
                {
                    if (child.valid()) { (void)union_builder.insert(child.value()); }
                }
                Value current = union_builder.build();
                auto  current_set = current.view().as_set();

                std::vector<Value> removed;
                for (const ValueView &value : out.values())
                {
                    if (!current_set.contains(value)) { removed.emplace_back(value); }
                }
                std::vector<Value> added;
                for (const ValueView &value : current_set.values())
                {
                    if (!out.contains(value)) { added.emplace_back(value); }
                }
                apply_tss_delta(out, removed, added);
            }
        };

        struct keys_tsd
        {
            static constexpr auto name = "keys_tsd";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                // The zero-copy TSS projection serves TSS requests only; a
                // requested TS[Set[K]] snapshot goes to keys_tsd_as_set.
                const auto *requested = resolution.find_ts("__out__");
                return requested == nullptr || requested->kind == TSTypeKind::TSS;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                if (schema == nullptr) { return; }
                resolution.bind_ts("__out__", TypeRegistry::instance().tss(schema->key_type()));
            }

            static WiringPortRef compose(Wiring &, NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                return subgraph_wiring_detail::tsd_key_set_ref(ts.erased());
            }
        };

        [[nodiscard]] inline bool tss_equal(const TSSInputView &lhs, const TSSInputView &rhs)
        {
            if (lhs.size() != rhs.size()) { return false; }
            for (const ValueView &key : lhs.values())
            {
                if (!rhs.contains(key)) { return false; }
            }
            return true;
        }

        [[nodiscard]] inline bool tsd_equal(const TSDInputView &lhs, const TSDInputView &rhs)
        {
            if (lhs.size() != rhs.size()) { return false; }
            for (const auto [key, lhs_child] : lhs.items())
            {
                TSInputView rhs_child = rhs.at(key);
                if (!lhs_child.valid() || !rhs_child.valid() || !lhs_child.value().equals(rhs_child.value()))
                {
                    return false;
                }
            }
            return true;
        }

        struct not_tss
        {
            static constexpr auto name = "not_tss";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
            {
                if (!collection_input_semantics::has_bound_source(ts)) { return; }
                // hgraph parity: ticks only when the answer CHANGES.
                const Bool value = ts.empty();
                if (!out.valid() || out.value().checked_as<Bool>() != value) { out.set(value); }
            }
        };

        struct not_tsd
        {
            static constexpr auto name = "not_tsd";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Out<TS<Bool>> out)
            {
                if (!collection_input_semantics::has_bound_source(ts)) { return; }
                // hgraph parity: ticks only when the answer CHANGES.
                const Bool value = ts.empty();
                if (!out.valid() || out.value().checked_as<Bool>() != value) { out.set(value); }
            }
        };

        struct and_tss
        {
            static constexpr auto name = "and_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>> lhs, In<"rhs", TSS<ScalarVar<"K">>> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(!lhs.empty() && !rhs.empty());
            }
        };

        struct or_tss
        {
            static constexpr auto name = "or_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>> lhs, In<"rhs", TSS<ScalarVar<"K">>> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(!lhs.empty() || !rhs.empty());
            }
        };

        struct eq_tss
        {
            static constexpr auto name = "eq_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>> lhs, In<"rhs", TSS<ScalarVar<"K">>> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(tss_equal(lhs, rhs));
            }
        };

        /** eq_(tsd, tsd, epsilon=ts): float dictionaries compare within a
            ticking tolerance (hgraph's epsilon comparison). */
        struct eq_tsd_epsilon
        {
            static constexpr auto name = "eq_tsd_epsilon";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TS<Float>>> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TS<Float>>> rhs, In<"epsilon", TS<Float>> epsilon,
                             Out<TS<Bool>> out)
            {
                const TSDInputView &lhs_dict = lhs;
                const TSDInputView &rhs_dict = rhs;
                const Float         eps      = epsilon.value();

                bool equal = lhs_dict.size() == rhs_dict.size();
                if (equal)
                {
                    for (auto &&[key, child] : lhs_dict.items())
                    {
                        if (!rhs_dict.contains(key)) { equal = false; break; }
                        auto other = rhs_dict.at(key);
                        if (!child.valid() || !other.valid()) { equal = false; break; }
                        const Float a = child.value().checked_as<Float>();
                        const Float b = other.value().checked_as<Float>();
                        const Float diff = a > b ? a - b : b - a;
                        if (diff > eps) { equal = false; break; }
                    }
                }
                out.set(equal);
            }
        };

        struct eq_tsd
        {
            static constexpr auto name = "eq_tsd";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(tsd_equal(lhs, rhs));
            }
        };

        struct min_tss_unary
        {
            static constexpr auto name = "min_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"K">>> out)
            {
                const TSSInputView &set = ts;
                std::optional<Value> best;
                for (const ValueView &key : set.values())
                {
                    if (!best.has_value() || key.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(key);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tss_unary
        {
            static constexpr auto name = "max_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"K">>> out)
            {
                const TSSInputView &set = ts;
                std::optional<Value> best;
                for (const ValueView &key : set.values())
                {
                    if (!best.has_value() || key.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(key);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        /** min_/max_ over a TSS with a default for the EMPTY set. */
        template <bool Min>
        struct extremum_tss_default
        {
            static constexpr auto name = Min ? "min_tss_default" : "max_tss_default";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                             In<"default_value", TS<ScalarVar<"K">>, InputValidity::Unchecked> default_value,
                             Out<TS<ScalarVar<"K">>> out)
            {
                const TSSInputView  &set = ts;
                std::optional<Value> best;
                for (const ValueView &key : set.values())
                {
                    if (!best.has_value() ||
                        (Min ? key.compare(best->view()) == std::partial_ordering::less
                             : key.compare(best->view()) == std::partial_ordering::greater))
                    {
                        best.emplace(key);
                    }
                }
                if (best.has_value())
                {
                    out.apply(best->view());
                    return;
                }
                if (default_value.valid()) { out.apply(default_value.base().value()); }
            }
        };

        struct min_tsd_unary
        {
            static constexpr auto name = "min_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                const TSDInputView &dict = ts;
                std::optional<Value> best;
                for (const TSInputView &child : dict.valid_values())
                {
                    const ValueView value = child.value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tsd_unary
        {
            static constexpr auto name = "max_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                const TSDInputView &dict = ts;
                std::optional<Value> best;
                for (const TSInputView &child : dict.valid_values())
                {
                    const ValueView value = child.value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct min_tsl_unary
        {
            static constexpr auto name = "min_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<ScalarVar<"V">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                std::optional<Value> best;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const ValueView value = child.base().value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::less)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        struct max_tsl_unary
        {
            static constexpr auto name = "max_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<ScalarVar<"V">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                             Out<TS<ScalarVar<"V">>> out)
            {
                std::optional<Value> best;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const ValueView value = child.base().value();
                    if (!best.has_value() || value.compare(best->view()) == std::partial_ordering::greater)
                    {
                        best.emplace(value);
                    }
                }
                if (best.has_value()) { out.apply(best->view()); }
            }
        };

        template <typename T>
        struct sum_tss_unary
        {
            static constexpr auto name = "sum_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (const T &key : ts.values()) { total += key; }
                out.set(total);
            }
        };

        template <typename T, auto N>
        [[nodiscard]] inline std::tuple<Float, std::size_t, std::size_t> tsl_sum_valid_and_size(
            const In<"ts", TSL<TS<T>, N>, InputValidity::Unchecked> &ts)
        {
            Float       total       = 0.0;
            std::size_t valid_count = 0;
            const auto  size        = ts.size();
            for (std::size_t i = 0; i < size; ++i)
            {
                auto child = ts[i];
                if (!child.valid()) { continue; }
                total += static_cast<Float>(child.value());
                ++valid_count;
            }
            return {total, valid_count, size};
        }

        template <typename T>
        struct sum_tsl_unary
        {
            static constexpr auto name = "sum_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (child.valid()) { total += child.value(); }
                }
                out.set(total);
            }
        };

        template <typename T>
        struct mean_tsl_unary
        {
            static constexpr auto name = "mean_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, _, size] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                out.set(size == 0 ? std::numeric_limits<Float>::quiet_NaN()
                                  : total / static_cast<Float>(size));
            }
        };

        template <typename T>
        struct var_tsl_unary
        {
            static constexpr auto name = "var_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, valid_count, _] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                if (valid_count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(valid_count);
                Float       sum_sq     = 0.0;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(valid_count - 1));
            }
        };

        template <typename T>
        struct std_tsl_unary
        {
            static constexpr auto name = "std_tsl_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSL<TS<T>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, valid_count, _] = tsl_sum_valid_and_size(ts);
                static_cast<void>(_);
                if (valid_count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(valid_count);
                Float       sum_sq     = 0.0;
                for (std::size_t i = 0; i < ts.size(); ++i)
                {
                    auto child = ts[i];
                    if (!child.valid()) { continue; }
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(valid_count - 1)));
            }
        };

        template <typename T>
        [[nodiscard]] inline std::pair<Float, std::size_t> tss_sum_and_count(
            const In<"ts", TSS<T>, InputValidity::Unchecked> &ts)
        {
            Float       total = 0.0;
            std::size_t count = 0;
            for (const T &key : ts.values())
            {
                total += static_cast<Float>(key);
                ++count;
            }
            return {total, count};
        }

        template <typename T>
        struct mean_tss_unary
        {
            static constexpr auto name = "mean_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                out.set(count == 0 ? std::numeric_limits<Float>::quiet_NaN() : total / static_cast<Float>(count));
            }
        };

        template <typename T>
        struct var_tss_unary
        {
            static constexpr auto name = "var_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const T &key : ts.values())
                {
                    const Float delta = static_cast<Float>(key) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(count - 1));
            }
        };

        template <typename T>
        struct std_tss_unary
        {
            static constexpr auto name = "std_tss_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSS<T>, InputValidity::Unchecked> ts, Out<TS<Float>> out)
            {
                const auto [total, count] = tss_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const T &key : ts.values())
                {
                    const Float delta = static_cast<Float>(key) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(count - 1)));
            }
        };

        template <typename T>
        struct sum_tsd_unary
        {
            static constexpr auto name = "sum_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts, Out<TS<T>> out)
            {
                T total{};
                for (const auto child : ts.valid_values()) { total += child.value(); }
                out.set(total);
            }
        };

        template <typename T>
        [[nodiscard]] inline std::pair<Float, std::size_t> tsd_sum_and_count(
            const In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> &ts)
        {
            Float       total = 0.0;
            std::size_t count = 0;
            for (const auto child : ts.valid_values())
            {
                total += static_cast<Float>(child.value());
                ++count;
            }
            return {total, count};
        }

        template <typename T>
        struct mean_tsd_unary
        {
            static constexpr auto name = "mean_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                out.set(count == 0 ? std::numeric_limits<Float>::quiet_NaN() : total / static_cast<Float>(count));
            }
        };

        template <typename T>
        struct var_tsd_unary
        {
            static constexpr auto name = "var_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const auto child : ts.valid_values())
                {
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(sum_sq / static_cast<Float>(count - 1));
            }
        };

        template <typename T>
        struct std_tsd_unary
        {
            static constexpr auto name = "std_tsd_unary";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<T>>, InputValidity::Unchecked> ts,
                             Out<TS<Float>> out)
            {
                const auto [total, count] = tsd_sum_and_count(ts);
                if (count <= 1)
                {
                    out.set(0.0);
                    return;
                }

                const Float mean_value = total / static_cast<Float>(count);
                Float       sum_sq     = 0.0;
                for (const auto child : ts.valid_values())
                {
                    const Float delta = static_cast<Float>(child.value()) - mean_value;
                    sum_sq += delta * delta;
                }
                out.set(std::sqrt(sum_sq / static_cast<Float>(count - 1)));
            }
        };

        /**
         * Binary TSS union — the fold step. Removal semantics mirror Python's
         * ``union_multiple_tss``: an element leaves the union only when no
         * input still holds it.
         */
        struct union_tss_binary
        {
            static constexpr auto name = "union_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) && !rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!out.contains(key)) { added.emplace_back(key); }
                }
                for (const ValueView &key : rhs_set.values())
                {
                    if (!lhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        struct intersection_tss_binary
        {
            static constexpr auto name = "intersection_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) || !rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        struct difference_tss_binary
        {
            static constexpr auto name = "difference_tss";

            // hgraph gating: BOTH inputs must be valid (test: lhs valid
            // before rhs emits NOTHING until rhs arrives).
            static void eval(In<"lhs", TSS<ScalarVar<"K">>> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (!lhs_set.contains(key) || rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        /** TSS +/- a SCALAR element (hgraph's add_tss_and_scalar /
            sub_tss_and_scalar): reconcile own output against
            lhs u {rhs} / lhs \ {rhs}. */
        template <bool Add>
        struct tss_scalar_adjust
        {
            static constexpr auto name = Add ? "add_tss_scalar" : "sub_tss_scalar";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>> lhs, In<"rhs", TS<ScalarVar<"K">>> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                // hgraph parity (add_tss_and_scalar / sub_tss_and_scalar):
                // forward lhs's DELTA, adjusted for the scalar - elements
                // added by PAST scalar ticks persist unless lhs removes them.
                const auto          scalar = rhs.base().value();
                const TSSInputView &set    = lhs;   // erased delta accessors

                std::vector<Value> added;
                std::vector<Value> removed;
                for (const ValueView &key : set.added())
                {
                    // The scalar's membership is decided below, not by lhs.
                    if (key.equals(scalar)) { continue; }
                    added.emplace_back(key);
                }
                for (const ValueView &key : set.removed())
                {
                    if (Add && key.equals(scalar)) { continue; }   // the scalar keeps it alive
                    removed.emplace_back(key);
                }
                if (Add)
                {
                    if (!out.contains(scalar)) { added.emplace_back(Value{scalar}); }
                }
                else
                {
                    if (out.contains(scalar)) { removed.emplace_back(Value{scalar}); }
                }
                apply_tss_delta(out, removed, added);
            }
        };

        struct symmetric_difference_tss_binary
        {
            static constexpr auto name = "symmetric_difference_tss";

            static void eval(In<"lhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSS<ScalarVar<"K">>, InputValidity::Unchecked> rhs,
                             Out<TSS<ScalarVar<"K">>> out)
            {
                auto lhs_set = lhs.data_view();
                auto rhs_set = rhs.data_view();

                std::vector<Value> removed;
                for (const ValueView &key : out.values())
                {
                    if (lhs_set.contains(key) == rhs_set.contains(key)) { removed.emplace_back(key); }
                }

                std::vector<Value> added;
                for (const ValueView &key : lhs_set.values())
                {
                    if (!rhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }
                for (const ValueView &key : rhs_set.values())
                {
                    if (!lhs_set.contains(key) && !out.contains(key)) { added.emplace_back(key); }
                }

                apply_tss_delta(out, removed, added);
            }
        };

        template <typename Keep>
        inline void erase_tsd_keys_not_matching(TSDDataMutationView &mutation, const TSDOutputView &out, Keep keep)
        {
            std::vector<Value> removed;
            for (const ValueView &key : out.keys())
            {
                if (!keep(key)) { removed.emplace_back(key); }
            }
            for (const Value &key : removed) { (void)mutation.erase(key.view()); }
        }

        inline void copy_tsd_child_if_changed(TSDDataMutationView &mutation, const TSDOutputView &out,
                                              const ValueView &key, const TSInputView &source)
        {
            if (!source.valid()) { return; }

            TSOutputView current = out.at(key);
            if (current.valid() && current.value().equals(source.value())) { return; }
            mutation.set(key, source.value());
        }

        inline void copy_value_if_changed(TSDDataMutationView &mutation, const TSDOutputView &out,
                                          const ValueView &key, const ValueView &value)
        {
            TSOutputView current = out.at(key);
            if (current.valid() && current.value().equals(value)) { return; }
            mutation.set(key, value);
        }

        inline void set_nested_tsd_child(TSDDataMutationView &outer_mutation, DateTime evaluation_time,
                                         const ValueView &outer_key, const ValueView &inner_key,
                                         const TSInputView &source)
        {
            if (!source.valid()) { return; }

            TSDataView       inner = outer_mutation.at(outer_key);
            TSDDataView      inner_dict = inner.as_dict();
            auto             inner_mutation = inner_dict.begin_mutation(evaluation_time);
            TSDataView       current = inner_dict.at(inner_key);
            if (current.valid() && current.value().equals(source.value())) { return; }
            inner_mutation.set(inner_key, source.value());
        }

        inline void erase_nested_tsd_child(TSDDataMutationView &outer_mutation, DateTime evaluation_time,
                                           const ValueView &outer_key, const ValueView &inner_key)
        {
            TSDataView       inner = outer_mutation.at(outer_key);
            TSDDataView      inner_dict = inner.as_dict();
            auto             inner_mutation = inner_dict.begin_mutation(evaluation_time);
            (void)inner_mutation.erase(inner_key);
        }

        inline bool tsd_key_has_modified_valid_child(const TSDInputView &tsd, const ValueView &key)
        {
            const std::size_t slot = tsd.find_slot(key);
            return slot != TS_DATA_NO_CHILD_ID && tsd.slot_modified(slot) && tsd.at_slot(slot).valid();
        }

        using FlippedPreviousIndex = ankerl::unordered_dense::map<Value, Value, ValueKeyHash, ValueKeyEqual>;

        inline FlippedPreviousIndex build_flipped_previous_index(const TSDOutputView &out)
        {
            FlippedPreviousIndex previous;
            previous.reserve(out.size());
            for (const auto [flipped_key, source_key] : out.valid_items())
            {
                previous.insert_or_assign(Value{source_key.value()}, Value{flipped_key});
            }
            return previous;
        }

        struct rekey_tsd_scalar
        {
            static constexpr auto name = "rekey_tsd_scalar";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"new_keys", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>, InputValidity::Unchecked> new_keys,
                             RecordableState<TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>> previous,
                             Out<TSD<ScalarVar<"K1">, TsVar<"V">>> out)
            {
                TSDOutputView          &out_dict = out;
                TSDOutputView          &prev     = previous;
                auto                    out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                auto                    prev_mutation = prev.begin_mutation(prev.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                }

                for (const ValueView &source_key : new_keys.removed_keys())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                    (void)prev_mutation.erase(source_key);
                }

                for (const auto [source_key, new_key] : new_keys.modified_items())
                {
                    if (!new_key.valid())
                    {
                        TSOutputView mapped_key = prev.at(source_key);
                        if (mapped_key.valid()) { (void)out_mutation.erase(mapped_key.value()); }
                        (void)prev_mutation.erase(source_key);
                        continue;
                    }

                    const TSInputView &new_key_base = new_key.base();
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid() && !mapped_key.value().equals(new_key_base.value()))
                    {
                        (void)out_mutation.erase(mapped_key.value());
                    }

                    prev_mutation.set(source_key, new_key_base.value());
                    TSInputView source = static_cast<const TSDInputView &>(ts).at(source_key);
                    copy_tsd_child_if_changed(out_mutation, out_dict, new_key_base.value(), source);
                }

                for (const auto [source_key, source] : ts.modified_items())
                {
                    TSOutputView mapped_key = prev.at(source_key);
                    if (mapped_key.valid())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, mapped_key.value(), source);
                    }
                }
            }
        };

        inline void publish_tsd_refs(const TSOutputView &out,
                                      const std::vector<std::pair<Value, Value>> &pairs);   // defined below

        // ----- key pivots (collapse / uncollapse / flip_keys) --------------

        /** The nested-TSD key components of ``schema`` down to the non-TSD
            leaf: TSD[K, TSD[K1, V]] -> {K, K1}, leaf V. */
        inline void nested_tsd_key_path(const TSValueTypeMetaData *schema,
                                        std::vector<const ValueTypeMetaData *> &keys,
                                        const TSValueTypeMetaData *&leaf)
        {
            auto &registry = TypeRegistry::instance();
            const auto *current = registry.dereference(schema);
            while (const auto *tsd = time_series_schema_as<AnyTSD>(current))
            {
                keys.push_back(tsd->key_type());
                current = registry.dereference(tsd->element_ts());
            }
            leaf = current;
        }

        /** collapse_keys(TSD[K, TSD[K1, ...]]) -> TSD[(K, K1, ...), REF[leaf]]
            (hgraph's collapse_keys_tsd; ANY nesting depth in one erased walk -
            the runtime can recurse schemas where python needed type-level
            composition). Elements are REFERENCES to the leaves. */
        struct collapse_keys_tsd
        {
            static constexpr auto name = "collapse_keys_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                const auto *element = schema != nullptr ? time_series_schema_as<AnyTSD>(schema->element_ts())
                                                        : nullptr;
                return element != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                std::vector<const ValueTypeMetaData *> keys;
                const TSValueTypeMetaData             *leaf = nullptr;
                nested_tsd_key_path(time_series_schema_at(context, 0, SchemaRefMode::Direct), keys, leaf);
                if (keys.size() < 2 || leaf == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                resolution.bind_ts("__out__", registry.tsd(registry.tuple(keys), registry.ref(leaf)));
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Out<TsVar<"__out__">> out)
            {
                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        dict_out = erased.data_view().as_dict();
                const auto  evaluation_time = erased.evaluation_time();
                auto        mutation = dict_out.begin_mutation(evaluation_time);
                const auto *tuple_meta = erased.schema()->key_type();
                const auto tuple_binding = value_type_for_active_realization(tuple_meta);
                const std::size_t depth = tuple_meta->field_count;

                // Removed SUBTREES erase every output tuple-key with the
                // matching prefix (own-output reconciliation - the removed
                // child's keys are no longer readable).
                const auto erase_prefix = [&](const std::vector<Value> &prefix) {
                    std::vector<Value> stale;
                    for (const auto [key, child] : dict_out.items())
                    {
                        auto components = key.as_indexed_view();
                        bool match = true;
                        for (std::size_t index = 0; index < prefix.size(); ++index)
                        {
                            if (!components.at(index).equals(prefix[index].view())) { match = false; break; }
                        }
                        if (match) { stale.emplace_back(key); }
                    }
                    for (const Value &key : stale) { (void)mutation.erase(key.view()); }
                };

                const auto make_tuple_key = [&](const std::vector<Value> &components) {
                    BundleBuilder builder{tuple_binding};
                    for (std::size_t index = 0; index < components.size(); ++index)
                    {
                        builder.set(index, Value{components[index].view()});
                    }
                    return builder.build();
                };

                // Recursive delta walk: at each level removed keys erase the
                // prefix, modified children descend; at the leaf level the
                // element's REFERENCE publishes under the full tuple key.
                const std::function<void(const TSDInputView &, std::vector<Value> &)> walk =
                    [&](const TSDInputView &level, std::vector<Value> &path) {
                        for (const ValueView &removed : level.removed_keys())
                        {
                            path.emplace_back(removed);
                            erase_prefix(path);
                            path.pop_back();
                        }
                        for (const auto [key, child] : level.modified_items())
                        {
                            path.emplace_back(key);
                            if (path.size() == depth)
                            {
                                if (child.valid())
                                {
                                    Value tuple_key = make_tuple_key(path);
                                    auto  element   = mutation.at(tuple_key.view());
                                    Value reference{child.reference()};
                                    if (!(element.has_current_value() &&
                                          element.value().checked_as<TimeSeriesReference>() ==
                                              reference.view().checked_as<TimeSeriesReference>()))
                                    {
                                        auto element_mutation =
                                            TSOutputView{erased.output(), element, evaluation_time}
                                                .begin_mutation(evaluation_time);
                                        static_cast<void>(element_mutation.move_value_from(std::move(reference)));
                                    }
                                }
                            }
                            else if (child.valid())
                            {
                                walk(TSDInputView{child.borrowed_ref()}, path);
                            }
                            path.pop_back();
                        }
                    };

                const TSDInputView &root = ts;
                std::vector<Value>  path;
                walk(root, path);
            }
        };

        /** uncollapse_keys(TSD[(K, K1, ...), REF[V]]) -> TSD[K, TSD[K1, ...]]
            (hgraph's uncollapse_keys_tsd; ANY tuple arity in one erased
            walk). ``remove_empty`` erases a group whose members all left. */
        struct uncollapse_keys_tsd
        {
            static constexpr auto name = "uncollapse_keys_tsd";

            static auto defaults() { return std::tuple{arg<"remove_empty">(Bool{true})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                if (schema == nullptr || schema->key_type() == nullptr)
                {
                    return false;
                }
                const auto *key = schema->key_type();
                return key->value_kind() == ValueTypeKind::Tuple && key->field_count >= 2;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                if (schema == nullptr) { return; }
                const auto *key = schema->key_type();
                if (key == nullptr || key->value_kind() != ValueTypeKind::Tuple || key->field_count < 2) { return; }
                const TSValueTypeMetaData *out =
                    registry.ref(registry.dereference(schema->element_ts()));
                for (std::size_t index = key->field_count; index-- > 0;)
                {
                    out = registry.tsd(key->fields[index].type, out);
                }
                resolution.bind_ts("__out__", out);
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Scalar<"remove_empty", Bool> remove_empty, Out<TsVar<"__out__">> out)
            {
                const auto &erased          = static_cast<const TSOutputView &>(out);
                const auto  evaluation_time = erased.evaluation_time();
                auto        root_dict       = erased.data_view().as_dict();
                auto        root_mutation   = root_dict.begin_mutation(evaluation_time);
                const std::size_t depth     = erased.schema() != nullptr ? 0 : 0;
                (void)depth;

                // Descend to the group holding the tuple's LAST component,
                // creating intermediate dictionaries as needed.
                const auto leaf_group = [&](const ValueView &tuple_key, auto &&fn) {
                    auto components = tuple_key.as_indexed_view();
                    TSDataView current;
                    for (std::size_t index = 0; index + 1 < components.size(); ++index)
                    {
                        if (index == 0) { current = root_mutation.at(components.at(0)); }
                        else
                        {
                            auto dict = current.as_dict();
                            auto mut  = dict.begin_mutation(evaluation_time);
                            current   = mut.at(components.at(index));
                        }
                    }
                    auto dict = current.as_dict();
                    auto mut  = dict.begin_mutation(evaluation_time);
                    fn(mut, components.at(components.size() - 1));
                };

                const TSDInputView &source = ts;
                for (const ValueView &tuple_key : source.removed_keys())
                {
                    leaf_group(tuple_key, [&](TSDDataMutationView &group, const ValueView &leaf_key) {
                        (void)group.erase(leaf_key);
                    });
                }

                for (const auto [tuple_key, child] : source.modified_items())
                {
                    if (!child.valid()) { continue; }
                    leaf_group(tuple_key, [&](TSDDataMutationView &group, const ValueView &leaf_key) {
                        auto element = group.at(leaf_key);
                        Value reference{child.reference()};
                        if (element.has_current_value() &&
                            element.value().checked_as<TimeSeriesReference>() ==
                                reference.view().checked_as<TimeSeriesReference>())
                        {
                            return;
                        }
                        auto element_mutation = TSOutputView{erased.output(), element, evaluation_time}
                                                    .begin_mutation(evaluation_time);
                        static_cast<void>(element_mutation.move_value_from(std::move(reference)));
                    });
                }

                if (remove_empty.value())
                {
                    // Erase groups (recursively) whose members all left.
                    const std::function<bool(TSDDataMutationView &)> prune =
                        [&](TSDDataMutationView &mutation) -> bool {
                        std::vector<Value> empty_keys;
                        auto               mutation_view = mutation.view();
                        {
                            for (auto &&[key, child] : mutation_view.items())
                            {
                                if (child.schema() != nullptr && child.schema()->kind == TSTypeKind::TSD)
                                {
                                    auto child_dict = child.as_dict();
                                    auto child_mut  = child_dict.begin_mutation(evaluation_time);
                                    if (prune(child_mut)) { empty_keys.emplace_back(key); }
                                }
                            }
                        }
                        for (const Value &key : empty_keys) { (void)mutation.erase(key.view()); }
                        return mutation_view.size() == 0;
                    };
                    (void)prune(root_mutation);
                }
            }
        };

        /** filter_tsd_by_matches(ts, matches): entries whose match is TRUE
            as references (hgraph's _filter_by_tsd; stateless own-output
            reconciliation - matches drive membership, ts drives values). */
        struct filter_tsd_by_matches_impl
        {
            static constexpr auto name = "filter_tsd_by_matches";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                if (schema == nullptr) { return; }
                const auto *element = registry.dereference(schema->element_ts());
                resolution.bind_ts("__out__", registry.tsd(schema->key_type(), registry.ref(element)));
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"matches", TSD<ScalarVar<"K">, TS<Bool>>, InputValidity::Unchecked> matches,
                             Out<TsVar<"__out__">> out)
            {
                const TSDInputView &dict      = ts;
                const TSDInputView &match_map = matches;

                std::vector<std::pair<Value, Value>> pairs;
                for (auto &&[key, match] : match_map.items())
                {
                    if (!match.valid() || !match.value().checked_as<Bool>()) { continue; }
                    const std::size_t slot = dict.find_slot(key);
                    if (slot == TS_DATA_NO_CHILD_ID) { continue; }
                    pairs.emplace_back(Value{key}, Value{dict.at_slot(slot).reference()});
                }
                publish_tsd_refs(static_cast<const TSOutputView &>(out), pairs);
            }
        };

        /** flip_keys(TSD[K, TSD[K1, REF]]) -> TSD[K1, TSD[K, REF]] - the
            pivot (hgraph's flip_keys_tsd): rebuild the desired mapping from
            the FULL input and reconcile against own output. */
        struct flip_keys_tsd
        {
            static constexpr auto name = "flip_keys_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                const auto *element = schema != nullptr ? time_series_schema_as<AnyTSD>(schema->element_ts())
                                                        : nullptr;
                return element != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *schema = time_series_schema_at_as<AnyTSD>(context, 0);
                if (schema == nullptr) { return; }
                const auto *element = time_series_schema_as<AnyTSD>(schema->element_ts());
                if (element == nullptr) { return; }
                const auto *leaf = registry.dereference(element->element_ts());
                resolution.bind_ts("__out__", registry.tsd(element->key_type(),
                                                           registry.tsd(schema->key_type(), registry.ref(leaf))));
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             Out<TsVar<"__out__">> out)
            {
                const auto &erased          = static_cast<const TSOutputView &>(out);
                const auto  evaluation_time = erased.evaluation_time();
                auto        root_dict       = erased.data_view().as_dict();

                // The desired pivot: inner key -> {outer key -> leaf ref}.
                using Members = std::vector<std::pair<Value, Value>>;
                ankerl::unordered_dense::map<Value, Members, ValueKeyHash, ValueKeyEqual> desired;
                const TSDInputView &source = ts;
                for (auto &&[outer_key, child] : source.items())
                {
                    if (!child.valid()) { continue; }
                    TSDInputView inner{child.borrowed_ref()};
                    for (auto &&[inner_key, leaf] : inner.items())
                    {
                        if (!leaf.valid()) { continue; }
                        desired[Value{inner_key}].emplace_back(Value{outer_key}, Value{leaf.reference()});
                    }
                }

                auto root_mutation = root_dict.begin_mutation(evaluation_time);

                std::vector<Value> stale_groups;
                for (const auto [group_key, group] : root_dict.items())
                {
                    if (!desired.contains(Value{group_key})) { stale_groups.emplace_back(group_key); }
                }
                for (const Value &group_key : stale_groups) { (void)root_mutation.erase(group_key.view()); }

                for (const auto &[group_key, members] : desired)
                {
                    // READ-side compare first (the flip lesson): only touch
                    // groups whose membership or references changed.
                    std::vector<Value> gone;
                    bool               changes = false;
                    if (root_dict.contains(group_key.view()))
                    {
                        auto current      = root_dict.at(group_key.view());
                        auto current_dict = current.as_dict();
                        for (const auto [member_key, member] : current_dict.items())
                        {
                            bool keep = false;
                            for (const auto &entry : members)
                            {
                                if (entry.first.view().equals(member_key)) { keep = true; break; }
                            }
                            if (!keep) { gone.emplace_back(member_key); }
                        }
                        for (const auto &[member_key, reference] : members)
                        {
                            if (!current_dict.contains(member_key.view())) { changes = true; break; }
                            auto element = current_dict.at(member_key.view());
                            if (!element.valid() || !element.has_current_value() ||
                                !(element.value().checked_as<TimeSeriesReference>() ==
                                  reference.view().checked_as<TimeSeriesReference>()))
                            {
                                changes = true;
                                break;
                            }
                        }
                    }
                    else { changes = true; }
                    if (gone.empty() && !changes) { continue; }

                    auto group          = root_mutation.at(group_key.view());
                    auto group_dict     = group.as_dict();
                    auto group_mutation = group_dict.begin_mutation(evaluation_time);
                    for (const Value &member_key : gone) { (void)group_mutation.erase(member_key.view()); }
                    for (const auto &[member_key, reference] : members)
                    {
                        auto element = group_mutation.at(member_key.view());
                        if (element.has_current_value() &&
                            element.value().checked_as<TimeSeriesReference>() ==
                                reference.view().checked_as<TimeSeriesReference>())
                        {
                            continue;
                        }
                        auto element_mutation = TSOutputView{erased.output(), element, evaluation_time}
                                                    .begin_mutation(evaluation_time);
                        static_cast<void>(element_mutation.move_value_from(Value{reference.view()}));
                    }
                }
            }
        };

        /** rekey(ts, new_keys: TSD[K, TSS[K1]]): each source key maps to a
            SET of output keys (hgraph's rekey with set-valued mappings). */
        struct rekey_tsd_set
        {
            static constexpr auto name = "rekey_tsd_set";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"new_keys", TSD<ScalarVar<"K">, TSS<ScalarVar<"K1">>>,
                                InputValidity::Unchecked> new_keys,
                             RecordableState<TSD<ScalarVar<"K">, TSS<ScalarVar<"K1">>>> previous,
                             Out<TSD<ScalarVar<"K1">, TsVar<"V">>> out)
            {
                TSDOutputView &out_dict = out;
                TSDOutputView &prev     = previous;
                auto           out_mutation  = out_dict.begin_mutation(out_dict.evaluation_time());
                auto           prev_mutation = prev.begin_mutation(prev.evaluation_time());

                const auto erase_mapped = [&](const ValueView &source_key) {
                    TSOutputView mapped = prev.at(source_key);
                    if (!mapped.valid()) { return; }
                    auto mapped_set = mapped.data_view().as_set();
                    for (const ValueView &new_key : mapped_set.values()) { (void)out_mutation.erase(new_key); }
                };

                for (const ValueView &source_key : ts.removed_keys()) { erase_mapped(source_key); }

                for (const ValueView &source_key : new_keys.removed_keys())
                {
                    erase_mapped(source_key);
                    (void)prev_mutation.erase(source_key);
                }

                const TSDInputView &source_dict = ts;
                for (const auto [source_key, mapping] : new_keys.modified_items())
                {
                    if (!mapping.valid())
                    {
                        erase_mapped(source_key);
                        (void)prev_mutation.erase(source_key);
                        continue;
                    }

                    const TSInputView &mapping_base = mapping.base();
                    const auto         new_set      = mapping_base.value();
                    auto               new_view     = new_set.as_set();

                    // Output keys dropped from the mapping erase.
                    TSOutputView mapped = prev.at(source_key);
                    if (mapped.valid())
                    {
                        auto mapped_set = mapped.data_view().as_set();
                        for (const ValueView &old_key : mapped_set.values())
                        {
                            if (!new_view.contains(old_key)) { (void)out_mutation.erase(old_key); }
                        }
                    }

                    prev_mutation.set(source_key, new_set);
                    TSInputView source = source_dict.at(source_key);
                    for (const ValueView &new_key : new_view.values())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, new_key, source);
                    }
                }

                for (const auto [source_key, source] : ts.modified_items())
                {
                    TSOutputView mapped = prev.at(source_key);
                    if (!mapped.valid()) { continue; }
                    auto mapped_set = mapped.data_view().as_set();
                    for (const ValueView &new_key : mapped_set.values())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, new_key, source);
                    }
                }
            }
        };

        struct flip_tsd_unique
        {
            static constexpr auto name = "flip_tsd_unique";

            static auto defaults() { return std::tuple{arg<"unique">(Bool{true})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const Bool *unique = context.scalar_as<Bool>("unique");
                return unique == nullptr || *unique;
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>, InputValidity::Unchecked> ts,
                             Scalar<"unique", Bool>,
                             Out<TSD<ScalarVar<"K1">, TS<ScalarVar<"K">>>> out)
            {
                TSDOutputView &out_dict = out;
                auto           previous = build_flipped_previous_index(out_dict);
                auto           out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    auto old_value = previous.find(Value{source_key});
                    if (old_value != previous.end()) { (void)out_mutation.erase(old_value->second.view()); }
                }

                for (const auto [source_key, value] : ts.modified_items())
                {
                    if (!value.valid())
                    {
                        auto old_value = previous.find(Value{source_key});
                        if (old_value != previous.end()) { (void)out_mutation.erase(old_value->second.view()); }
                        continue;
                    }

                    const TSInputView &value_base = value.base();
                    auto old_value = previous.find(Value{source_key});
                    if (old_value != previous.end() && !old_value->second.equals(value_base.value()))
                    {
                        (void)out_mutation.erase(old_value->second.view());
                    }

                    copy_value_if_changed(out_mutation, out_dict, value_base.value(), source_key);
                }
            }
        };

        /** flip(ts, unique=False): GROUP source keys by value - the output
            is TSD[V, TSS[K]]; empty groups REMOVE (hgraph's non-unique
            flip). Full own-output reconciliation, no node state. */
        struct flip_tsd_non_unique
        {
            static constexpr auto name = "flip_tsd_non_unique";

            static auto defaults() { return std::tuple{arg<"unique">(Bool{true})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const Bool *unique = context.scalar_as<Bool>("unique");
                return unique != nullptr && !*unique;
            }

            static void eval(In<"ts", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>, InputValidity::Unchecked> ts,
                             Scalar<"unique", Bool>,
                             Out<TSD<ScalarVar<"K1">, TSS<ScalarVar<"K">>>> out)
            {
                TSDOutputView &out_dict = out;
                const auto     evaluation_time = out_dict.evaluation_time();
                const TSDInputView &dict = ts;

                ankerl::unordered_dense::map<Value, std::vector<Value>, ValueKeyHash, ValueKeyEqual> groups;
                for (auto &&[key, child] : dict.items())
                {
                    if (child.valid()) { groups[Value{child.value()}].emplace_back(key); }
                }

                auto out_mutation = out_dict.begin_mutation(evaluation_time);

                std::vector<Value> stale;
                for (const auto [group_key, members] : out_dict.items())
                {
                    if (!groups.contains(Value{group_key})) { stale.emplace_back(group_key); }
                }
                for (const Value &group_key : stale) { (void)out_mutation.erase(group_key.view()); }

                for (const auto &[group_key, members] : groups)
                {
                    // Compare on the READ side first: mutation.at() marks the
                    // key modified, so an unchanged group must not reach it.
                    std::vector<Value> gone;
                    bool               additions = false;
                    {
                        TSOutputView current = out_dict.at(group_key.view());
                        if (current.valid())
                        {
                            auto current_set = current.data_view().as_set();
                            for (const ValueView &member : current_set.values())
                            {
                                const bool keep = std::ranges::any_of(members, [&](const Value &candidate) {
                                    return candidate.view().equals(member);
                                });
                                if (!keep) { gone.emplace_back(member); }
                            }
                            for (const Value &member : members)
                            {
                                if (!current_set.contains(member.view()))
                                {
                                    additions = true;
                                    break;
                                }
                            }
                        }
                        else { additions = true; }
                        if (gone.empty() && !additions) { continue; }
                    }

                    auto inner          = out_mutation.at(group_key.view());
                    auto inner_set      = inner.as_set();
                    auto inner_mutation = inner_set.begin_mutation(evaluation_time);
                    for (const Value &member : gone) { (void)inner_mutation.remove(member.view()); }
                    for (const Value &member : members) { (void)inner_mutation.add(member.view()); }
                }
            }
        };

        struct partition_tsd_scalar
        {
            static constexpr auto name = "partition_tsd_scalar";

            static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                             In<"partitions", TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>,
                                InputValidity::Unchecked> partitions,
                             RecordableState<TSD<ScalarVar<"K">, TS<ScalarVar<"K1">>>> previous,
                             Out<TSD<ScalarVar<"K1">, TSD<ScalarVar<"K">, TsVar<"V">>>> out)
            {
                TSDOutputView &out_dict = out;
                TSDOutputView &prev     = previous;
                const auto     evaluation_time = out_dict.evaluation_time();
                auto           out_mutation = out_dict.begin_mutation(evaluation_time);
                auto           prev_mutation = prev.begin_mutation(prev.evaluation_time());

                for (const ValueView &source_key : ts.removed_keys())
                {
                    TSOutputView old_partition = prev.at(source_key);
                    if (old_partition.valid())
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }
                }

                for (const ValueView &source_key : partitions.removed_keys())
                {
                    TSOutputView old_partition = prev.at(source_key);
                    if (old_partition.valid())
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }
                    (void)prev_mutation.erase(source_key);
                }

                for (const auto [source_key, partition] : partitions.modified_items())
                {
                    if (!partition.valid())
                    {
                        TSOutputView old_partition = prev.at(source_key);
                        if (old_partition.valid())
                        {
                            erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                        }
                        (void)prev_mutation.erase(source_key);
                        continue;
                    }

                    const TSInputView &partition_base = partition.base();
                    TSOutputView       old_partition = prev.at(source_key);
                    const bool         partition_changed =
                        !old_partition.valid() || !old_partition.value().equals(partition_base.value());

                    if (old_partition.valid() && partition_changed)
                    {
                        erase_nested_tsd_child(out_mutation, evaluation_time, old_partition.value(), source_key);
                    }

                    prev_mutation.set(source_key, partition_base.value());
                    if (partition_changed)
                    {
                        TSInputView source = static_cast<const TSDInputView &>(ts).at(source_key);
                        set_nested_tsd_child(out_mutation, evaluation_time, partition_base.value(), source_key, source);
                    }
                }

                for (const auto [source_key, source] : ts.modified_items())
                {
                    TSOutputView partition = prev.at(source_key);
                    if (partition.valid())
                    {
                        set_nested_tsd_child(out_mutation, evaluation_time, partition.value(), source_key, source);
                    }
                }
            }
        };

        struct unpartition_tsd
        {
            static constexpr auto name = "unpartition_tsd";

            static void eval(In<"ts", TSD<ScalarVar<"K1">, TSD<ScalarVar<"K">, TsVar<"V">>>,
                                InputValidity::Unchecked> ts,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                TSDOutputView &out_dict = out;
                auto           out_mutation = out_dict.begin_mutation(out_dict.evaluation_time());

                for (const auto [outer_key, inner] : ts.modified_items())
                {
                    static_cast<void>(outer_key);
                    const TSDInputView &inner_dict = inner;
                    for (const ValueView &inner_key : inner_dict.removed_keys())
                    {
                        (void)out_mutation.erase(inner_key);
                    }
                    for (const auto [inner_key, child] : inner_dict.modified_items())
                    {
                        copy_tsd_child_if_changed(out_mutation, out_dict, inner_key, child);
                    }
                }
            }
        };

        struct difference_tsd_binary
        {
            static constexpr auto name = "difference_tsd";

            // hgraph gating: BOTH inputs valid (lhs-before-rhs emits nothing).
            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) && !rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (!rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
                // Gated first cycles: lhs keys the output never saw (ticked
                // while rhs was still invalid) backfill on this evaluation.
                for (const auto [key, child] : lhs.items())
                {
                    if (!rhs_dict.contains(key) && !out_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, child);
                    }
                }
            }
        };

        struct intersection_tsd_binary
        {
            static constexpr auto name = "intersection_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) && rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : rhs.added_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
                for (const auto [key, child] : lhs.items())
                {
                    if (rhs_dict.contains(key) && !out_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, child);
                    }
                }
                // A DISJOINT first tick still validates (emits the empty
                // dict) - touch LAST per the write discipline.
                mutation.touch();
            }
        };

        struct union_tsd_binary
        {
            static constexpr auto name = "union_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) || rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    copy_tsd_child_if_changed(mutation, out_dict, key, child);
                }
                for (const auto [key, child] : rhs.modified_items())
                {
                    if (!tsd_key_has_modified_valid_child(lhs_dict, key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, child);
                    }
                }
                for (const ValueView &key : lhs.removed_keys())
                {
                    if (rhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, rhs_dict.at(key));
                    }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        struct symmetric_difference_tsd_binary
        {
            static constexpr auto name = "symmetric_difference_tsd";

            static void eval(In<"lhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> rhs,
                             Out<TSD<ScalarVar<"K">, TsVar<"V">>> out)
            {
                const TSDInputView  &lhs_dict = lhs;
                const TSDInputView  &rhs_dict = rhs;
                const TSDOutputView &out_dict = out;

                auto mutation = out_dict.begin_mutation(out_dict.evaluation_time());
                erase_tsd_keys_not_matching(mutation, out_dict, [&](const ValueView &key) {
                    return lhs_dict.contains(key) != rhs_dict.contains(key);
                });

                for (const auto [key, child] : lhs.modified_items())
                {
                    if (!rhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const auto [key, child] : rhs.modified_items())
                {
                    if (!lhs_dict.contains(key)) { copy_tsd_child_if_changed(mutation, out_dict, key, child); }
                }
                for (const ValueView &key : lhs.removed_keys())
                {
                    if (rhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, rhs_dict.at(key));
                    }
                }
                for (const ValueView &key : rhs.removed_keys())
                {
                    if (lhs_dict.contains(key))
                    {
                        copy_tsd_child_if_changed(mutation, out_dict, key, lhs_dict.at(key));
                    }
                }
            }
        };

        [[nodiscard]] inline bool all_args_are_tss(OperatorCallContext context)
        {
            if (context.args.empty()) { return false; }
            for (std::size_t index = 0; index < context.args.size(); ++index)
            {
                if (!time_series_arg_matches<AnyTSS>(context, index)) { return false; }
            }
            return true;
        }

        inline void resolve_output_to_first_arg(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            bind_output_like_arg(resolution, context, 0, "O");
        }

        /** ``union(*ts)`` — n-ary TSS union, folded pairwise at wiring time. */
        struct union_tss_fold
        {
            static constexpr auto name = "union_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("union: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::union_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return acc.erased();
            }
        };

        /** ``intersection(*ts)`` — n-ary TSS intersection, folded pairwise at wiring time. */
        struct intersection_tss_fold
        {
            static constexpr auto name = "intersection_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("intersection: requires at least one input"); }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::intersection_tss_binary>(w, acc, Port<void>{w, ts[i]});
                }
                return acc.erased();
            }
        };

        /** ``difference(lhs, rhs)`` — binary TSS set difference. */
        struct difference_tss_fold
        {
            static constexpr auto name = "difference_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("difference: requires at least one input"); }
                if (ts.size() == 1) { return ts[0]; }
                if (ts.size() > 2) { throw std::invalid_argument("difference: more than two inputs is not supported"); }
                Port<void> out = wire<collection_impl_detail::difference_tss_binary>(
                    w, Port<void>{w, ts[0]}, Port<void>{w, ts[1]});
                return out.erased();
            }
        };

        /** ``symmetric_difference(*ts)`` — n-ary TSS symmetric difference, folded pairwise. */
        struct symmetric_difference_tss_fold
        {
            static constexpr auto name = "symmetric_difference_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return all_args_are_tss(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_output_to_first_arg(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"S">> ts)
            {
                if (ts.empty())
                {
                    throw std::invalid_argument("symmetric_difference: requires at least one input");
                }
                Port<void> acc{w, ts[0]};
                for (std::size_t i = 1; i < ts.size(); ++i)
                {
                    acc = wire<collection_impl_detail::symmetric_difference_tss_binary>(w, acc,
                                                                                        Port<void>{w, ts[i]});
                }
                return acc.erased();
            }
        };
        // -------------------------------------------------------------
        // MAP-SCALAR transforms: per-tick operations over TS[frozendict]
        // values (hgraph's frozendict operator family). Erased and
        // kind-gated; the TSD forms coexist via requires_ (TSD-kind
        // schemas reject these, TS-over-Map schemas reject those).
        // -------------------------------------------------------------

        [[nodiscard]] inline const ValueTypeMetaData *ts_map_meta(const TSValueTypeMetaData *ts) noexcept
        {
            return ts_map_value_schema(ts);
        }

        [[nodiscard]] inline ValueTypeRef map_scalar_type(const ValueTypeMetaData *key,
                                                                        const ValueTypeMetaData *value)
        {
            const auto *meta = TypeRegistry::instance().map(key, value);
            return value_type_for_active_realization(meta);
        }

        [[nodiscard]] inline MapBuilder make_map_builder(const ValueTypeMetaData *key,
                                                         const ValueTypeMetaData *value)
        {
            return MapBuilder{value_type_for_active_realization(key),
                              value_type_for_active_realization(value)};
        }

        inline void publish_value(const TSOutputView &out, Value &&value)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(value)));
        }

        /** keys_ over a map scalar -> TS[frozenset[K]] (default) or TSS[K]. */
        struct keys_map_scalar
        {
            static constexpr auto name = "keys_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                if (ts_map_meta(resolution.find_ts("S")) == nullptr) { return false; }
                const auto *out = resolution.find_ts("O");
                if (out == nullptr) { return true; }
                if (out->kind == TSTypeKind::TSS) { return true; }
                return out->kind == TSTypeKind::TS && out->value_schema != nullptr &&
                       out->value_schema->value_kind() == ValueTypeKind::Set;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *map_meta = ts_map_meta(resolution.find_ts("S"));
                if (map_meta == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(resolution, registry.ts(registry.set(map_meta->key_type)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                auto map = ts.base().value().as_map();
                const auto *key_meta = ts.base().schema()->value_schema->key_type;
                SetBuilder builder{value_type_for_active_realization(key_meta)};
                for (const auto key : map.keys()) { builder.insert(key); }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** values_ over a map scalar -> TS[tuple[V, ...]] (iteration order). */
        struct values_map_scalar
        {
            static constexpr auto name = "values_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                return ts_map_meta(resolution.find_ts("S")) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *map_meta = ts_map_meta(resolution.find_ts("S"));
                if (map_meta == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution, registry.ts(registry.list(map_meta->element_type, 0, true)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                auto map = ts.base().value().as_map();
                const auto *value_meta = ts.base().schema()->value_schema->element_type;
                const auto *output_meta =
                    static_cast<const TSOutputView &>(out).schema()->value_schema;
                ListBuilder builder{
                    value_type_for_active_realization(value_meta), *output_meta};
                for (const auto [key, item] : map) { builder.push_back(item); }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** rekey(ts, new_keys) over map scalars: out[new_keys[k]] = ts[k]. */
        struct rekey_map_scalar
        {
            static constexpr auto name = "rekey_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                const auto *keys = ts_map_meta(resolution.find_ts("K"));
                return ts != nullptr && keys != nullptr && keys->key_type == ts->key_type;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts   = ts_map_meta(resolution.find_ts("S"));
                const auto *keys = ts_map_meta(resolution.find_ts("K"));
                if (ts == nullptr || keys == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution, registry.ts(registry.map(keys->element_type, ts->element_type)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, In<"new_keys", TsVar<"K">> new_keys, Out<TsVar<"O">> out)
            {
                auto data = ts.base().value().as_map();
                auto mapping = new_keys.base().value().as_map();
                const auto *ts_meta = ts.base().schema()->value_schema;
                const auto *key_meta = new_keys.base().schema()->value_schema;
                auto builder = make_map_builder(key_meta->element_type, ts_meta->element_type);
                for (const auto [key, item] : data)
                {
                    if (!mapping.contains(key)) { continue; }
                    builder.set_item(mapping.at(key), item);
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** flip over a map scalar: {v: k}. */
        struct flip_map_scalar
        {
            static constexpr auto name = "flip_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                return ts_map_meta(resolution.find_ts("S")) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                if (ts == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution, registry.ts(registry.map(ts->element_type, ts->key_type)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                auto map = ts.base().value().as_map();
                const auto *meta = ts.base().schema()->value_schema;
                auto builder = make_map_builder(meta->element_type, meta->key_type);
                for (const auto [key, item] : map) { builder.set_item(item, key); }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** partition(ts, partitions) over map scalars: group by partition label. */
        struct partition_map_scalar
        {
            static constexpr auto name = "partition_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                const auto *parts = ts_map_meta(resolution.find_ts("P"));
                return ts != nullptr && parts != nullptr && parts->key_type == ts->key_type;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts    = ts_map_meta(resolution.find_ts("S"));
                const auto *parts = ts_map_meta(resolution.find_ts("P"));
                if (ts == nullptr || parts == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *inner = registry.map(ts->key_type, ts->element_type);
                bind_local_output(
                    resolution, registry.ts(registry.map(parts->element_type, inner)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, In<"partitions", TsVar<"P">> partitions, Out<TsVar<"O">> out)
            {
                auto data = ts.base().value().as_map();
                auto labels = partitions.base().value().as_map();
                const auto *ts_meta = ts.base().schema()->value_schema;
                const auto *part_meta = partitions.base().schema()->value_schema;
                const auto *inner_meta = TypeRegistry::instance().map(ts_meta->key_type, ts_meta->element_type);

                // label -> inner builder (ordered by first appearance)
                std::vector<std::pair<Value, MapBuilder>> groups;
                for (const auto [key, item] : data)
                {
                    if (!labels.contains(key)) { continue; }
                    auto label = labels.at(key);
                    MapBuilder *group = nullptr;
                    for (auto &[seen, builder] : groups)
                    {
                        if (seen.view().equals(label)) { group = &builder; break; }
                    }
                    if (group == nullptr)
                    {
                        groups.emplace_back(Value{label},
                                            make_map_builder(ts_meta->key_type, ts_meta->element_type));
                        group = &groups.back().second;
                    }
                    group->set_item(key, item);
                }
                auto builder = make_map_builder(part_meta->element_type, inner_meta);
                for (auto &[label, inner] : groups)
                {
                    Value built = inner.build();
                    builder.set_item(label.view(), built.view());
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        [[nodiscard]] inline const ValueTypeMetaData *nested_map_meta(const ValueTypeMetaData *meta) noexcept
        {
            if (meta == nullptr || meta->try_value_kind() != ValueTypeKind::Map) { return nullptr; }
            const auto *inner = meta->element_type;
            return inner != nullptr && inner->try_value_kind() == ValueTypeKind::Map ? inner : nullptr;
        }

        /** flip_keys over a NESTED map scalar: {k: {k1: v}} -> {k1: {k: v}}. */
        struct flip_keys_map_scalar
        {
            static constexpr auto name = "flip_keys_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                return ts != nullptr && nested_map_meta(ts) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts    = ts_map_meta(resolution.find_ts("S"));
                const auto *inner = nested_map_meta(ts);
                if (ts == nullptr || inner == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *flipped_inner = registry.map(ts->key_type, inner->element_type);
                bind_local_output(
                    resolution, registry.ts(registry.map(inner->key_type, flipped_inner)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                const auto *meta  = ts.base().schema()->value_schema;
                const auto *inner = nested_map_meta(meta);
                const auto *flipped_inner = TypeRegistry::instance().map(meta->key_type, inner->element_type);

                std::vector<std::pair<Value, MapBuilder>> groups;
                const auto outer_values = ts.base().value().as_map();
                for (const auto [outer_key, inner_value] : outer_values)
                {
                    const auto inner_values = inner_value.as_map();
                    for (const auto [inner_key, item] : inner_values)
                    {
                        MapBuilder *group = nullptr;
                        for (auto &[seen, builder] : groups)
                        {
                            if (seen.view().equals(inner_key)) { group = &builder; break; }
                        }
                        if (group == nullptr)
                        {
                            groups.emplace_back(Value{inner_key},
                                                make_map_builder(meta->key_type, inner->element_type));
                            group = &groups.back().second;
                        }
                        group->set_item(outer_key, item);
                    }
                }
                auto builder = make_map_builder(inner->key_type, flipped_inner);
                for (auto &[label, group] : groups)
                {
                    Value built = group.build();
                    builder.set_item(label.view(), built.view());
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** collapse_keys over a NESTED map scalar: {k: {k1: v}} -> {(k, k1): v}. */
        struct collapse_keys_map_scalar
        {
            static constexpr auto name = "collapse_keys_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                return ts != nullptr && nested_map_meta(ts) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts    = ts_map_meta(resolution.find_ts("S"));
                const auto *inner = nested_map_meta(ts);
                if (ts == nullptr || inner == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *pair = registry.tuple({ts->key_type, inner->key_type});
                bind_local_output(
                    resolution, registry.ts(registry.map(pair, inner->element_type)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                const auto *meta  = ts.base().schema()->value_schema;
                const auto *inner = nested_map_meta(meta);
                const auto *pair  = TypeRegistry::instance().tuple({meta->key_type, inner->key_type});
                const auto pair_binding = value_type_for_active_realization(pair);

                auto builder = make_map_builder(pair, inner->element_type);
                const auto outer_values = ts.base().value().as_map();
                for (const auto [outer_key, inner_value] : outer_values)
                {
                    const auto inner_values = inner_value.as_map();
                    for (const auto [inner_key, item] : inner_values)
                    {
                        BundleBuilder key_builder{pair_binding};
                        key_builder.set(0, Value{outer_key});
                        key_builder.set(1, Value{inner_key});
                        Value pair_key = key_builder.build();
                        builder.set_item(pair_key.view(), item);
                    }
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** uncollapse_keys over a map scalar with TUPLE keys: {(k, k1): v} -> {k: {k1: v}}. */
        struct uncollapse_keys_map_scalar
        {
            static constexpr auto name = "uncollapse_keys_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                return ts != nullptr && ts->key_type != nullptr &&
                       ts->key_type->value_kind() == ValueTypeKind::Tuple && ts->key_type->field_count == 2;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *ts = ts_map_meta(resolution.find_ts("S"));
                if (ts == nullptr || ts->key_type == nullptr || ts->key_type->value_kind() != ValueTypeKind::Tuple ||
                    ts->key_type->field_count != 2) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *inner = registry.map(ts->key_type->fields[1].type, ts->element_type);
                bind_local_output(
                    resolution, registry.ts(registry.map(ts->key_type->fields[0].type, inner)), "O");
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"O">> out)
            {
                const auto *meta  = ts.base().schema()->value_schema;
                const auto *outer_meta = meta->key_type->fields[0].type;
                const auto *inner_key  = meta->key_type->fields[1].type;
                const auto *inner_map  = TypeRegistry::instance().map(inner_key, meta->element_type);

                std::vector<std::pair<Value, MapBuilder>> groups;
                const auto values = ts.base().value().as_map();
                for (const auto [key, item] : values)
                {
                    auto pair = key.as_indexed_view();
                    auto outer = pair.at(0);
                    MapBuilder *group = nullptr;
                    for (auto &[seen, builder] : groups)
                    {
                        if (seen.view().equals(outer)) { group = &builder; break; }
                    }
                    if (group == nullptr)
                    {
                        groups.emplace_back(Value{outer}, make_map_builder(inner_key, meta->element_type));
                        group = &groups.back().second;
                    }
                    group->set_item(pair.at(1), item);
                }
                auto builder = make_map_builder(outer_meta, inner_map);
                for (auto &[label, group] : groups)
                {
                    Value built = group.build();
                    builder.set_item(label.view(), built.view());
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };
        /** combine_map from a single (key, value) TS pair. */
        struct combine_map_pair
        {
            static constexpr auto name = "combine_map_pair";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                return keys != nullptr && values != nullptr && keys->value_kind() != ValueTypeKind::List &&
                       values->value_kind() != ValueTypeKind::List;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                if (keys == nullptr || values == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(resolution, registry.ts(registry.map(keys, values)), "O");
            }

            static void eval(In<"keys", TsVar<"A">> keys, In<"values", TsVar<"B">> values, Out<TsVar<"O">> out)
            {
                auto key = keys.base().value();
                auto item = values.base().value();
                auto builder = make_map_builder(key.schema(), item.schema());
                builder.set_item(key, item);
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** combine_map from two same-length TUPLE scalars (zip). */
        struct combine_map_tuples
        {
            static constexpr auto name = "combine_map_tuples";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                return keys != nullptr && values != nullptr && keys->value_kind() == ValueTypeKind::List &&
                       values->value_kind() == ValueTypeKind::List;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                if (keys == nullptr || values == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution, registry.ts(registry.map(keys->element_type, values->element_type)), "O");
            }

            static void eval(In<"keys", TsVar<"A">> keys, In<"values", TsVar<"B">> values, Out<TsVar<"O">> out)
            {
                auto key_list  = keys.base().value().as_indexed_view();
                auto item_list = values.base().value().as_indexed_view();
                if (key_list.size() != item_list.size())
                {
                    throw std::invalid_argument("combine_map: keys and values must have the same length");
                }
                const auto *keys_meta   = keys.base().schema()->value_schema;
                const auto *values_meta = values.base().schema()->value_schema;
                auto builder = make_map_builder(keys_meta->element_type, values_meta->element_type);
                for (std::size_t index = 0; index < key_list.size(); ++index)
                {
                    builder.set_item(key_list.at(index), item_list.at(index));
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };

        /** combine_map from two fixed TSLs (zip; every element must be valid). */
        struct combine_map_tsls
        {
            static constexpr auto name = "combine_map_tsls";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *keys   = time_series_schema_as<AnyTSL>(resolution.find_ts("A"));
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                if (keys == nullptr || values == nullptr) { return; }
                const auto *key_element   = time_series_schema_as<AnyTS>(keys->element_ts());
                const auto *value_element = time_series_schema_as<AnyTS>(values->element_ts());
                if (key_element == nullptr || value_element == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution,
                    registry.ts(registry.map(key_element->value_schema, value_element->value_schema)),
                    "O");
            }

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *keys   = time_series_schema_as<AnyTSL>(resolution.find_ts("A"));
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                return keys != nullptr && values != nullptr && keys->fixed_size() == values->fixed_size() &&
                       keys->fixed_size() != 0;
            }

            static void eval(In<"keys", TsVar<"A">> keys, In<"values", TsVar<"B">> values, Out<TsVar<"O">> out)
            {
                const auto *out_meta = static_cast<const TSOutputView &>(out).schema()->value_schema;
                auto builder = make_map_builder(out_meta->key_type, out_meta->element_type);
                const auto  size = keys.base().schema()->fixed_size();
                for (std::size_t index = 0; index < size; ++index)
                {
                    auto key_child  = keys.base().indexed_child_at(index);
                    auto item_child = values.base().indexed_child_at(index);
                    if (!key_child.valid() || !item_child.valid()) { continue; }
                    builder.set_item(key_child.value(), item_child.value());
                }
                publish_value(static_cast<const TSOutputView &>(out), builder.build());
            }
        };
        // ----- combine_tsd (hgraph's combine[TSD] family) -----------------

        /** Write ``mapping`` into the owned TSD-of-REF output: apply each
            pair's reference (same-ref dedup keeps unchanged elements silent),
            erase keys no longer present. */
        inline void publish_tsd_refs(const TSOutputView &out,
                                     const std::vector<std::pair<Value, Value>> &pairs)
        {
            auto dict_out = out.data_view().as_dict();
            auto mutation = dict_out.begin_mutation(out.evaluation_time());

            std::vector<Value> stale;
            for (auto &&[key, child] : dict_out.items())
            {
                bool keep = false;
                for (const auto &pair : pairs)
                {
                    if (pair.first.view().equals(key)) { keep = true; break; }
                }
                if (!keep) { stale.emplace_back(key); }
            }
            for (const Value &key : stale) { (void)mutation.erase(key.view()); }

            for (const auto &[key, reference] : pairs)
            {
                auto element = mutation.at(key.view());
                // SAME-REFERENCE dedup (the getitem_ lesson): re-applying an
                // unchanged reference must not record modified.
                if (element.has_current_value() &&
                    element.value().checked_as<TimeSeriesReference>() ==
                        reference.view().checked_as<TimeSeriesReference>())
                {
                    continue;
                }
                auto element_mutation =
                    TSOutputView{out.output(), element, out.evaluation_time()}.begin_mutation(out.evaluation_time());
                static_cast<void>(element_mutation.move_value_from(Value{reference.view()}));
            }
            // Touch LAST: the once-per-time notification rides the FIRST
            // record - it must carry the real slot changes (derived
            // structures such as the from-REF proxy sync on it); the trailing
            // touch only validates an otherwise-empty tick.
            mutation.touch();
        }

        /** combine_tsd(keys TSL[TS[K]], values TSL[REF[V]]): zip valid key
            elements to value references; keys vanished from the zip erase
            (hgraph's combine_tsd_from_tsl_and_tsl). */
        struct combine_tsd_tsls
        {
            static constexpr auto name = "combine_tsd_tsls";
            // The value side is reference topology. Its references are bound
            // before graph start and do not produce a data tick merely because
            // the input becomes active, so publish the initial topology
            // explicitly in the start cycle.
            static constexpr bool schedule_on_start = true;

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *keys   = time_series_schema_as<AnyTSL>(resolution.find_ts("A"));
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                return keys != nullptr && values != nullptr && keys->fixed_size() == values->fixed_size() &&
                       keys->fixed_size() != 0 &&
                       time_series_schema_as<AnyTS>(keys->element_ts()) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *keys   = time_series_schema_as<AnyTSL>(resolution.find_ts("A"));
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                const auto *key_element = keys != nullptr ? time_series_schema_as<AnyTS>(keys->element_ts())
                                                          : nullptr;
                if (keys == nullptr || values == nullptr || key_element == nullptr) { return; }
                auto       &registry = TypeRegistry::instance();
                const auto *element  = registry.dereference(values->element_ts());
                bind_local_output(
                    resolution, registry.tsd(key_element->value_schema, registry.ref(element)), "O");
            }

            static auto defaults() { return std::tuple{arg<"__strict__">(Bool{true})}; }

            static void eval(In<"keys", TsVar<"A">, InputValidity::Unchecked> keys,
                             In<"values", TsVar<"B">, InputValidity::Unchecked> values,
                             Scalar<"__strict__", Bool> strict, Out<TsVar<"O">> out)
            {
                const auto size = keys.base().schema()->fixed_size();
                std::vector<std::pair<Value, Value>> pairs;
                pairs.reserve(size);
                for (std::size_t index = 0; index < size; ++index)
                {
                    auto key_child  = keys.base().indexed_child_at(index);
                    auto item_child = values.base().indexed_child_at(index);
                    if (!key_child.valid() || !item_child.valid())
                    {
                        if (strict.value()) { return; }   // strict: wait for the full zip
                        continue;
                    }
                    pairs.emplace_back(Value{key_child.value()}, Value{item_child.reference()});
                }
                publish_tsd_refs(static_cast<const TSOutputView &>(out), pairs);
            }
        };

        /** combine_tsd(keys tuple scalar, values TSL[REF[V]]): the static
            key set (hgraph's combine_tsd_from_tuple_and_tsl; the TSD.from_ts
            shape - the variadic values collect through the graph overload). */
        struct combine_tsd_tuple_values
        {
            static constexpr auto name = "combine_tsd_tuple_values";
            static constexpr bool schedule_on_start = true;

            [[nodiscard]] static const ValueTypeMetaData *keys_meta(OperatorCallContext context)
            {
                const WiringArg *arg = context.scalar("keys");
                return arg != nullptr ? arg->scalar_value.schema() : nullptr;
            }

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                const auto *keys   = keys_meta(context);
                return values != nullptr && values->fixed_size() != 0 &&
                       keys != nullptr && keys->value_kind() == ValueTypeKind::List;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *values = time_series_schema_as<AnyTSL>(resolution.find_ts("B"));
                const auto *keys   = keys_meta(context);
                if (values == nullptr || keys == nullptr) { return; }
                auto       &registry = TypeRegistry::instance();
                const auto *element  = registry.dereference(values->element_ts());
                bind_local_output(
                    resolution, registry.tsd(keys->element_type, registry.ref(element)), "O");
            }

            static auto defaults() { return std::tuple{arg<"__strict__">(Bool{true})}; }

            static void eval(Scalar<"keys", ScalarVar<"KS">> keys,
                             In<"values", TsVar<"B">, InputValidity::Unchecked> values,
                             Scalar<"__strict__", Bool> strict, Out<TsVar<"O">> out)
            {
                auto       key_list = keys.value().as_indexed_view();
                const auto size     = values.base().schema()->fixed_size();
                if (key_list.size() != size)
                {
                    throw std::invalid_argument("combine_tsd: keys and values must have the same length");
                }
                std::vector<std::pair<Value, Value>> pairs;
                pairs.reserve(size);
                for (std::size_t index = 0; index < size; ++index)
                {
                    auto item_child = values.base().indexed_child_at(index);
                    if (!item_child.valid())
                    {
                        if (strict.value()) { return; }
                        continue;
                    }
                    pairs.emplace_back(Value{key_list.at(index)}, Value{item_child.reference()});
                }
                publish_tsd_refs(static_cast<const TSOutputView &>(out), pairs);
            }
        };

        /** The VARIADIC calling shape combine_tsd(keys_tuple, ts1, ts2, ...):
            collect the element ports into a structural TSL (the race collect
            pattern) and re-dispatch onto combine_tsd_tuple_values. */
        struct combine_tsd_variadic
        {
            static constexpr auto name = "combine_tsd_variadic";

            static auto defaults() { return std::tuple{arg<"__strict__">(Bool{true})}; }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                if (context.args.size() < 2 || context.args[0].kind != WiringArg::Kind::Scalar) { return; }
                const auto *keys = context.args[0].scalar_value.schema();
                if (keys == nullptr || keys->value_kind() != ValueTypeKind::List) { return; }
                // The VarIn tail normalises LAST (after fixed params + the
                // injected __strict__ default): the first VALUE port is the
                // first TimeSeries argument from the back-half.
                const WiringArg *first_value = nullptr;
                for (const WiringArg &arg : context.args.subspan(1))
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries) { first_value = &arg; break; }
                }
                if (first_value == nullptr) { return; }
                auto       &registry = TypeRegistry::instance();
                const auto *element  = registry.dereference(first_value->port.schema);
                if (element == nullptr) { return; }
                bind_output(resolution, registry.tsd(keys->element_type, registry.ref(element)));
            }

            static WiringPortRef compose(Wiring &w, Scalar<"keys", ScalarVar<"KS">> keys,
                                         VarIn<"values", TsVar<"V">> values, Scalar<"__strict__", Bool> strict)
            {
                if (values.empty()) { throw std::invalid_argument("combine_tsd requires at least one value"); }

                auto       &registry   = TypeRegistry::instance();
                const auto *target     = registry.dereference(values[0].schema);
                const auto *ref_schema = registry.ref(target);
                std::vector<WiringPortRef> children;
                children.reserve(values.size());
                for (const WiringPortRef &port : values)
                {
                    children.push_back(graph_wiring_detail::adapt_source_for_input(w, ref_schema, port));
                }
                WiringPortRef packed = WiringPortRef::structural_source(registry.tsl(ref_schema, values.size()),
                                                                        std::move(children));

                std::array<WiringArg, 3> args{};
                args[0].kind         = WiringArg::Kind::Scalar;
                args[0].scalar_value = Value{keys.value()};
                args[0].scalar_meta  = args[0].scalar_value.schema();
                args[1].kind         = WiringArg::Kind::TimeSeries;
                args[1].port         = packed;
                args[2].kind         = WiringArg::Kind::Scalar;
                args[2].scalar_value = Value{strict.value()};
                args[2].scalar_meta  = args[2].scalar_value.schema();
                args[2].name         = "__strict__";
                auto result = wire_operator(w, "combine_tsd", {args.data(), args.size()}, true);
                return result.output.erased();
            }
        };

        /** combine_tsd(keys TS[tuple[K,...]], values TS[tuple[V,...]]):
            VALUE elements diffed against own output (hgraph's
            combine_tsd_from_tuple_and_tuple). */
        struct combine_tsd_tuples
        {
            static constexpr auto name = "combine_tsd_tuples";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                return keys != nullptr && values != nullptr && keys->value_kind() == ValueTypeKind::List &&
                       values->value_kind() == ValueTypeKind::List;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *keys   = ts_value_schema(resolution.find_ts("A"));
                const auto *values = ts_value_schema(resolution.find_ts("B"));
                if (keys == nullptr || values == nullptr) { return; }
                auto &registry = TypeRegistry::instance();
                bind_local_output(
                    resolution, registry.tsd(keys->element_type, registry.ts(values->element_type)), "O");
            }

            static void eval(In<"keys", TsVar<"A">> keys, In<"values", TsVar<"B">> values, Out<TsVar<"O">> out)
            {
                auto key_list  = keys.base().value().as_indexed_view();
                auto item_list = values.base().value().as_indexed_view();
                if (key_list.size() != item_list.size())
                {
                    throw std::invalid_argument("combine_tsd: keys and values must have the same length");
                }

                const auto &erased   = static_cast<const TSOutputView &>(out);
                auto        dict_out = erased.data_view().as_dict();
                auto        mutation = dict_out.begin_mutation(erased.evaluation_time());

                std::vector<Value> stale;
                for (auto &&[key, child] : dict_out.items())
                {
                    bool keep = false;
                    for (std::size_t index = 0; index < key_list.size(); ++index)
                    {
                        if (key_list.at(index).equals(key)) { keep = true; break; }
                    }
                    if (!keep) { stale.emplace_back(key); }
                }
                for (const Value &key : stale) { (void)mutation.erase(key.view()); }

                for (std::size_t index = 0; index < key_list.size(); ++index)
                {
                    auto element = mutation.at(key_list.at(index));
                    auto element_mutation = TSOutputView{erased.output(), element, erased.evaluation_time()}
                                                .begin_mutation(erased.evaluation_time());
                    static_cast<void>(element_mutation.copy_value_from(item_list.at(index)));
                }
                mutation.touch();   // LAST (see publish_tsd_refs)
            }
        };

        // ----- TS[tuple-scalar] operator family (hgraph tuple ops) ---------

        [[nodiscard]] inline const ValueTypeMetaData *ts_list_meta(OperatorCallContext context, std::size_t index)
        {
            const auto *schema = time_series_schema_at_as<AnyTS>(context, index);
            if (schema == nullptr ||
                schema->value_schema == nullptr ||
                schema->value_schema->value_kind() != ValueTypeKind::List)
            {
                return nullptr;
            }
            return schema->value_schema;
        }

        [[nodiscard]] inline ListBuilder make_list_builder(const ValueTypeMetaData *list_meta)
        {
            return ListBuilder{
                value_type_for_active_realization(list_meta->element_type), *list_meta};
        }

        /** mul_(tuple, n): python tuple repetition. */
        struct mul_tuple_int
        {
            static constexpr auto name = "mul_tuple_int";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return ts_list_meta(context, 0) != nullptr &&
                       time_series_arg_matches<AnyTS>(context, 1);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list));
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<Int>> rhs, Out<TsVar<"__out__">> out)
            {
                const auto  value = lhs.base().value();
                auto        items = value.as_indexed_view();
                auto        builder = make_list_builder(value.schema());
                const auto  repeats = rhs.value();
                for (Int r = 0; r < repeats; ++r)
                {
                    for (std::size_t index = 0; index < items.size(); ++index)
                    {
                        builder.push_back(items.at(index));
                    }
                }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(builder.build()));
            }
        };

        /** getitem_(tuple, ts_index). */
        struct getitem_ts_list
        {
            static constexpr auto name = "getitem_ts_list";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const WiringArg *key = scalar_arg_at(context, 1);
                return ts_list_meta(context, 0) != nullptr &&
                       (time_series_arg_matches<AnyTS>(context, 1) ||
                        (key != nullptr && key->scalar_value.try_as<Int>() != nullptr));
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list->element_type));
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"key", TS<Int>> key, Out<TsVar<"__out__">> out)
            {
                const auto value = ts.base().value();
                auto       items = value.as_indexed_view();
                Int        index = key.value();
                if (index < 0) { index += static_cast<Int>(items.size()); }
                if (index < 0 || static_cast<std::size_t>(index) >= items.size()) { return; }
                const auto &erased  = static_cast<const TSOutputView &>(out);
                const auto  element = items.at(static_cast<std::size_t>(index));
                if (erased.data_view().has_current_value() && erased.value().equals(element)) { return; }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(element));
            }
        };

        /** index_of(tuple, item): first index or -1. */
        struct index_of_ts_list
        {
            static constexpr auto name = "index_of_ts_list";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return ts_list_meta(context, 0) != nullptr;
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"item", TS<ScalarVar<"E">>> item,
                             Out<TS<Int>> out)
            {
                const auto value  = ts.base().value();
                auto       items  = value.as_indexed_view();
                const auto needle = item.base().value();
                Int found = -1;
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    if (items.at(index).equals(needle)) { found = static_cast<Int>(index); break; }
                }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                Value result{found};
                static_cast<void>(mutation.move_value_from(std::move(result)));
            }
        };

        /** getitem_ over a FIXED (composite) homogeneous tuple value. */
        struct getitem_ts_fixed_tuple
        {
            static constexpr auto name = "getitem_ts_fixed_tuple";

            [[nodiscard]] static const ValueTypeMetaData *homogeneous_tuple(OperatorCallContext context)
            {
                const auto *schema = time_series_schema_at_as<AnyTS>(context, 0);
                if (schema == nullptr ||
                    schema->value_schema == nullptr ||
                    schema->value_schema->value_kind() != ValueTypeKind::Tuple || schema->value_schema->field_count == 0)
                {
                    return nullptr;
                }
                const auto *element = schema->value_schema->fields[0].type;
                for (std::size_t index = 1; index < schema->value_schema->field_count; ++index)
                {
                    if (schema->value_schema->fields[index].type != element) { return nullptr; }
                }
                return element;
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const WiringArg *key = scalar_arg_at(context, 1);
                return homogeneous_tuple(context) != nullptr &&
                       (time_series_arg_matches<AnyTS>(context, 1) ||
                        (key != nullptr && key->scalar_value.try_as<Int>() != nullptr));
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *element = homogeneous_tuple(context);
                if (element == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(element));
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"key", TS<Int>> key, Out<TsVar<"__out__">> out)
            {
                const auto value = ts.base().value();
                auto       items = value.as_indexed_view();
                Int        index = key.value();
                if (index < 0) { index += static_cast<Int>(items.size()); }
                if (index < 0 || static_cast<std::size_t>(index) >= items.size()) { return; }
                const auto &erased  = static_cast<const TSOutputView &>(out);
                const auto  element = items.at(static_cast<std::size_t>(index));
                if (erased.data_view().has_current_value() && erased.value().equals(element)) { return; }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(element));
            }
        };

        /** contains_(tuple, element). */
        struct contains_ts_list
        {
            static constexpr auto name = "contains_ts_list";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return ts_list_meta(context, 0) != nullptr;
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"item", TS<ScalarVar<"E">>> item,
                             Out<TS<Bool>> out)
            {
                const auto value = ts.base().value();
                auto       items = value.as_indexed_view();
                const auto needle = item.base().value();
                bool found = false;
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    if (items.at(index).equals(needle)) { found = true; break; }
                }
                out.set(found);
            }
        };

        /** min_/max_ over N same-typed LIST-valued TS: whole-value ordering
            via Value::compare (tuples compare lexicographically); an erased
            node over the packed TSL (numeric kernels keep the scalar space). */
        template <bool Min>
        struct extremum_ts_list_node
        {
            static constexpr auto name = Min ? "min_ts_list" : "max_ts_list";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *tsl = fixed_tsl_arg(context, 0);
                if (tsl == nullptr) { return; }
                const auto *element = time_series_schema_as<AnyTS>(tsl->element_ts());
                if (element == nullptr) { return; }
                bind_output(resolution, element);
            }

            static void eval(In<"tsl", TSL<TS<ScalarVar<"T">>, SIZE<"N">>> tsl, Out<TsVar<"__out__">> out)
            {
                std::optional<Value> best;
                for (std::size_t index = 0; index < tsl.size(); ++index)
                {
                    auto child = tsl[index];
                    if (!child.valid()) { return; }   // strict: every input
                    const auto value = child.base().value();
                    const bool better =
                        !best.has_value() ||
                        (Min ? value.compare(best->view()) == std::partial_ordering::less
                             : value.compare(best->view()) == std::partial_ordering::greater);
                    if (better) { best.emplace(value); }
                }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(*best)));
            }
        };

        /** The min_/max_ graph overload gating LIST-valued TS args: collect
            and wire the packed node. */
        template <bool Min>
        struct extremum_ts_list_graph
        {
            static constexpr auto name = Min ? "min_ts_list_graph" : "max_ts_list_graph";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                if (context.args.empty()) { return false; }
                for (std::size_t index = 0; index < context.args.size(); ++index)
                {
                    if (context.args[index].kind != WiringArg::Kind::TimeSeries ||
                        ts_list_meta(context, index) == nullptr)
                    {
                        return false;
                    }
                }
                return true;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list));
            }

            static WiringPortRef compose(Wiring &w, VarIn<"tsl", TS<ScalarVar<"T">>> ts)
            {
                if (ts.empty()) { throw std::invalid_argument("min_/max_ requires at least one input"); }
                auto &registry = TypeRegistry::instance();
                std::vector<WiringPortRef> children{ts.begin(), ts.end()};
                WiringPortRef packed =
                    WiringPortRef::structural_source(registry.tsl(ts[0].schema, ts.size()), std::move(children));
                std::array<WiringArg, 1> args{};
                args[0].kind = WiringArg::Kind::TimeSeries;
                args[0].port = packed;
                auto result = wire_operator(w, Min ? "min_ts_list" : "max_ts_list", {args.data(), args.size()}, true);
                return result.output.erased();
            }
        };

        /** add_(tuple, tuple) = concat; non-strict emits whichever sides are
            valid. add_(tuple, scalar) = append. sub_(tuple, scalar) = remove
            every matching element. */
        struct add_ts_list_concat
        {
            static constexpr auto name = "add_ts_list_concat";

            static auto defaults() { return std::tuple{arg<"__strict__">(Bool{true})}; }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return ts_list_meta(context, 0) != nullptr && ts_list_meta(context, 1) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list));
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<ScalarVar<"T">>, InputValidity::Unchecked> rhs,
                             Scalar<"__strict__", Bool> strict, Out<TsVar<"__out__">> out)
            {
                if (strict.value() && (!lhs.valid() || !rhs.valid())) { return; }
                if (!lhs.valid() && !rhs.valid()) { return; }
                const auto *list_meta =
                    static_cast<const TSOutputView &>(out).schema()->value_schema;
                auto builder = make_list_builder(list_meta);
                const auto push_all = [&](const ValueView &value) {
                    auto items = value.as_indexed_view();
                    for (std::size_t index = 0; index < items.size(); ++index)
                    {
                        builder.push_back(items.at(index));
                    }
                };
                if (lhs.valid()) { push_all(lhs.base().value()); }
                if (rhs.valid()) { push_all(rhs.base().value()); }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(builder.build()));
            }
        };

        struct add_ts_list_scalar
        {
            static constexpr auto name = "add_ts_list_scalar";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr || ts_list_meta(context, 1) != nullptr) { return false; }
                const auto *rhs = time_series_schema_at_as<AnyTS>(context, 1);
                return rhs != nullptr && rhs->value_schema == list->element_type;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list));
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"E">>> rhs,
                             Out<TsVar<"__out__">> out)
            {
                const auto value = lhs.base().value();
                auto       items = value.as_indexed_view();
                auto       builder = make_list_builder(value.schema());
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    builder.push_back(items.at(index));
                }
                builder.push_back(rhs.base().value());
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(builder.build()));
            }
        };

        struct sub_ts_list_scalar
        {
            static constexpr auto name = "sub_ts_list_scalar";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr || ts_list_meta(context, 1) != nullptr) { return false; }
                const auto *rhs = time_series_schema_at_as<AnyTS>(context, 1);
                return rhs != nullptr && rhs->value_schema == list->element_type;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *list = ts_list_meta(context, 0);
                if (list == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().ts(list));
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"E">>> rhs,
                             Out<TsVar<"__out__">> out)
            {
                const auto value  = lhs.base().value();
                auto       items  = value.as_indexed_view();
                const auto needle = rhs.base().value();
                auto builder = make_list_builder(value.schema());
                for (std::size_t index = 0; index < items.size(); ++index)
                {
                    if (!items.at(index).equals(needle)) { builder.push_back(items.at(index)); }
                }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(builder.build()));
            }
        };

        /** combine[TS[CompoundScalar]](a=..., b=...): assemble a BUNDLE
            value from field ports by NAME - valid inputs set their fields,
            the rest stay UNSET (CS IS a Bundle value; py from_ts wires a
            structural TSB into this node). */
        template <bool Strict>
        void combine_cs_from_fields_eval(const TSInputView &fields, const TSOutputView &erased)
        {
            const auto *target = erased.schema()->value_schema;
            const auto *snapshot = active_type_realization();
            const auto target_binding = snapshot != nullptr
                                            ? snapshot->exact_type_for(target)
                                            : ValuePlanFactory::instance().type_for(target);
            const bool policy_materialization =
                target_binding.ops_ref().has_source_materialization_policy();
            const auto assembly_binding = policy_materialization
                                              ? BundleBuilder::assembly_type(target_binding)
                                              : target_binding;
            BundleBuilder builder{assembly_binding};

            const auto *input_schema = fields.schema();
            for (std::size_t index = 0; index < input_schema->field_count(); ++index)
            {
                const char *field_name = input_schema->fields()[index].name;
                if (field_name == nullptr) { continue; }
                auto child = fields.indexed_child_at(index);
                if constexpr (Strict)
                {
                    // hgraph default: EVERY supplied field must be valid.
                    if (!child.valid()) { return; }
                }
                else if (!child.valid()) { continue; }
                for (std::size_t target_index = 0; target_index < target->field_count; ++target_index)
                {
                    if (target->fields[target_index].name != nullptr &&
                        std::string_view{target->fields[target_index].name} == field_name)
                    {
                        builder.set(target_index, child.value());
                        break;
                    }
                }
            }
            Value source = builder.build();
            if (policy_materialization &&
                !target_binding.ops_ref().can_materialize_source(source.binding(), source.view().data()))
            {
                return;
            }

            Value bundle;
            if (assembly_binding == target_binding)
            {
                bundle = std::move(source);
            }
            else
            {
                bundle = Value{target_binding};
                target_binding.ops_ref().move_assign_from(
                    target_binding, const_cast<void *>(bundle.view().data()),
                    source.binding(), const_cast<void *>(source.view().data()));
            }
            if (erased.data_view().has_current_value() && erased.value().equals(bundle.view())) { return; }
            auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
            static_cast<void>(mutation.move_value_from(std::move(bundle)));
        }

        template <bool Strict>
        struct combine_cs_from_fields;

        template <>
        struct combine_cs_from_fields<true>
        {
            static constexpr auto name = "combine_cs_from_fields";

            static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TsVar<"__out__">> out)
            {
                combine_cs_from_fields_eval<true>(ts, static_cast<const TSOutputView &>(out));
            }
        };

        template <>
        struct combine_cs_from_fields<false>
        {
            static constexpr auto name = "combine_cs_from_fields_lenient";

            static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                             Scalar<"__strict__", Bool>,
                             Out<TsVar<"__out__">> out)
            {
                combine_cs_from_fields_eval<false>(ts, static_cast<const TSOutputView &>(out));
            }
        };

        inline void make_tsd_eval(const TSInputView &key, const TSInputView &value,
                                  const TSInputView *remove_key, const TSOutputView &out)
        {
            const bool remove = remove_key != nullptr && remove_key->valid() &&
                                remove_key->value().checked_as<Bool>() &&
                                (remove_key->modified() || key.modified());
            const bool publish = !remove && key.valid() && value.valid() &&
                                 (key.modified() || value.modified());
            if (!remove && !publish) { return; }

            auto dict = out.as_dict();
            auto mutation = dict.begin_mutation(out.evaluation_time());
            const auto key_value = key.value();
            if (remove)
            {
                if (dict.contains(key_value)) { static_cast<void>(mutation.erase(key_value)); }
                return;
            }

            auto element = mutation.at(key_value);
            TSOutputView child{out.output(), element, out.evaluation_time()};
            if (key.modified() || !element.has_current_value())
            {
                apply_current_value(child, value.value());
            }
            else
            {
                const Value delta = capture_delta(value);
                apply_delta(child, delta.view());
            }
        }

        struct make_tsd_impl
        {
            static constexpr auto name = "make_tsd";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *key_value = resolution.find_scalar("K");
                const auto *value = resolution.find_ts("V");
                if (key_value == nullptr)
                {
                    const auto *key = time_series_schema_at_as<AnyTS>(context, 0);
                    key_value = key != nullptr ? key->value_schema : nullptr;
                }
                if (value == nullptr) { value = time_series_schema_at(context, 1); }
                if (key_value == nullptr || value == nullptr) { return; }
                bind_output(resolution, TypeRegistry::instance().tsd(key_value, value));
            }

            static void eval(In<"key", TS<ScalarVar<"K">>> key,
                             In<"value", TsVar<"V">, InputValidity::Unchecked> value,
                             Out<TsVar<"__out__">> out)
            {
                make_tsd_eval(key.base(), value.base(), nullptr,
                              static_cast<const TSOutputView &>(out));
            }
        };

        struct make_tsd_with_remove_impl
        {
            static constexpr auto name = "make_tsd_with_remove";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                make_tsd_impl::resolve_default_types(resolution, context);
            }

            static void eval(In<"key", TS<ScalarVar<"K">>> key,
                             In<"value", TsVar<"V">, InputValidity::Unchecked> value,
                             In<"remove_key", TS<Bool>, InputValidity::Unchecked> remove_key,
                             Out<TsVar<"__out__">> out)
            {
                const auto &remove = remove_key.base();
                make_tsd_eval(key.base(), value.base(), &remove,
                              static_cast<const TSOutputView &>(out));
            }
        };

        /** convert[TS[CS]](tsb): the whole-bundle VALUE of a TSB. Ordinary
            composite Bundles remain all-fields-valid. An erased owning
            representation may declare that an incomplete indexed source is
            constructible (for example a Python dataclass with defaults or
            init=False fields). */
        struct convert_tsb_to_cs_impl
        {
            static constexpr auto name = "convert_tsb_to_cs";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *out = time_series_schema_at_as<AnyTSB>(context, 0);
                const auto *requested = resolution.find_ts("__out__");
                if (out == nullptr || requested == nullptr ||
                    requested->kind != TSTypeKind::TS)
                {
                    return false;
                }
                const auto *bundle_value = requested->value_schema;
                return bundle_value != nullptr && bundle_value->value_kind() == ValueTypeKind::Bundle;
            }

            static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TsVar<"__out__">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                const auto  value  = ts.base().value();
                if (!ts.base().all_valid())
                {
                    const auto target = erased.data_view().layout().value_binding;
                    if (!target || !target.ops_ref().can_materialize_source(value.binding(), value.data()))
                    {
                        return;
                    }
                }
                Value materialized{value};
                if (erased.data_view().has_current_value() && erased.value().equals(materialized.view())) { return; }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(materialized)));
            }
        };

        /** The lenient (__strict__=False) TSB->CS variant: partial fields
            emit with UNSET holes. */
        struct convert_tsb_to_cs_lenient_impl
        {
            static constexpr auto name = "convert_tsb_to_cs_lenient";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                return convert_tsb_to_cs_impl::requires_(resolution, context);
            }

            static void eval(In<"ts", TsVar<"S">> ts, Scalar<"__strict__", Bool>, Out<TsVar<"__out__">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                const auto  value  = ts.base().value();
                Value materialized{value};
                if (erased.data_view().has_current_value() && erased.value().equals(materialized.view())) { return; }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(materialized)));
            }
        };

        /** convert[TSB](ts_cs): distribute a whole-bundle VALUE onto the TSB
            output's fields. */
        struct convert_cs_to_tsb_impl
        {
            static constexpr auto name = "convert_cs_to_tsb";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext context)
            {
                const auto *requested = resolution.find_ts("__out__");
                const auto *in        = time_series_schema_at_as<AnyTS>(context, 0);
                return requested != nullptr && requested->kind == TSTypeKind::TSB &&
                       in != nullptr && in->value_schema != nullptr &&
                       in->value_schema->value_kind() == ValueTypeKind::Bundle;
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto &erased  = static_cast<const TSOutputView &>(out);
                auto        mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(ts.base().value()));
            }
        };

        // ----- TSB aggregates (homogeneous field bundles) -------------------

        [[nodiscard]] inline const TSValueTypeMetaData *homogeneous_tsb(OperatorCallContext context,
                                                                        std::size_t index = 0)
        {
            const auto *schema = time_series_schema_at_as<AnyTSB>(context, index);
            if (schema == nullptr || schema->field_count() == 0) { return nullptr; }
            const auto *field = schema->fields()[0].type;
            if (field == nullptr || field->kind != TSTypeKind::TS) { return nullptr; }
            for (std::size_t i = 1; i < schema->field_count(); ++i)
            {
                if (schema->fields()[i].type != field) { return nullptr; }
            }
            return schema;
        }

        enum class TsbAggregate : std::uint8_t
        {
            Sum,
            Mean,
            Std,
            Min,
            Max,
        };

        /** min_/max_ over a HOMOGENEOUS bundle: fully ERASED via
            Value::compare - no numeric kernel needed. */
        template <TsbAggregate Op>
        struct tsb_extremum_impl
        {
            static_assert(Op == TsbAggregate::Min || Op == TsbAggregate::Max);
            static constexpr auto name = Op == TsbAggregate::Min ? "min_tsb" : "max_tsb";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return context.args.size() == 1 && homogeneous_tsb(context) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                const auto *schema = homogeneous_tsb(context);
                if (schema == nullptr) { return; }
                bind_output(
                    resolution, TypeRegistry::instance().ts(schema->fields()[0].type->value_schema));
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto        &erased = static_cast<const TSOutputView &>(out);
                const TSInputView &bundle = ts;
                const std::size_t  count  = bundle.schema()->field_count();

                std::optional<Value> best;
                for (std::size_t index = 0; index < count; ++index)
                {
                    const auto value  = bundle.indexed_child_at(index).value();
                    const bool better = !best.has_value() ||
                                        (Op == TsbAggregate::Min
                                             ? value.compare(best->view()) == std::partial_ordering::less
                                             : value.compare(best->view()) == std::partial_ordering::greater);
                    if (better) { best.emplace(value); }
                }
                auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(*best)));
            }
        };

        /** sum_/mean/std over a HOMOGENEOUS NUMERIC bundle. The element type
            is a TEMPLATE parameter selected at node-selection time
            (requires_ gates on the element meta) - the per-tick path never
            branches on type. */
        template <TsbAggregate Op, typename T>
        struct tsb_numeric_aggregate_impl
        {
            static_assert(Op == TsbAggregate::Sum || Op == TsbAggregate::Mean || Op == TsbAggregate::Std);

            static constexpr auto name = Op == TsbAggregate::Sum    ? (std::same_as<T, Int> ? "sum_tsb_int" : "sum_tsb_float")
                                         : Op == TsbAggregate::Mean ? (std::same_as<T, Int> ? "mean_tsb_int" : "mean_tsb_float")
                                                                    : (std::same_as<T, Int> ? "std_tsb_int" : "std_tsb_float");

            [[nodiscard]] static bool matches(OperatorCallContext context)
            {
                if (context.args.size() != 1) { return false; }
                const auto *schema = homogeneous_tsb(context);
                return schema != nullptr &&
                       schema->fields()[0].type->value_schema == scalar_descriptor<T>::value_meta();
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context) { return matches(context); }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                if (!matches(context)) { return; }
                auto &registry = TypeRegistry::instance();
                const auto *element = Op == TsbAggregate::Sum ? scalar_descriptor<T>::value_meta()
                                                              : scalar_descriptor<Float>::value_meta();
                bind_output(resolution, registry.ts(element));
            }

            static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"__out__">> out)
            {
                const auto        &erased = static_cast<const TSOutputView &>(out);
                const TSInputView &bundle = ts;
                const std::size_t  count  = bundle.schema()->field_count();

                const auto publish = [&](Value &&value) {
                    auto mutation = erased.data_view().begin_mutation(erased.evaluation_time());
                    static_cast<void>(mutation.move_value_from(std::move(value)));
                };

                T total{};
                for (std::size_t index = 0; index < count; ++index)
                {
                    total += bundle.indexed_child_at(index).value().checked_as<T>();
                }
                if constexpr (Op == TsbAggregate::Sum) { publish(Value{total}); }
                else if constexpr (Op == TsbAggregate::Mean)
                {
                    publish(Value{static_cast<Float>(total) / static_cast<Float>(count)});
                }
                else
                {
                    const Float mean     = static_cast<Float>(total) / static_cast<Float>(count);
                    Float       variance = 0.0;
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        const auto x =
                            static_cast<Float>(bundle.indexed_child_at(index).value().checked_as<T>());
                        variance += (x - mean) * (x - mean);
                    }
                    // hgraph uses the SAMPLE standard deviation (ddof=1).
                    publish(Value{count > 1 ? std::sqrt(variance / static_cast<Float>(count - 1)) : 0.0});
                }
            }
        };

        /** sub_ over strings is a WIRING error (hgraph's str overload raises;
            the itemwise TSB sub_ then reports it during composition). */
        struct sub_str_invalid
        {
            static constexpr auto name = "sub_str_invalid";
            static constexpr bool compose_noreturn = true;

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                auto &registry = TypeRegistry::instance();
                const auto *str_meta = registry.value_type("str");
                return ts_value_schema_at(context, 0) == str_meta &&
                       ts_value_schema_at(context, 1) == str_meta;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution)) { return; }
                bind_output_like_arg(resolution, context, 0);
            }

            [[noreturn]] static WiringPortRef compose(Wiring &, NamedPort<"lhs", TS<Str>>, NamedPort<"rhs", TS<Str>>)
            {
                throw std::invalid_argument("Cannot subtract one string from another");
            }
        };

        /** eq_ over TSBs: whole-bundle value equality. Bundles of DIFFERENT
            schemas are simply never equal. */
        struct eq_tsb_impl
        {
            static constexpr auto name = "eq_tsb";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return time_series_arg_matches<AnyTSB>(context, 0) &&
                       time_series_arg_matches<AnyTSB>(context, 1);
            }

            static void eval(In<"lhs", TsVar<"L">> lhs, In<"rhs", TsVar<"R">> rhs, Out<TS<Bool>> out)
            {
                const TSInputView &left  = lhs;
                const TSInputView &right = rhs;
                if (left.schema() != right.schema())
                {
                    out.set(false);
                    return;
                }
                out.set(left.value().equals(right.value()));
            }
        };

        /** combine(orig, delta) over BUNDLE scalars: recursive right-over-
            left merge honouring FIELD VALIDITY - delta's UNSET fields keep
            the original (hgraph's combine_compound_scalars; C++-first
            ruling 2026-07-06: CompoundScalar IS a C++ Bundle value). */
        struct combine_bundles_impl
        {
            static constexpr auto name = "combine_bundles";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *orig  = ts_value_schema(resolution.find_ts("A"));
                const auto *delta = ts_value_schema(resolution.find_ts("B"));
                return orig != nullptr && delta != nullptr && orig->value_kind() == ValueTypeKind::Bundle &&
                       delta->value_kind() == ValueTypeKind::Bundle;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext)
            {
                if (local_output_bound(resolution, "O")) { return; }
                const auto *orig = resolution.find_ts("A");
                bind_local_output(resolution, orig, "O");
            }

            static Value merge(const ValueView &orig, const ValueView &delta)
            {
                const auto *meta = orig.schema();
                BundleBuilder builder{value_type_for_active_realization(meta)};
                auto orig_fields  = orig.as_indexed_view();
                auto delta_fields = delta.as_indexed_view();
                for (std::size_t index = 0; index < meta->field_count; ++index)
                {
                    auto original = orig_fields.at(index);
                    auto update   = delta_fields.at(index);
                    if (!update.valid())
                    {
                        if (original.valid()) { builder.set(index, Value{original}); }
                        continue;
                    }
                    if (original.valid() && meta->fields[index].type->value_kind() == ValueTypeKind::Bundle)
                    {
                        builder.set(index, merge(original, update));
                        continue;
                    }
                    builder.set(index, Value{update});
                }
                return builder.build();
            }

            static void eval(In<"orig", TsVar<"A">> orig, In<"delta", TsVar<"B">, InputValidity::Unchecked> delta,
                             Out<TsVar<"O">> out)
            {
                const auto &erased = static_cast<const TSOutputView &>(out);
                Value result = delta.base().valid() ? merge(orig.base().value(), delta.base().value())
                                                    : Value{orig.base().value()};
                auto mutation = erased.begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.move_value_from(std::move(result)));
            }
        };
    }  // namespace collection_impl_detail

    template <typename Operator, template <typename...> class Kernel>
    inline void register_numeric_binary_collection_overloads()
    {
        register_overload<Operator, lift<Kernel<Int, Int>>>();
        register_overload<Operator, lift<Kernel<Float, Float>>>();
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    template <typename Operator, template <typename...> class Kernel>
    inline void register_numeric_binary_tsl_lifted_maps()
    {
        using tsl_itemwise_impl_detail::tsl_binary_lifted_map;
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Int, Int>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Float, Float>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Int, Float>>>>();
        register_graph_overload<Operator, tsl_binary_lifted_map<lift<Kernel<Float, Int>>>>();
    }

    void register_collection_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COLLECTION_IMPL_H
