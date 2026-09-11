#ifndef HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H
#define HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H

#include <hgraph/types/static_node.h>

#include <concepts>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace output_mutation_detail
    {
        template <typename TValue> [[nodiscard]] ValueView key_view(const Out<TSS<TValue>> &out, const TValue &value) {
            return ValueView{out.data_view().layout().key_binding, std::addressof(value)};
        }

        template <typename TSchema> using output_value_t = typename Out<TSchema>::value_type;

        template <typename TSchema, typename TValue>
        concept stageable_output =
            requires { typename output_value_t<TSchema>; } && std::constructible_from<output_value_t<TSchema>, TValue> &&
            std::is_nothrow_move_constructible_v<output_value_t<TSchema>> &&
            std::is_nothrow_move_assignable_v<output_value_t<TSchema>> &&
            requires(const Out<TSchema> &out, output_value_t<TSchema> value) { out.set(std::move(value)); };

        template <typename TSchema, typename TValue>
            requires stageable_output<TSchema, TValue>
        [[nodiscard]] output_value_t<TSchema> stage(TValue &&value) {
            return output_value_t<TSchema>{std::forward<TValue>(value)};
        }

        template <auto N>
        inline constexpr bool is_dynamic_list_extent = [] {
            using descriptor = static_schema_detail::size_parameter_descriptor<N>;
            if constexpr (!descriptor::is_concrete()) { return false; }
            return descriptor::concrete_size() == unbounded_tsl_size;
        }();
    }  // namespace output_mutation_detail

    /** Insert an absent member into a set output. */
    template <typename TValue> void insert(const Out<TSS<TValue>> &out, const TValue &value) {
        if (out.contains(output_mutation_detail::key_view(out, value))) {
            throw std::invalid_argument("insert requires an absent TSS member");
        }
        if (!out.add(value)) { throw std::logic_error("TSS insert did not add the absent member"); }
    }

    /** Ensure that a member exists in a set output. */
    template <typename TValue> void upsert(const Out<TSS<TValue>> &out, const TValue &value) {
        if (!out.contains(output_mutation_detail::key_view(out, value))) { static_cast<void>(out.add(value)); }
    }

    /** Remove a present member from a set output. */
    template <typename TValue> void remove(const Out<TSS<TValue>> &out, const TValue &value) {
        if (!out.contains(output_mutation_detail::key_view(out, value))) {
            throw std::out_of_range("remove requires a present TSS member");
        }
        if (!out.remove(value)) { throw std::logic_error("TSS remove did not remove the present member"); }
    }

    /** Remove a member from a set output if it exists. */
    template <typename TValue> void discard(const Out<TSS<TValue>> &out, const TValue &value) {
        if (out.contains(output_mutation_detail::key_view(out, value))) { static_cast<void>(out.remove(value)); }
    }

    /** Remove every member from a set output. */
    template <typename TValue> void clear(const Out<TSS<TValue>> &out) {
        if (!out.empty()) { out.clear(); }
    }

    /** Insert and initialize an absent dictionary child. */
    template <typename TKey, typename TValueSchema, typename TValue>
        requires output_mutation_detail::stageable_output<TValueSchema, TValue>
    void insert(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        if (out.contains(key)) { throw std::invalid_argument("insert requires an absent TSD key"); }

        auto resolved = output_mutation_detail::stage<TValueSchema>(std::forward<TValue>(value));
        auto rollback = make_scope_exit<true>([&] { static_cast<void>(out.erase(key)); });
        out[key].set(std::move(resolved));
        rollback.release();
    }

    /** Update and tick an existing dictionary child. */
    template <typename TKey, typename TValueSchema, typename TValue>
        requires output_mutation_detail::stageable_output<TValueSchema, TValue>
    void update(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        if (!out.contains(key)) { throw std::out_of_range("update requires a present TSD key"); }
        auto resolved = output_mutation_detail::stage<TValueSchema>(std::forward<TValue>(value));
        out.at_slot(out.find_slot(key)).set(std::move(resolved));
    }

    /** Insert or update a dictionary child. */
    template <typename TKey, typename TValueSchema, typename TValue>
        requires output_mutation_detail::stageable_output<TValueSchema, TValue>
    void upsert(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        auto       resolved = output_mutation_detail::stage<TValueSchema>(std::forward<TValue>(value));
        const bool existed  = out.contains(key);
        auto       rollback = make_scope_exit<true>([&] {
            if (!existed) { static_cast<void>(out.erase(key)); }
        });
        out[key].set(std::move(resolved));
        rollback.release();
    }

    /** Remove a present dictionary child. */
    template <typename TKey, typename TValueSchema> void remove(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key) {
        if (!out.contains(key)) { throw std::out_of_range("remove requires a present TSD key"); }
        if (!out.erase(key)) { throw std::logic_error("TSD remove did not remove the present key"); }
    }

    /** Remove a dictionary child if it exists. */
    template <typename TKey, typename TValueSchema> void discard(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key) {
        if (out.contains(key)) { static_cast<void>(out.erase(key)); }
    }

    /** Invalidate an existing dictionary child without removing its key. */
    template <typename TKey, typename TValueSchema> void invalidate(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key) {
        if (!out.contains(key)) { throw std::out_of_range("invalidate requires a present TSD key"); }
        auto child = out.at_slot(out.find_slot(key));
        static_cast<void>(child.begin_mutation(child.evaluation_time()).invalidate());
    }

    /** Remove every child from a dictionary output. */
    template <typename TKey, typename TValueSchema> void clear(const Out<TSD<TKey, TValueSchema>> &out) {
        if (!out.empty()) { out.clear(); }
    }

    /** Append and initialize a child on an unbounded list output. */
    template <typename TElementSchema, auto N, typename TValue>
        requires(output_mutation_detail::is_dynamic_list_extent<N> &&
                 output_mutation_detail::stageable_output<TElementSchema, TValue>)
    void push(const Out<TSL<TElementSchema, N>> &out, TValue &&value) {
        auto              resolved      = output_mutation_detail::stage<TElementSchema>(std::forward<TValue>(value));
        const std::size_t previous_size = out.size();
        out.resize(previous_size + 1);
        auto rollback = make_scope_exit<true>([&] { out.resize(previous_size); });
        out[previous_size].set(std::move(resolved));
        rollback.release();
    }

    /** Remove the trailing child from a non-empty unbounded list output. */
    template <typename TElementSchema, auto N>
        requires output_mutation_detail::is_dynamic_list_extent<N>
    void pop(const Out<TSL<TElementSchema, N>> &out) {
        if (out.empty()) { throw std::out_of_range("pop requires a non-empty unbounded TSL"); }
        out.resize(out.size() - 1);
    }

    /** Remove every child from an unbounded list output. */
    template <typename TElementSchema, auto N>
        requires output_mutation_detail::is_dynamic_list_extent<N>
    void clear(const Out<TSL<TElementSchema, N>> &out) {
        out.resize(0);
    }

    /** Invalidate an existing list child without changing the list length. */
    template <typename TElementSchema, auto N> void invalidate(const Out<TSL<TElementSchema, N>> &out, std::size_t index) {
        if (index >= out.size()) { throw std::out_of_range("invalidate requires an existing TSL index"); }
        auto child = out[index];
        static_cast<void>(child.begin_mutation(child.evaluation_time()).invalidate());
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H
