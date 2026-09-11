#ifndef HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H
#define HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H

#include <hgraph/types/static_node.h>

#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace output_mutation_detail
    {
        template <typename TValue> [[nodiscard]] ValueView key_view(const Out<TSS<TValue>> &out, const TValue &value) {
            return ValueView{out.data_view().layout().key_binding, std::addressof(value)};
        }

        template <typename TSchema, typename TValue>
        concept settable_output = requires(const Out<TSchema> &out, TValue &&value) { out.set(std::forward<TValue>(value)); };
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
        requires output_mutation_detail::settable_output<TValueSchema, TValue>
    void insert(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        if (out.contains(key)) { throw std::invalid_argument("insert requires an absent TSD key"); }

        auto rollback = make_scope_exit<true>([&] { static_cast<void>(out.erase(key)); });
        out[key].set(std::forward<TValue>(value));
        rollback.release();
    }

    /** Update and tick an existing dictionary child. */
    template <typename TKey, typename TValueSchema, typename TValue>
        requires output_mutation_detail::settable_output<TValueSchema, TValue>
    void update(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        if (!out.contains(key)) { throw std::out_of_range("update requires a present TSD key"); }
        out.at_slot(out.find_slot(key)).set(std::forward<TValue>(value));
    }

    /** Insert or update a dictionary child. */
    template <typename TKey, typename TValueSchema, typename TValue>
        requires output_mutation_detail::settable_output<TValueSchema, TValue>
    void upsert(const Out<TSD<TKey, TValueSchema>> &out, const TKey &key, TValue &&value) {
        const bool existed  = out.contains(key);
        auto       rollback = make_scope_exit<true>([&] {
            if (!existed) { static_cast<void>(out.erase(key)); }
        });
        out[key].set(std::forward<TValue>(value));
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
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_OUTPUT_MUTATION_H
