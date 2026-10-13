#ifndef HGL_GENERIC_COLLECTIONS_H
#define HGL_GENERIC_COLLECTIONS_H
// Set and map access by value view, for runtime bodies whose set element or
// map key type is generic. hgraph's typed In<TSS<T>>/Out<TSS<T>> selectors
// read and write a concrete C++ T; with a ScalarVar pattern there is none, so
// generated code reaches the untyped views those selectors derive from. The
// output mutations keep the strictness and messages of
// <hgraph/types/time_series/output_mutation.h>.
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_input/set_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/output_mutation.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/scope.h>
#include <stdexcept>
#include <type_traits>
#include <utility>
namespace hgl::generic
{
    [[nodiscard]] inline const hgraph::TSSInputView &set(const hgraph::TSSInputView &view) noexcept { return view; }
    [[nodiscard]] inline const hgraph::TSSOutputView &set(const hgraph::TSSOutputView &view) noexcept { return view; }
    [[nodiscard]] inline const hgraph::TSDInputView &map(const hgraph::TSDInputView &view) noexcept { return view; }
    [[nodiscard]] inline const hgraph::TSDOutputView &map(const hgraph::TSDOutputView &view) noexcept { return view; }

    /** Insert an absent member into a set output. */
    inline void insert(const hgraph::TSSOutputView &out, const hgraph::ValueView &member) {
        if (out.contains(member)) { throw std::invalid_argument("insert requires an absent TSS member"); }
        if (!out.begin_mutation(out.evaluation_time()).add(member)) {
            throw std::logic_error("TSS insert did not add the absent member");
        }
    }

    /** Ensure that a member exists in a set output. */
    inline void upsert(const hgraph::TSSOutputView &out, const hgraph::ValueView &member) {
        if (!out.contains(member)) { static_cast<void>(out.begin_mutation(out.evaluation_time()).add(member)); }
    }

    /** Remove a present member from a set output. */
    inline void remove(const hgraph::TSSOutputView &out, const hgraph::ValueView &member) {
        if (!out.contains(member)) { throw std::out_of_range("remove requires a present TSS member"); }
        if (!out.begin_mutation(out.evaluation_time()).remove(member)) {
            throw std::logic_error("TSS remove did not remove the present member");
        }
    }

    /** Remove a member from a set output if it exists. */
    inline void discard(const hgraph::TSSOutputView &out, const hgraph::ValueView &member) {
        if (out.contains(member)) { static_cast<void>(out.begin_mutation(out.evaluation_time()).remove(member)); }
    }

    namespace detail
    {
        template <typename TKey, typename TValueSchema>
        [[nodiscard]] hgraph::TSDDataMutationView map_mutation(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out) {
            return static_cast<const hgraph::TSDOutputView &>(out).begin_mutation(out.evaluation_time());
        }

        /** The child for `key`, created when absent, as Out<TSD>::operator[] does for a typed key. */
        template <typename TKey, typename TValueSchema>
        [[nodiscard]] hgraph::Out<TValueSchema> child(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out,
                                                      const hgraph::ValueView &key) {
            auto mutation = map_mutation(out);
            return hgraph::Out<TValueSchema>{hgraph::TSOutputView{out.base().output(), mutation.at(key), out.evaluation_time()},
                                             out.evaluation_time()};
        }

        template <typename TKey, typename TValueSchema>
        [[nodiscard]] bool contains(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key) {
            return static_cast<const hgraph::TSDOutputView &>(out).contains(key);
        }

        /** Prepare a child's value before the child exists, so a failure leaves the
            map untouched: a value view (a generic payload) and an owned value (an
            atomic collection) are already complete; a typed value is staged. */
        template <typename TValueSchema, typename TValue>
        [[nodiscard]] auto stage(TValue &&value) {
            using Plain = std::remove_cvref_t<TValue>;
            if constexpr (std::is_same_v<Plain, hgraph::Value>) {
                return hgraph::Value{std::forward<TValue>(value)};
            } else if constexpr (std::is_convertible_v<TValue &&, const hgraph::ValueView &>) {
                return hgraph::ValueView{value};
            } else {
                return hgraph::output_mutation_detail::stage<TValueSchema>(std::forward<TValue>(value));
            }
        }

        /** Publish a staged value into its child. */
        template <typename TValueSchema, typename TStaged>
        void commit(const hgraph::Out<TValueSchema> &child, TStaged &&staged) {
            using Plain = std::remove_cvref_t<TStaged>;
            if constexpr (std::is_same_v<Plain, hgraph::Value>) {
                child.apply(staged.view());
            } else if constexpr (std::is_same_v<Plain, hgraph::ValueView>) {
                child.apply(staged);
            } else {
                child.set(std::forward<TStaged>(staged));
            }
        }
    }  // namespace detail

    /** Insert and initialize an absent map child under a generic key. */
    template <typename TKey, typename TValueSchema, typename TValue>
    void insert(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key, TValue &&value) {
        if (detail::contains(out, key)) { throw std::invalid_argument("insert requires an absent TSD key"); }
        auto staged   = detail::stage<TValueSchema>(std::forward<TValue>(value));
        auto rollback = hgraph::make_scope_exit<true>([&] { static_cast<void>(detail::map_mutation(out).erase(key)); });
        detail::commit(detail::child(out, key), std::move(staged));
        rollback.release();
    }

    /** Update and tick an existing map child under a generic key. */
    template <typename TKey, typename TValueSchema, typename TValue>
    void update(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key, TValue &&value) {
        if (!detail::contains(out, key)) { throw std::out_of_range("update requires a present TSD key"); }
        auto staged = detail::stage<TValueSchema>(std::forward<TValue>(value));
        detail::commit(out.at_slot(static_cast<const hgraph::TSDOutputView &>(out).find_slot(key)), std::move(staged));
    }

    /** Insert or update a map child under a generic key. */
    template <typename TKey, typename TValueSchema, typename TValue>
    void upsert(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key, TValue &&value) {
        auto       staged   = detail::stage<TValueSchema>(std::forward<TValue>(value));
        const bool existed  = detail::contains(out, key);
        auto       rollback = hgraph::make_scope_exit<true>([&] {
            if (!existed) { static_cast<void>(detail::map_mutation(out).erase(key)); }
        });
        detail::commit(detail::child(out, key), std::move(staged));
        rollback.release();
    }

    /** Remove a present map child under a generic key. */
    template <typename TKey, typename TValueSchema>
    void remove(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key) {
        if (!detail::contains(out, key)) { throw std::out_of_range("remove requires a present TSD key"); }
        if (!detail::map_mutation(out).erase(key)) { throw std::logic_error("TSD remove did not remove the present key"); }
    }

    /** Remove a map child under a generic key if it exists. */
    template <typename TKey, typename TValueSchema>
    void discard(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key) {
        if (detail::contains(out, key)) { static_cast<void>(detail::map_mutation(out).erase(key)); }
    }

    /** Invalidate an existing map child under a generic key without removing the key. */
    template <typename TKey, typename TValueSchema>
    void invalidate(const hgraph::Out<hgraph::TSD<TKey, TValueSchema>> &out, const hgraph::ValueView &key) {
        if (!detail::contains(out, key)) { throw std::out_of_range("invalidate requires a present TSD key"); }
        auto child = out.at_slot(static_cast<const hgraph::TSDOutputView &>(out).find_slot(key));
        static_cast<void>(child.begin_mutation(child.evaluation_time()).invalidate());
    }
}  // namespace hgl::generic
#endif
