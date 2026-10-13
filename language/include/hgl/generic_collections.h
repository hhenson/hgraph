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
#include <hgraph/types/value/value_view.h>
#include <stdexcept>
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
}  // namespace hgl::generic
#endif
