#ifndef HGRAPH_TYPES_METADATA_DETAIL_REALIZED_VALUE_SEAMS_H
#define HGRAPH_TYPES_METADATA_DETAIL_REALIZED_VALUE_SEAMS_H

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/python_object.h>

#include <cstddef>
#include <span>
#include <vector>

/**
 * Private seams of the realized-value families (RFC 0035, PR 3): what the
 * bridge's ``realized_conversions.cpp`` needs from the plan factory, the type
 * realization and the pooled polymorphic entry, with no Python in it.
 *
 * The composite and array contexts are plain data the conversions read; the
 * owned, shared, closed-bundle and pooled entries keep their allocation and
 * validity logic here (exception-safe replacement, active-type switching)
 * and expose it as *assign* seams that take a fill callback, so the bridge
 * converts into the payload the seam hands it and never touches an
 * allocation. This header is private to ``hgraph_runtime``: both sides
 * compile into the same library, nothing installs it.
 */
namespace hgraph::realized_detail
{
    /** Layout of a realized Tuple / Bundle (``value_plan_factory.cpp``). */
    struct CompositeIndexedContext
    {
        const ValueTypeMetaData  *schema{nullptr};
        std::vector<ValueTypeRef> child_bindings{};
        std::vector<std::size_t>  offsets{};
        std::size_t               validity_offset{0};
        std::size_t               validity_word_count{0};
    };

    /** Field validity (holes read back as None; None marks a hole). */
    [[nodiscard]] bool composite_field_is_set(const CompositeIndexedContext *state, const void *memory,
                                              std::size_t index) noexcept;
    void composite_set_field_validity(const CompositeIndexedContext *state, void *memory, std::size_t index, bool set);
    void composite_set_all_validity(const CompositeIndexedContext *state, void *memory, bool set);

    /** Layout of a realized fixed / bounded array. */
    struct ArrayIndexedContext
    {
        const ValueTypeMetaData *schema{nullptr};
        ValueTypeRef             element_binding{nullptr};
        std::size_t              capacity{0};
        std::size_t              stride{0};
        std::size_t              size_offset{0};
        std::size_t              data_offset{0};
        bool                     bounded{false};
    };

    [[nodiscard]] std::size_t array_size(const void *context, const void *memory) noexcept;
    void array_resize(const void *context, void *memory, std::size_t size);

    /** Convert into ``payload``, which the seam has constructed as ``target``. */
    using FillFn = void (*)(void *fill_context, ValueTypeRef target, void *payload);

    // -- owned value entry (an allocation of the owned schema, or none) --------
    [[nodiscard]] ValueTypeRef owned_entry_active_type(const void *memory) noexcept;
    [[nodiscard]] const void *owned_entry_payload(const void *memory) noexcept;
    void owned_entry_reset(void *memory) noexcept;
    void owned_entry_assign(const void *context, void *memory, FillFn fill, void *fill_context);

    // -- shared value entry (a pooled allocation, or none) ---------------------
    [[nodiscard]] ValueTypeRef shared_entry_active_type(const void *memory) noexcept;
    [[nodiscard]] const void *shared_entry_payload(const void *memory) noexcept;
    void shared_entry_reset(void *memory) noexcept;
    void shared_entry_assign(const void *context, void *memory, FillFn fill, void *fill_context);

    // -- closed Bundle (``type_realization.cpp``) ------------------------------
    /** The declared closed Bundle and its realized alternatives: what a
        Python source is resolved against. */
    struct PolymorphicAlternatives
    {
        const ValueTypeMetaData       *declared{nullptr};
        std::span<const ValueTypeRef>  alternatives{};
    };

    [[nodiscard]] ValueTypeRef union_entry_active_type(const void *context, const void *memory) noexcept;
    [[nodiscard]] const void *union_entry_payload(const void *context, const void *memory) noexcept;
    [[nodiscard]] const PolymorphicAlternatives &union_entry_alternatives(const void *context) noexcept;
    /** Switch the active alternative to ``requested`` (restoring the default on
        failure) and fill it. */
    void union_entry_assign(const void *context, void *memory, ValueTypeRef requested, FillFn fill,
                            void *fill_context);

    // -- pooled closed Bundle (``pooled_polymorphic_value_type.cpp``) ----------
    [[nodiscard]] ValueTypeRef pooled_entry_active_type(const void *context, const void *memory) noexcept;
    [[nodiscard]] const void *pooled_entry_payload(const void *context, const void *memory) noexcept;
    /** Resolve a Python source through the entry's registered resolver. */
    [[nodiscard]] ValueTypeRef pooled_entry_resolve_source(const void *context, PyRef source);
    /** Replace the stored allocation with one of the alternative matching
        ``external_type``'s schema, filled by ``fill``. */
    void pooled_entry_assign(const void *context, void *memory, ValueTypeRef external_type, FillFn fill,
                             void *fill_context);
}  // namespace hgraph::realized_detail

#endif  // HGRAPH_TYPES_METADATA_DETAIL_REALIZED_VALUE_SEAMS_H
