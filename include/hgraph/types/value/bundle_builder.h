#ifndef HGRAPH_TYPES_VALUE_BUNDLE_BUILDER_H
#define HGRAPH_TYPES_VALUE_BUNDLE_BUILDER_H

#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Where one composite binding keeps its fields and their validity bits,
     * resolved once.
     *
     * A composite's plan lists a component per field, and a Bundle or a
     * Tuple adds one more holding the field-validity words
     * (core_concepts.rst). ``BundleBuilder`` uses this to mark a field live,
     * and a bulk reader or writer that touches many rows of one composite
     * uses it to reach a field without a call per access. Only composite
     * (assembly) storage has one: a wrapper such as ``Shared<Bundle>`` keeps
     * its fields behind its own ops.
     */
    class CompositeFieldLayout
    {
      public:
        [[nodiscard]] static std::optional<CompositeFieldLayout> of(const ValueTypeRef &binding)
        {
            const auto *plan = binding ? binding.plan() : nullptr;
            const auto *meta = binding ? binding.schema() : nullptr;
            if (plan == nullptr || meta == nullptr || !plan->is_composite()) { return std::nullopt; }
            const auto kind = meta->try_value_kind();
            if (kind != ValueTypeKind::Bundle && kind != ValueTypeKind::Tuple && kind != ValueTypeKind::Optional)
            {
                return std::nullopt;
            }
            const auto components = plan->components();
            if (components.size() < meta->field_count) { return std::nullopt; }
            CompositeFieldLayout result;
            result.components_ = components;
            result.field_count_ = meta->field_count;
            if (components.size() > meta->field_count) { result.validity_offset_ = components[meta->field_count].offset; }
            return result;
        }

        [[nodiscard]] std::size_t field_count() const noexcept { return field_count_; }
        [[nodiscard]] const void *field(const void *row, std::size_t index) const noexcept
        {
            return static_cast<const std::byte *>(row) + components_[index].offset;
        }
        [[nodiscard]] void *field(void *row, std::size_t index) const noexcept
        {
            return static_cast<std::byte *>(row) + components_[index].offset;
        }
        /** Dense composites (no validity words) have every field set. */
        [[nodiscard]] bool field_set(const void *row, std::size_t index) const noexcept
        {
            if (validity_offset_ == npos) { return true; }
            const auto *words = static_cast<const std::uint64_t *>(
                static_cast<const void *>(static_cast<const std::byte *>(row) + validity_offset_));
            return (words[index / bits_per_word] >> (index % bits_per_word)) & std::uint64_t{1};
        }
        void mark_field(void *row, std::size_t index) const noexcept
        {
            if (validity_offset_ == npos) { return; }
            auto *words = static_cast<std::uint64_t *>(
                static_cast<void *>(static_cast<std::byte *>(row) + validity_offset_));
            words[index / bits_per_word] |= std::uint64_t{1} << (index % bits_per_word);
        }

      private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);
        static constexpr std::size_t bits_per_word = sizeof(std::uint64_t) * 8U;

        std::span<const MemoryUtils::CompositeComponent> components_{};
        std::size_t field_count_{0};
        std::size_t validity_offset_{npos};
    };

    // -----------------------------------------------------------------
    // BundleBuilder — assemble a compact (immutable) Bundle / Tuple
    // ``Value`` from prebuilt field ``Value``s.
    //
    // Composite fields cannot be populated through ``begin_mutation`` when
    // they are themselves immutable containers (``Set`` / ``Map`` / …):
    // ``MutableIndexedValueView::at`` begin-mutates each field, which an
    // immutable field refuses. Instead each field is set by a whole-value
    // ``copy_assign`` / ``move_assign`` at its layout offset, over the
    // default-constructed field. This is how the canonical delta bundles
    // (``Bundle{added: Set<T>, removed: Set<T>}`` for ``TSS``, the per-field
    // delta bundle for ``TSB``) are constructed for tests/wiring so they
    // match the runtime ``delta_value_schema`` exactly.
    // -----------------------------------------------------------------
    class BundleBuilder
    {
      public:
        explicit BundleBuilder(const ValueTypeRef &bundle_binding)
            : target_binding_{bundle_binding},
              binding_{assembly_binding(bundle_binding)},
              value_{binding_}
        {
        }

        /** Return the composite field-assembly binding for ``target``. */
        [[nodiscard]] static ValueTypeRef assembly_type(ValueTypeRef target)
        {
            return assembly_binding(target);
        }

        /** Set field ``index`` to a copy of ``field`` (whole-value copy-assign). */
        BundleBuilder &set(std::size_t index, const ValueView &field)
        {
            ensure_not_built();
            if (!field.has_value())
            {
                throw std::invalid_argument("BundleBuilder::set(ValueView) requires a live field value");
            }
            const auto &comp = component(index);
            const auto destination = field_binding(index);
            destination.ops_ref().copy_assign_from(
                destination, field_memory(comp.offset), field.binding(), field.data());
            mark_field(index);
            return *this;
        }

        /** Set the named field ``name`` to a copy of ``field``. */
        BundleBuilder &set(std::string_view name, const ValueView &field) { return set(index_of(name), field); }

        /** Set field ``index`` by moving an owned ``field`` value into the bundle. */
        BundleBuilder &set(std::size_t index, Value &&field)
        {
            ensure_not_built();
            if (!field.has_value())
            {
                throw std::invalid_argument("BundleBuilder::set(Value&&) requires a live field value");
            }
            const auto &comp = component(index);
            const auto destination = field_binding(index);
            const auto source = field.view().binding();
            destination.ops_ref().move_assign_from(
                destination, field_memory(comp.offset), source,
                const_cast<void *>(field.view().data()));
            mark_field(index);
            return *this;
        }

        /** Set the named field ``name`` by moving an owned ``field`` value into the bundle. */
        BundleBuilder &set(std::string_view name, Value &&field) { return set(index_of(name), std::move(field)); }

        [[nodiscard]] std::size_t size() const noexcept { return field_count(); }
        /** The binding field ``index`` / ``name`` is built as (the assembly
            binding's component): what a value assigned to that field must be
            convertible to, and what a caller builds it as to avoid a copy.
            Answered from the composite plan, never the registry. */
        [[nodiscard]] ValueTypeRef field_binding(std::size_t index) const
        {
            const auto *ops = checked_value_ops<IndexedValueOps>(binding_, "BundleBuilder");
            const auto field = ops->element_binding(ops->context, value_.view().data(), index);
            if (!field) { throw std::logic_error("BundleBuilder: field binding is unresolved"); }
            return field;
        }
        [[nodiscard]] ValueTypeRef field_binding(std::string_view name) const { return field_binding(index_of(name)); }

        /** Field validity (core_concepts.rst): set() marks the field live,
            of a Bundle and of a Tuple alike. */
        void mark_field(std::size_t index)
        {
            const auto layout = CompositeFieldLayout::of(binding_);
            if (!layout.has_value() || layout->field_count() == 0) { return; }
            if (index >= layout->field_count()) { throw std::out_of_range("BundleBuilder: field index out of range"); }
            layout->mark_field(const_cast<void *>(value_.view().data()), index);
        }

        [[nodiscard]] Value build()
        {
            ensure_not_built();
            built_ = true;
            // ``binding_`` is already the owning representation
            // (``owning_assembly``), so the target needs no conversion when
            // it owns as the assembly does — the caller receives the portable
            // value either way.
            if (value_owning_type(target_binding_) == binding_) { return std::move(value_); }

            Value result{target_binding_};
            // Dispatch on what ``result`` IS, not on what it was asked for:
            // ``Value`` materialises the owning representation, so
            // ``target_binding_``'s ops would run over its owner's memory.
            const auto result_binding = result.view().binding();
            result_binding.ops_ref().move_assign_from(
                result_binding,
                const_cast<void *>(result.view().data()),
                binding_,
                const_cast<void *>(value_.view().data()));
            return result;
        }

      private:
        [[nodiscard]] static ValueTypeRef assembly_binding(ValueTypeRef target)
        {
            if (!target)
            {
                throw std::invalid_argument("BundleBuilder requires a bound target");
            }
            if (target.checked_plan().is_composite()) { return owning_assembly(target); }
            const auto *schema = target.schema();
            if (schema == nullptr || schema->try_value_kind() != ValueTypeKind::Bundle ||
                schema->wrapped_un_named == nullptr)
            {
                throw std::logic_error(
                    "BundleBuilder requires composite storage or a named Bundle with a structural twin");
            }
            const auto *indexed =
                checked_value_ops<IndexedValueOps>(target, "BundleBuilder target");
            std::vector<ValueTypeRef> fields;
            fields.reserve(schema->field_count);
            for (std::size_t index = 0; index < schema->field_count; ++index)
            {
                const auto field =
                    indexed->element_binding(indexed->context, nullptr, index);
                if (!field)
                {
                    throw std::logic_error(
                        "BundleBuilder target has an unresolved field binding");
                }
                fields.push_back(field);
            }
            const auto structural =
                ValuePlanFactory::instance().realized_composite_type_for(
                    schema->wrapped_un_named, fields);
            if (!structural || !structural.checked_plan().is_composite())
            {
                throw std::logic_error(
                    "BundleBuilder structural assembly binding is unavailable");
            }
            return owning_assembly(structural);
        }

        /** The representation the builder's own storage will be.

            ``Value`` always materialises the OWNING representation of the
            binding it is handed (``value_owning_type``): a graph-local
            realization publishes an external owner, and the two are
            plan-compatible but NOT interchangeable — their polymorphic
            fields are distinct closed-Bundle entries with their own
            ``TypeRecord``s. Deriving the field bindings from the binding the
            caller passed while writing into storage of its owner leaves each
            field carrying a record the reader's entry has never seen, and the
            first consumer to ask for the field's concrete type fails with
            "closed Bundle source alternative ... is outside this graph
            snapshot". So the assembly binding IS the owning representation,
            and ``build`` converts to the caller's target from there. */
        [[nodiscard]] static ValueTypeRef owning_assembly(ValueTypeRef assembly)
        {
            const auto owning = value_owning_type(assembly);
            if (!owning || owning == assembly) { return assembly; }
            if (!owning.checked_plan().is_composite())
            {
                throw std::logic_error(
                    "BundleBuilder target's owning representation is not composite storage");
            }
            return owning;
        }

        [[nodiscard]] const MemoryUtils::CompositeState &state() const
        {
            const auto *s =
                static_cast<const MemoryUtils::CompositeState *>(binding_.checked_plan().lifecycle_context);
            if (s == nullptr) { throw std::logic_error("BundleBuilder: binding has no composite state"); }
            return *s;
        }

        [[nodiscard]] const MemoryUtils::CompositeComponent &component(std::size_t index) const
        {
            const auto &st = state();
            if (index >= field_count()) { throw std::out_of_range("BundleBuilder: field index out of range"); }
            return st.components()[index];
        }


        [[nodiscard]] std::size_t index_of(std::string_view name) const
        {
            const auto &st = state();
            for (std::size_t i = 0; i < field_count(); ++i)
            {
                const auto &c = st.components()[i];
                if (c.name != nullptr && name == c.name) { return i; }
            }
            throw std::out_of_range("BundleBuilder: field name not found");
        }

        // The value_ storage is owned and being assembled here, so writing
        // through its raw base pointer is sound (no begin_mutation gating).
        [[nodiscard]] void *field_memory(std::size_t offset)
        {
            return static_cast<std::byte *>(const_cast<void *>(value_.view().data())) + offset;
        }

        [[nodiscard]] std::size_t field_count() const noexcept
        {
            const auto *meta = binding_.schema();
            if (meta != nullptr && meta->try_value_kind() == ValueTypeKind::Bundle) { return meta->field_count; }
            return state().component_count;
        }

        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("BundleBuilder is single-use"); }
        }

        ValueTypeRef target_binding_{nullptr};
        ValueTypeRef binding_{nullptr};
        Value                   value_{};
        bool                    built_{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_BUNDLE_BUILDER_H
