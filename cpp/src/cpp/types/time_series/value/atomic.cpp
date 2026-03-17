#include <hgraph/hgraph_base.h>
#ifndef HGRAPH_DISABLE_TS_REFERENCE_ATOMIC
#include <hgraph/types/ref.h>
#endif
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/builder.h>

namespace hgraph
{

    namespace detail
    {

        template <typename T> struct AtomicStateOps final : ValueBuilderOps
        {
            void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override {
                static_cast<void>(schema);
                builder.cache_layout(sizeof(AtomicState<T>), alignof(AtomicState<T>));
                builder.cache_lifecycle(!std::is_trivially_destructible_v<AtomicState<T>>, !InlineValueEligible<T>,
                                        InlineValueEligible<T>);
            }

            [[nodiscard]] const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept override {
                static_cast<void>(schema);
                return atomic_view_dispatch<T>();
            }

            [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override {
                static_cast<void>(schema);
                return !std::is_trivially_destructible_v<AtomicState<T>>;
            }

            [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override {
                static_cast<void>(schema);
                return !InlineValueEligible<T>;
            }

            [[nodiscard]] bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept override {
                static_cast<void>(schema);
                return InlineValueEligible<T>;
            }

            void construct(void *memory) const override
            {
                std::construct_at(state(memory), AtomicState<T>{::hgraph::atomic_default_value(std::type_identity<T>{})});
            }

            void destroy(void *memory) const noexcept override { std::destroy_at(state(memory)); }

            void copy_construct(void *dst, const void *src) const override { std::construct_at(state(dst), *state(src)); }

            void move_construct(void *dst, void *src) const override { std::construct_at(state(dst), std::move(*state(src))); }

          private:
            [[nodiscard]] static AtomicState<T> *state(void *memory) noexcept {
                return std::launder(reinterpret_cast<AtomicState<T> *>(memory));
            }

            [[nodiscard]] static const AtomicState<T> *state(const void *memory) noexcept {
                return std::launder(reinterpret_cast<const AtomicState<T> *>(memory));
            }
        };

        template <typename T> [[nodiscard]] const ValueBuilderOps &atomic_state_ops() noexcept {
            static const AtomicStateOps<T> ops{};
            return ops;
        }

        template <typename T> [[nodiscard]] const ValueBuilder &atomic_builder(MutationTracking tracking) noexcept {
            static const ValueBuilder plain_builder{*value::scalar_type_meta<T>(), MutationTracking::Plain, atomic_state_ops<T>()};
            static const ValueBuilder delta_builder{*value::scalar_type_meta<T>(), MutationTracking::Delta, atomic_state_ops<T>()};
            return tracking == MutationTracking::Delta ? delta_builder : plain_builder;
        }

        const ValueBuilder *atomic_builder_for(const value::TypeMeta *schema, MutationTracking tracking) {
            if (schema == nullptr || schema->kind != value::TypeKind::Atomic) { return nullptr; }

#define HGRAPH_ATOMIC_BUILDER_CASE(type_)                                                                                          \
    if (schema == value::scalar_type_meta<type_>()) { return &atomic_builder<type_>(tracking); }

            HGRAPH_ATOMIC_BUILDER_CASE(bool)
            HGRAPH_ATOMIC_BUILDER_CASE(int8_t)
            HGRAPH_ATOMIC_BUILDER_CASE(int16_t)
            HGRAPH_ATOMIC_BUILDER_CASE(int32_t)
            HGRAPH_ATOMIC_BUILDER_CASE(int64_t)
            HGRAPH_ATOMIC_BUILDER_CASE(uint8_t)
            HGRAPH_ATOMIC_BUILDER_CASE(uint16_t)
            HGRAPH_ATOMIC_BUILDER_CASE(uint32_t)
            HGRAPH_ATOMIC_BUILDER_CASE(uint64_t)
            HGRAPH_ATOMIC_BUILDER_CASE(size_t)
            HGRAPH_ATOMIC_BUILDER_CASE(float)
            HGRAPH_ATOMIC_BUILDER_CASE(double)
            HGRAPH_ATOMIC_BUILDER_CASE(std::string)
            HGRAPH_ATOMIC_BUILDER_CASE(nb::object)
#ifndef HGRAPH_DISABLE_TS_REFERENCE_ATOMIC
            HGRAPH_ATOMIC_BUILDER_CASE(TimeSeriesReference)
#endif
            HGRAPH_ATOMIC_BUILDER_CASE(engine_date_t)
            HGRAPH_ATOMIC_BUILDER_CASE(engine_time_t)
            HGRAPH_ATOMIC_BUILDER_CASE(engine_time_delta_t)

#undef HGRAPH_ATOMIC_BUILDER_CASE
            return nullptr;
        }

    }  // namespace detail

}  // namespace hgraph
