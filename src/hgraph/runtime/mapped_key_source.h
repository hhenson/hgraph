#ifndef HGRAPH_RUNTIME_MAPPED_KEY_SOURCE_H
#define HGRAPH_RUNTIME_MAPPED_KEY_SOURCE_H

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::runtime_detail
{
    struct MappedKeySourceStorage
    {
        const Value *key{nullptr};
        TSDataTracking tracking{};
    };

    namespace mapped_key_source_detail
    {
        struct Context
        {
            const TSValueTypeMetaData *schema{nullptr};
            TSDataLayout layout{};
            TSDataOps    ops{};
        };

        [[nodiscard]] inline const Context &context_for(
            const TSValueTypeMetaData &schema,
            ValueTypeRef value_binding)
        {
            if (schema.kind != TSTypeKind::TS)
            {
                throw std::invalid_argument("mapped key source requires a scalar TS<K> schema");
            }
            if (schema.value_schema == nullptr || schema.delta_value_schema == nullptr)
            {
                throw std::invalid_argument("mapped key source requires value and delta schemas");
            }

            static std::mutex mutex;
            static auto      *contexts = new std::vector<std::unique_ptr<Context>>;

            if (!value_binding || value_binding.schema() != schema.value_schema)
            {
                throw std::logic_error("mapped key source could not resolve value bindings");
            }
            const auto delta_binding = value_binding;

            std::lock_guard lock{mutex};
            for (const auto &context : *contexts)
            {
                if (context != nullptr && context->schema == &schema &&
                    context->layout.value_binding == value_binding &&
                    context->layout.delta_binding == delta_binding)
                {
                    return *context;
                }
            }

            auto context = std::make_unique<Context>();
            context->schema = &schema;
            context->layout = TSDataLayout{
                .value_binding   = value_binding,
                .delta_binding   = delta_binding,
                .value_offset    = 0,
                .tracking_offset = 0,
            };
            context->ops = TSDataOps{
                .context                   = context.get(),
                .kind                      = TSTypeKind::TS,
                .allows_mutation           = false,
                .layout_impl               = [](const void *ctx) noexcept -> const TSDataLayout * {
                    return &static_cast<const Context *>(ctx)->layout;
                },
                .tracking_impl             = [](const void *, const void *memory) noexcept -> const TSDataTracking * {
                    return memory != nullptr ? &static_cast<const MappedKeySourceStorage *>(memory)->tracking
                                             : nullptr;
                },
                .mutable_tracking_impl     = [](const void *, void *memory) noexcept -> TSDataTracking * {
                    return memory != nullptr ? &static_cast<MappedKeySourceStorage *>(memory)->tracking : nullptr;
                },
                .has_current_value_impl    = [](const void *, const void *memory) noexcept -> bool {
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    return storage != nullptr && storage->key != nullptr && storage->key->has_value();
                },
                .all_valid_impl            = [](const void *, const void *memory) noexcept -> bool {
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    return storage != nullptr && storage->key != nullptr && storage->key->has_value();
                },
                .value_memory_impl         = [](const void *, const void *memory) noexcept -> const void * {
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    return storage != nullptr && storage->key != nullptr ? storage->key->view().data() : nullptr;
                },
                .mutable_value_memory_impl = &ts_data_detail::missing_mutable_value_memory,
                .delta_memory_impl         = [](const void *, const void *memory) noexcept -> const void * {
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    return storage != nullptr && storage->key != nullptr ? storage->key->view().data() : nullptr;
                },
                .mutable_delta_memory_impl = &ts_data_detail::missing_mutable_delta_memory,
                .copy_value_from_impl      = [](const void *, void *, const ValueView &, DateTime) -> bool {
                    throw std::logic_error("mapped key source is read-only");
                },
                .empty_delta_impl          = &ts_data_detail::empty_delta_atomic,
                .capture_delta_impl        = &ts_data_detail::capture_delta_ts,
                .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_atomic,
                .apply_delta_impl          = &ts_data_detail::missing_apply_delta,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .from_python_impl          = [](const void *, void *, nb::handle, DateTime) -> bool {
                    throw std::logic_error("mapped key source is read-only");
                },
                .to_python_impl            = [](const void *ctx, const void *memory) -> nb::object {
                    const auto *context = static_cast<const Context *>(ctx);
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    if (storage == nullptr || storage->key == nullptr || !storage->key->has_value())
                    {
                        return nb::none();
                    }
                    return context->layout.value_binding.ops_ref().to_python(storage->key->view().data());
                },
                .delta_to_python_impl      = [](const void *ctx, const void *memory, DateTime evaluation_time) -> nb::object {
                    const auto *context = static_cast<const Context *>(ctx);
                    const auto *storage = static_cast<const MappedKeySourceStorage *>(memory);
                    if (storage == nullptr || storage->key == nullptr || !storage->key->has_value() ||
                        storage->tracking.last_modified_time != evaluation_time)
                    {
                        return nb::none();
                    }
                    return context->layout.delta_binding.ops_ref().to_python(storage->key->view().data());
                },
#endif
            };

            const auto *result = context.get();
            contexts->push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] inline TSOutputTypeRef type_for(
            const TSValueTypeMetaData &schema,
            ValueTypeRef value_binding)
        {
            const Context &context = context_for(schema, value_binding);
            return checked_ts_role_type(
                intern_ts_type(schema, TypeRole::Output, MemoryUtils::plan_for<MappedKeySourceStorage>(), context.ops),
                std::integral_constant<TypeRole, TypeRole::Output>{});
        }
    }  // namespace mapped_key_source_detail

    class MappedKeySource
    {
      public:
        MappedKeySource() = default;
        ~MappedKeySource() noexcept
        {
            storage_.tracking.observers.invalidate(&storage_.tracking);
        }

        MappedKeySource(const MappedKeySource &)            = delete;
        MappedKeySource &operator=(const MappedKeySource &) = delete;
        MappedKeySource(MappedKeySource &&)                 = delete;
        MappedKeySource &operator=(MappedKeySource &&)      = delete;

        void bind(const TSValueTypeMetaData &schema, const Value &key, DateTime evaluation_time)
        {
            if (!key.has_value())
            {
                throw std::invalid_argument("mapped key source requires a live key value");
            }
            if (evaluation_time == MIN_DT)
            {
                throw std::invalid_argument("mapped key source requires a concrete evaluation time");
            }

            const TSOutputTypeRef type = mapped_key_source_detail::type_for(schema, key.binding());
            const auto           &ops = type.ops_ref();
            const auto           *layout = ops.layout_impl(ops.context);
            if (layout == nullptr || key.binding() != layout->value_binding)
            {
                throw std::invalid_argument("mapped key source key schema does not match TS<K>");
            }

            type_ = type;
            storage_.key = &key;
            storage_.tracking.last_modified_time = evaluation_time;
        }

        [[nodiscard]] bool bound() const noexcept
        {
            return type_ && storage_.key != nullptr && storage_.key->has_value();
        }

        [[nodiscard]] TSOutputView view(DateTime evaluation_time) const
        {
            if (!bound()) { return {}; }
            return TSOutputView{&owner_, TSDataView{type_.as_role(), &storage_}, evaluation_time};
        }

      private:
        TSOutput               owner_{};
        MappedKeySourceStorage storage_{};
        TSOutputTypeRef        type_{};
    };
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_MAPPED_KEY_SOURCE_H
