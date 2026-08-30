#include <hgraph/persistence/value_codec.h>

#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/json_codec.h>

#include <algorithm>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace hgraph::persistence::store
{
    namespace
    {
        struct Registration
        {
            std::shared_ptr<void> context{};
            ValueCodecOps         ops{};
        };

        ValueCodecBinding json_bind(void *context,
                                    const ValueTypeMetaData *schema);
        void json_encode(void *context, const void *bound, const ValueView &value,
                         ObjectBytes &out);
        Value json_decode(void *context, const void *bound,
                          std::span<const std::byte> encoded);

        struct Registry
        {
            /** The baseline codec is seeded here rather than by an installer
                call, because a build without json is not a conforming
                persistence build (RFC 0030) and ValueStore's advertised default
                names it. Seeding at construction means a standalone consumer
                that never runs an extension's registration still gets a working
                default store; going through register_value_codec() instead
                would deadlock on the mutex this constructor is building. */
            Registry()
            {
                codecs.emplace(std::string{JSON_VALUE_CODEC},
                               Registration{nullptr, ValueCodecOps{.bind = &json_bind,
                                                                   .encode = &json_encode,
                                                                   .decode = &json_decode}});
            }

            TypeSystemMutex                              mutex{};
            std::unordered_map<std::string, Registration> codecs{};
        };

        Registry &registry()
        {
            static Registry instance{};
            return instance;
        }

        [[nodiscard]] bool same_ops(const ValueCodecOps &lhs, const ValueCodecOps &rhs) noexcept
        {
            return lhs.bind == rhs.bind && lhs.encode == rhs.encode &&
                   lhs.decode == rhs.decode;
        }

        /** The baseline codec. Binding owns a complete converter plan for the
            active run, including realised value bindings and polymorphic
            alternatives. Synthesis may lock, so it happens here and never in
            encode/decode. The ad-hoc JSON helpers are not used below because
            they resolve schema state per call. */
        ValueCodecBinding json_bind(void * /*context*/,
                                    const ValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("value codec bind requires a schema");
            }
            auto binding = std::make_shared<BoundJsonConverter>(
                bind_json_converter(schema));
            return ValueCodecBinding{binding, binding.get()};
        }

        void json_encode(void * /*context*/, const void *bound, const ValueView &value,
                         ObjectBytes &out)
        {
            std::string text;
            static_cast<const BoundJsonConverter *>(bound)->write(value, text);
            const auto bytes = std::as_bytes(std::span{text.data(), text.size()});
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        Value json_decode(void * /*context*/, const void *bound,
                          std::span<const std::byte> encoded)
        {
            const std::string_view text{reinterpret_cast<const char *>(encoded.data()),
                                        encoded.size()};
            return from_json_string(
                *static_cast<const BoundJsonConverter *>(bound), text);
        }

        ValueCodecBinding empty_bind(void *,
                                     const ValueTypeMetaData *) noexcept
        {
            return {};
        }

        void empty_encode(void *, const void *, const ValueView &, ObjectBytes &)
        {
            throw std::logic_error("value codec is not bound to a schema");
        }

        Value empty_decode(void *, const void *, std::span<const std::byte>)
        {
            throw std::logic_error("value codec is not bound to a schema");
        }
    }  // namespace

    const ValueCodecOps &BoundValueCodec::empty_ops() noexcept
    {
        static const ValueCodecOps ops{&empty_bind, &empty_encode, &empty_decode};
        return ops;
    }

    BoundValueCodec::BoundValueCodec() noexcept : ops_{empty_ops()} {}

    BoundValueCodec::BoundValueCodec(ValueCodecOps ops,
                                     std::shared_ptr<void> context,
                                     const ValueTypeMetaData *schema,
                                     ValueCodecBinding binding)
        : ops_{ops}, context_{std::move(context)},
          binding_owner_{std::move(binding.owner)}, schema_{schema},
          bound_{binding.handle}
    {
        if (schema_ == nullptr || bound_ == nullptr || ops_.bind == nullptr ||
            ops_.encode == nullptr || ops_.decode == nullptr)
        {
            throw std::invalid_argument(
                "bound value codec requires a schema, handle and complete operations");
        }
    }

    BoundValueCodec::BoundValueCodec(BoundValueCodec &&other) noexcept
        : ops_{other.ops_}, context_{std::move(other.context_)},
          binding_owner_{std::move(other.binding_owner_)},
          schema_{other.schema_}, bound_{other.bound_}
    {
        other.ops_ = empty_ops();
        other.schema_ = nullptr;
        other.bound_ = nullptr;
    }

    BoundValueCodec &BoundValueCodec::operator=(BoundValueCodec &&other) noexcept
    {
        if (this != &other)
        {
            ops_ = other.ops_;
            context_ = std::move(other.context_);
            binding_owner_ = std::move(other.binding_owner_);
            schema_ = other.schema_;
            bound_ = other.bound_;
            other.ops_ = empty_ops();
            other.schema_ = nullptr;
            other.bound_ = nullptr;
        }
        return *this;
    }

    const ValueCodecOps &ValueCodec::empty_ops() noexcept
    {
        static const ValueCodecOps ops{&empty_bind, &empty_encode, &empty_decode};
        return ops;
    }

    ValueCodec::ValueCodec() noexcept : ops_{empty_ops()} {}

    ValueCodec::ValueCodec(std::string name, std::shared_ptr<void> context,
                           ValueCodecOps ops) noexcept
        : name_{std::move(name)}, context_{std::move(context)}, ops_{ops}
    {
    }

    ValueCodec::ValueCodec(ValueCodec &&other) noexcept
        : name_{std::move(other.name_)}, context_{std::move(other.context_)},
          ops_{other.ops_}
    {
        other.name_.clear();
        other.ops_ = empty_ops();
    }

    ValueCodec &ValueCodec::operator=(ValueCodec &&other) noexcept
    {
        if (this != &other)
        {
            name_ = std::move(other.name_);
            context_ = std::move(other.context_);
            ops_ = other.ops_;
            other.name_.clear();
            other.ops_ = empty_ops();
        }
        return *this;
    }

    void BoundValueCodec::encode(const ValueView &value, ObjectBytes &out) const
    {
        if (!*this)
        {
            throw std::logic_error("value codec is not bound to a schema");
        }
        if (!value.valid() || value.schema() != schema_)
        {
            throw std::invalid_argument(
                "value codec received a value with a different schema");
        }
        ops_.encode(context_.get(), bound_, value, out);
    }

    ObjectBytes BoundValueCodec::encode(const ValueView &value) const
    {
        ObjectBytes out;
        encode(value, out);
        return out;
    }

    Value BoundValueCodec::decode(std::span<const std::byte> encoded) const
    {
        if (!*this)
        {
            throw std::logic_error("value codec is not bound to a schema");
        }
        Value decoded = ops_.decode(context_.get(), bound_, encoded);
        if (decoded.schema() != schema_)
        {
            throw std::invalid_argument(
                "value codec returned a value with a different schema");
        }
        return decoded;
    }

    BoundValueCodec ValueCodec::bind(const ValueTypeMetaData *schema) const
    {
        if (!valid())
        {
            throw std::logic_error("value codec is not bound to an implementation");
        }
        if (schema == nullptr)
        {
            throw std::invalid_argument("value codec bind requires a schema");
        }
        return BoundValueCodec{ops_, context_, schema,
                               ops_.bind(context_.get(), schema)};
    }

    void ValueCodec::encode(const ValueView &value, ObjectBytes &out) const
    {
        bind(value.schema()).encode(value, out);
    }

    Value ValueCodec::decode(const ValueTypeMetaData *schema,
                             std::span<const std::byte> encoded) const
    {
        return bind(schema).decode(encoded);
    }

    void register_value_codec(std::string_view name, std::shared_ptr<void> context,
                              const ValueCodecOps &ops)
    {
        if (name.empty())
        {
            throw std::invalid_argument("value codec name must not be empty");
        }
        if (ops.bind == nullptr || ops.encode == nullptr || ops.decode == nullptr)
        {
            throw std::invalid_argument(
                "value codec requires bind, encode and decode");
        }
        auto           &state = registry();
        std::lock_guard lock{state.mutex};
        const auto      found = state.codecs.find(std::string{name});
        if (found != state.codecs.end())
        {
            // Re-registering the same implementation is how an idempotent
            // install behaves; two different implementations under one name is
            // a build error, not a last-writer-wins race.
            if (!same_ops(found->second.ops, ops))
            {
                throw std::invalid_argument("value codec '" + std::string{name} +
                                            "' is already registered with a different "
                                            "implementation");
            }
            return;
        }
        state.codecs.emplace(std::string{name}, Registration{std::move(context), ops});
    }

    ValueCodec value_codec(std::string_view name)
    {
        auto           &state = registry();
        std::lock_guard lock{state.mutex};
        const auto      found = state.codecs.find(std::string{name});
        if (found == state.codecs.end())
        {
            throw std::invalid_argument("unknown value codec '" + std::string{name} + "'");
        }
        return ValueCodec{std::string{name}, found->second.context, found->second.ops};
    }

    bool value_codec_registered(std::string_view name) noexcept
    {
        auto           &state = registry();
        std::lock_guard lock{state.mutex};
        return state.codecs.contains(std::string{name});
    }

    std::vector<std::string> value_codec_names()
    {
        auto                    &state = registry();
        std::lock_guard          lock{state.mutex};
        std::vector<std::string> names;
        names.reserve(state.codecs.size());
        for (const auto &[name, registration] : state.codecs)
        {
            static_cast<void>(registration);
            names.push_back(name);
        }
        std::ranges::sort(names);
        return names;
    }

    void register_builtin_value_codecs()
    {
        register_value_codec(JSON_VALUE_CODEC, nullptr,
                             ValueCodecOps{.bind = &json_bind,
                                           .encode = &json_encode,
                                           .decode = &json_decode});
    }

}  // namespace hgraph::persistence::store
