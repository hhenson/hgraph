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

        const void *json_bind(void *context, const ValueTypeMetaData *schema);
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

        /** The baseline codec. Binding resolves the core's interned
            JsonConverter, which is synthesized once per schema and locks to do
            it -- so it happens here, in bind, and never again. to_json_string
            and from_json_string(meta, ...) would repeat that lookup per value,
            which is why neither is used below. */
        const void *json_bind(void * /*context*/, const ValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("value codec bind requires a schema");
            }
            return &json_converter(schema);
        }

        void json_encode(void * /*context*/, const void *bound, const ValueView &value,
                         ObjectBytes &out)
        {
            std::string text;
            static_cast<const JsonConverter *>(bound)->write(value, text);
            const auto bytes = std::as_bytes(std::span{text.data(), text.size()});
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        Value json_decode(void * /*context*/, const void *bound,
                          std::span<const std::byte> encoded)
        {
            const std::string_view text{reinterpret_cast<const char *>(encoded.data()),
                                        encoded.size()};
            return from_json_string(*static_cast<const JsonConverter *>(bound), text);
        }
    }  // namespace

    ValueCodec::ValueCodec(std::string name, std::shared_ptr<void> context,
                           ValueCodecOps ops) noexcept
        : name_{std::move(name)}, context_{std::move(context)}, ops_{ops}
    {
    }

    void BoundValueCodec::encode(const ValueView &value, ObjectBytes &out) const
    {
        if (!*this)
        {
            throw std::logic_error("value codec is not bound to a schema");
        }
        ops_.encode(context_, bound_, value, out);
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
        return ops_.decode(context_, bound_, encoded);
    }

    BoundValueCodec ValueCodec::bind(const ValueTypeMetaData *schema) const
    {
        if (!valid())
        {
            throw std::logic_error("value codec is not bound to an implementation");
        }
        return BoundValueCodec{ops_, context_.get(), ops_.bind(context_.get(), schema)};
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
