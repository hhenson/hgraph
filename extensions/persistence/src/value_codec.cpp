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
            std::string           extension{};
            std::shared_ptr<void> context{};
            ValueCodecOps         ops{};
        };

        struct Registry
        {
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
            return lhs.encode == rhs.encode && lhs.decode == rhs.decode;
        }

        /** The baseline codec. Delegates to the core's interned JsonConverter,
            which is synthesized once per schema; this adds no format of its
            own. */
        void json_encode(void * /*context*/, const ValueView &value, ObjectBytes &out)
        {
            const std::string text = to_json_string(value);
            const auto        bytes = std::as_bytes(std::span{text.data(), text.size()});
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        Value json_decode(void * /*context*/, const ValueTypeMetaData *schema,
                          std::span<const std::byte> encoded)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("value codec decode requires a schema");
            }
            const std::string_view text{reinterpret_cast<const char *>(encoded.data()),
                                        encoded.size()};
            return from_json_string(schema, text);
        }
    }  // namespace

    ValueCodec::ValueCodec(std::string name, std::string extension,
                           std::shared_ptr<void> context, ValueCodecOps ops) noexcept
        : name_{std::move(name)}, extension_{std::move(extension)},
          context_{std::move(context)}, ops_{ops}
    {
    }

    void ValueCodec::encode(const ValueView &value, ObjectBytes &out) const
    {
        if (!valid())
        {
            throw std::logic_error("value codec is not bound to an implementation");
        }
        ops_.encode(context_.get(), value, out);
    }

    Value ValueCodec::decode(const ValueTypeMetaData *schema,
                             std::span<const std::byte> encoded) const
    {
        if (!valid())
        {
            throw std::logic_error("value codec is not bound to an implementation");
        }
        return ops_.decode(context_.get(), schema, encoded);
    }

    void register_value_codec(std::string_view name, std::string_view extension,
                              std::shared_ptr<void> context, const ValueCodecOps &ops)
    {
        if (name.empty())
        {
            throw std::invalid_argument("value codec name must not be empty");
        }
        if (extension.empty() || extension.front() == '.' ||
            extension.find('/') != std::string_view::npos)
        {
            throw std::invalid_argument(
                "value codec extension must be a bare suffix such as \"json\"");
        }
        if (ops.encode == nullptr || ops.decode == nullptr)
        {
            throw std::invalid_argument("value codec requires both encode and decode");
        }
        auto           &state = registry();
        std::lock_guard lock{state.mutex};
        const auto      found = state.codecs.find(std::string{name});
        if (found != state.codecs.end())
        {
            // Re-registering the same implementation is how an idempotent
            // install behaves; two different implementations under one name is
            // a build error, not a last-writer-wins race.
            if (!same_ops(found->second.ops, ops) || found->second.extension != extension)
            {
                throw std::invalid_argument("value codec '" + std::string{name} +
                                            "' is already registered with a different "
                                            "implementation");
            }
            return;
        }
        for (const auto &[existing_name, registration] : state.codecs)
        {
            // Two codecs claiming one extension would make a stored object
            // ambiguous, which is the one thing the key-based scheme cannot
            // tolerate.
            if (registration.extension == extension)
            {
                throw std::invalid_argument("value codec extension '" + std::string{extension} +
                                            "' is already claimed by '" + existing_name + "'");
            }
        }
        state.codecs.emplace(std::string{name},
                             Registration{std::string{extension}, std::move(context), ops});
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
        return ValueCodec{std::string{name}, found->second.extension, found->second.context,
                          found->second.ops};
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

    std::optional<std::string> value_codec_for_extension(std::string_view extension)
    {
        auto           &state = registry();
        std::lock_guard lock{state.mutex};
        for (const auto &[name, registration] : state.codecs)
        {
            if (registration.extension == extension)
            {
                return name;
            }
        }
        return std::nullopt;
    }

    void register_builtin_value_codecs()
    {
        register_value_codec(JSON_VALUE_CODEC, "json", nullptr,
                             ValueCodecOps{.encode = &json_encode, .decode = &json_decode});
    }

}  // namespace hgraph::persistence::store
