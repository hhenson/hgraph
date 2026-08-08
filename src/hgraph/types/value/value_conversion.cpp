#include <hgraph/types/value/value_conversion.h>

#include <hgraph/types/metadata/value_plan_factory.h>

#include <fmt/format.h>

#include <stdexcept>

namespace hgraph
{
    ValueConversionRegistry &ValueConversionRegistry::instance()
    {
        static ValueConversionRegistry registry;
        return registry;
    }

    std::size_t ValueConversionRegistry::KeyHash::operator()(const Key &key) const noexcept
    {
        const auto source = std::hash<const void *>{}(key.source);
        const auto target = std::hash<const void *>{}(key.target);
        return source ^ (target + 0x9e3779b97f4a7c15ULL + (source << 6U) + (source >> 2U));
    }

    void ValueConversionRegistry::register_converter(const ValueTypeMetaData *source,
                                                      const ValueTypeMetaData *target,
                                                      Converter converter)
    {
        if (source == nullptr || target == nullptr || converter == nullptr)
        {
            throw std::invalid_argument("value conversion registration requires source, target, and converter");
        }
        const Key key{source, target};
        const auto [it, inserted] = converters_.emplace(key, converter);
        if (!inserted && it->second != converter)
        {
            throw std::logic_error(fmt::format(
                "value conversion {} -> {} is already registered with a different implementation",
                source->name(), target->name()));
        }
    }

    Value ValueConversionRegistry::convert(const ValueView &source,
                                            const ValueTypeMetaData *target) const
    {
        if (!source.valid()) { throw std::invalid_argument("cannot convert an invalid runtime value"); }
        if (target == nullptr) { throw std::invalid_argument("runtime conversion target is unresolved"); }
        if (source.schema() == target) { return Value{source}; }

        Converter converter = nullptr;
        const auto it = converters_.find(Key{source.schema(), target});
        if (it != converters_.end()) { converter = it->second; }
        if (converter == nullptr)
        {
            throw std::invalid_argument(fmt::format(
                "no runtime value conversion is registered from {} to {}",
                source.schema()->name(), target->name()));
        }
        Value result = converter(source);
        if (!result.has_value() || result.schema() != target)
        {
            throw std::logic_error("runtime value converter returned the wrong schema");
        }
        return result;
    }

    void ValueConversionRegistry::reset() noexcept
    {
        converters_.clear();
    }
}  // namespace hgraph
