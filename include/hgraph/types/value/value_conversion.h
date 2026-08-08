#ifndef HGRAPH_TYPES_VALUE_VALUE_CONVERSION_H
#define HGRAPH_TYPES_VALUE_VALUE_CONVERSION_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>

#include <unordered_map>

namespace hgraph
{
    /** Runtime scalar conversion keyed by the concrete source and target
        schemas retained by a type-erased ``Value``. Registration/reset are
        wiring/test operations; evaluation performs lock-free lookups against
        the immutable table, like ``OperatorRegistry``. */
    class HGRAPH_EXPORT ValueConversionRegistry
    {
      public:
        using Converter = Value (*)(const ValueView &source);

        static ValueConversionRegistry &instance();

        void register_converter(const ValueTypeMetaData *source,
                                const ValueTypeMetaData *target,
                                Converter converter);

        [[nodiscard]] Value convert(const ValueView &source,
                                    const ValueTypeMetaData *target) const;

        void reset() noexcept;

      private:
        struct Key
        {
            const ValueTypeMetaData *source{nullptr};
            const ValueTypeMetaData *target{nullptr};

            friend bool operator==(const Key &, const Key &) noexcept = default;
        };

        struct KeyHash
        {
            [[nodiscard]] std::size_t operator()(const Key &key) const noexcept;
        };

        std::unordered_map<Key, Converter, KeyHash> converters_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_VALUE_CONVERSION_H
