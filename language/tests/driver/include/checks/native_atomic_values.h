#ifndef HGL_TEST_NATIVE_ATOMIC_VALUES_H
#define HGL_TEST_NATIVE_ATOMIC_VALUES_H

#include <compare>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/binary_codec.h>
#include <ostream>
#include <string>

namespace checks::native_atomic_values
{
    template <int Tag> struct TextValue
    {
        std::string          text{};
        auto                 operator<=>(const TextValue &) const = default;
        friend std::ostream &operator<<(std::ostream &out, const TextValue &value) { return out << value.text; }
    };
    using Token      = TextValue<0>;
    using TokenOther = TextValue<1>;
    // Physical operations are deliberately a superset. The HGL descriptor
    // exposes only owning copy, text and serialization for this identity.
    using TextOnly = TextValue<2>;

    inline Token       token(const hgraph::Str &text) { return {text}; }
    inline hgraph::Str token_text(const Token &value) { return value.text; }
    inline TokenOther  token_other(const hgraph::Str &text) { return {text}; }
    inline hgraph::Str token_other_text(const TokenOther &value) { return value.text; }
    inline TextOnly    text_only(const hgraph::Str &text) { return {text}; }
    inline hgraph::Str text_only_text(const TextOnly &value) { return value.text; }
}  // namespace checks::native_atomic_values

template <int Tag> struct std::hash<checks::native_atomic_values::TextValue<Tag>>
{
    std::size_t operator()(const checks::native_atomic_values::TextValue<Tag> &value) const noexcept {
        return std::hash<std::string>{}(value.text);
    }
};

namespace hgraph
{
    template <int Tag> struct scalar_descriptor<checks::native_atomic_values::TextValue<Tag>>
    {
        static constexpr bool           is_concrete() noexcept { return true; }
        static const ValueTypeMetaData *value_meta() {
            using T                 = checks::native_atomic_values::TextValue<Tag>;
            constexpr auto identity = Tag == 0 ? "hgraph.std::Token" : Tag == 1 ? "hgraph.std::TokenOther" : "hgraph.std::TextOnly";
            if (const auto existing = TypeRegistry::instance().scalar_type<T>()) { return existing.schema(); }
            const auto *schema = TypeRegistry::instance().register_scalar<T>(identity);
            register_binary_atom(schema, BinaryAtomOps{
                                             .write =
                                                 [](const void *value, const void *, std::string &out) {
                                                     const auto &text = static_cast<const T *>(value)->text;
                                                     write_varint(text.size(), out);
                                                     out.append(text);
                                                 },
                                             .read =
                                                 [](void *value, const void *, BinaryReader &reader) {
                                                     const auto size = static_cast<std::size_t>(read_varint(reader));
                                                     static_cast<T *>(value)->text.assign(
                                                         reinterpret_cast<const char *>(reader.take(size)), size);
                                                 },
                                         });
            return schema;
        }
    };
}  // namespace hgraph
#endif
