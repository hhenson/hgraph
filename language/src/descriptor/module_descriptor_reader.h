#ifndef HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H
#define HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H

#include "descriptor/module_descriptor.h"

#include <optional>
#include <string>
#include <string_view>

namespace hgl::descriptor
{
    struct ReadError
    {
        std::string path{};
        std::string message{};

        friend bool operator==(const ReadError &, const ReadError &) = default;
    };

    struct ReadResult
    {
        std::optional<ModuleDescriptor> value{};
        std::optional<ReadError>        error{};

        [[nodiscard]] explicit operator bool() const noexcept { return value.has_value(); }
    };

    /// Parse and validate one versioned HGL module descriptor without loading
    /// native code or consulting a registry. Unknown object members are
    /// ignored within a supported format version; duplicate members, invalid
    /// enums and dangling schema references are rejected.
    [[nodiscard]] ReadResult read_json(std::string_view json);

    /// Validate a descriptor assembled by another producer. This applies the
    /// same semantic integrity rules as read_json after JSON decoding.
    [[nodiscard]] std::optional<ReadError> validate(const ModuleDescriptor &descriptor);

    /// A single HGL identifier, and a dot-separated run of them. An identity is
    /// not just a label: generated C++ derives a namespace from it and spells
    /// it into the source, so a descriptor's identities are validated before
    /// anything is built from them.
    [[nodiscard]] bool is_identifier(std::string_view text) noexcept;
    [[nodiscard]] bool is_qualified_identifier(std::string_view text) noexcept;
}  // namespace hgl::descriptor

#endif  // HGL_DESCRIPTOR_MODULE_DESCRIPTOR_READER_H
