#ifndef HGRAPH_FABRIC_KEYS_H
#define HGRAPH_FABRIC_KEYS_H

#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace hgraph::fabric
{
    inline constexpr std::size_t FABRIC_ORDINAL_WIDTH{19U};

    /** Canonical, reversible store-key segment for one validated UTF-8 data id. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    encode_data_id_segment(std::string_view data_id);

    /** Decode a canonical data-id segment. Uppercase or otherwise alternate
        spellings fail rather than aliasing the same durable identity. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Str
    decode_data_id_segment(std::string_view encoded);

    /** Fixed-width positive signed-64 ordinal whose lexical and numeric order agree. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    encode_fabric_ordinal(Int value);

    [[nodiscard]] HGRAPH_FABRIC_EXPORT Int
    decode_fabric_ordinal(std::string_view encoded);

    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    data_id_key_prefix(std::string_view fabric_prefix, std::string_view data_id);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    data_version_key(std::string_view fabric_prefix, std::string_view data_id,
                     DataVersion version);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    revision_key_prefix(std::string_view fabric_prefix, std::string_view data_id);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    revision_key(std::string_view fabric_prefix, std::string_view data_id,
                 RevisionId revision);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    as_of_key_prefix(std::string_view fabric_prefix, std::string_view data_id);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    as_of_key(std::string_view fabric_prefix, std::string_view data_id,
              DateTime as_of);
    [[nodiscard]] HGRAPH_FABRIC_EXPORT std::string
    latest_key(std::string_view fabric_prefix, std::string_view data_id);
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_KEYS_H
