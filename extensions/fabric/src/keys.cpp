#include <hgraph/fabric/keys.h>

#include <hgraph/persistence/store_location.h>

#include <charconv>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace hgraph::fabric
{
    namespace
    {
        void require_valid_fabric_key(std::string_view key)
        {
            persistence::store::require_valid_key(key);
            if (key.size() > MAX_FABRIC_KEY_BYTES)
            {
                throw std::invalid_argument(
                    "fabric durable key exceeds the portable 1024-byte limit");
            }
        }

        [[nodiscard]] std::string checked_root(std::string_view fabric_prefix,
                                               std::string_view data_id)
        {
            persistence::store::require_valid_key(fabric_prefix);
            std::string key =
                std::string{fabric_prefix} + "/" + encode_data_id_segment(data_id);
            require_valid_fabric_key(key);
            return key;
        }

        [[nodiscard]] std::string ordinal_key(std::string_view fabric_prefix,
                                              std::string_view data_id,
                                              std::string_view category, Int ordinal)
        {
            std::string key = checked_root(fabric_prefix, data_id);
            key += '/';
            key += category;
            key += '/';
            key += encode_fabric_ordinal(ordinal);
            require_valid_fabric_key(key);
            return key;
        }

        [[nodiscard]] std::string category_prefix(
            std::string_view fabric_prefix, std::string_view data_id,
            std::string_view category)
        {
            std::string key = checked_root(fabric_prefix, data_id);
            key += '/';
            key += category;
            require_valid_fabric_key(key);
            return key;
        }
    }  // namespace

    std::string encode_data_id_segment(std::string_view data_id)
    {
        require_data_id(data_id);
        // 'b' marks the segment as encoded. The alphabet belongs to the store;
        // the marker and the path shape around it belong to fabric.
        return "b" + persistence::store::encode_key_segment(data_id);
    }

    Str decode_data_id_segment(std::string_view encoded)
    {
        if (encoded.size() < 3U || encoded.front() != 'b')
        {
            throw std::invalid_argument("invalid fabric encoded data-id segment");
        }
        Str decoded = persistence::store::decode_key_segment(encoded.substr(1U));
        require_data_id(decoded);
        // Canonicality: one data id has exactly one encoding, so a key cannot
        // be spelled two ways and resolve to the same object.
        if (encode_data_id_segment(decoded) != encoded)
        {
            throw std::invalid_argument("non-canonical fabric encoded data-id segment");
        }
        return decoded;
    }

    std::string encode_fabric_ordinal(Int value)
    {
        // Fixed width so keys sort lexicographically in revision order, which
        // is what lets a prefix listing return a range in sequence.
        return persistence::store::encode_ordered_ordinal(value, FABRIC_ORDINAL_WIDTH);
    }

    Int decode_fabric_ordinal(std::string_view encoded)
    {
        return persistence::store::decode_ordered_ordinal(encoded, FABRIC_ORDINAL_WIDTH);
    }

    std::string data_id_key_prefix(std::string_view fabric_prefix,
                                   std::string_view data_id)
    {
        return checked_root(fabric_prefix, data_id);
    }

    std::string data_version_key(std::string_view fabric_prefix,
                                 std::string_view data_id, DataVersion version)
    {
        return ordinal_key(fabric_prefix, data_id, "data", version);
    }

    std::string revision_key_prefix(std::string_view fabric_prefix,
                                    std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "revision");
    }

    std::string revision_key(std::string_view fabric_prefix,
                             std::string_view data_id, RevisionId revision)
    {
        return ordinal_key(fabric_prefix, data_id, "revision", revision);
    }

    std::string as_of_key_prefix(std::string_view fabric_prefix,
                                 std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "as_of");
    }

    std::string as_of_key(std::string_view fabric_prefix, std::string_view data_id,
                          DateTime as_of)
    {
        return ordinal_key(fabric_prefix, data_id, "as_of",
                           as_of.time_since_epoch().count());
    }

    std::string latest_key(std::string_view fabric_prefix,
                           std::string_view data_id)
    {
        return category_prefix(fabric_prefix, data_id, "latest");
    }
}  // namespace hgraph::fabric
