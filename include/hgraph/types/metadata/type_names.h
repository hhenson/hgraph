#ifndef HGRAPH_TYPES_METADATA_TYPE_NAMES_H
#define HGRAPH_TYPES_METADATA_TYPE_NAMES_H

/**
 * @file type_names.h
 * A type serialises as its name (RFC 0042).
 *
 * The registry names every schema it interns (``int``, ``VariadicTuple[str]``,
 * ``TSD[str,TS[int]]``, ``hgraph::TableSchema``). The parsers below read those
 * names back: a registered name (a scalar, a named bundle or enum, a named
 * TSB, an alias) resolves by lookup, and a composite (``Family[...]``,
 * ``Bundle{...}``, ``TSB{...}``) is built through the registry constructor
 * that printed it, so the result is the interned schema itself. A named type
 * must be registered in the decoding process first, as the binary codec
 * already requires of a named bundle.
 *
 * A type value's serialised form names its kind too, because a named TSB and
 * its value-side bundle share one name: ``scalar:int``, ``ts:TS[int]``,
 * ``size:3``. Its text (``to_string``) stays the bare name.
 *
 * Parsing is a cold path: ``parse_type_value`` caches by serialised form, and
 * the cache drops itself when the registry is reset.
 */

#include <hgraph/hgraph_export.h>
#include <hgraph/types/type_carrier.h>

#include <string>
#include <string_view>

namespace hgraph
{
    /** The value-layer schema a registry-printed name denotes; throws
        ``std::invalid_argument`` when the name does not resolve. */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *parse_value_type_name(std::string_view name);

    /** The time-series schema a registry-printed name denotes; throws
        ``std::invalid_argument`` when the name does not resolve. */
    [[nodiscard]] HGRAPH_EXPORT const TSValueTypeMetaData *parse_ts_type_name(std::string_view name);

    /** A type value's serialised form: its kind and its name. */
    [[nodiscard]] HGRAPH_EXPORT std::string serialise_type_value(const TypeCarrier &type);

    /** The type value a serialised form names (cached by form). */
    [[nodiscard]] HGRAPH_EXPORT TypeCarrier parse_type_value(std::string_view serialised);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_METADATA_TYPE_NAMES_H
