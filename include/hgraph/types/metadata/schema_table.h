#ifndef HGRAPH_TYPES_METADATA_SCHEMA_TABLE_H
#define HGRAPH_TYPES_METADATA_SCHEMA_TABLE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/binary_codec.h>

#include <ankerl/unordered_dense.h>

#include <cstddef>
#include <string>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;
    struct TSValueTypeMetaData;

    /**
     * A table of the schemas a byte stream refers to, so that each is written
     * once and named by index afterwards (RFC 0039).
     *
     * An entry is a schema's name, its manifest descriptor, and -- when the
     * name alone does not resolve it -- a structural recipe over earlier
     * entries: list, set, map, tuple and bundle values, ``Frame[Row, Meta]``
     * and ``Series[T]``; ``TS``, ``TSS``, ``TSD``, ``TSL``, ``TSB``, ``TSW``
     * and ``REF`` endpoints. A reader rebuilds each schema through the type
     * registry and refuses one whose descriptor no longer matches, so a
     * changed type is detected once per schema rather than once per value.
     *
     * Recipes are schema-kind knowledge, which is why the table lives in the
     * type layer and not with the runtime codecs that use it.
     *
     * Build-time machinery: it resolves schemas through the locked registry
     * and is never reachable from evaluation.
     */
    class HGRAPH_CLASS_EXPORT SchemaTableWriter
    {
      public:
        /** Index of ``schema``, adding it and everything it is built from. */
        std::size_t value_ref(const ValueTypeMetaData *schema);
        std::size_t ts_ref(const TSValueTypeMetaData *schema);

        [[nodiscard]] std::size_t value_count() const noexcept { return values_.size(); }
        [[nodiscard]] const ValueTypeMetaData *value_at(std::size_t index) const { return values_.at(index); }

        /** Append the value table, then the time-series table. */
        void write(std::string &out) const;

      private:
        std::size_t value_ref(const ValueTypeMetaData *schema, std::size_t depth);
        std::size_t ts_ref(const TSValueTypeMetaData *schema, std::size_t depth);

        // Children are added before their owner, so a recipe only ever names a
        // smaller index and a reader needs no forward pass.
        std::vector<const ValueTypeMetaData *> values_{};
        std::size_t ts_count_{0};
        std::string value_records_{};
        std::string ts_records_{};
        ankerl::unordered_dense::map<const ValueTypeMetaData *, std::size_t> value_index_{};
        ankerl::unordered_dense::map<const TSValueTypeMetaData *, std::size_t> ts_index_{};
    };

    class HGRAPH_CLASS_EXPORT SchemaTableReader
    {
      public:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        /** Read both tables, resolving and checking every entry. */
        void read(BinaryReader &reader);

        [[nodiscard]] std::size_t value_count() const noexcept { return values_.size(); }
        [[nodiscard]] std::size_t ts_count() const noexcept { return ts_.size(); }
        /** Throws when ``index`` names no entry. */
        [[nodiscard]] const ValueTypeMetaData *value_at(std::size_t index) const;
        [[nodiscard]] const TSValueTypeMetaData *ts_at(std::size_t index) const;
        /** Where the writer put ``schema``; ``npos`` when the stream never named it. */
        [[nodiscard]] std::size_t value_index_of(const ValueTypeMetaData *schema) const noexcept;

      private:
        std::vector<const ValueTypeMetaData *> values_{};
        std::vector<const TSValueTypeMetaData *> ts_{};
        ankerl::unordered_dense::map<const ValueTypeMetaData *, std::size_t> value_positions_{};
    };
}

#endif
