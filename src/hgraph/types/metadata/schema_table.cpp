#include <hgraph/types/metadata/schema_table.h>

#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>

#include <bit>
#include <limits>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t max_depth{256};

        enum class ValueRecipe : std::uint8_t { Named = 0, Structural = 1, Frame = 2, Series = 3, OpaquePython = 4 };

        [[noreturn]] void malformed(std::string_view detail)
        {
            throw std::runtime_error("invalid schema table: " + std::string{detail});
        }

        void check_depth(std::size_t depth)
        {
            if (depth > max_depth) { malformed("nesting exceeds supported depth"); }
        }

        [[nodiscard]] std::string_view as_text(const std::vector<std::byte> &bytes) noexcept
        {
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        void write_text(std::string_view text, std::string &out)
        {
            write_varint(text.size(), out);
            out.append(text);
        }

        [[nodiscard]] std::string_view read_text(BinaryReader &reader)
        {
            const auto length = read_varint(reader);
            if (length > reader.remaining()) { malformed("truncated text"); }
            const auto *bytes = reader.take(static_cast<std::size_t>(length));
            return {reinterpret_cast<const char *>(bytes), static_cast<std::size_t>(length)};
        }

        [[nodiscard]] bool read_boolean(BinaryReader &reader)
        {
            const auto value = read_varint(reader);
            if (value > 1) { malformed("invalid boolean"); }
            return value != 0;
        }

        [[nodiscard]] std::size_t read_size(BinaryReader &reader)
        {
            const auto value = read_varint(reader);
            if (value > std::numeric_limits<std::size_t>::max()) { malformed("size out of range"); }
            return static_cast<std::size_t>(value);
        }

        // Every entry occupies at least one byte, so a count larger than the
        // remaining input is refused before it can size an allocation.
        [[nodiscard]] std::size_t read_count(BinaryReader &reader)
        {
            const auto value = read_varint(reader);
            if (value > reader.remaining()) { malformed("invalid sequence size"); }
            reader.consume_work(value);
            return static_cast<std::size_t>(value);
        }

        /** Which value entries stand in for the writer's, and whether the
            entry being read has just named one of them. */
        struct Substitutes
        {
            const ankerl::unordered_dense::set<std::size_t> *entries{nullptr};
            bool                                             named{false};
        };

        template <typename T>
        [[nodiscard]] T earlier(BinaryReader &reader, const std::vector<T> &table, const char *what,
                                Substitutes *substitutes = nullptr)
        {
            const auto index = read_varint(reader);
            if (index >= table.size()) { malformed(std::string{"unknown "} + what + " index"); }
            if (substitutes != nullptr && substitutes->entries != nullptr &&
                substitutes->entries->contains(static_cast<std::size_t>(index)))
            {
                substitutes->named = true;
            }
            return table[static_cast<std::size_t>(index)];
        }

        [[nodiscard]] const ValueTypeMetaData *read_structural_value(
            BinaryReader &reader, TypeRegistry &registry, const std::vector<const ValueTypeMetaData *> &values,
            Substitutes &substitutes)
        {
            const auto value = [&] { return earlier(reader, values, "value schema", &substitutes); };
            const auto kind = read_varint(reader);
            if (kind > static_cast<std::uint64_t>(ValueTypeKind::Any)) { malformed("invalid value kind"); }
            const auto raw_flags = read_varint(reader);
            if (raw_flags > std::numeric_limits<std::uint32_t>::max()) { malformed("invalid value flags"); }
            const auto flags = static_cast<ValueTypeFlags>(raw_flags);
            const auto extent = read_size(reader);
            const auto flag = [flags](ValueTypeFlags wanted) { return (flags & wanted) != ValueTypeFlags::None; };
            switch (static_cast<ValueTypeKind>(kind))
            {
                case ValueTypeKind::List: {
                    const auto *element = value();
                    if (flag(ValueTypeFlags::ShapedArray)) { return registry.array(element, extent); }
                    if (flag(ValueTypeFlags::Nullable)) { return registry.nullable_tuple(element); }
                    if (flag(ValueTypeFlags::Mutable)) { return registry.mutable_list(element); }
                    if (flag(ValueTypeFlags::FixedEmpty)) { return registry.fixed_list(element, extent); }
                    return registry.list(element, extent, flag(ValueTypeFlags::VariadicTuple));
                }
                case ValueTypeKind::Set: {
                    const auto *element = value();
                    return flag(ValueTypeFlags::Mutable) ? registry.mutable_set(element) : registry.set(element);
                }
                case ValueTypeKind::Map: {
                    const auto *key = value();
                    const auto *element = value();
                    return flag(ValueTypeFlags::Mutable) ? registry.mutable_map(key, element)
                                                         : registry.map(key, element);
                }
                case ValueTypeKind::CyclicBuffer: {
                    const auto *element = value();
                    return registry.cyclic_buffer(element, extent);
                }
                case ValueTypeKind::Queue: {
                    const auto *element = value();
                    return registry.queue(element, extent);
                }
                case ValueTypeKind::Any: {
                    // Only the unconstrained box is structural; a named Any
                    // resolves by name like any other registered schema.
                    return registry.any();
                }
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: {
                    const auto fields_count = read_count(reader);
                    std::vector<const ValueTypeMetaData *> elements;
                    std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                    elements.reserve(fields_count);
                    fields.reserve(fields_count);
                    for (std::size_t index = 0; index < fields_count; ++index)
                    {
                        std::string field{read_text(reader)};
                        const auto *element = value();
                        elements.push_back(element);
                        fields.emplace_back(std::move(field), element);
                    }
                    return kind == static_cast<std::uint64_t>(ValueTypeKind::Tuple) ? registry.tuple(elements)
                                                                                    : registry.un_named_bundle(fields);
                }
                default: malformed("unsupported value schema recipe");
            }
        }
    }

    std::size_t SchemaTableWriter::value_ref(const ValueTypeMetaData *schema) { return value_ref(schema, 0); }
    std::size_t SchemaTableWriter::ts_ref(const TSValueTypeMetaData *schema) { return ts_ref(schema, 0); }

    std::size_t SchemaTableWriter::value_ref(const ValueTypeMetaData *schema, std::size_t depth)
    {
        check_depth(depth);
        if (schema == nullptr) { malformed("missing value schema"); }
        if (const auto found = value_index_.find(schema); found != value_index_.end()) { return found->second; }
        auto &registry = TypeRegistry::instance();
        std::string record;
        write_text(schema->name(), record);
        write_text(as_text(manifest::value_descriptor(schema)), record);
        if (registry.value_type(schema->name()) == schema)
        {
            write_varint(static_cast<std::uint8_t>(ValueRecipe::Named), record);
        }
        else if (registry.is_frame(schema))
        {
            write_varint(static_cast<std::uint8_t>(ValueRecipe::Frame), record);
            write_varint(value_ref(schema->element_type, depth + 1), record);
            write_varint(schema->key_type != nullptr, record);
            if (schema->key_type != nullptr) { write_varint(value_ref(schema->key_type, depth + 1), record); }
        }
        else if (registry.is_series(schema))
        {
            write_varint(static_cast<std::uint8_t>(ValueRecipe::Series), record);
            write_varint(value_ref(schema->element_type, depth + 1), record);
        }
        else if (schema->is_opaque_python())
        {
            // A class used as a type. Its name is all there is to write; see
            // the reader for why that is enough.
            write_varint(static_cast<std::uint8_t>(ValueRecipe::OpaquePython), record);
        }
        else
        {
            write_varint(static_cast<std::uint8_t>(ValueRecipe::Structural), record);
            write_varint(static_cast<std::uint64_t>(schema->value_kind()), record);
            write_varint(static_cast<std::uint64_t>(schema->flags), record);
            write_varint(schema->fixed_size, record);
            switch (schema->value_kind())
            {
                case ValueTypeKind::List:
                case ValueTypeKind::Set:
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue:
                    write_varint(value_ref(schema->element_type, depth + 1), record);
                    break;
                case ValueTypeKind::Any:
                    // A box inside a box names the box schema itself (RFC 0040).
                    if (schema != registry.any())
                    {
                        malformed("unregistered Any schema '" + std::string{schema->name()} +
                                  "' cannot be reconstructed");
                    }
                    break;
                case ValueTypeKind::Map:
                    write_varint(value_ref(schema->key_type, depth + 1), record);
                    write_varint(value_ref(schema->element_type, depth + 1), record);
                    break;
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    write_varint(schema->field_count, record);
                    for (std::size_t index = 0; index < schema->field_count; ++index)
                    {
                        const auto *name = schema->fields[index].name;
                        write_text(name == nullptr ? std::string_view{} : std::string_view{name}, record);
                        write_varint(value_ref(schema->fields[index].type, depth + 1), record);
                    }
                    break;
                default:
                    malformed("unregistered value schema '" + std::string{schema->name()} +
                              "' cannot be reconstructed");
            }
        }
        const auto index = values_.size();
        value_index_.emplace(schema, index);
        values_.push_back(schema);
        value_records_.append(record);
        return index;
    }

    std::size_t SchemaTableWriter::ts_ref(const TSValueTypeMetaData *schema, std::size_t depth)
    {
        check_depth(depth);
        if (schema == nullptr) { malformed("missing endpoint schema"); }
        if (const auto found = ts_index_.find(schema); found != ts_index_.end()) { return found->second; }
        std::string record;
        write_text(schema->name(), record);
        write_text(as_text(manifest::ts_descriptor(schema)), record);
        const bool named = TypeRegistry::instance().time_series_type(schema->name()) == schema;
        write_varint(named, record);
        if (!named)
        {
            write_varint(static_cast<std::uint64_t>(schema->kind), record);
            switch (schema->kind)
            {
                case TSTypeKind::REF:
                    write_varint(ts_ref(schema->referenced_ts(), depth + 1), record);
                    break;
                case TSTypeKind::TS:
                    write_varint(value_ref(schema->value_type, depth + 1), record);
                    break;
                case TSTypeKind::TSS:
                    write_varint(value_ref(schema->value_type->element_type, depth + 1), record);
                    break;
                case TSTypeKind::TSW:
                    write_varint(value_ref(schema->value_schema->element_type, depth + 1), record);
                    write_varint(schema->is_duration_based(), record);
                    if (schema->is_duration_based())
                    {
                        write_varint(std::bit_cast<std::uint64_t>(
                            static_cast<std::int64_t>(schema->time_range().count())), record);
                        write_varint(std::bit_cast<std::uint64_t>(
                            static_cast<std::int64_t>(schema->min_time_range().count())), record);
                    }
                    else
                    {
                        write_varint(schema->period(), record);
                        write_varint(schema->min_period(), record);
                    }
                    break;
                case TSTypeKind::TSD:
                    write_varint(value_ref(schema->data.tsd.key_type, depth + 1), record);
                    write_varint(ts_ref(schema->data.tsd.value_ts, depth + 1), record);
                    break;
                case TSTypeKind::TSL:
                    write_varint(schema->data.tsl.fixed_size, record);
                    write_varint(ts_ref(schema->data.tsl.element_ts, depth + 1), record);
                    break;
                case TSTypeKind::TSB:
                    write_varint(schema->data.tsb.field_count, record);
                    for (std::size_t index = 0; index < schema->data.tsb.field_count; ++index)
                    {
                        write_text(schema->data.tsb.fields[index].name, record);
                        write_varint(ts_ref(schema->data.tsb.fields[index].type, depth + 1), record);
                    }
                    break;
                default:
                    malformed("unregistered endpoint schema '" + std::string{schema->name()} +
                              "' cannot be reconstructed");
            }
        }
        const auto index = ts_count_++;
        ts_index_.emplace(schema, index);
        ts_records_.append(record);
        return index;
    }

    void SchemaTableWriter::write(std::string &out) const
    {
        write_varint(values_.size(), out);
        out.append(value_records_);
        write_varint(ts_count_, out);
        out.append(ts_records_);
    }

    void SchemaTableReader::read(BinaryReader &reader)
    {
        auto &registry = TypeRegistry::instance();
        const auto value_entries = read_count(reader);
        values_.reserve(value_entries);
        for (std::size_t entry = 0; entry < value_entries; ++entry)
        {
            const auto name = read_text(reader);
            const auto description = read_text(reader);
            const ValueTypeMetaData *schema{nullptr};
            // An entry that stands in for the writer's rather than being it, or
            // that is built over one that does. Its identity is not checked
            // below, because it is known not to be the writer's.
            bool        substituted = false;
            Substitutes substitutes{.entries = &substituted_};
            switch (static_cast<ValueRecipe>(read_varint(reader)))
            {
                case ValueRecipe::Named: schema = registry.value_type(name); break;
                case ValueRecipe::Frame: {
                    const auto *row = earlier(reader, values_, "value schema", &substitutes);
                    const auto *metadata =
                        read_boolean(reader) ? earlier(reader, values_, "value schema", &substitutes) : nullptr;
                    schema = registry.frame(row, metadata);
                    break;
                }
                case ValueRecipe::Series:
                    schema = registry.series(earlier(reader, values_, "value schema", &substitutes));
                    break;
                case ValueRecipe::Structural:
                    schema = read_structural_value(reader, registry, values_, substitutes);
                    break;
                case ValueRecipe::OpaquePython:
                    // The bridge names an annotation after its identity in the
                    // process that wrote it, so only that process -- an
                    // in-process worker -- has the exact schema. Anywhere else
                    // the unconstrained box stands in for it. That loses
                    // nothing a reader needs: either is a box holding a Python
                    // object, the bytes are the same, and the object's own
                    // pickle says what it is.
                    schema = registry.named_opaque_python(name);
                    if (schema == nullptr)
                    {
                        schema = registry.any();
                        substituted = true;
                    }
                    break;
                default: malformed("unsupported value schema recipe");
            }
            if (schema == nullptr) { malformed("unknown value schema " + std::string{name}); }
            // A schema built over a stand-in is named after the stand-in, not
            // after what the writer had, so it cannot be checked against the
            // writer's name either. Its shape came from the recipe itself.
            substituted = substituted || substitutes.named;
            if (!substituted &&
                (schema->name() != name || as_text(manifest::value_descriptor(schema)) != description))
                malformed("value schema changed");
            if (substituted) { substituted_.insert(values_.size()); }
            value_positions_.try_emplace(schema, values_.size());
            values_.push_back(schema);
        }

        const auto ts_entries = read_count(reader);
        ts_.reserve(ts_entries);
        const auto value = [&] { return earlier(reader, values_, "value schema"); };
        const auto endpoint = [&] { return earlier(reader, ts_, "endpoint schema"); };
        for (std::size_t entry = 0; entry < ts_entries; ++entry)
        {
            const auto name = read_text(reader);
            const auto description = read_text(reader);
            const TSValueTypeMetaData *schema{nullptr};
            if (read_boolean(reader)) { schema = registry.time_series_type(name); }
            else
            {
                const auto kind = read_varint(reader);
                if (kind > static_cast<std::uint64_t>(TSTypeKind::SIGNAL)) { malformed("invalid endpoint kind"); }
                switch (static_cast<TSTypeKind>(kind))
                {
                    case TSTypeKind::REF: schema = registry.ref(endpoint()); break;
                    case TSTypeKind::TS: schema = registry.ts(value()); break;
                    case TSTypeKind::TSS: schema = registry.tss(value()); break;
                    case TSTypeKind::TSW: {
                        const auto *element = value();
                        const bool duration = read_boolean(reader);
                        const auto period = read_varint(reader);
                        const auto minimum = read_varint(reader);
                        if (duration)
                        {
                            const auto range = std::bit_cast<std::int64_t>(period);
                            const auto min_range = std::bit_cast<std::int64_t>(minimum);
                            if (range <= 0 || min_range < 0 || min_range > range)
                                malformed("invalid duration window extent");
                            schema = registry.tsw_duration(element, TimeDelta{range}, TimeDelta{min_range});
                        }
                        else
                        {
                            if (period == 0 || period > std::numeric_limits<std::size_t>::max() || minimum > period)
                                malformed("invalid count window extent");
                            schema = registry.tsw(element, static_cast<std::size_t>(period),
                                                  static_cast<std::size_t>(minimum));
                        }
                        break;
                    }
                    case TSTypeKind::TSD: {
                        const auto *key = value();
                        schema = registry.tsd(key, endpoint());
                        break;
                    }
                    case TSTypeKind::TSL: {
                        const auto extent = read_size(reader);
                        schema = registry.tsl(endpoint(), extent);
                        break;
                    }
                    case TSTypeKind::TSB: {
                        const auto fields_count = read_count(reader);
                        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                        fields.reserve(fields_count);
                        for (std::size_t index = 0; index < fields_count; ++index)
                        {
                            std::string field{read_text(reader)};
                            fields.emplace_back(std::move(field), endpoint());
                        }
                        schema = registry.un_named_tsb(fields);
                        break;
                    }
                    default: malformed("unsupported endpoint schema recipe");
                }
            }
            if (schema == nullptr) { malformed("unknown endpoint schema " + std::string{name}); }
            if (schema->name() != name || as_text(manifest::ts_descriptor(schema)) != description)
                malformed("endpoint schema changed");
            ts_.push_back(schema);
        }
    }

    const ValueTypeMetaData *SchemaTableReader::value_at(std::size_t index) const
    {
        if (index >= values_.size()) { malformed("unknown value schema index"); }
        return values_[index];
    }

    const TSValueTypeMetaData *SchemaTableReader::ts_at(std::size_t index) const
    {
        if (index >= ts_.size()) { malformed("unknown endpoint schema index"); }
        return ts_[index];
    }

    std::size_t SchemaTableReader::value_index_of(const ValueTypeMetaData *schema) const noexcept
    {
        const auto found = value_positions_.find(schema);
        return found == value_positions_.end() ? npos : found->second;
    }
}
