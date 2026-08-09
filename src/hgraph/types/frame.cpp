#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <map>
#include <ostream>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view metadata_version{"1"};

        [[nodiscard]] bool is_hgraph_metadata_key(std::string_view key) noexcept
        {
            return key.starts_with(frame_metadata_prefix);
        }

        [[nodiscard]] std::shared_ptr<arrow::KeyValueMetadata>
        metadata_without_hgraph_entries(const std::shared_ptr<const arrow::KeyValueMetadata> &source)
        {
            auto result = source != nullptr ? source->Copy()
                                            : std::make_shared<arrow::KeyValueMetadata>();
            for (std::int64_t index = result->size(); index-- > 0;)
            {
                if (!is_hgraph_metadata_key(result->key(index))) { continue; }
                const auto status = result->Delete(index);
                if (!status.ok())
                {
                    throw std::runtime_error("frame metadata: could not remove reserved Arrow metadata: " +
                                             status.ToString());
                }
            }
            return result;
        }

        [[nodiscard]] const ValueTypeMetaData *require_metadata_schema(
            const ValueTypeMetaData *schema)
        {
            if (schema == nullptr || !schema->is_named_bundle())
            {
                throw std::invalid_argument(
                    "frame metadata must use a named Bundle schema");
            }
            return schema;
        }

        [[nodiscard]] std::string quote_json_string(std::string_view text)
        {
            return to_json_string(Value{Str{std::string{text}}}.view());
        }

        [[nodiscard]] std::string atomic_to_plain_text(const ValueView &field)
        {
            std::string encoded = to_json_string(field);
            if (encoded.size() >= 2 && encoded.front() == '"' && encoded.back() == '"')
            {
                return std::string{
                    from_json_string(scalar_descriptor<Str>::value_meta(), encoded).as<Str>()};
            }
            return encoded;
        }

        [[nodiscard]] Value atomic_from_plain_text(
            const ValueTypeMetaData *schema, std::string_view encoded)
        {
            const auto &converter = json_converter(schema);
            switch (converter.atomic_tag)
            {
                case JsonConverter::AtomicTag::Bool:
                case JsonConverter::AtomicTag::Int:
                case JsonConverter::AtomicTag::Float:
                    return from_json_string(schema, encoded);
                case JsonConverter::AtomicTag::Str:
                case JsonConverter::AtomicTag::Date:
                case JsonConverter::AtomicTag::DateTime:
                case JsonConverter::AtomicTag::TimeDelta:
                case JsonConverter::AtomicTag::Time:
                case JsonConverter::AtomicTag::CivilDateTime:
                case JsonConverter::AtomicTag::ZoneId:
                case JsonConverter::AtomicTag::ZonedDateTime:
                    return from_json_string(schema, quote_json_string(encoded));
                case JsonConverter::AtomicTag::Period:
                case JsonConverter::AtomicTag::InstantRange:
                case JsonConverter::AtomicTag::CivilDateRange:
                case JsonConverter::AtomicTag::InstantRangeSet:
                case JsonConverter::AtomicTag::CivilDateRangeSet:
                    return from_json_string(schema, encoded);
                case JsonConverter::AtomicTag::None:
                    // Named enums are atomic but use their member name as a JSON string.
                    if (schema->is_enum())
                    {
                        return from_json_string(schema, quote_json_string(encoded));
                    }
                    break;
            }
            throw std::invalid_argument(
                "frame metadata field has an unsupported atomic schema '" +
                std::string{schema->name()} + "'");
        }

        [[nodiscard]] std::map<std::string, std::string>
        hgraph_metadata_entries(const Frame &frame)
        {
            std::map<std::string, std::string> result;
            if (!frame.has_value()) { return result; }
            const auto &metadata = frame.table->schema()->metadata();
            if (metadata == nullptr) { return result; }
            for (std::int64_t index = 0; index < metadata->size(); ++index)
            {
                if (is_hgraph_metadata_key(metadata->key(index)))
                {
                    result[metadata->key(index)] = metadata->value(index);
                }
            }
            return result;
        }

        [[nodiscard]] std::string required_metadata_value(
            const arrow::KeyValueMetadata &metadata, std::string_view key)
        {
            auto value = metadata.Get(key);
            if (!value.ok())
            {
                throw std::invalid_argument("frame metadata is missing required Arrow key '" +
                                            std::string{key} + "'");
            }
            return std::move(*value);
        }

        [[nodiscard]] const ValueTypeMetaData *metadata_decode_schema(
            const arrow::KeyValueMetadata &encoded,
            const ValueTypeMetaData *metadata_schema)
        {
            const auto *expected = metadata_schema != nullptr
                                       ? require_metadata_schema(metadata_schema)
                                       : nullptr;
            if (!encoded.Contains(frame_metadata_schema_key))
            {
                if (expected == nullptr)
                {
                    throw std::invalid_argument(
                        "Frame metadata does not identify its schema; provide a metadata "
                        "schema to decode markerless metadata");
                }
                return expected;
            }

            const std::string encoded_schema =
                required_metadata_value(encoded, frame_metadata_schema_key);
            const auto *actual = TypeRegistry::instance().value_type(encoded_schema);
            if (actual == nullptr)
            {
                throw std::invalid_argument(
                    "frame metadata identifies unknown schema '" + encoded_schema + "'");
            }
            if (expected != nullptr &&
                !TypeRegistry::instance().bundle_is_a(actual, expected))
            {
                throw std::invalid_argument(
                    "frame metadata schema '" + encoded_schema +
                    "' is incompatible with declared schema '" +
                    std::string{expected->name()} + "'");
            }
            return require_metadata_schema(actual);
        }
    }  // namespace

    std::ostream &operator<<(std::ostream &os, const Frame &value)
    {
        if (!value.has_value()) { return os << "frame[empty]"; }
        return os << "frame[" << value.table->num_rows() << " x " << value.table->num_columns() << "]";
    }
    std::int64_t frame_rows(const Frame &value) noexcept
    {
        return value.has_value() ? value.table->num_rows() : 0;
    }

    bool Frame::has_metadata() const noexcept { return has_frame_metadata(*this); }

    Frame with_frame_metadata(Frame frame, Value metadata)
    {
        if (!frame.has_value())
        {
            throw std::invalid_argument("cannot attach metadata to an empty Frame");
        }
        if (!metadata.has_value())
        {
            throw std::invalid_argument("cannot attach an unset frame metadata value");
        }
        const auto *schema = require_metadata_schema(metadata.schema());
        const auto bundle = metadata.as_bundle();
        auto encoded = metadata_without_hgraph_entries(frame.table->schema()->metadata());
        encoded->Append(std::string{frame_metadata_schema_key}, std::string{schema->name()});
        encoded->Append(std::string{frame_metadata_version_key}, std::string{metadata_version});

        for (std::size_t index = 0; index < schema->field_count; ++index)
        {
            const auto field = bundle.at(index);
            if (!field.has_value()) { continue; }
            const auto *field_schema = schema->fields[index].type;
            std::string value;
            if (field_schema->value_kind() == ValueTypeKind::Atomic)
            {
                // Resolve the codec here even though atomic_to_plain_text also uses it:
                // unsupported atomics must fail before mutating the Arrow schema.
                (void)json_converter(field_schema);
                value = atomic_to_plain_text(field);
            }
            else
            {
                value = to_json_string(field);
            }
            encoded->Append(std::string{frame_metadata_field_prefix} +
                                std::string{schema->fields[index].name},
                            std::move(value));
        }
        frame.table = frame.table->ReplaceSchemaMetadata(std::move(encoded));
        return frame;
    }

    Value frame_metadata(const Frame &frame, const ValueTypeMetaData *metadata_schema)
    {
        if (!frame.has_value())
        {
            throw std::invalid_argument("cannot decode metadata from an empty Frame");
        }
        const auto &encoded = frame.table->schema()->metadata();
        if (encoded == nullptr || !has_frame_metadata(frame))
        {
            throw std::invalid_argument("Frame does not carry hgraph metadata");
        }
        const std::string version = required_metadata_value(*encoded, frame_metadata_version_key);
        if (version != metadata_version)
        {
            throw std::invalid_argument("unsupported hgraph frame metadata version '" + version + "'");
        }

        const auto *actual = metadata_decode_schema(*encoded, metadata_schema);
        std::set<std::string> seen;
        for (std::int64_t index = 0; index < encoded->size(); ++index)
        {
            const std::string &key = encoded->key(index);
            if (!is_hgraph_metadata_key(key)) { continue; }
            if (!seen.insert(key).second)
            {
                throw std::invalid_argument(
                    "frame metadata contains duplicate Arrow key '" + key + "'");
            }
            if (key == frame_metadata_schema_key || key == frame_metadata_version_key)
            {
                continue;
            }
            if (!std::string_view{key}.starts_with(frame_metadata_field_prefix))
            {
                throw std::invalid_argument(
                    "frame metadata contains unknown reserved Arrow key '" + key + "'");
            }
            const std::string_view field_name =
                std::string_view{key}.substr(frame_metadata_field_prefix.size());
            const bool declared = std::ranges::any_of(
                std::span{actual->fields, actual->field_count},
                [&](const ValueFieldMetaData &field) {
                    return field.name != nullptr && field_name == field.name;
                });
            if (!declared)
            {
                throw std::invalid_argument(
                    "frame metadata contains field '" + std::string{field_name} +
                    "' that is not declared by schema '" +
                    std::string{actual->name()} + "'");
            }
        }

        BundleBuilder builder{ValuePlanFactory::instance().type_for(actual)};
        for (std::size_t index = 0; index < actual->field_count; ++index)
        {
            const std::string key = std::string{frame_metadata_field_prefix} +
                                    std::string{actual->fields[index].name};
            auto value = encoded->Get(key);
            if (!value.ok()) { continue; }
            const auto *field_schema = actual->fields[index].type;
            Value field = field_schema->value_kind() == ValueTypeKind::Atomic
                              ? atomic_from_plain_text(field_schema, *value)
                              : from_json_string(field_schema, *value);
            builder.set(index, std::move(field));
        }
        return builder.build();
    }

    bool has_frame_metadata(const Frame &frame) noexcept
    {
        if (!frame.has_value()) { return false; }
        const auto &metadata = frame.table->schema()->metadata();
        if (metadata == nullptr) { return false; }
        for (std::int64_t index = 0; index < metadata->size(); ++index)
        {
            if (is_hgraph_metadata_key(metadata->key(index))) { return true; }
        }
        return false;
    }

    bool frame_metadata_equal(const Frame &lhs, const Frame &rhs) noexcept
    {
        try
        {
            auto lhs_entries = hgraph_metadata_entries(lhs);
            auto rhs_entries = hgraph_metadata_entries(rhs);
            const auto lhs_schema = lhs_entries.find(std::string{frame_metadata_schema_key});
            const auto rhs_schema = rhs_entries.find(std::string{frame_metadata_schema_key});
            if (lhs_schema != lhs_entries.end() && rhs_schema != rhs_entries.end() &&
                lhs_schema->second != rhs_schema->second)
            {
                return false;
            }
            lhs_entries.erase(std::string{frame_metadata_schema_key});
            rhs_entries.erase(std::string{frame_metadata_schema_key});
            return lhs_entries == rhs_entries;
        }
        catch (...)
        {
            return false;
        }
    }

    Frame without_frame_metadata(Frame frame)
    {
        if (!frame.has_value() || !frame.has_metadata()) { return frame; }
        auto metadata = metadata_without_hgraph_entries(frame.table->schema()->metadata());
        frame.table = frame.table->ReplaceSchemaMetadata(
            metadata->size() == 0 ? nullptr : std::move(metadata));
        return frame;
    }
}  // namespace hgraph
