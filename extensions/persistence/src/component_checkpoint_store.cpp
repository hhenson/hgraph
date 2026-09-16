#include <hgraph/persistence/component_checkpoint_store.h>

#include <hgraph/persistence/store_location.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/json_codec.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace hgraph::persistence
{
    namespace
    {
        constexpr std::string_view format_name{"hgraph.component-checkpoint.v2"};
        constexpr std::size_t max_depth{256};

        [[noreturn]] void malformed(std::string_view detail)
        {
            throw std::runtime_error("invalid component checkpoint: " + std::string{detail});
        }

        void check(const arrow::Status &status)
        {
            if (!status.ok()) { throw std::runtime_error(status.ToString()); }
        }

        void check_depth(std::size_t depth)
        {
            if (depth > max_depth) { malformed("nesting exceeds supported depth"); }
        }

        // The envelope frames metadata, topology and existing JSON Value
        // encodings. It is deliberately separate from the Value codec: the
        // persistence layer never interprets the payload of a scalar value.
        struct Writer
        {
            std::string bytes{};

            void number(std::uint64_t value)
            {
                for (unsigned shift = 0; shift < 64; shift += 8)
                {
                    bytes.push_back(static_cast<char>((value >> shift) & 0xffu));
                }
            }

            void text(std::string_view value)
            {
                number(value.size());
                bytes.append(value);
            }

            void time(DateTime value)
            {
                number(std::bit_cast<std::uint64_t>(value.time_since_epoch().count()));
            }
        };

        struct Reader
        {
            std::string_view bytes;
            std::size_t offset{0};

            [[nodiscard]] std::uint64_t number()
            {
                if (bytes.size() - offset < 8) { malformed("truncated integer"); }
                std::uint64_t result{0};
                for (unsigned shift = 0; shift < 64; shift += 8)
                {
                    result |= static_cast<std::uint64_t>(
                                  static_cast<unsigned char>(bytes[offset++])) << shift;
                }
                return result;
            }

            [[nodiscard]] bool boolean()
            {
                const auto value = number();
                if (value > 1) { malformed("invalid boolean"); }
                return value != 0;
            }

            [[nodiscard]] std::size_t count()
            {
                const auto value = number();
                // Every sequence element has at least an eight-byte field.
                // Bound counts before allocating storage for malformed files.
                if (value > (bytes.size() - offset) / 8) { malformed("invalid sequence size"); }
                return static_cast<std::size_t>(value);
            }

            [[nodiscard]] std::string_view text()
            {
                const auto length = number();
                if (length > bytes.size() - offset) { malformed("truncated text"); }
                const auto result = bytes.substr(offset, static_cast<std::size_t>(length));
                offset += static_cast<std::size_t>(length);
                return result;
            }

            [[nodiscard]] DateTime time()
            {
                return DateTime{TimeDelta{std::bit_cast<std::int64_t>(number())}};
            }
        };

        [[nodiscard]] std::string bytes_text(const std::vector<std::byte> &bytes)
        {
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] std::string value_description(const ValueTypeMetaData *schema)
        {
            return bytes_text(manifest::value_descriptor(schema));
        }

        [[nodiscard]] std::string ts_description(const TSValueTypeMetaData *schema)
        {
            return bytes_text(manifest::ts_descriptor(schema));
        }

        void encode_value_schema(Writer &writer, const ValueTypeMetaData *schema, std::size_t depth)
        {
            check_depth(depth);
            writer.text(schema->name());
            writer.text(value_description(schema));
            const bool named = TypeRegistry::instance().value_type(schema->name()) == schema;
            writer.number(named);
            if (named) { return; }
            writer.number(static_cast<std::uint64_t>(schema->value_kind()));
            writer.number(static_cast<std::uint64_t>(schema->flags));
            writer.number(schema->fixed_size);
            switch (schema->value_kind())
            {
                case ValueTypeKind::List:
                case ValueTypeKind::Set:
                    encode_value_schema(writer, schema->element_type, depth + 1);
                    break;
                case ValueTypeKind::Map:
                    encode_value_schema(writer, schema->key_type, depth + 1);
                    encode_value_schema(writer, schema->element_type, depth + 1);
                    break;
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    writer.number(schema->field_count);
                    for (std::size_t index = 0; index < schema->field_count; ++index)
                    {
                        writer.text(schema->fields[index].name == nullptr ? "" : schema->fields[index].name);
                        encode_value_schema(writer, schema->fields[index].type, depth + 1);
                    }
                    break;
                default:
                    malformed("unregistered value schema cannot be reconstructed");
            }
        }

        [[nodiscard]] const ValueTypeMetaData *decode_value_schema(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            const auto name = reader.text();
            const auto description = reader.text();
            auto &registry = TypeRegistry::instance();
            const ValueTypeMetaData *schema{};
            if (reader.boolean()) { schema = registry.value_type(name); }
            else
            {
                const auto kind = reader.number();
                if (kind > static_cast<std::uint64_t>(ValueTypeKind::Any)) { malformed("invalid value kind"); }
                const auto raw_flags = reader.number();
                if (raw_flags > std::numeric_limits<std::uint32_t>::max()) { malformed("invalid value flags"); }
                const auto flags = static_cast<ValueTypeFlags>(raw_flags);
                const auto extent = reader.number();
                if (extent > std::numeric_limits<std::size_t>::max()) { malformed("value extent out of range"); }
                const auto size = static_cast<std::size_t>(extent);
                const auto flag = [flags](ValueTypeFlags value) { return (flags & value) != ValueTypeFlags::None; };
                switch (static_cast<ValueTypeKind>(kind))
                {
                    case ValueTypeKind::List: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        if (flag(ValueTypeFlags::ShapedArray)) { schema = registry.array(element, size); }
                        else if (flag(ValueTypeFlags::Nullable)) { schema = registry.nullable_tuple(element); }
                        else if (flag(ValueTypeFlags::Mutable)) { schema = registry.mutable_list(element); }
                        else if (flag(ValueTypeFlags::FixedEmpty)) { schema = registry.fixed_list(element, size); }
                        else { schema = registry.list(element, size, flag(ValueTypeFlags::VariadicTuple)); }
                        break;
                    }
                    case ValueTypeKind::Set: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        schema = flag(ValueTypeFlags::Mutable) ? registry.mutable_set(element) : registry.set(element);
                        break;
                    }
                    case ValueTypeKind::Map: {
                        const auto *key = decode_value_schema(reader, depth + 1);
                        const auto *element = decode_value_schema(reader, depth + 1);
                        schema = flag(ValueTypeFlags::Mutable) ? registry.mutable_map(key, element) : registry.map(key, element);
                        break;
                    }
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::Bundle: {
                        const auto count = reader.count();
                        std::vector<const ValueTypeMetaData *> elements;
                        std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            std::string field{reader.text()};
                            const auto *element = decode_value_schema(reader, depth + 1);
                            elements.push_back(element);
                            fields.emplace_back(std::move(field), element);
                        }
                        schema = kind == static_cast<std::uint64_t>(ValueTypeKind::Tuple)
                                     ? registry.tuple(elements) : registry.un_named_bundle(fields);
                        break;
                    }
                    default:
                        malformed("unsupported value schema recipe");
                }
            }
            if (schema == nullptr) { malformed("unknown value schema " + std::string{name}); }
            if (schema->name() != name || value_description(schema) != description)
            {
                malformed("value schema changed");
            }
            return schema;
        }

        void encode_ts_schema(Writer &writer, const TSValueTypeMetaData *schema, std::size_t depth)
        {
            check_depth(depth);
            writer.text(schema->name());
            writer.text(ts_description(schema));
            const bool named = TypeRegistry::instance().time_series_type(schema->name()) == schema;
            writer.number(named);
            if (named) { return; }
            writer.number(static_cast<std::uint64_t>(schema->kind));
            switch (schema->kind)
            {
                case TSTypeKind::TS:
                    encode_value_schema(writer, schema->value_type, depth + 1);
                    break;
                case TSTypeKind::TSS:
                    encode_value_schema(writer, schema->value_type->element_type, depth + 1);
                    break;
                case TSTypeKind::TSW:
                    encode_value_schema(writer, schema->value_schema->element_type, depth + 1);
                    writer.number(schema->is_duration_based());
                    if (schema->is_duration_based())
                    {
                        writer.number(std::bit_cast<std::uint64_t>(schema->time_range().count()));
                        writer.number(std::bit_cast<std::uint64_t>(schema->min_time_range().count()));
                    }
                    else
                    {
                        writer.number(schema->period());
                        writer.number(schema->min_period());
                    }
                    break;
                case TSTypeKind::TSD:
                    encode_value_schema(writer, schema->data.tsd.key_type, depth + 1);
                    encode_ts_schema(writer, schema->data.tsd.value_ts, depth + 1);
                    break;
                case TSTypeKind::TSL:
                    writer.number(schema->data.tsl.fixed_size);
                    encode_ts_schema(writer, schema->data.tsl.element_ts, depth + 1);
                    break;
                case TSTypeKind::TSB:
                    writer.number(schema->data.tsb.field_count);
                    for (std::size_t index = 0; index < schema->data.tsb.field_count; ++index)
                    {
                        writer.text(schema->data.tsb.fields[index].name);
                        encode_ts_schema(writer, schema->data.tsb.fields[index].type, depth + 1);
                    }
                    break;
                default:
                    malformed("unregistered endpoint schema cannot be reconstructed");
            }
        }

        [[nodiscard]] const TSValueTypeMetaData *decode_ts_schema(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            const auto name = reader.text();
            const auto description = reader.text();
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *schema{};
            if (reader.boolean()) { schema = registry.time_series_type(name); }
            else
            {
                const auto kind = reader.number();
                if (kind > static_cast<std::uint64_t>(TSTypeKind::SIGNAL)) { malformed("invalid endpoint kind"); }
                switch (static_cast<TSTypeKind>(kind))
                {
                    case TSTypeKind::TS:
                        schema = registry.ts(decode_value_schema(reader, depth + 1));
                        break;
                    case TSTypeKind::TSS:
                        schema = registry.tss(decode_value_schema(reader, depth + 1));
                        break;
                    case TSTypeKind::TSW: {
                        const auto *element = decode_value_schema(reader, depth + 1);
                        const bool duration = reader.boolean();
                        const auto period = reader.number();
                        const auto minimum = reader.number();
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
                        const auto *key = decode_value_schema(reader, depth + 1);
                        const auto *element = decode_ts_schema(reader, depth + 1);
                        schema = registry.tsd(key, element);
                        break;
                    }
                    case TSTypeKind::TSL: {
                        const auto size = reader.number();
                        if (size > std::numeric_limits<std::size_t>::max()) { malformed("list extent out of range"); }
                        schema = registry.tsl(decode_ts_schema(reader, depth + 1), static_cast<std::size_t>(size));
                        break;
                    }
                    case TSTypeKind::TSB: {
                        const auto count = reader.count();
                        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            std::string field{reader.text()};
                            const auto *element = decode_ts_schema(reader, depth + 1);
                            fields.emplace_back(std::move(field), element);
                        }
                        schema = registry.un_named_tsb(fields);
                        break;
                    }
                    default:
                        malformed("unsupported endpoint schema recipe");
                }
            }
            if (schema == nullptr) { malformed("unknown endpoint schema " + std::string{name}); }
            if (schema->name() != name || ts_description(schema) != description)
            {
                malformed("endpoint schema changed");
            }
            return schema;
        }

        void encode_value(Writer &writer, const Value &value)
        {
            writer.number(value.has_value());
            if (!value.has_value()) { return; }
            encode_value_schema(writer, value.schema(), 0);
            writer.text(to_json_string(value.view()));
        }

        [[nodiscard]] Value decode_value(Reader &reader)
        {
            if (!reader.boolean()) { return {}; }
            const auto *schema = decode_value_schema(reader, 0);
            return from_json_string(schema, reader.text());
        }

        void encode_ts(Writer &writer, const TSCheckpointImage &image, std::size_t depth)
        {
            check_depth(depth);
            if (image.schema == nullptr) { malformed("missing endpoint schema"); }
            if (image.version != TSCheckpointImage::current_version)
            {
                malformed("unsupported endpoint image version");
            }
            writer.number(image.version);
            encode_ts_schema(writer, image.schema, 0);
            writer.time(image.last_modified_time);
            encode_value(writer, image.payload);
            writer.number(image.window_times.size());
            for (const auto time : image.window_times) { writer.time(time); }
            writer.number(image.slot_capacity);
            writer.time(image.key_set_last_modified_time);
            writer.number(image.keys.size());
            for (const auto &key : image.keys) { encode_value(writer, key); }
            writer.number(image.slots.size());
            for (const auto slot : image.slots) { writer.number(slot); }
            writer.number(image.free_slots.size());
            for (const auto slot : image.free_slots) { writer.number(slot); }
            writer.number(image.published.size());
            for (const auto published : image.published) { writer.number(published); }
            writer.number(image.children.size());
            for (const auto &child : image.children) { encode_ts(writer, child, depth + 1); }
        }

        [[nodiscard]] TSCheckpointImage decode_ts(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            TSCheckpointImage image;
            const auto version = reader.number();
            if (version != TSCheckpointImage::current_version) { malformed("unsupported endpoint image version"); }
            image.version = static_cast<std::uint32_t>(version);
            image.schema = decode_ts_schema(reader, 0);
            image.last_modified_time = reader.time();
            image.payload = decode_value(reader);
            auto count = reader.count();
            image.window_times.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.window_times.push_back(reader.time()); }
            const auto capacity = reader.number();
            if (capacity > std::numeric_limits<std::size_t>::max()) { malformed("slot capacity out of range"); }
            image.slot_capacity = static_cast<std::size_t>(capacity);
            image.key_set_last_modified_time = reader.time();
            count = reader.count();
            image.keys.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.keys.push_back(decode_value(reader)); }
            count = reader.count();
            image.slots.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto slot = reader.number();
                if (slot > std::numeric_limits<std::size_t>::max()) { malformed("slot out of range"); }
                image.slots.push_back(static_cast<std::size_t>(slot));
            }
            count = reader.count();
            image.free_slots.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto slot = reader.number();
                if (slot > std::numeric_limits<std::size_t>::max()) { malformed("free slot out of range"); }
                image.free_slots.push_back(static_cast<std::size_t>(slot));
            }
            count = reader.count();
            image.published.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.published.push_back(reader.boolean()); }
            count = reader.count();
            image.children.reserve(count);
            for (std::size_t index = 0; index < count; ++index) { image.children.push_back(decode_ts(reader, depth + 1)); }
            return image;
        }

        void encode_graph(Writer &writer, const GraphCheckpointImage &graph, std::size_t depth)
        {
            check_depth(depth);
            writer.number(graph.nodes.size());
            for (const auto &node : graph.nodes)
            {
                writer.text(node.id);
                writer.text(node.signature);
                for (const auto *endpoint : {&node.output, &node.error, &node.recordable_state, &node.ingress})
                {
                    writer.number(endpoint->has_value());
                    if (*endpoint) { encode_ts(writer, **endpoint, depth + 1); }
                }
                writer.number(node.input_activity.size());
                for (const auto &activity : node.input_activity)
                {
                    writer.number(activity.path.size());
                    for (const auto slot : activity.path) { writer.number(slot); }
                    if (activity.mode != TSInputActivityMode::Value &&
                        activity.mode != TSInputActivityMode::Structural)
                    {
                        malformed("unsupported input activity mode");
                    }
                    writer.number(static_cast<std::uint8_t>(activity.mode));
                }
                encode_value(writer, node.custom.payload);
                writer.number(node.custom.endpoints.size());
                for (const auto &endpoint : node.custom.endpoints) { encode_ts(writer, endpoint, depth + 1); }
                writer.number(node.custom.children.size());
                for (const auto &child : node.custom.children)
                {
                    writer.number(child.slot);
                    encode_value(writer, child.key);
                    writer.time(child.key_last_modified_time);
                    if (!child.graph) { malformed("missing child graph image"); }
                    encode_graph(writer, *child.graph, depth + 1);
                }
            }
        }

        [[nodiscard]] GraphCheckpointImage decode_graph(Reader &reader, std::size_t depth)
        {
            check_depth(depth);
            GraphCheckpointImage graph;
            const auto count = reader.count();
            graph.nodes.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                NodeCheckpointImage node;
                node.id = reader.text();
                node.signature = reader.text();
                for (auto *endpoint : {&node.output, &node.error, &node.recordable_state, &node.ingress})
                {
                    if (reader.boolean()) { *endpoint = decode_ts(reader, depth + 1); }
                }
                const auto activities = reader.count();
                node.input_activity.reserve(activities);
                for (std::size_t activity_index = 0; activity_index < activities; ++activity_index)
                {
                    TSInputActivityEntry activity;
                    const auto length = reader.count();
                    activity.path.reserve(length);
                    for (std::size_t part = 0; part < length; ++part)
                    {
                        const auto slot = reader.number();
                        if (slot > std::numeric_limits<std::size_t>::max()) { malformed("input path out of range"); }
                        activity.path.push_back(static_cast<std::size_t>(slot));
                    }
                    const auto mode = reader.number();
                    if (mode != static_cast<std::uint8_t>(TSInputActivityMode::Value) &&
                        mode != static_cast<std::uint8_t>(TSInputActivityMode::Structural))
                    {
                        malformed("unsupported input activity mode");
                    }
                    activity.mode = static_cast<TSInputActivityMode>(mode);
                    node.input_activity.push_back(std::move(activity));
                }
                node.custom.payload = decode_value(reader);
                const auto endpoints = reader.count();
                node.custom.endpoints.reserve(endpoints);
                for (std::size_t endpoint = 0; endpoint < endpoints; ++endpoint)
                    node.custom.endpoints.push_back(decode_ts(reader, depth + 1));
                const auto children = reader.count();
                node.custom.children.reserve(children);
                for (std::size_t child_index = 0; child_index < children; ++child_index)
                {
                    ChildGraphCheckpoint child;
                    const auto slot = reader.number();
                    if (slot > std::numeric_limits<std::size_t>::max()) { malformed("child slot out of range"); }
                    child.slot = static_cast<std::size_t>(slot);
                    child.key = decode_value(reader);
                    child.key_last_modified_time = reader.time();
                    child.graph = std::make_shared<GraphCheckpointImage>(decode_graph(reader, depth + 1));
                    node.custom.children.push_back(std::move(child));
                }
                graph.nodes.push_back(std::move(node));
            }
            return graph;
        }

        [[nodiscard]] Frame encode_checkpoint(const ComponentCheckpoint &checkpoint,
                                              std::string_view predecessor)
        {
            if (checkpoint.version != ComponentCheckpoint::current_version || checkpoint.component_id.empty() ||
                checkpoint.completed_until <= checkpoint.cut)
            {
                malformed("invalid completed component boundary");
            }
            Writer writer;
            writer.text(format_name);
            writer.number(checkpoint.version);
            writer.text(checkpoint.component_id);
            writer.text(checkpoint.graph_signature);
            writer.time(checkpoint.cut);
            writer.time(checkpoint.completed_until);
            writer.text(predecessor);
            encode_graph(writer, checkpoint.graph, 0);
            arrow::LargeBinaryBuilder builder;
            check(builder.Append(writer.bytes));
            std::shared_ptr<arrow::Array> array;
            check(builder.Finish(&array));
            const auto metadata = arrow::key_value_metadata(
                {"hgraph.checkpoint.format", "hgraph.checkpoint.predecessor"},
                {std::string{format_name}, std::string{predecessor}});
            return Frame{arrow::Table::Make(
                arrow::schema({arrow::field("checkpoint", arrow::large_binary(), false)}, metadata),
                {std::move(array)})};
        }

        [[nodiscard]] ComponentCheckpoint decode_checkpoint(const Frame &frame)
        {
            if (!frame.has_value() || frame.table->num_columns() != 1 ||
                frame.table->num_rows() != 1 ||
                frame.table->field(0)->name() != "checkpoint" ||
                frame.table->field(0)->type()->id() != arrow::Type::LARGE_BINARY)
            {
                malformed("expected a single checkpoint image");
            }
            auto combined = frame.table->CombineChunks();
            if (!combined.ok()) { throw std::runtime_error(combined.status().ToString()); }
            const auto &array = static_cast<const arrow::LargeBinaryArray &>(
                *(*combined)->column(0)->chunk(0));
            if (array.IsNull(0)) { malformed("null image"); }
            Reader reader{array.GetView(0)};
            if (reader.text() != format_name) { malformed("unsupported format"); }
            ComponentCheckpoint checkpoint;
            const auto version = reader.number();
            if (version != ComponentCheckpoint::current_version) { malformed("unsupported component image version"); }
            checkpoint.version = static_cast<std::uint32_t>(version);
            checkpoint.component_id = reader.text();
            checkpoint.graph_signature = reader.text();
            checkpoint.cut = reader.time();
            checkpoint.completed_until = reader.time();
            if (checkpoint.component_id.empty() || checkpoint.completed_until <= checkpoint.cut)
            {
                malformed("invalid completed component boundary");
            }
            const auto predecessor = reader.text();
            const auto metadata = frame.table->schema()->metadata();
            if (!metadata) { malformed("missing format metadata"); }
            auto format = metadata->Get("hgraph.checkpoint.format");
            auto parent = metadata->Get("hgraph.checkpoint.predecessor");
            if (!format.ok() || *format != format_name || !parent.ok() || *parent != predecessor)
            {
                malformed("inconsistent format metadata");
            }
            checkpoint.graph = decode_graph(reader, 0);
            if (reader.offset != reader.bytes.size()) { malformed("trailing data"); }
            return checkpoint;
        }

        void require_same_value(const Value &expected, const Value &actual)
        {
            if (expected.schema() != actual.schema() || expected.has_value() != actual.has_value() ||
                (expected.has_value() && expected != actual))
            {
                malformed("value codec does not preserve checkpoint state");
            }
        }

        void require_same_values(const TSCheckpointImage &expected, const TSCheckpointImage &actual)
        {
            require_same_value(expected.payload, actual.payload);
            if (expected.window_times != actual.window_times)
                malformed("endpoint codec changed window timestamps");
            if (expected.keys.size() != actual.keys.size() || expected.children.size() != actual.children.size())
            {
                malformed("endpoint codec changed image shape");
            }
            for (std::size_t index = 0; index < expected.keys.size(); ++index)
            {
                require_same_value(expected.keys[index], actual.keys[index]);
            }
            for (std::size_t index = 0; index < expected.children.size(); ++index)
            {
                require_same_values(expected.children[index], actual.children[index]);
            }
        }

        void require_same_values(const GraphCheckpointImage &expected, const GraphCheckpointImage &actual)
        {
            if (expected.nodes.size() != actual.nodes.size()) { malformed("graph codec changed image shape"); }
            for (std::size_t index = 0; index < expected.nodes.size(); ++index)
            {
                const auto &before = expected.nodes[index];
                const auto &after = actual.nodes[index];
                if (before.input_activity.size() != after.input_activity.size())
                {
                    malformed("input activity codec changed image shape");
                }
                for (std::size_t activity = 0; activity < before.input_activity.size(); ++activity)
                {
                    if (before.input_activity[activity].path != after.input_activity[activity].path ||
                        before.input_activity[activity].mode != after.input_activity[activity].mode)
                    {
                        malformed("input activity codec changed state");
                    }
                }
                for (const auto &[source, result] : {
                         std::pair{&before.output, &after.output},
                         std::pair{&before.error, &after.error},
                         std::pair{&before.recordable_state, &after.recordable_state},
                         std::pair{&before.ingress, &after.ingress}})
                {
                    if (source->has_value() != result->has_value()) { malformed("endpoint codec changed inventory"); }
                    if (*source) { require_same_values(**source, **result); }
                }
                require_same_value(before.custom.payload, after.custom.payload);
                if (before.custom.endpoints.size() != after.custom.endpoints.size())
                    malformed("hidden endpoint codec changed image shape");
                for (std::size_t endpoint = 0; endpoint < before.custom.endpoints.size(); ++endpoint)
                    require_same_values(before.custom.endpoints[endpoint], after.custom.endpoints[endpoint]);
                if (before.custom.children.size() != after.custom.children.size())
                {
                    malformed("child graph codec changed image shape");
                }
                for (std::size_t child = 0; child < before.custom.children.size(); ++child)
                {
                    require_same_value(before.custom.children[child].key, after.custom.children[child].key);
                    require_same_values(*before.custom.children[child].graph, *after.custom.children[child].graph);
                }
            }
        }
    }

    ComponentCheckpointStore::ComponentCheckpointStore(store::FrameStoreConfig config)
    {
        if (!config.immutable)
        {
            throw std::invalid_argument("component checkpoints require immutable storage");
        }
        frames_ = store::make_frame_store(std::move(config));
    }

    bool ComponentCheckpointStore::contains(std::string_view key) const
    {
        return frames_.contains(key);
    }

    ComponentCheckpoint ComponentCheckpointStore::read(std::string_view key) const
    {
        const auto frame = frames_.read(key);
        if (!frame.has_value())
        {
            throw std::runtime_error("component checkpoint not found: " + std::string{key});
        }
        return decode_checkpoint(frame);
    }

    void ComponentCheckpointStore::write(std::string_view key,
                                         const ComponentCheckpoint &checkpoint,
                                         std::optional<std::string_view> predecessor) const
    {
        store::require_valid_key(key);
        if (predecessor)
        {
            store::require_valid_key(*predecessor);
            if (*predecessor == key) { throw std::invalid_argument("checkpoint cannot replace its predecessor"); }
            if (!frames_.contains(*predecessor))
            {
                throw std::runtime_error("checkpoint predecessor does not exist");
            }
        }
        auto frame = encode_checkpoint(checkpoint, predecessor.value_or(""));
        // A successful publication must be recoverable. Existing Value codecs
        // can reject particular payloads (for example non-finite JSON numbers)
        // or lose information, even when their schema is supported. Validate
        // the complete schema envelope and scalar round trip before writing.
        const auto decoded = decode_checkpoint(frame);
        require_same_values(checkpoint.graph, decoded.graph);
        frames_.write(key, std::move(frame));
    }

    void configure_component_recovery(GlobalStateView state, const ComponentCheckpointStore &store,
                                      std::string component_id, std::string checkpoint_key,
                                      std::optional<std::string> restore_key, std::string revision)
    {
        store::require_valid_key(checkpoint_key);
        if (restore_key) { store::require_valid_key(*restore_key); }
        if (restore_key && *restore_key == checkpoint_key)
        {
            throw std::invalid_argument("checkpoint key must differ from restore key");
        }
        if (store.contains(checkpoint_key))
        {
            throw std::runtime_error("component checkpoint already exists: " + checkpoint_key);
        }
        ComponentRecoveryConfig config;
        config.component_id = std::move(component_id);
        config.revision = std::move(revision);
        if (restore_key)
        {
            config.load = [store, key = *restore_key]() -> std::optional<ComponentCheckpoint> {
                return store.read(key);
            };
        }
        config.commit = [store, key = std::move(checkpoint_key), predecessor = std::move(restore_key)](
                            const ComponentCheckpoint &checkpoint) {
            store.write(key, checkpoint,
                        predecessor ? std::optional<std::string_view>{*predecessor} : std::nullopt);
        };
        hgraph::configure_component_recovery(state, std::move(config));
    }
}
