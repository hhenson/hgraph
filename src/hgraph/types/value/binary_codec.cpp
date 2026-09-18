#include <hgraph/types/value/binary_codec.h>

#include <hgraph/runtime/logger.h>
#include <hgraph/types/value/binary_session.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/scope.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>

namespace hgraph
{
    namespace
    {
        [[noreturn]] void short_buffer()
        {
            throw std::runtime_error("binary codec: truncated buffer");
        }

        void refuse_write(const BinaryConverter &, const ValueView &, BinaryWriter &)
        { throw std::logic_error("binary codec: unbound converter"); }
        Value refuse_read(const BinaryConverter &, BinaryReader &)
        { throw std::logic_error("binary codec: unbound converter"); }
        std::uint64_t refuse_hash(const BinaryConverter &, const ValueView &)
        { throw std::logic_error("binary codec: unbound converter"); }

        /** A reader gives up the storage it built (``Value::AdoptStorage``). */
        template <typename Storage>
        [[nodiscard]] Value adopt_storage(const ValueTypeRef &binding, Storage &storage)
        {
            return Value{binding, &storage, Value::AdoptStorage{}};
        }

        // --- composites ----------------------------------------------------
        // A presence bitmap precedes the present fields. It is not an
        // optimisation: a Bundle field may be UNSET rather than defaulted, and
        // without the bitmap a decoder can tell neither UNSET from a value nor
        // where the next variable-width field begins.

        [[nodiscard]] std::size_t bitmap_bytes(std::size_t fields) noexcept
        {
            return fields / 8 + static_cast<std::size_t>(fields % 8 != 0);
        }

        void write_composite(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const std::size_t fields = self.children.size();
            const std::size_t bitmap_at = out.size();
            out.append(bitmap_bytes(fields), '\0');

            const auto concrete = view.concrete();
            if (concrete.schema() != self.meta)
                throw std::logic_error("binary codec: derived Bundle requires a bound converter");
            const auto indexed = concrete.as_indexed_view();
            for (std::size_t i = 0; i < fields; ++i)
            {
                const auto field = indexed.at(i);
                if (!field.has_value()) { continue; }
                out[bitmap_at + (i / 8)] =
                    static_cast<char>(static_cast<unsigned char>(out[bitmap_at + (i / 8)]) |
                                      (1u << (i % 8)));
                self.children[i]->write(field, writer);
            }
        }

        Value read_composite(const BinaryConverter &self, BinaryReader &reader)
        {
            const std::size_t fields = self.children.size();
            reader.consume_work(fields);
            const auto       *bitmap = reader.take(bitmap_bytes(fields));

            BundleBuilder builder{self.binding};
            for (std::size_t i = 0; i < fields; ++i)
            {
                const auto byte = static_cast<unsigned char>(bitmap[i / 8]);
                if ((byte & (1u << (i % 8))) == 0) { continue; }
                builder.set(i, self.children[i]->read(reader));
            }
            return builder.build();
        }

        // --- atoms ---------------------------------------------------------

        void write_atom(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            // Trivially copyable and fixed width, so the canonical wire form is
            // the storage image on a little-endian host. That covers every
            // numeric and temporal atom without enumerating the taxonomy.
            const auto *bytes = static_cast<const char *>(view.data());
            out.append(bytes, self.atom_size);
        }

        Value read_atom(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto *bytes = reader.take(self.atom_size);
            if (self.binding.ops() == &ops_for<Bool>() && std::to_integer<unsigned>(*bytes) > 1)
                throw std::runtime_error("binary codec: invalid boolean representation");
            Value result{self.binding};
            std::memcpy(result.begin_mutation().mutable_data(), bytes, self.atom_size);
            return result;
        }

        void write_string(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto &text = view.checked_as<Str>();
            write_varint(text.size(), out);
            out.append(text);
        }

        Value read_string(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto  size  = static_cast<std::size_t>(read_varint(reader));
            const auto *bytes = reader.take(size);
            Str         text{reinterpret_cast<const char *>(bytes), size};
            return Value{self.binding, &text};
        }

        // ``Bytes`` is the same length-prefixed shape as ``Str`` with a
        // different accessor -- it wraps the buffer rather than being one.
        void write_bytes(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto &blob = view.checked_as<Bytes>();
            write_varint(blob.data.size(), out);
            out.append(blob.data);
        }

        Value read_bytes(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto  size  = static_cast<std::size_t>(read_varint(reader));
            const auto *raw   = reader.take(size);
            Bytes       blob{std::string{reinterpret_cast<const char *>(raw), size}};
            return Value{self.binding, &blob};
        }

        // Zone identifiers contain process-local intern handles even though
        // their C++ representation is trivially copyable. Transport names and
        // re-intern on decode; never transmit their in-memory handles.
        void write_zone(const ZoneId &zone, std::string &out)
        {
            const auto name = zone.valid() ? zone.name() : std::string_view{};
            write_varint(name.size(), out);
            out.append(name);
        }

        ZoneId read_zone(BinaryReader &reader)
        {
            const auto size = read_varint(reader);
            const auto *data = reader.take(size);
            return size == 0 ? ZoneId{} : ZoneId{std::string_view{reinterpret_cast<const char *>(data), size}};
        }

        void write_zone_id(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            write_zone(view.checked_as<ZoneId>(), out);
        }

        Value read_zone_id(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto value = read_zone(reader);
            return Value{self.binding, &value};
        }

        void write_zoned_time(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto value = view.checked_as<ZonedDateTime>();
            const auto instant = value.instant();
            const auto offset = value.offset_seconds();
            out.append(reinterpret_cast<const char *>(&instant), sizeof(instant));
            write_zone(value.zone(), out);
            out.append(reinterpret_cast<const char *>(&offset), sizeof(offset));
        }

        Value read_zoned_time(const BinaryConverter &self, BinaryReader &reader)
        {
            Instant instant{};
            std::memcpy(&instant, reader.take(sizeof(instant)), sizeof(instant));
            const auto zone = read_zone(reader);
            std::int32_t offset{};
            std::memcpy(&offset, reader.take(sizeof(offset)), sizeof(offset));
            const auto value = zone.valid() ? ZonedDateTime::from_resolved(instant, zone, offset) : ZonedDateTime{};
            return Value{self.binding, &value};
        }

        template <typename Range>
        void write_range_value(const Range &range, std::string &out)
        {
            if (range.empty()) { out.push_back('\0'); return; }
            const unsigned tag = 1u | (range.lower_bounded() ? 2u : 0u) | (range.upper_bounded() ? 4u : 0u)
                               | (range.lower_boundary() == Boundary::Closed ? 8u : 0u)
                               | (range.upper_boundary() == Boundary::Closed ? 16u : 0u);
            out.push_back(static_cast<char>(tag));
            if (range.lower_bounded()) out.append(reinterpret_cast<const char *>(&range.lower_value()), sizeof(typename Range::value_type));
            if (range.upper_bounded()) out.append(reinterpret_cast<const char *>(&range.upper_value()), sizeof(typename Range::value_type));
        }

        template <typename Range>
        Range read_range_value(BinaryReader &reader)
        {
            const auto tag = std::to_integer<unsigned>(*reader.take(1));
            if (tag == 0) return Range{};
            if ((tag & ~31u) != 0 || (tag & 1u) == 0 || ((tag & 8u) != 0 && (tag & 2u) == 0)
                || ((tag & 16u) != 0 && (tag & 4u) == 0))
                throw std::runtime_error("binary codec: invalid range flags");
            typename Range::value_type lower{}, upper{};
            if (tag & 2u) std::memcpy(&lower, reader.take(sizeof(lower)), sizeof(lower));
            if (tag & 4u) std::memcpy(&upper, reader.take(sizeof(upper)), sizeof(upper));
            const auto low = tag & 8u ? Boundary::Closed : Boundary::Open;
            const auto high = tag & 16u ? Boundary::Closed : Boundary::Open;
            if ((tag & 6u) == 6u) return Range::bounded(lower, upper, low, high);
            if (tag & 2u) return Range::from(lower, low);
            if (tag & 4u) return Range::until(upper, high);
            return Range::all();
        }

        template <typename Range>
        void write_range(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            write_range_value(view.checked_as<Range>(), out);
        }

        template <typename Range>
        Value read_range(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto range = read_range_value<Range>(reader);
            return Value{self.binding, &range};
        }

        template <typename Ranges>
        void write_ranges(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto &ranges = view.checked_as<Ranges>();
            write_varint(ranges.size(), out);
            for (const auto &range : ranges) write_range_value(range, out);
        }

        template <typename Ranges>
        Value read_ranges(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count = read_varint(reader);
            reader.consume_work(count);
            if (count > Ranges::capacity()) throw std::runtime_error("binary codec: range set exceeds capacity");
            std::array<typename Ranges::value_type, Ranges::capacity()> ranges;
            for (std::size_t i = 0; i < count; ++i) ranges[i] = read_range_value<typename Ranges::value_type>(reader);
            const Ranges result{std::span<const typename Ranges::value_type>{ranges.data(), static_cast<std::size_t>(count)}};
            return Value{self.binding, &result};
        }

        template <typename T> T arrow_value(arrow::Result<T> result)
        {
            if (!result.ok()) throw std::runtime_error("binary codec: " + result.status().ToString());
            return std::move(result).ValueUnsafe();
        }

        void arrow_status(const arrow::Status &status)
        {
            if (!status.ok()) throw std::runtime_error("binary codec: " + status.ToString());
        }

        void write_table(const std::shared_ptr<arrow::Table> &table, std::string &out)
        {
            if (!table) { write_varint(0, out); return; }
            auto stream = arrow_value(arrow::io::BufferOutputStream::Create());
            auto writer = arrow_value(arrow::ipc::MakeStreamWriter(stream, table->schema()));
            arrow_status(writer->WriteTable(*table));
            arrow_status(writer->Close());
            auto buffer = arrow_value(stream->Finish());
            write_varint(static_cast<std::uint64_t>(buffer->size()), out);
            out.append(reinterpret_cast<const char *>(buffer->data()), buffer->size());
        }

        std::shared_ptr<arrow::Table> read_table(BinaryReader &reader)
        {
            const auto size = read_varint(reader);
            if (size == 0) return {};
            const auto *data = reader.take(size);
            // Decoded arrays may retain slices of the IPC buffer after this
            // function returns. Transfer an owning buffer to Arrow rather
            // than borrowing the transport frame's transient memory.
            auto buffer = arrow::Buffer::FromString(std::string{reinterpret_cast<const char *>(data), size});
            auto stream = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
            auto ipc = arrow_value(arrow::ipc::RecordBatchStreamReader::Open(std::move(stream)));
            auto table = arrow_value(ipc->ToTable());
            arrow_status(ipc->Close());
            return table;
        }

        void write_frame(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            write_table(view.checked_as<Frame>().table, out);
        }

        Value read_frame(const BinaryConverter &self, BinaryReader &reader)
        {
            const Frame value{read_table(reader)};
            return Value{self.binding, &value};
        }

        void write_series(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto &array = view.checked_as<Series>().array;
            write_table(array ? arrow::Table::Make(arrow::schema({arrow::field("value", array->type())}),
                                                   {std::make_shared<arrow::ChunkedArray>(array)}) : nullptr, out);
        }

        Value read_series(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto table = read_table(reader);
            Series value;
            if (table)
            {
                if (table->num_columns() != 1)
                    throw std::runtime_error("binary codec: Series IPC must contain exactly one column");
                const auto contiguous = arrow_value(table->CombineChunks());
                const auto column = contiguous->column(0);
                value.array = column->num_chunks() == 0 ? arrow_value(arrow::MakeArrayOfNull(column->type(), 0))
                                                        : column->chunk(0);
            }
            return Value{self.binding, &value};
        }

        void write_indirect(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto concrete = view.concrete();
            const bool present = concrete.has_value() && concrete.schema() != self.meta;
            out.push_back(present ? '\1' : '\0');
            if (present) self.children[0]->write(concrete, writer);
        }

        Value read_indirect(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto present = std::to_integer<unsigned>(*reader.take(1));
            if (present > 1) throw std::runtime_error("binary codec: invalid indirect presence tag");
            if (!present) return Value{self.binding};
            const auto pointee = self.children[0]->read(reader);
            return Value{self.binding, pointee.view()};
        }

        // --- sequences -----------------------------------------------------

        void write_list(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto list = view.as_list();
            write_varint(list.size(), out);
            const bool nullable = self.meta->has(ValueTypeFlags::Nullable);
            const auto bitmap_at = out.size();
            if (nullable) out.append(bitmap_bytes(list.size()), '\0');
            for (std::size_t i = 0; i < list.size(); ++i)
            {
                const auto item = list.at(i);
                if (!item.has_value())
                {
                    if (!nullable) throw std::runtime_error("binary codec: non-nullable list contains an unset element");
                    continue;
                }
                if (nullable) out[bitmap_at + i / 8] = static_cast<char>(
                    static_cast<unsigned char>(out[bitmap_at + i / 8]) | (1u << (i % 8)));
                self.children[0]->write(item, writer);
            }
        }

        Value read_list(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto  count   = static_cast<std::size_t>(read_varint(reader));
            reader.consume_work(count);
            const auto  element = self.children[0]->binding;
            ListBuilder builder{element, *self.meta};
            const auto *bitmap = self.meta->has(ValueTypeFlags::Nullable) ? reader.take(bitmap_bytes(count)) : nullptr;
            for (std::size_t i = 0; i < count; ++i)
            {
                if (bitmap != nullptr && (std::to_integer<unsigned>(bitmap[i / 8]) & (1u << (i % 8))) == 0)
                {
                    builder.push_back_unset();
                    continue;
                }
                Value item = self.children[0]->read(reader);
                builder.push_back(item.view());
            }
            ListStorage storage = builder.build_storage();
            return adopt_storage(self.realization_bound ? self.binding : compact_list_type(element, *self.meta), storage);
        }

        void write_set(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto set = view.as_set();
            // The count precedes the elements and the view knows it, so the
            // elements are written in place. They used to be built in a scratch
            // buffer and copied, which cost every set a second pass.
            const std::size_t expected = set.size();
            write_varint(expected, out);
            std::size_t count = 0;
            for (const auto element : set)
            {
                self.children[0]->write(element, writer);
                ++count;
            }
            if (count != expected) { throw std::logic_error("binary codec: set size disagrees with its elements"); }
        }

        Value read_set(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count   = static_cast<std::size_t>(read_varint(reader));
            reader.consume_work(count);
            const auto element = self.children[0]->binding;
            SetBuilder builder{element};
            for (std::size_t i = 0; i < count; ++i)
            {
                Value item = self.children[0]->read(reader);
                (void)builder.insert(item.view());
            }
            SetStorage storage = builder.build_storage();
            return adopt_storage(self.realization_bound ? self.binding : compact_set_type(element), storage);
        }

        void write_map(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto  map = view.as_map();
            const std::size_t expected = map.size();
            write_varint(expected, out);
            std::size_t count = 0;
            for (const auto entry : map)
            {
                self.children[0]->write(entry.first, writer);
                out.push_back(entry.second.has_value() ? '\1' : '\0');
                if (entry.second.has_value()) self.children[1]->write(entry.second, writer);
                ++count;
            }
            if (count != expected) { throw std::logic_error("binary codec: map size disagrees with its entries"); }
        }

        Value read_map(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count = static_cast<std::size_t>(read_varint(reader));
            reader.consume_work(count);
            const auto key   = self.children[0]->binding;
            const auto value = self.children[1]->binding;
            MapBuilder builder{key, value};
            for (std::size_t i = 0; i < count; ++i)
            {
                Value k = self.children[0]->read(reader);
                const auto present = std::to_integer<unsigned>(*reader.take(1));
                if (present > 1) throw std::runtime_error("binary codec: invalid map value presence tag");
                if (present)
                {
                    Value v = self.children[1]->read(reader);
                    builder.set_item(k.view(), v.view());
                }
                else builder.set_item_unset(k.view());
            }
            MapStorage storage = builder.build_storage();
            return adopt_storage(self.realization_bound ? self.binding : compact_map_type(key, value), storage);
        }

        void write_buffer(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto sequence = view.as_indexed_view();
            write_varint(sequence.size(), out);
            for (std::size_t i = 0; i < sequence.size(); ++i)
                self.children[0]->write(sequence.at(i), writer);
        }

        Value read_cyclic_buffer(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count = read_varint(reader);
            reader.consume_work(count);
            if (count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: cyclic buffer exceeds declared capacity");
            CyclicBufferBuilder builder{self.children[0]->binding, self.meta->fixed_size};
            for (std::size_t i = 0; i < count; ++i) builder.push_back(self.children[0]->read(reader).view());
            auto storage = builder.build_storage();
            return adopt_storage(self.realization_bound ? self.binding : compact_cyclic_buffer_type(self.children[0]->binding, self.meta->fixed_size), storage);
        }

        Value read_queue(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count = read_varint(reader);
            reader.consume_work(count);
            if (self.meta->fixed_size != 0 && count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: queue exceeds declared capacity");
            QueueBuilder builder{self.children[0]->binding, self.meta->fixed_size};
            for (std::size_t i = 0; i < count; ++i) builder.push(self.children[0]->read(reader).view());
            auto storage = builder.build_storage();
            return adopt_storage(self.realization_bound ? self.binding : compact_queue_type(self.children[0]->binding, self.meta->fixed_size), storage);
        }

        // --- Fast: sequences of fixed-width atoms ------------------------------
        // A sequence of trivially copyable atoms is one block on the wire --
        // count, then the atoms back to back -- so it costs a copy, not a
        // dispatch per element. The bytes are the ones the field-wise path
        // writes; what differs is that no ``ValueView`` is built per element on
        // the way out and no ``Value`` on the way in: the reader hands the block
        // to the container's storage, which copies it as one.

        /** Copy one atom of ``width`` bytes. The common widths are fixed-size copies. */
        inline void copy_atom(char *to, const void *from, std::size_t width) noexcept
        {
            switch (width)
            {
                case 1: std::memcpy(to, from, 1); break;
                case 2: std::memcpy(to, from, 2); break;
                case 4: std::memcpy(to, from, 4); break;
                case 8: std::memcpy(to, from, 8); break;
                default: std::memcpy(to, from, width); break;
            }
        }

        [[nodiscard]] std::size_t block_bytes(std::size_t count, std::size_t width)
        {
            if (width != 0 && count > std::numeric_limits<std::size_t>::max() / width)
                throw std::runtime_error("binary codec: sequence length overflows");
            return count * width;
        }

        /** Append ``count`` atoms read through ``atom_at`` as one block. */
        template <typename AtomAt>
        void append_block(std::string &out, std::size_t count, std::size_t width, AtomAt &&atom_at)
        {
            const std::size_t at = out.size();
            out.resize_and_overwrite(at + block_bytes(count, width), [&](char *data, std::size_t size) {
                char *to = data + at;
                for (std::size_t index = 0; index < count; ++index, to += width) { copy_atom(to, atom_at(index), width); }
                return size;
            });
        }

        /** The block of ``count`` atoms, checked where the atom has invalid images. */
        [[nodiscard]] ElementSpan take_block(const BinaryConverter &atom, BinaryReader &reader, std::size_t count)
        {
            reader.consume_work(count);
            const auto *bytes = reader.take(block_bytes(count, atom.atom_size));
            if (atom.binding.ops() == &ops_for<Bool>())
            {
                for (std::size_t index = 0; index < count; ++index)
                {
                    if (std::to_integer<unsigned>(bytes[index]) > 1)
                        throw std::runtime_error("binary codec: invalid boolean representation");
                }
            }
            return ElementSpan{.bytes = bytes, .size = count, .stride = atom.atom_size, .plan = atom.binding.plan()};
        }

        void write_atom_sequence(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            const auto *ops = specialized_view_detail::checked_indexed_ops(view.binding(), "binary codec");
            const auto *memory = view.data();
            const std::size_t count = ops->size(ops->context, memory);
            write_varint(count, writer.out);
            append_block(writer.out, count, self.children[0]->atom_size, [&](std::size_t index) {
                if (ops->element_valid != nullptr && !ops->element_valid(ops->context, memory, index))
                    throw std::runtime_error("binary codec: non-nullable sequence contains an unset element");
                return ops->element_at(ops->context, memory, index);
            });
        }

        Value read_atom_list(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            ListStorage storage{atom.binding, take_block(atom, reader, count)};
            return adopt_storage(self.realization_bound ? self.binding : compact_list_type(atom.binding, *self.meta),
                                 storage);
        }

        Value read_atom_cyclic_buffer(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            if (count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: cyclic buffer exceeds declared capacity");
            // The wire holds logical order, so the rebuilt ring starts at its head.
            CyclicBufferStorage storage{atom.binding, take_block(atom, reader, count), 0};
            return adopt_storage(self.realization_bound
                                     ? self.binding
                                     : compact_cyclic_buffer_type(atom.binding, self.meta->fixed_size),
                                 storage);
        }

        Value read_atom_queue(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            if (self.meta->fixed_size != 0 && count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: queue exceeds declared capacity");
            QueueStorage storage{atom.binding, take_block(atom, reader, count)};
            return adopt_storage(self.realization_bound ? self.binding
                                                        : compact_queue_type(atom.binding, self.meta->fixed_size),
                                 storage);
        }

        void write_atom_set(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            const auto        set = view.as_set();
            const std::size_t count = set.size();
            const std::size_t width = self.children[0]->atom_size;
            write_varint(count, writer.out);
            const std::size_t at = writer.out.size();
            writer.out.resize_and_overwrite(at + block_bytes(count, width), [&](char *data, std::size_t size) {
                char       *to = data + at;
                std::size_t written = 0;
                for (const auto element : set)
                {
                    if (written == count) { break; }
                    copy_atom(to, element.data(), width);
                    to += width;
                    ++written;
                }
                if (written != count) { throw std::logic_error("binary codec: set size disagrees with its elements"); }
                return size;
            });
        }

        Value read_atom_set(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            SetStorage storage{atom.binding, take_block(atom, reader, count)};
            if (storage.size() != count) { throw std::runtime_error("binary codec: set repeats an element"); }
            return adopt_storage(self.realization_bound ? self.binding : compact_set_type(atom.binding), storage);
        }

        // A map of fixed-width keys and values is two blocks, keys then values.
        // Interleaving them, as the field-wise form does, leaves nothing to
        // copy in bulk. A value that is unset still has its place in the block
        // -- zeroed -- and a bitmap says which; the flag byte says whether there
        // is a bitmap at all, because almost no map has an unset value.

        void write_atom_map(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto             &out = writer.out;
            const auto        map = view.as_map();
            const std::size_t count = map.size();
            const std::size_t key_width = self.children[0]->atom_size;
            const std::size_t value_width = self.children[1]->atom_size;
            write_varint(count, out);

            const std::size_t keys_at = out.size();
            const std::size_t flag_at = keys_at + block_bytes(count, key_width);
            const std::size_t bitmap_at = flag_at + 1;
            // Sized for the bitmap too; it is dropped again if every value is set.
            const std::size_t values_with_bitmap_at = bitmap_at + bitmap_bytes(count);
            out.resize(values_with_bitmap_at + block_bytes(count, value_width), '\0');

            std::size_t index = 0;
            bool        holes = false;
            for (const auto entry : map)
            {
                if (index == count) { throw std::logic_error("binary codec: map size disagrees with its entries"); }
                copy_atom(out.data() + keys_at + index * key_width, entry.first.data(), key_width);
                if (entry.second.has_value())
                {
                    out[bitmap_at + index / 8] = static_cast<char>(
                        static_cast<unsigned char>(out[bitmap_at + index / 8]) | (1u << (index % 8)));
                    copy_atom(out.data() + values_with_bitmap_at + index * value_width, entry.second.data(), value_width);
                }
                else { holes = true; }
                ++index;
            }
            if (index != count) { throw std::logic_error("binary codec: map size disagrees with its entries"); }
            out[flag_at] = holes ? '\1' : '\0';
            if (!holes)
            {
                // Close the gap the unused bitmap left.
                out.erase(bitmap_at, bitmap_bytes(count));
            }
        }

        Value read_atom_map(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &key = *self.children[0];
            const auto &value = *self.children[1];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            const auto  keys = take_block(key, reader, count);
            const auto  flag = std::to_integer<unsigned>(*reader.take(1));
            if (flag > 1) { throw std::runtime_error("binary codec: invalid map value presence flag"); }
            std::vector<bool> validity;
            if (flag == 1)
            {
                const auto *bitmap = reader.take(bitmap_bytes(count));
                validity.resize(count);
                for (std::size_t index = 0; index < count; ++index)
                {
                    validity[index] = (std::to_integer<unsigned>(bitmap[index / 8]) & (1u << (index % 8))) != 0;
                }
            }
            const auto values = take_block(value, reader, count);
            MapStorage storage{key.binding, value.binding, keys, values, std::move(validity)};
            if (storage.size() != count) { throw std::runtime_error("binary codec: map repeats a key"); }
            return adopt_storage(self.realization_bound ? self.binding : compact_map_type(key.binding, value.binding),
                                 storage);
        }

        // --- Fast: a sequence of composites, by column ---------------------------
        // Rows are stored side by side and the field-wise form writes them that
        // way: a bitmap and the fields of row 0, then row 1. Every field of every
        // row is then a dispatch, and on the way in a ``Value`` and a builder
        // per row. Written by column -- for each field a presence flag, a bitmap
        // only if some row lacks it, then that field for every row -- a
        // fixed-width field is a strided copy in each direction, and the reader
        // fills rows it constructed once, in place.
        //
        // An unset fixed-width field keeps its place in the block, zeroed, so
        // the block is addressed by row. A variable-width field is written for
        // the rows that have it, in row order.

        /** One field of the rows being written, however the rows are stored. */
        struct RowFields
        {
            std::optional<CompositeFieldLayout> layout{};
            const IndexedValueOps              *row_ops{nullptr};

            [[nodiscard]] bool set(const void *row, std::size_t field) const
            {
                if (layout.has_value()) { return layout->field_set(row, field); }
                return row_ops->element_valid != nullptr ? row_ops->element_valid(row_ops->context, row, field)
                                                         : row_ops->element_at(row_ops->context, row, field) != nullptr;
            }
            [[nodiscard]] const void *at(const void *row, std::size_t field) const
            {
                return layout.has_value() ? layout->field(row, field) : row_ops->element_at(row_ops->context, row, field);
            }
        };

        /** True for an atom that travels as fixed-width bytes, under either profile. */
        [[nodiscard]] bool is_fixed_atom(const BinaryConverter &converter) noexcept;
        // ``Compact``'s column forms, defined with the rest of that profile below.
        void write_column_block(const BinaryConverter &atom, std::string_view block, std::size_t count, std::string &out);
        void read_column(const BinaryConverter &atom, BinaryReader &reader, std::size_t count, std::string &block);
        void write_text_column(const std::vector<std::string_view> &texts, std::string &out);
        [[nodiscard]] std::vector<std::string_view> read_text_column(BinaryReader &reader, std::size_t count);

        // ``Compact`` writes each column for the rows that have the field, in
        // its own encoding; ``Fast`` writes a block addressed by row.
        template <bool Compact>
        void write_row_columns(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto       &out = writer.out;
            const auto &row_converter = *self.children[0];
            const auto *ops = specialized_view_detail::checked_indexed_ops(view.binding(), "binary codec");
            const auto *memory = view.data();
            const std::size_t count = ops->size(ops->context, memory);
            write_varint(count, out);
            if (count == 0) { return; }

            // Row addresses once, not once per field. Rows of one list share a
            // representation; one that did not could not be addressed by column.
            std::vector<const void *> rows(count);
            const auto row_binding = ops->element_binding(ops->context, memory, 0);
            for (std::size_t index = 0; index < count; ++index)
            {
                if (ops->element_valid != nullptr && !ops->element_valid(ops->context, memory, index))
                    throw std::runtime_error("binary codec: non-nullable list contains an unset element");
                if (index != 0 && ops->element_binding(ops->context, memory, index) != row_binding)
                    throw std::logic_error("binary codec: the rows of a list do not share one representation");
                rows[index] = ops->element_at(ops->context, memory, index);
            }
            if (row_binding.schema() != row_converter.meta)
                throw std::logic_error("binary codec: derived Bundle requires a bound converter");

            RowFields fields{.layout = CompositeFieldLayout::of(row_binding),
                             .row_ops = specialized_view_detail::checked_indexed_ops(row_binding, "binary codec")};
            const std::size_t field_count = row_converter.children.size();
            for (std::size_t field = 0; field < field_count; ++field)
            {
                const auto &child = *row_converter.children[field];
                // Almost every column is complete, so look before writing a
                // bitmap rather than writing one to throw away.
                bool holes = false;
                for (std::size_t index = 0; index < count && !holes; ++index) { holes = !fields.set(rows[index], field); }
                out.push_back(holes ? '\1' : '\0');
                if (holes)
                {
                    const std::size_t bitmap_at = out.size();
                    out.append(bitmap_bytes(count), '\0');
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        if (!fields.set(rows[index], field)) { continue; }
                        out[bitmap_at + index / 8] = static_cast<char>(
                            static_cast<unsigned char>(out[bitmap_at + index / 8]) | (1u << (index % 8)));
                    }
                }
                const auto present = [&](std::size_t index) { return !holes || fields.set(rows[index], field); };

                if (is_fixed_atom(child))
                {
                    const std::size_t width = child.atom_size;
                    if constexpr (Compact)
                    {
                        std::string block;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            if (present(index)) { block.append(static_cast<const char *>(fields.at(rows[index], field)), width); }
                        }
                        write_column_block(child, block, block.size() / std::max<std::size_t>(width, 1), out);
                        continue;
                    }
                    const std::size_t block_at = out.size();
                    out.resize_and_overwrite(block_at + block_bytes(count, width), [&](char *data, std::size_t size) {
                        char *to = data + block_at;
                        for (std::size_t index = 0; index < count; ++index, to += width)
                        {
                            if (present(index)) { copy_atom(to, fields.at(rows[index], field), width); }
                            else { std::memset(to, 0, width); }
                        }
                        return size;
                    });
                    continue;
                }

                const auto field_binding = fields.row_ops->element_binding(fields.row_ops->context, rows[0], field);
                if (child.write_ == &write_string)
                {
                    // Text is a block of lengths and then the characters, so
                    // neither direction dispatches per row. The rows share one
                    // representation, so checking the first checks them all.
                    std::size_t first = 0;
                    while (first < count && !present(first)) { ++first; }
                    if (first < count) { static_cast<void>(ValueView{field_binding, fields.at(rows[first], field)}.checked_as<Str>()); }
                    const auto text_at = [&](std::size_t index) -> const Str & {
                        return *static_cast<const Str *>(fields.at(rows[index], field));
                    };
                    if constexpr (Compact)
                    {
                        std::vector<std::string_view> texts;
                        texts.reserve(count);
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            if (present(index)) { texts.emplace_back(text_at(index)); }
                        }
                        write_text_column(texts, out);
                        continue;
                    }
                    std::size_t characters = 0;
                    const std::size_t lengths_at = out.size();
                    out.resize_and_overwrite(lengths_at + block_bytes(count, sizeof(std::uint32_t)),
                                             [&](char *data, std::size_t size) {
                        char *to = data + lengths_at;
                        for (std::size_t index = 0; index < count; ++index, to += sizeof(std::uint32_t))
                        {
                            const std::size_t length = present(index) ? text_at(index).size() : 0;
                            if (length > std::numeric_limits<std::uint32_t>::max())
                                throw std::length_error("binary codec: a string in a column exceeds 4 GiB");
                            const auto narrow = static_cast<std::uint32_t>(length);
                            std::memcpy(to, &narrow, sizeof(narrow));
                            characters += length;
                        }
                        return size;
                    });
                    const std::size_t characters_at = out.size();
                    out.resize_and_overwrite(characters_at + characters, [&](char *data, std::size_t size) {
                        char *to = data + characters_at;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            if (!present(index)) { continue; }
                            const Str &text = text_at(index);
                            std::memcpy(to, text.data(), text.size());
                            to += text.size();
                        }
                        return size;
                    });
                    continue;
                }

                for (std::size_t index = 0; index < count; ++index)
                {
                    if (!present(index)) { continue; }
                    child.write(ValueView{field_binding, fields.at(rows[index], field)}, writer);
                }
            }
        }

        template <bool Compact>
        Value read_row_columns(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &row_converter = *self.children[0];
            const auto  row_binding = row_converter.binding;
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            reader.consume_work(count);

            ListBuilder builder{row_binding, *self.meta};
            const auto  result_binding = self.realization_bound ? self.binding : compact_list_type(row_binding, *self.meta);
            if (count == 0)
            {
                ListStorage empty = builder.build_storage();
                return adopt_storage(result_binding, empty);
            }

            // The count is the writer's claim, and the rows are about to be
            // allocated on the strength of it -- all of them, before a single
            // field has been read. So the claim is tested first: the work of
            // every field of every row is charged now, and the bytes present
            // must be at least what these columns could possibly occupy. The
            // field-wise reader never needed this, because it built a row only
            // once it had read one.
            const std::size_t claimed_fields = row_converter.children.size();
            reader.consume_work(block_bytes(count, claimed_fields));
            std::size_t least = claimed_fields;   // a presence flag per column
            for (const auto *child : row_converter.children)
            {
                const bool text = child->write_ == &write_string;
                if (!is_fixed_atom(*child) && !text) { continue; }
                if constexpr (Compact)
                {
                    // Past the length a constant column may have, every
                    // encoding spends at least a bit a row -- as does the
                    // bitmap of a column that some rows lack.
                    if (count > 1024) { least += bitmap_bytes(count); }
                }
                else { least += block_bytes(count, text ? sizeof(std::uint32_t) : child->atom_size); }
            }
            if (reader.remaining() < least)
            {
                throw std::runtime_error(fmt::format(
                    "binary codec: a list claims {} rows, which its {} remaining bytes cannot hold", count,
                    reader.remaining()));
            }

            // Every row once, default-constructed with no field set; the
            // columns then fill them where they stand.
            builder.append_default(count);
            const auto layout = CompositeFieldLayout::of(row_binding);
            const auto *row_ops = specialized_view_detail::checked_indexed_ops(row_binding, "binary codec");
            if (!layout.has_value()) { throw std::logic_error("binary codec: row storage is not composite"); }

            const std::size_t field_count = row_converter.children.size();
            for (std::size_t field = 0; field < field_count; ++field)
            {
                const auto &child = *row_converter.children[field];
                const auto  flag = std::to_integer<unsigned>(*reader.take(1));
                if (flag > 1) { throw std::runtime_error("binary codec: invalid column presence flag"); }
                const auto *bitmap = flag == 1 ? reader.take(bitmap_bytes(count)) : nullptr;
                const auto  present = [&](std::size_t index) {
                    return bitmap == nullptr || (std::to_integer<unsigned>(bitmap[index / 8]) & (1u << (index % 8))) != 0;
                };
                std::size_t present_count = count;
                if (bitmap != nullptr)
                {
                    present_count = 0;
                    for (std::size_t index = 0; index < count; ++index) { present_count += present(index) ? 1 : 0; }
                }

                if (is_fixed_atom(child))
                {
                    const std::size_t width = child.atom_size;
                    if constexpr (Compact)
                    {
                        std::string block;
                        read_column(child, reader, present_count, block);
                        const char *from = block.data();
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            if (!present(index)) { continue; }
                            void *row = builder.element_memory(index);
                            copy_atom(static_cast<char *>(layout->field(row, field)), from, width);
                            layout->mark_field(row, field);
                            from += width;
                        }
                        continue;
                    }
                    const auto        block = take_block(child, reader, count);
                    const auto       *from = static_cast<const char *>(block.bytes);
                    for (std::size_t index = 0; index < count; ++index, from += width)
                    {
                        if (!present(index)) { continue; }
                        void *row = builder.element_memory(index);
                        copy_atom(static_cast<char *>(layout->field(row, field)), from, width);
                        layout->mark_field(row, field);
                    }
                    continue;
                }

                reader.consume_work(count);
                const auto destination = row_ops->element_binding(row_ops->context, nullptr, field);
                if (!destination) { throw std::logic_error("binary codec: row field binding is unresolved"); }
                if (child.write_ == &write_string)
                {
                    // Checked once: the rows share a representation, and a
                    // default-constructed row holds a live, empty string.
                    static_cast<void>(ValueView{destination, layout->field(builder.element_memory(0), field)}.checked_as<Str>());
                    if constexpr (Compact)
                    {
                        const auto  texts = read_text_column(reader, present_count);
                        std::size_t from = 0;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            if (!present(index)) { continue; }
                            void *row = builder.element_memory(index);
                            static_cast<Str *>(layout->field(row, field))->assign(texts[from]);
                            layout->mark_field(row, field);
                            ++from;
                        }
                        continue;
                    }
                    const auto *lengths = reader.take(block_bytes(count, sizeof(std::uint32_t)));
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        std::uint32_t length = 0;
                        std::memcpy(&length, lengths + index * sizeof(std::uint32_t), sizeof(length));
                        if (!present(index))
                        {
                            if (length != 0) { throw std::runtime_error("binary codec: an unset string has a length"); }
                            continue;
                        }
                        const auto *characters = reader.take(length);
                        void       *row = builder.element_memory(index);
                        static_cast<Str *>(layout->field(row, field))->assign(reinterpret_cast<const char *>(characters), length);
                        layout->mark_field(row, field);
                    }
                    continue;
                }
                for (std::size_t index = 0; index < count; ++index)
                {
                    if (!present(index)) { continue; }
                    Value item = child.read(reader);
                    void *row = builder.element_memory(index);
                    destination.ops_ref().move_assign_from(destination, layout->field(row, field), item.view().binding(),
                                                           const_cast<void *>(item.view().data()));
                    layout->mark_field(row, field);
                }
            }
            ListStorage storage = builder.build_storage();
            return adopt_storage(result_binding, storage);
        }

        // --- Compact: atoms ------------------------------------------------------
        // ``Compact`` (RFC 0040) is for bytes that are stored. An integer, a
        // duration and an enum ordinal are almost always small, so each is a
        // varint. An instant is not small -- microseconds since the epoch fill
        // seven bytes -- so alone it stays eight bytes, but it is marked an
        // integer because a column of instants is what delta encoding is for.
        // Each form is its own function so that a column can tell, from the
        // converter it was handed, what kind of atom it holds.

        [[nodiscard]] inline std::uint64_t zigzag(std::int64_t value) noexcept
        {
            return (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(value >> 63);
        }

        [[nodiscard]] inline std::int64_t unzigzag(std::uint64_t value) noexcept
        {
            return static_cast<std::int64_t>(value >> 1) ^ -static_cast<std::int64_t>(value & 1);
        }

        [[nodiscard]] inline std::int64_t load_int(const void *memory) noexcept
        {
            std::int64_t value;
            std::memcpy(&value, memory, sizeof(value));
            return value;
        }

        void write_int_atom(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            write_varint(zigzag(load_int(view.data())), writer.out);
        }

        Value read_int_atom(const BinaryConverter &self, BinaryReader &reader)
        {
            const std::int64_t value = unzigzag(read_varint(reader));
            Value              result{self.binding};
            std::memcpy(result.begin_mutation().mutable_data(), &value, sizeof(value));
            return result;
        }

        // An ordinal is read as the unsigned image of its storage, whatever the
        // enum's own signedness, so every value round trips.
        void write_ordinal_atom(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            std::uint64_t ordinal = 0;
            std::memcpy(&ordinal, view.data(), self.atom_size);
            write_varint(ordinal, writer.out);
        }

        Value read_ordinal_atom(const BinaryConverter &self, BinaryReader &reader)
        {
            const std::uint64_t ordinal = read_varint(reader);
            if (self.atom_size < sizeof(ordinal) && (ordinal >> (8 * self.atom_size)) != 0)
                throw std::runtime_error("binary codec: enum ordinal does not fit its storage");
            Value result{self.binding};
            std::memcpy(result.begin_mutation().mutable_data(), &ordinal, self.atom_size);
            return result;
        }

        // --- Compact: a column of atoms ------------------------------------------
        // Wherever ``Compact`` meets a run of one fixed-width atom -- a list, a
        // set, a map's keys, one field of many rows -- it writes a column: an
        // encoding byte chosen from one look at the values, then the values.
        //
        //   raw       the atoms back to back
        //   varint    integers, each a zig-zag varint
        //   delta     integers: the first, then each step from the one before --
        //             what sorted keys and timestamps want
        //   constant  one atom, when they are all the same
        //   bits      booleans, eight to the byte
        //
        // Floating point is raw or constant; it is left to block compression.

        enum class ColumnEncoding : std::uint8_t
        {
            Raw = 0,
            Varint = 1,
            Delta = 2,
            Constant = 3,
            Bits = 4,
        };

        using AtomClass = BinaryConverter::ColumnAtom;

        [[nodiscard]] AtomClass atom_class(const BinaryConverter &atom) noexcept
        {
            return atom.column_atom;
        }

        [[nodiscard]] inline std::size_t varint_bytes(std::uint64_t value) noexcept
        {
            std::size_t bytes = 1;
            while (value >= 0x80)
            {
                value >>= 7;
                ++bytes;
            }
            return bytes;
        }

        /** Write ``count`` atoms of ``width`` bytes, already gathered into ``block``, as a column. */
        void write_column_bytes(AtomClass kind, std::size_t width, std::string_view block, std::size_t count,
                                std::string &out)
        {
            if (count == 0) { return; }
            const bool constant = [&] {
                for (std::size_t index = 1; index < count; ++index)
                {
                    if (std::memcmp(block.data(), block.data() + index * width, width) != 0) { return false; }
                }
                return true;
            }();
            // The framed-reader budget accounts for up to sixteen atoms per
            // byte, including repeated row/field work charges. Every
            // other encoding spends at least a bit per element, so what it
            // writes is always within that; ``constant`` alone can name any
            // number of elements in a handful of bytes, and a value that was
            // written must be readable. So it is used only while it stays
            // inside the budget. A longer run of one integer is a delta column
            // of zero steps, and block compression flattens what is left.
            if (constant && count > 1 && count <= 16 * (1 + width))
            {
                out.push_back(static_cast<char>(ColumnEncoding::Constant));
                out.append(block.data(), width);
                return;
            }

            switch (kind)
            {
                case AtomClass::Boolean: {
                    out.push_back(static_cast<char>(ColumnEncoding::Bits));
                    const std::size_t at = out.size();
                    out.append(bitmap_bytes(count), '\0');
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        if (block[index] == 0) { continue; }
                        out[at + index / 8] = static_cast<char>(static_cast<unsigned char>(out[at + index / 8]) |
                                                                (1u << (index % 8)));
                    }
                    return;
                }
                case AtomClass::Integer: {
                    // One pass prices both forms; differences wrap, so every
                    // pair of values has a step and decoding adds it back.
                    std::size_t  as_varint = 0;
                    std::size_t  as_delta = 0;
                    std::int64_t previous = 0;
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        const std::int64_t value = load_int(block.data() + index * width);
                        as_varint += varint_bytes(zigzag(value));
                        const auto step = static_cast<std::int64_t>(static_cast<std::uint64_t>(value) -
                                                                    static_cast<std::uint64_t>(previous));
                        as_delta += varint_bytes(zigzag(step));
                        previous = value;
                    }
                    const std::size_t as_raw = block.size();
                    if (as_delta < as_varint && as_delta < as_raw)
                    {
                        out.push_back(static_cast<char>(ColumnEncoding::Delta));
                        previous = 0;
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            const std::int64_t value = load_int(block.data() + index * width);
                            write_varint(zigzag(static_cast<std::int64_t>(static_cast<std::uint64_t>(value) -
                                                                          static_cast<std::uint64_t>(previous))),
                                         out);
                            previous = value;
                        }
                        return;
                    }
                    if (as_varint < as_raw)
                    {
                        out.push_back(static_cast<char>(ColumnEncoding::Varint));
                        for (std::size_t index = 0; index < count; ++index)
                        {
                            write_varint(zigzag(load_int(block.data() + index * width)), out);
                        }
                        return;
                    }
                    break;
                }
                case AtomClass::Opaque: break;
            }
            out.push_back(static_cast<char>(ColumnEncoding::Raw));
            out.append(block);
        }

        void write_column_block(const BinaryConverter &atom, std::string_view block, std::size_t count, std::string &out)
        {
            write_column_bytes(atom_class(atom), atom.atom_size, block, count, out);
        }

        /** Gather ``count`` atoms through ``atom_at`` and write them as a column. */
        template <typename AtomAt>
        void write_column(const BinaryConverter &atom, std::size_t count, AtomAt &&atom_at, std::string &out)
        {
            std::string block;
            append_block(block, count, atom.atom_size, std::forward<AtomAt>(atom_at));
            write_column_block(atom, block, count, out);
        }

        /** Read a column of ``count`` atoms into ``block``, as the atoms back to back. */
        void read_column_bytes(AtomClass kind, std::size_t width, BinaryReader &reader, std::size_t count,
                               std::string &block)
        {
            block.clear();
            if (count == 0) { return; }
            reader.consume_work(count);
            const auto encoding = std::to_integer<std::uint8_t>(*reader.take(1));
            const auto        check_boolean = [&](const std::byte *bytes, std::size_t values) {
                if (kind != AtomClass::Boolean) { return; }
                for (std::size_t index = 0; index < values; ++index)
                {
                    if (std::to_integer<unsigned>(bytes[index]) > 1)
                        throw std::runtime_error("binary codec: invalid boolean representation");
                }
            };
            switch (static_cast<ColumnEncoding>(encoding))
            {
                case ColumnEncoding::Raw: {
                    const auto *bytes = reader.take(block_bytes(count, width));
                    check_boolean(bytes, count);
                    block.assign(reinterpret_cast<const char *>(bytes), count * width);
                    return;
                }
                case ColumnEncoding::Constant: {
                    const auto *bytes = reader.take(width);
                    check_boolean(bytes, 1);
                    block.resize(block_bytes(count, width));
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        std::memcpy(block.data() + index * width, bytes, width);
                    }
                    return;
                }
                case ColumnEncoding::Bits: {
                    if (kind != AtomClass::Boolean) { break; }
                    const auto *bits = reader.take(bitmap_bytes(count));
                    block.resize(count);
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        block[index] = (std::to_integer<unsigned>(bits[index / 8]) & (1u << (index % 8))) != 0 ? '\1' : '\0';
                    }
                    return;
                }
                case ColumnEncoding::Varint:
                case ColumnEncoding::Delta: {
                    if (kind != AtomClass::Integer) { break; }
                    const bool delta = static_cast<ColumnEncoding>(encoding) == ColumnEncoding::Delta;
                    block.resize(block_bytes(count, width));
                    std::int64_t previous = 0;
                    for (std::size_t index = 0; index < count; ++index)
                    {
                        std::int64_t value = unzigzag(read_varint(reader));
                        if (delta)
                        {
                            value = static_cast<std::int64_t>(static_cast<std::uint64_t>(previous) +
                                                              static_cast<std::uint64_t>(value));
                            previous = value;
                        }
                        std::memcpy(block.data() + index * width, &value, sizeof(value));
                    }
                    return;
                }
            }
            throw std::runtime_error(fmt::format("binary codec: column encoding {} is not one for this atom", encoding));
        }

        void read_column(const BinaryConverter &atom, BinaryReader &reader, std::size_t count, std::string &block)
        {
            read_column_bytes(atom_class(atom), atom.atom_size, reader, count, block);
        }

        // Non-negative counts and positions, as a column of integers.
        void write_size_column(const std::vector<std::int64_t> &values, std::string &out)
        {
            write_column_bytes(AtomClass::Integer, sizeof(std::int64_t),
                               {reinterpret_cast<const char *>(values.data()), values.size() * sizeof(std::int64_t)},
                               values.size(), out);
        }

        [[nodiscard]] std::vector<std::size_t> read_size_column(BinaryReader &reader, std::size_t count)
        {
            std::string block;
            read_column_bytes(AtomClass::Integer, sizeof(std::int64_t), reader, count, block);
            std::vector<std::size_t> values(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const std::int64_t value = load_int(block.data() + index * sizeof(std::int64_t));
                if (value < 0) { throw std::runtime_error("binary codec: negative size in a column"); }
                values[index] = static_cast<std::size_t>(value);
            }
            return values;
        }

        // --- Compact: a column of text -------------------------------------------
        // The strings of one field across many rows repeat far more often than
        // not -- a symbol, a venue, a status -- so when few are distinct the
        // column is a dictionary and one index per row. The choice is made from
        // the one pass that gathers them: the dictionary is abandoned as soon
        // as it holds more than a quarter of the rows.

        enum class TextEncoding : std::uint8_t
        {
            Plain = 0,
            Dictionary = 1,
        };

        void write_text_run(const std::vector<std::string_view> &texts, std::string &out)
        {
            std::vector<std::int64_t> lengths;
            lengths.reserve(texts.size());
            std::size_t characters = 0;
            for (const auto text : texts)
            {
                lengths.push_back(static_cast<std::int64_t>(text.size()));
                characters += text.size();
            }
            write_size_column(lengths, out);
            out.reserve(out.size() + characters);
            for (const auto text : texts) { out.append(text); }
        }

        [[nodiscard]] std::vector<std::string_view> read_text_run(BinaryReader &reader, std::size_t count)
        {
            const auto lengths = read_size_column(reader, count);
            std::vector<std::string_view> texts;
            texts.reserve(count);
            for (const auto length : lengths)
            {
                const auto *characters = reader.take(length);
                texts.emplace_back(reinterpret_cast<const char *>(characters), length);
            }
            return texts;
        }

        void write_text_column(const std::vector<std::string_view> &texts, std::string &out)
        {
            if (texts.empty()) { return; }
            const std::size_t limit = std::max<std::size_t>(16, texts.size() / 4);
            ankerl::unordered_dense::map<std::string_view, std::int64_t> positions;
            std::vector<std::string_view> entries;
            std::vector<std::int64_t>     indices;
            indices.reserve(texts.size());
            bool dictionary = texts.size() >= 8;
            for (std::size_t index = 0; dictionary && index < texts.size(); ++index)
            {
                const auto [entry, added] = positions.try_emplace(texts[index], static_cast<std::int64_t>(entries.size()));
                if (added)
                {
                    entries.push_back(texts[index]);
                    dictionary = entries.size() <= limit;
                }
                indices.push_back(entry->second);
            }
            if (!dictionary)
            {
                out.push_back(static_cast<char>(TextEncoding::Plain));
                write_text_run(texts, out);
                return;
            }
            out.push_back(static_cast<char>(TextEncoding::Dictionary));
            write_varint(entries.size(), out);
            write_text_run(entries, out);
            write_size_column(indices, out);
        }

        /** The strings of a column, as views into the reader's bytes. */
        [[nodiscard]] std::vector<std::string_view> read_text_column(BinaryReader &reader, std::size_t count)
        {
            if (count == 0) { return {}; }
            reader.consume_work(count);
            const auto encoding = std::to_integer<std::uint8_t>(*reader.take(1));
            if (encoding == static_cast<std::uint8_t>(TextEncoding::Plain)) { return read_text_run(reader, count); }
            if (encoding != static_cast<std::uint8_t>(TextEncoding::Dictionary))
                throw std::runtime_error(fmt::format("binary codec: unknown text column encoding {}", encoding));
            const auto entry_count = static_cast<std::size_t>(read_varint(reader));
            reader.consume_work(entry_count);
            const auto entries = read_text_run(reader, entry_count);
            const auto indices = read_size_column(reader, count);
            std::vector<std::string_view> texts;
            texts.reserve(count);
            for (const auto index : indices)
            {
                if (index >= entries.size()) { throw std::runtime_error("binary codec: text index is outside its dictionary"); }
                texts.push_back(entries[index]);
            }
            return texts;
        }

        [[nodiscard]] ElementSpan block_span(const BinaryConverter &atom, const std::string &block, std::size_t count)
        {
            return ElementSpan{.bytes = block.data(), .size = count, .stride = atom.atom_size, .plan = atom.binding.plan()};
        }

        // --- Compact: containers of atoms ----------------------------------------

        void write_column_sequence(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            const auto *ops = specialized_view_detail::checked_indexed_ops(view.binding(), "binary codec");
            const auto *memory = view.data();
            const std::size_t count = ops->size(ops->context, memory);
            write_varint(count, writer.out);
            write_column(*self.children[0], count, [&](std::size_t index) {
                if (ops->element_valid != nullptr && !ops->element_valid(ops->context, memory, index))
                    throw std::runtime_error("binary codec: non-nullable sequence contains an unset element");
                return ops->element_at(ops->context, memory, index);
            }, writer.out);
        }

        Value read_column_list(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            std::string block;
            read_column(atom, reader, count, block);
            ListStorage storage{atom.binding, block_span(atom, block, count)};
            return adopt_storage(self.realization_bound ? self.binding : compact_list_type(atom.binding, *self.meta),
                                 storage);
        }

        Value read_column_cyclic_buffer(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            if (count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: cyclic buffer exceeds declared capacity");
            std::string block;
            read_column(atom, reader, count, block);
            CyclicBufferStorage storage{atom.binding, block_span(atom, block, count), 0};
            return adopt_storage(self.realization_bound
                                     ? self.binding
                                     : compact_cyclic_buffer_type(atom.binding, self.meta->fixed_size),
                                 storage);
        }

        Value read_column_queue(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            if (self.meta->fixed_size != 0 && count > self.meta->fixed_size)
                throw std::runtime_error("binary codec: queue exceeds declared capacity");
            std::string block;
            read_column(atom, reader, count, block);
            QueueStorage storage{atom.binding, block_span(atom, block, count)};
            return adopt_storage(self.realization_bound ? self.binding
                                                        : compact_queue_type(atom.binding, self.meta->fixed_size),
                                 storage);
        }

        void write_column_set(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            const auto        set = view.as_set();
            const std::size_t count = set.size();
            const auto       &atom = *self.children[0];
            write_varint(count, writer.out);
            std::string block;
            block.reserve(block_bytes(count, atom.atom_size));
            for (const auto element : set) { block.append(static_cast<const char *>(element.data()), atom.atom_size); }
            if (block.size() != count * atom.atom_size)
                throw std::logic_error("binary codec: set size disagrees with its elements");
            write_column_block(atom, block, count, writer.out);
        }

        Value read_column_set(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &atom = *self.children[0];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            std::string block;
            read_column(atom, reader, count, block);
            SetStorage storage{atom.binding, block_span(atom, block, count)};
            if (storage.size() != count) { throw std::runtime_error("binary codec: set repeats an element"); }
            return adopt_storage(self.realization_bound ? self.binding : compact_set_type(atom.binding), storage);
        }

        // Keys column, presence flag and bitmap, then a values column holding
        // only the values that are set.
        void write_column_map(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto             &out = writer.out;
            const auto        map = view.as_map();
            const std::size_t count = map.size();
            const auto       &key = *self.children[0];
            const auto       &value = *self.children[1];
            write_varint(count, out);

            std::string keys;
            std::string values;
            std::string bitmap(bitmap_bytes(count), '\0');
            std::size_t index = 0;
            std::size_t present = 0;
            for (const auto entry : map)
            {
                if (index == count) { throw std::logic_error("binary codec: map size disagrees with its entries"); }
                keys.append(static_cast<const char *>(entry.first.data()), key.atom_size);
                if (entry.second.has_value())
                {
                    bitmap[index / 8] = static_cast<char>(static_cast<unsigned char>(bitmap[index / 8]) | (1u << (index % 8)));
                    values.append(static_cast<const char *>(entry.second.data()), value.atom_size);
                    ++present;
                }
                ++index;
            }
            if (index != count) { throw std::logic_error("binary codec: map size disagrees with its entries"); }
            write_column_block(key, keys, count, out);
            out.push_back(present == count ? '\0' : '\1');
            if (present != count) { out.append(bitmap); }
            write_column_block(value, values, present, out);
        }

        Value read_column_map(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto &key = *self.children[0];
            const auto &value = *self.children[1];
            const auto  count = static_cast<std::size_t>(read_varint(reader));
            std::string keys;
            read_column(key, reader, count, keys);
            const auto flag = std::to_integer<unsigned>(*reader.take(1));
            if (flag > 1) { throw std::runtime_error("binary codec: invalid map value presence flag"); }
            std::vector<bool> validity;
            std::size_t       present = count;
            if (flag == 1)
            {
                const auto *bitmap = reader.take(bitmap_bytes(count));
                validity.resize(count);
                present = 0;
                for (std::size_t index = 0; index < count; ++index)
                {
                    validity[index] = (std::to_integer<unsigned>(bitmap[index / 8]) & (1u << (index % 8))) != 0;
                    present += validity[index] ? 1 : 0;
                }
            }
            std::string set_values;
            read_column(value, reader, present, set_values);
            // The storage wants a value slot per key, so the unset ones are
            // given a zeroed place between the ones that were written.
            std::string values;
            if (flag == 0) { values = std::move(set_values); }
            else
            {
                values.assign(block_bytes(count, value.atom_size), '\0');
                std::size_t from = 0;
                for (std::size_t index = 0; index < count; ++index)
                {
                    if (!validity[index]) { continue; }
                    std::memcpy(values.data() + index * value.atom_size, set_values.data() + from * value.atom_size,
                                value.atom_size);
                    ++from;
                }
            }
            MapStorage storage{key.binding, value.binding, block_span(key, keys, count), block_span(value, values, count),
                               std::move(validity)};
            if (storage.size() != count) { throw std::runtime_error("binary codec: map repeats a key"); }
            return adopt_storage(self.realization_bound ? self.binding : compact_map_type(key.binding, value.binding),
                                 storage);
        }

        bool is_fixed_atom(const BinaryConverter &converter) noexcept
        {
            return converter.atom_size != 0 &&
                   (converter.write_ == &write_atom || converter.write_ == &write_int_atom ||
                    converter.write_ == &write_ordinal_atom);
        }

        // --- a scalar's registered wire form (RFC 0040) --------------------------
        // Framed by the codec with its length. The form is somebody else's code:
        // one that read a byte too many would otherwise corrupt everything
        // after it, and the length is also what would let a reader that does
        // not know the form keep the bytes whole.

        void write_registered_atom(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            std::string bytes;
            self.atom_ops->write(view.data(), self.atom_ops->context, bytes);
            write_varint(bytes.size(), writer.out);
            writer.out.append(bytes);
        }

        Value read_registered_atom(const BinaryConverter &self, BinaryReader &reader)
        {
            auto  bytes = reader.subreader(static_cast<std::size_t>(read_varint(reader)));
            Value result{self.binding};
            self.atom_ops->read(const_cast<void *>(result.view().data()), self.atom_ops->context, bytes);
            if (bytes.remaining() != 0)
                throw std::runtime_error(fmt::format("binary codec: the wire form of '{}' left {} bytes unread",
                                                     self.meta->name(), bytes.remaining()));
            return result;
        }

        // --- Any -----------------------------------------------------------
        // The box names its content's schema by the session's table index, one
        // past it so that zero is the empty box, and the content follows under
        // that schema. The schema is not known until the value is met, which
        // is why this is the one kind that cannot be written without a session.

        void write_any(const BinaryConverter &, const ValueView &view, BinaryWriter &writer)
        {
            const auto box = view.as_any();
            if (!box.has_value())
            {
                write_varint(0, writer.out);
                return;
            }
            if (writer.session == nullptr)
            {
                throw std::logic_error(
                    "binary codec: an Any value names its schema in a session's table; encode it with "
                    "encode_binary_frame or a BinaryEncodeSession, not to_binary_string");
            }
            const std::size_t index = writer.session->value_ref(box.value_schema());
            write_varint(index + 1, writer.out);
            writer.session->converter_at(index).write(box.get(), writer);
        }

        Value read_any(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto tag = read_varint(reader);
            Value      result{self.binding};
            if (tag == 0) { return result; }
            if (reader.session == nullptr)
            {
                throw std::logic_error(
                    "binary codec: an Any value names its schema in a session's table; decode it with "
                    "decode_binary_frame or a BinaryDecodeSession, not from_binary_string");
            }
            Value content = reader.session->converter_at(static_cast<std::size_t>(tag - 1)).read(reader);
            result.as_any().begin_mutation().set(std::move(content));
            return result;
        }

        std::uint64_t hash_any(const BinaryConverter &, const ValueView &)
        {
            // A portable hash assigns a key to a worker and has no session to
            // name a schema in. An Any is not a usable partition key.
            throw std::logic_error("binary codec: an Any value has no portable hash and cannot be a partition key");
        }

        void write_polymorphic(const BinaryConverter &self, const ValueView &view, BinaryWriter &writer)
        {
            auto &out = writer.out;
            const auto concrete = view.concrete();
            const auto found = self.write_alternatives.find(concrete.schema());
            if (found == self.write_alternatives.end())
                throw std::invalid_argument("binary codec: concrete Bundle is outside the captured realization");
            const auto name = concrete.schema()->name();
            write_varint(name.size(), out);
            out.append(name);
            found->second->write(concrete, writer);
        }

        Value read_polymorphic(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto size = read_varint(reader);
            const auto *data = reader.take(size);
            const std::string_view name{reinterpret_cast<const char *>(data), size};
            const auto found = self.read_alternatives.find(name);
            if (found == self.read_alternatives.end())
                throw std::invalid_argument("binary codec: unknown concrete Bundle in captured realization");
            auto concrete = found->second->read(reader);
            return Value{self.binding, concrete.view()};
        }

        constexpr std::uint64_t hash_seed = 14695981039346656037ULL;

        std::uint64_t hash_bytes(std::string_view bytes)
        {
            std::uint64_t result = hash_seed;
            for (const unsigned char byte : bytes) result = (result ^ byte) * 1099511628211ULL;
            return result;
        }

        std::uint64_t hash_combine(std::uint64_t left, std::uint64_t right)
        {
            right ^= right >> 30;
            right *= 0xbf58476d1ce4e5b9ULL;
            right ^= right >> 27;
            right *= 0x94d049bb133111ebULL;
            right ^= right >> 31;
            return left ^ (right + 0x9e3779b97f4a7c15ULL + (left << 6) + (left >> 2));
        }

        std::uint64_t hash_encoded(const BinaryConverter &self, const ValueView &view)
        {
            std::string bytes;
            self.write(view, bytes);
            return hash_bytes(bytes);
        }

        std::uint64_t hash_atom(const BinaryConverter &self, const ValueView &view)
        {
            if ((self.binding.ops() == &ops_for<Float>() && view.checked_as<Float>() == 0)
                || (self.binding.ops() == &ops_for<float>() && view.checked_as<float>() == 0))
                return hash_seed; // +0 and -0 compare equal.
            return hash_bytes({static_cast<const char *>(view.data()), self.atom_size});
        }

        std::uint64_t hash_string(const BinaryConverter &, const ValueView &view)
        {
            return hash_bytes(view.checked_as<Str>());
        }

        std::uint64_t hash_blob(const BinaryConverter &, const ValueView &view)
        {
            return hash_bytes(view.checked_as<Bytes>().data);
        }

        std::uint64_t hash_zone(const BinaryConverter &, const ValueView &view)
        {
            const auto zone = view.checked_as<ZoneId>();
            return hash_bytes(zone.valid() ? zone.name() : std::string_view{});
        }

        std::uint64_t hash_composite(const BinaryConverter &self, const ValueView &view)
        {
            const auto concrete = view.concrete();
            if (concrete.schema() != self.meta)
                throw std::logic_error("binary codec: derived Bundle hash requires a bound converter");
            const auto indexed = concrete.as_indexed_view();
            auto hash = hash_seed;
            for (std::size_t i = 0; i < self.children.size(); ++i)
            {
                const auto field = indexed.at(i);
                hash = hash_combine(hash, field.has_value() ? self.children[i]->hash_(*self.children[i], field) : 0);
            }
            return hash;
        }

        std::uint64_t hash_sequence(const BinaryConverter &self, const ValueView &view)
        {
            const auto indexed = view.as_indexed_view();
            auto hash = hash_combine(hash_seed, indexed.size());
            for (std::size_t i = 0; i < indexed.size(); ++i)
            {
                const auto item = indexed.at(i);
                hash = hash_combine(hash, item.has_value() ? self.children[0]->hash_(*self.children[0], item) : 0);
            }
            return hash;
        }

        std::uint64_t hash_set(const BinaryConverter &self, const ValueView &view)
        {
            std::uint64_t sum = 0, xor_value = 0, count = 0;
            for (const auto item : view.as_set())
            {
                const auto hash = hash_combine(hash_seed, self.children[0]->hash_(*self.children[0], item));
                sum += hash;
                xor_value ^= hash;
                ++count;
            }
            return hash_combine(hash_combine(sum, xor_value), count);
        }

        std::uint64_t hash_map(const BinaryConverter &self, const ValueView &view)
        {
            std::uint64_t sum = 0, xor_value = 0, count = 0;
            for (const auto item : view.as_map())
            {
                const auto key = self.children[0]->hash_(*self.children[0], item.first);
                const auto value = item.second.has_value() ? self.children[1]->hash_(*self.children[1], item.second) : 0;
                const auto hash = hash_combine(key, value);
                sum += hash;
                xor_value ^= hash;
                ++count;
            }
            return hash_combine(hash_combine(sum, xor_value), count);
        }

        std::uint64_t hash_indirect(const BinaryConverter &self, const ValueView &view)
        {
            const auto concrete = view.concrete();
            return !concrete.has_value() || concrete.schema() == self.meta ? 0
                : self.children[0]->hash_(*self.children[0], concrete);
        }

        std::uint64_t hash_polymorphic(const BinaryConverter &self, const ValueView &view)
        {
            const auto concrete = view.concrete();
            const auto found = self.write_alternatives.find(concrete.schema());
            if (found == self.write_alternatives.end())
                throw std::invalid_argument("binary codec: concrete Bundle hash is outside the captured realization");
            return hash_combine(hash_bytes(concrete.schema()->name()), found->second->hash_(*found->second, concrete));
        }

        // --- synthesis -----------------------------------------------------

        TypeSystemMutex                                                          converters_mutex;
        ankerl::unordered_dense::map<const ValueTypeMetaData *,
                                     std::unique_ptr<BinaryConverter>>           converters;
        // Wire forms registered with a scalar, and scalars declared portable.
        // A converter holds the address of its form, and a run holds its
        // converters for as long as it lives, so a form is never freed by
        // being replaced: ``atom_forms`` names the current one and
        // ``retired_forms`` keeps every earlier one alive beside it. The same
        // goes for interned converters that a registration makes stale. Both
        // are build-time events, and rare, so what is kept is small; only a
        // registry reset, which invalidates every handle by contract, frees.
        ankerl::unordered_dense::map<const ValueTypeMetaData *, const BinaryAtomOps *> atom_forms;
        std::vector<std::unique_ptr<BinaryAtomOps>>                               owned_forms;
        std::vector<std::unique_ptr<BinaryConverter>>                             retired_converters;
        ankerl::unordered_dense::set<const ValueTypeMetaData *>                   portable_atoms;
        // Schemas whose binding has already warned that it will pickle.
        ankerl::unordered_dense::set<const ValueTypeMetaData *>                   opaque_warned;

        const BinaryConverter *converter_for_locked(const ValueTypeMetaData *meta,
                                                     std::vector<const ValueTypeMetaData *> &created)
        {
            if (meta == nullptr) { throw std::logic_error("binary codec: null schema"); }
            if (const auto found = converters.find(meta); found != converters.end())
            {
                return found->second.get();
            }

            auto  owned = std::make_unique<BinaryConverter>();
            auto *raw   = owned.get();
            converters.emplace(meta, std::move(owned));
            created.push_back(meta);
            auto unwind = make_scope_exit([&] { converters.erase(meta); });

            raw->meta    = meta;
            raw->hash_ = &hash_encoded;
            raw->binding = ValuePlanFactory::instance().type_for(meta);

            if (meta->is_indirect())
            {
                raw->children.push_back(converter_for_locked(meta->element_type, created));
                raw->write_ = &write_indirect;
                raw->hash_ = &hash_indirect;
                raw->read_ = &read_indirect;
                unwind.release();
                return raw;
            }

            switch (meta->value_kind())
            {
                case ValueTypeKind::Atomic: {
                    if (TypeRegistry::instance().is_frame(meta))
                    {
                        raw->write_ = &write_frame;
                        raw->read_ = &read_frame;
                        break;
                    }
                    if (TypeRegistry::instance().is_series(meta))
                    {
                        raw->write_ = &write_series;
                        raw->read_ = &read_series;
                        break;
                    }
                    if (meta == scalar_descriptor<ZoneId>::value_meta())
                    {
                        raw->write_ = &write_zone_id;
                        raw->hash_ = &hash_zone;
                        raw->read_ = &read_zone_id;
                        break;
                    }
                    if (meta == scalar_descriptor<ZonedDateTime>::value_meta())
                    {
                        raw->write_ = &write_zoned_time;
                        raw->read_ = &read_zoned_time;
                        break;
                    }
                    if (meta == scalar_descriptor<Str>::value_meta())
                    {
                        raw->write_ = &write_string;
                        raw->hash_ = &hash_string;
                        raw->read_  = &read_string;
                        break;
                    }
                    if (meta == scalar_descriptor<Bytes>::value_meta())
                    {
                        raw->write_ = &write_bytes;
                        raw->hash_ = &hash_blob;
                        raw->read_  = &read_bytes;
                        break;
                    }
                    if (meta == scalar_descriptor<InstantRange>::value_meta())
                    {
                        raw->write_ = &write_range<InstantRange>;
                        raw->read_ = &read_range<InstantRange>;
                        break;
                    }
                    if (meta == scalar_descriptor<CivilDateRange>::value_meta())
                    {
                        raw->write_ = &write_range<CivilDateRange>;
                        raw->read_ = &read_range<CivilDateRange>;
                        break;
                    }
                    if (meta == scalar_descriptor<InstantRangeSet>::value_meta())
                    {
                        raw->write_ = &write_ranges<InstantRangeSet>;
                        raw->read_ = &read_ranges<InstantRangeSet>;
                        break;
                    }
                    if (meta == scalar_descriptor<CivilDateRangeSet>::value_meta())
                    {
                        raw->write_ = &write_ranges<CivilDateRangeSet>;
                        raw->read_ = &read_ranges<CivilDateRangeSet>;
                        break;
                    }
                    // Not built in. A form registered with the scalar comes
                    // first; then a declaration that its storage image is its
                    // wire form; then the rule for plain numeric storage.
                    if (const auto form = atom_forms.find(meta); form != atom_forms.end())
                    {
                        raw->atom_ops = form->second;
                        raw->write_ = &write_registered_atom;
                        raw->read_ = &read_registered_atom;
                        break;
                    }
                    if (!meta->has(ValueTypeFlags::TriviallyCopyable)) { throw BinaryWireFormError{std::string{meta->name()}}; }
                    if (portable_atoms.contains(meta))
                    {
                        const auto *declared = raw->binding.plan();
                        if (declared == nullptr)
                        {
                            throw std::logic_error(
                                fmt::format("binary codec: atomic '{}' has no storage plan", meta->name()));
                        }
                        raw->atom_size = declared->layout.size;
                        raw->write_    = &write_atom;
                        raw->hash_     = &hash_atom;
                        raw->read_     = &read_atom;
                        break;
                    }
                    // Trivial copyability alone does not establish a wire
                    // contract: native handles and pointer-bearing structs can
                    // also be trivial. Accept numeric/chrono storage, enums,
                    // and the remaining explicit standard temporal atoms.
                    if (!meta->is_buffer_compatible() && !meta->is_enum()
                        && meta != scalar_descriptor<Time>::value_meta()
                        && meta != scalar_descriptor<CivilDateTime>::value_meta()
                        && meta != scalar_descriptor<Period>::value_meta())
                        throw BinaryWireFormError{std::string{meta->name()}};
                    const auto *plan = raw->binding.plan();
                    if (plan == nullptr)
                    {
                        throw std::logic_error(
                            fmt::format("binary codec: atomic '{}' has no storage plan", meta->name()));
                    }
                    // Wider than eight bytes is not plain numeric storage.
                    if (meta->is_buffer_compatible() && plan->layout.size > sizeof(std::uint64_t))
                        throw BinaryWireFormError{std::string{meta->name()}};
                    raw->atom_size = plan->layout.size;
                    raw->write_    = &write_atom;
                    raw->hash_     = &hash_atom;
                    raw->read_     = &read_atom;
                    break;
                }
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        raw->children.push_back(converter_for_locked(meta->fields[i].type, created));
                    }
                    raw->write_ = &write_composite;
                    raw->hash_ = &hash_composite;
                    raw->read_  = &read_composite;
                    break;
                }
                case ValueTypeKind::List: {
                    raw->children.push_back(converter_for_locked(meta->element_type, created));
                    raw->write_ = &write_list;
                    raw->hash_ = &hash_sequence;
                    raw->read_  = &read_list;
                    break;
                }
                case ValueTypeKind::Set: {
                    raw->children.push_back(converter_for_locked(meta->element_type, created));
                    raw->write_ = &write_set;
                    raw->hash_ = &hash_set;
                    raw->read_  = &read_set;
                    break;
                }
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue: {
                    raw->children.push_back(converter_for_locked(meta->element_type, created));
                    raw->write_ = &write_buffer;
                    raw->hash_ = &hash_sequence;
                    raw->read_ = meta->value_kind() == ValueTypeKind::CyclicBuffer ? &read_cyclic_buffer : &read_queue;
                    break;
                }
                case ValueTypeKind::Map: {
                    raw->children.push_back(converter_for_locked(meta->key_type, created));
                    raw->children.push_back(converter_for_locked(meta->element_type, created));
                    raw->write_ = &write_map;
                    raw->hash_ = &hash_map;
                    raw->read_  = &read_map;
                    break;
                }
                case ValueTypeKind::Any: {
                    raw->write_ = &write_any;
                    raw->hash_ = &hash_any;
                    raw->read_ = &read_any;
                    break;
                }
                default:
                    throw std::logic_error(
                        fmt::format("binary codec: unsupported value kind for '{}'", meta->name()));
            }
            unwind.release();
            return raw;
        }
    }  // namespace

    struct BoundBinaryConverter::Impl
    {
        std::shared_ptr<const TypeRealizationSnapshot> realization{};
        std::unordered_map<const ValueTypeMetaData *, std::unique_ptr<BinaryConverter>> declared{};
        std::unordered_map<const ValueTypeMetaData *, std::unique_ptr<BinaryConverter>> exact{};
        const BinaryConverter *root{};
        BinaryProfile profile{BinaryProfile::Compact};
        std::uint8_t revision{0};
        bool needs_session{false};

        /** True for an atom the block forms can carry: fixed width, trivially
            copyable, and stored at exactly its wire width. */
        [[nodiscard]] static bool is_block_atom(const BinaryConverter &converter) noexcept
        {
            const auto *plan = converter.binding.plan();
            return converter.atom_size != 0 && converter.write_ == &write_atom && plan != nullptr &&
                   plan->trivially_copyable && plan->layout.size == converter.atom_size;
        }

        /** True for a row the column form can carry: an exact composite whose
            storage the reader can fill in place. A polymorphic, indirect or
            wrapped row keeps the field-wise form. */
        [[nodiscard]] static bool is_column_row(const BinaryConverter &converter)
        {
            return converter.write_ == &write_composite && !converter.meta->is_indirect() &&
                   CompositeFieldLayout::of(converter.binding).has_value();
        }

        /** Swap in the ``Fast`` revision 1 forms where the shape allows one;
            every other shape keeps the field-wise form. */
        static void select_fast_forms(BinaryConverter &converter)
        {
            if (converter.meta->is_indirect() || converter.write_ == &write_polymorphic) { return; }
            switch (converter.meta->value_kind())
            {
                case ValueTypeKind::List:
                    if (converter.meta->has(ValueTypeFlags::Nullable)) { return; }
                    if (is_block_atom(*converter.children[0]))
                    {
                        converter.write_ = &write_atom_sequence;
                        converter.read_ = &read_atom_list;
                    }
                    else if (is_column_row(*converter.children[0]))
                    {
                        converter.write_ = &write_row_columns<false>;
                        converter.read_ = &read_row_columns<false>;
                    }
                    return;
                case ValueTypeKind::CyclicBuffer:
                    if (!is_block_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_atom_sequence;
                    converter.read_ = &read_atom_cyclic_buffer;
                    return;
                case ValueTypeKind::Queue:
                    if (!is_block_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_atom_sequence;
                    converter.read_ = &read_atom_queue;
                    return;
                case ValueTypeKind::Set:
                    if (!is_block_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_atom_set;
                    converter.read_ = &read_atom_set;
                    return;
                case ValueTypeKind::Map:
                    if (!is_block_atom(*converter.children[0]) || !is_block_atom(*converter.children[1])) { return; }
                    converter.write_ = &write_atom_map;
                    converter.read_ = &read_atom_map;
                    return;
                default: return;
            }
        }

        /** ``Compact`` revision 1: varint atoms, and a column wherever there
            is a run of one fixed-width atom. */
        static void select_compact_forms(BinaryConverter &converter)
        {
            if (converter.meta->is_indirect() || converter.write_ == &write_polymorphic) { return; }
            const auto column_atom = [](const BinaryConverter &child) {
                const auto *plan = child.binding.plan();
                return is_fixed_atom(child) && plan != nullptr && plan->trivially_copyable &&
                       plan->layout.size == child.atom_size;
            };
            switch (converter.meta->value_kind())
            {
                case ValueTypeKind::Atomic:
                    if (converter.write_ != &write_atom) { return; }
                    // An enum's payload is its member's assigned integer, held
                    // as an ``Int``, so it is an integer here too.
                    if (converter.atom_size == sizeof(std::int64_t) &&
                        (converter.meta == scalar_descriptor<Int>::value_meta() ||
                         converter.meta == scalar_descriptor<TimeDelta>::value_meta() || converter.meta->is_enum()))
                    {
                        converter.write_ = &write_int_atom;
                        converter.read_ = &read_int_atom;
                        converter.column_atom = AtomClass::Integer;
                    }
                    else if (converter.atom_size == sizeof(std::int64_t) &&
                             converter.meta == scalar_descriptor<DateTime>::value_meta())
                    {
                        converter.column_atom = AtomClass::Integer;
                    }
                    else if (converter.binding.ops() == &ops_for<Bool>()) { converter.column_atom = AtomClass::Boolean; }
                    else if (converter.meta->is_enum() && converter.atom_size <= sizeof(std::uint64_t))
                    {
                        converter.write_ = &write_ordinal_atom;
                        converter.read_ = &read_ordinal_atom;
                    }
                    return;
                case ValueTypeKind::List:
                    if (converter.meta->has(ValueTypeFlags::Nullable)) { return; }
                    if (column_atom(*converter.children[0]))
                    {
                        converter.write_ = &write_column_sequence;
                        converter.read_ = &read_column_list;
                    }
                    else if (is_column_row(*converter.children[0]))
                    {
                        converter.write_ = &write_row_columns<true>;
                        converter.read_ = &read_row_columns<true>;
                    }
                    return;
                case ValueTypeKind::CyclicBuffer:
                    if (!column_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_column_sequence;
                    converter.read_ = &read_column_cyclic_buffer;
                    return;
                case ValueTypeKind::Queue:
                    if (!column_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_column_sequence;
                    converter.read_ = &read_column_queue;
                    return;
                case ValueTypeKind::Set:
                    if (!column_atom(*converter.children[0])) { return; }
                    converter.write_ = &write_column_set;
                    converter.read_ = &read_column_set;
                    return;
                case ValueTypeKind::Map:
                    if (!column_atom(*converter.children[0]) || !column_atom(*converter.children[1])) { return; }
                    converter.write_ = &write_column_map;
                    converter.read_ = &read_column_map;
                    return;
                default: return;
            }
        }

        /** A value that exists only as a Python object leaves no choice but to
            pickle it, so that is never an error. It is slow, large and opaque
            to every native reader, though, so say so -- once per schema, and
            in the author's terms: the class they used as a type, not the atom
            the bridge holds it in. */
        void warn_if_opaque(const ValueTypeMetaData *root_meta) const
        {
            std::string pickled;
            for (const auto *cache : {&declared, &exact})
            {
                for (const auto &[meta, converter] : *cache)
                {
                    // A class used as a type is a box that can only ever hold
                    // an object of that class.
                    if (converter->write_ == &write_any && meta->is_opaque_python()) { pickled = std::string{meta->name()}; }
                    else if (pickled.empty() && converter->atom_ops != nullptr && converter->atom_ops->opaque &&
                             meta != root_meta)
                    {
                        pickled = std::string{meta->name()};
                    }
                }
            }
            if (pickled.empty()) { return; }
            {
                const std::lock_guard guard{converters_mutex};
                if (!opaque_warned.insert(root_meta).second) { return; }
            }
            log::logger().warn(
                "binary codec: '{}' holds Python objects ('{}'), which are pickled: slow, large and unreadable by "
                "native code. Consider giving the type a schema -- a dataclass or a CompoundScalar has one",
                root_meta->name(), pickled);
        }

        const BinaryConverter *build(const ValueTypeMetaData *meta, bool exact_type = false)
        {
            auto &cache = exact_type ? exact : declared;
            if (const auto found = cache.find(meta); found != cache.end()) return found->second.get();
            auto converter = std::make_unique<BinaryConverter>(binary_converter(meta));
            auto *raw = converter.get();
            cache.emplace(meta, std::move(converter));
            raw->binding = exact_type ? realization->exact_type_for(meta) : realization->type_for(meta);
            raw->realization_bound = true;
            raw->children.clear();
            if (!exact_type && !meta->is_indirect() && realization->is_polymorphic(meta))
            {
                raw->write_ = &write_polymorphic;
                raw->hash_ = &hash_polymorphic;
                raw->read_ = &read_polymorphic;
                for (const auto *alternative : realization->alternatives(meta))
                {
                    const auto *child = build(alternative, true);
                    raw->write_alternatives.emplace(alternative, child);
                    raw->read_alternatives.emplace(alternative->name(), child);
                }
            }
            else
            {
                for (const auto *child : binary_converter(meta).children) raw->children.push_back(build(child->meta));
            }
            if (profile == BinaryProfile::Fast && revision >= 1) { select_fast_forms(*raw); }
            if (profile == BinaryProfile::Compact && revision >= 1) { select_compact_forms(*raw); }
            // Sequence readers construct compact owning storage; publish the
            // matching binding so nested builders never pair inline plans
            // with compact container memory.
            switch (meta->value_kind())
            {
                case ValueTypeKind::List: raw->binding = compact_list_type(raw->children[0]->binding, *meta); break;
                case ValueTypeKind::Set: raw->binding = compact_set_type(raw->children[0]->binding); break;
                case ValueTypeKind::Map:
                    raw->binding = compact_map_type(raw->children[0]->binding, raw->children[1]->binding); break;
                case ValueTypeKind::CyclicBuffer:
                    raw->binding = compact_cyclic_buffer_type(raw->children[0]->binding, meta->fixed_size); break;
                case ValueTypeKind::Queue:
                    raw->binding = compact_queue_type(raw->children[0]->binding, meta->fixed_size); break;
                default: break;
            }
            return raw;
        }
    };

    BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta)
    {
        // No profile named means the bytes this function has always produced:
        // the RFC 0017 field-wise encoding, which is revision 0. Whoever calls
        // it -- and ``to_binary_string`` and ``from_binary_string`` with it --
        // has no frame to record a revision in, so its bytes must not move
        // when a profile's do.
        return bind_binary_converter(meta, BinaryProfile::Compact, 0);
    }

    std::uint8_t binary_profile_revision(BinaryProfile profile) noexcept
    {
        // Revision 0 of either profile is the RFC 0017 field-wise encoding.
        // Fast 1 (RFC 0040 stage 2): maps of fixed-width keys and values are
        // two blocks, and a list of composite rows is written by column.
        // Compact 1 (stage 3): integers, durations and enum ordinals are
        // varints, and a run of one fixed-width atom is an encoded column.
        static_cast<void>(profile);
        return 1;
    }

    BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta, BinaryProfile profile)
    {
        return bind_binary_converter(meta, profile, binary_profile_revision(profile));
    }

    BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta, BinaryProfile profile,
                                               std::uint8_t revision)
    {
        if (revision > binary_profile_revision(profile))
        {
            throw std::runtime_error(fmt::format("binary codec: {} revision {} is later than this build writes ({})",
                                                 profile == BinaryProfile::Fast ? "Fast" : "Compact", revision,
                                                 binary_profile_revision(profile)));
        }
        // A converter is synthesized field-wise and then, for a revision that
        // has them, given that profile's forms where the shape allows one.
        auto plan = std::make_shared<BoundBinaryConverter::Impl>();
        plan->profile = profile;
        plan->revision = revision;
        const auto *active = active_type_realization();
        plan->realization = active != nullptr ? active->shared_from_this()
                                             : TypeRealizationSnapshot::capture(TypeRegistry::instance());
        plan->root = plan->build(meta);
        for (const auto *cache : {&plan->declared, &plan->exact})
        {
            for (const auto &[schema, converter] : *cache)
            {
                static_cast<void>(schema);
                plan->needs_session = plan->needs_session || converter->write_ == &write_any;
            }
        }
        plan->warn_if_opaque(meta);
        return BoundBinaryConverter{std::move(plan)};
    }

    ValueTypeRef BoundBinaryConverter::binding() const noexcept
    {
        return impl_ ? impl_->root->binding : ValueTypeRef{};
    }

    BinaryProfile BoundBinaryConverter::profile() const noexcept
    {
        return impl_ ? impl_->profile : BinaryProfile::Compact;
    }

    std::uint8_t BoundBinaryConverter::revision() const noexcept
    {
        return impl_ ? impl_->revision : std::uint8_t{0};
    }

    bool BoundBinaryConverter::needs_session() const noexcept { return impl_ && impl_->needs_session; }

    std::uint64_t BoundBinaryConverter::portable_hash(const ValueView &view) const
    {
        if (!impl_) throw std::logic_error("binary codec: unbound converter");
        return impl_->root->hash_(*impl_->root, view);
    }

    void BoundBinaryConverter::write(const ValueView &view, std::string &out) const
    {
        BinaryWriter writer{out};
        write(view, writer);
    }

    void BoundBinaryConverter::write(const ValueView &view, BinaryWriter &writer) const
    {
        if (!impl_) throw std::logic_error("binary codec: unbound converter");
        impl_->root->write(view, writer);
    }

    void BoundBinaryConverter::write_run(std::span<const Value> values, BinaryWriter &writer) const
    {
        if (!impl_) throw std::logic_error("binary codec: unbound converter");
        const auto &root = *impl_->root;
        if (impl_->revision == 0 || !is_fixed_atom(root) || values.empty())
        {
            for (const auto &value : values) { root.write(value.view(), writer); }
            return;
        }
        const auto atom_at = [&](std::size_t index) {
            const auto view = values[index].view();
            if (!view.has_value() || view.schema() != root.meta)
                throw std::logic_error("binary codec: a run holds a value that is not of its schema");
            return view.data();
        };
        if (impl_->profile == BinaryProfile::Fast) { append_block(writer.out, values.size(), root.atom_size, atom_at); }
        else { write_column(root, values.size(), atom_at, writer.out); }
    }

    void BoundBinaryConverter::read_run(std::size_t count, BinaryReader &reader, std::vector<Value> &out) const
    {
        if (!impl_) throw std::logic_error("binary codec: unbound converter");
        const auto &root = *impl_->root;
        out.reserve(out.size() + count);
        if (impl_->revision == 0 || !is_fixed_atom(root) || count == 0)
        {
            for (std::size_t index = 0; index < count; ++index) { out.push_back(root.read(reader)); }
            return;
        }
        std::string block;
        if (impl_->profile == BinaryProfile::Fast)
        {
            const auto span = take_block(root, reader, count);
            block.assign(static_cast<const char *>(span.bytes), count * root.atom_size);
        }
        else { read_column(root, reader, count, block); }
        for (std::size_t index = 0; index < count; ++index)
        {
            Value value{root.binding};
            std::memcpy(value.begin_mutation().mutable_data(), block.data() + index * root.atom_size, root.atom_size);
            out.push_back(std::move(value));
        }
    }

    const ValueTypeMetaData *BoundBinaryConverter::schema() const noexcept
    {
        return impl_ ? impl_->root->meta : nullptr;
    }

    Value BoundBinaryConverter::read(BinaryReader &reader) const
    {
        if (!impl_) throw std::logic_error("binary codec: unbound converter");
        return impl_->root->read(reader);
    }

    BinaryConverter::BinaryConverter() noexcept
        : write_(&refuse_write), read_(&refuse_read), hash_(&refuse_hash) {}

    BinaryConverter::BinaryConverter(BinaryConverter &&other) noexcept : BinaryConverter()
    { swap(other); }

    BinaryConverter &BinaryConverter::operator=(BinaryConverter &&other) noexcept
    {
        if (this != &other)
        {
            BinaryConverter moved{std::move(other)};
            swap(moved);
        }
        return *this;
    }

    void BinaryConverter::swap(BinaryConverter &other) noexcept
    {
        using std::swap;
        swap(write_, other.write_);
        swap(read_, other.read_);
        swap(hash_, other.hash_);
        swap(meta, other.meta);
        swap(binding, other.binding);
        swap(atom_size, other.atom_size);
        swap(realization_bound, other.realization_bound);
        swap(children, other.children);
        swap(write_alternatives, other.write_alternatives);
        swap(read_alternatives, other.read_alternatives);
        swap(atom_ops, other.atom_ops);
        swap(column_atom, other.column_atom);
    }

    Value BinaryConverter::read(BinaryReader &reader) const
    {
        auto depth = reader.enter();
        return read_(*this, reader);
    }

    BinaryDecodeLimits binary_decode_limits_for_bytes(std::size_t bytes) noexcept
    {
        BinaryDecodeLimits limits;
        constexpr std::uint64_t work_per_byte = 16 * 6;
        const auto available = std::numeric_limits<std::uint64_t>::max() - limits.max_work;
        limits.max_work += bytes > available / work_per_byte ? available : bytes * work_per_byte;
        return limits;
    }

    void BinaryReader::consume_work(std::uint64_t count)
    {
        auto &state = budget();
        if (count > state.limits.max_work - state.work)
            throw std::runtime_error("binary codec: decode work limit exceeded");
        state.work += count;
    }

    void BinaryReader::enter_value()
    {
        auto &state = budget();
        if (state.depth >= state.limits.max_depth)
            throw std::runtime_error("binary codec: decode depth limit exceeded");
        consume_work(1);
        ++state.depth;
    }

    BinaryReader BinaryReader::subreader(std::size_t count)
    {
        const auto *bytes = take(count);
        BinaryReader child{std::string_view{reinterpret_cast<const char *>(bytes), count}};
        child.shared_ = &budget();
        child.session = session;
        return child;
    }

    const std::byte *BinaryReader::take(std::size_t count)
    {
        if (offset > buffer.size() || remaining() < count) { short_buffer(); }
        const auto *at = reinterpret_cast<const std::byte *>(buffer.data() + offset);
        offset += count;
        return at;
    }

    void write_varint(std::uint64_t value, std::string &out)
    {
        while (value >= 0x80)
        {
            out.push_back(static_cast<char>(static_cast<unsigned char>(value) | 0x80u));
            value >>= 7;
        }
        out.push_back(static_cast<char>(static_cast<unsigned char>(value)));
    }

    std::uint64_t read_varint(BinaryReader &reader)
    {
        std::uint64_t result = 0;
        unsigned      shift  = 0;
        while (true)
        {
            if (shift > 63) { throw std::runtime_error("binary codec: varint overflow"); }
            const auto byte = static_cast<unsigned char>(*reader.take(1));
            if (shift == 63 && (byte & 0xFEu) != 0)
            {
                throw std::runtime_error("binary codec: varint overflow");
            }
            result |= static_cast<std::uint64_t>(byte & 0x7Fu) << shift;
            if ((byte & 0x80u) == 0) { break; }
            shift += 7;
        }
        return result;
    }

    const BinaryConverter &binary_converter(const ValueTypeMetaData *meta)
    {
        // Build-time machinery, so the lock is sanctioned; a per-tick caller
        // resolves once and keeps the result (CLAUDE.md, single-threaded
        // evaluation).
        const std::lock_guard guard{converters_mutex};
        std::vector<const ValueTypeMetaData *> created;
        auto rollback = UnwindCleanupGuard([&] {
            for (const auto *schema : created) converters.erase(schema);
        });
        const auto *result = converter_for_locked(meta, created);
        rollback.release();
        return *result;
    }

    void clear_binary_converters() noexcept
    {
        const std::lock_guard guard{converters_mutex};
        converters.clear();
        // Registered against schemas the reset is about to discard.
        atom_forms.clear();
        owned_forms.clear();
        retired_converters.clear();
        portable_atoms.clear();
        opaque_warned.clear();
    }

    namespace
    {
        // A registration changes what a schema synthesizes to, so the interned
        // converters are set aside for new lookups to rebuild. Set aside, not
        // destroyed: ``binary_converter`` hands out references, and a caller
        // may still be holding one.
        void retire_interned_converters_locked()
        {
            retired_converters.reserve(retired_converters.size() + converters.size());
            for (auto &[meta, converter] : converters)
            {
                static_cast<void>(meta);
                retired_converters.push_back(std::move(converter));
            }
            converters.clear();
        }
    }  // namespace

    void register_binary_atom(const ValueTypeMetaData *scalar, BinaryAtomOps ops)
    {
        if (scalar == nullptr || scalar->try_value_kind() != ValueTypeKind::Atomic)
            throw std::invalid_argument("binary codec: a wire form is registered for a scalar");
        if (ops.write == nullptr || ops.read == nullptr)
            throw std::invalid_argument("binary codec: a wire form needs both a writer and a reader");
        const std::lock_guard guard{converters_mutex};
        owned_forms.push_back(std::make_unique<BinaryAtomOps>(ops));
        atom_forms[scalar] = owned_forms.back().get();
        retire_interned_converters_locked();
    }

    void declare_portable_binary_atom(const ValueTypeMetaData *scalar)
    {
        if (scalar == nullptr || scalar->try_value_kind() != ValueTypeKind::Atomic)
            throw std::invalid_argument("binary codec: only a scalar can be declared portable");
        if (!scalar->has(ValueTypeFlags::TriviallyCopyable))
        {
            throw std::invalid_argument(fmt::format(
                "binary codec: '{}' is not trivially copyable, so its storage cannot be its wire form; "
                "register one with register_binary_atom",
                scalar->name()));
        }
        const std::lock_guard guard{converters_mutex};
        portable_atoms.insert(scalar);
        retire_interned_converters_locked();
    }

    namespace
    {
        [[nodiscard]] std::string wire_form_message(std::string_view scalar, std::string_view needed_by,
                                                    std::string_view schema)
        {
            std::string message = fmt::format("binary codec: scalar '{}' has no wire form", scalar);
            if (!needed_by.empty())
            {
                message += fmt::format(", required by {}", needed_by);
                if (!schema.empty() && schema != scalar) { message += fmt::format(" ({})", schema); }
            }
            message += fmt::format(
                ".\nRegister one with register_binary_atom<T>(write, read) beside the scalar's registration, or "
                "declare it portable with declare_portable_binary_atom if it is trivially copyable and has no "
                "pointers or padding-dependent layout.");
            return message;
        }
    }  // namespace

    BinaryWireFormError::BinaryWireFormError(std::string scalar)
        : std::logic_error(wire_form_message(scalar, {}, {})), scalar_(std::move(scalar))
    {
    }

    BinaryWireFormError::BinaryWireFormError(std::string scalar, const std::string &message)
        : std::logic_error(message), scalar_(std::move(scalar))
    {
    }

    BinaryWireFormError BinaryWireFormError::with_context(std::string_view needed_by, std::string_view schema) const
    {
        return BinaryWireFormError{scalar_, wire_form_message(scalar_, needed_by, schema)};
    }

    BoundBinaryConverter bind_binary_converter_for(const ValueTypeMetaData *schema, BinaryProfile profile,
                                                   std::string_view needed_by)
    {
        try
        {
            return bind_binary_converter(schema, profile);
        }
        catch (const BinaryWireFormError &error)
        {
            throw error.with_context(needed_by, schema != nullptr ? schema->name() : std::string_view{});
        }
    }

    void to_binary_string(const ValueView &view, std::string &out)
    {
        bind_binary_converter(view.schema()).write(view, out);
    }

    std::string to_binary_string(const ValueView &view)
    {
        std::string out;
        to_binary_string(view, out);
        return out;
    }

    Value from_binary_string(const ValueTypeMetaData *meta, std::string_view bytes, BinaryDecodeLimits limits)
    {
        BinaryReader reader{bytes, 0, limits};
        Value        result = bind_binary_converter(meta).read(reader);
        if (reader.remaining() != 0)
        {
            throw std::runtime_error("binary codec: trailing bytes after one value");
        }
        return result;
    }
}  // namespace hgraph
