#include <hgraph/types/value/binary_codec.h>

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

                if (child.atom_size != 0 && child.write_ == &write_atom)
                {
                    const std::size_t width = child.atom_size;
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

                if (child.atom_size != 0 && child.write_ == &write_atom)
                {
                    const auto        block = take_block(child, reader, count);
                    const std::size_t width = child.atom_size;
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
                    const auto *lengths = reader.take(block_bytes(count, sizeof(std::uint32_t)));
                    // Checked once: the rows share a representation, and a
                    // default-constructed row holds a live, empty string.
                    static_cast<void>(ValueView{destination, layout->field(builder.element_memory(0), field)}.checked_as<Str>());
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
                    if (!meta->has(ValueTypeFlags::TriviallyCopyable))
                    {
                        throw std::logic_error(
                            fmt::format("binary codec: atomic '{}' is not trivially copyable and "
                                        "has no wire form yet",
                                        meta->name()));
                    }
                    // Trivial copyability alone does not establish a wire
                    // contract: native handles and pointer-bearing structs can
                    // also be trivial. Accept numeric/chrono storage, enums,
                    // and the remaining explicit standard temporal atoms.
                    if (!meta->is_buffer_compatible() && !meta->is_enum()
                        && meta != scalar_descriptor<Time>::value_meta()
                        && meta != scalar_descriptor<CivilDateTime>::value_meta()
                        && meta != scalar_descriptor<Period>::value_meta())
                        throw std::logic_error(fmt::format("binary codec: atomic '{}' has no portable wire form", meta->name()));
                    const auto *plan = raw->binding.plan();
                    if (plan == nullptr)
                    {
                        throw std::logic_error(
                            fmt::format("binary codec: atomic '{}' has no storage plan", meta->name()));
                    }
                    if (meta->is_buffer_compatible() && plan->layout.size > sizeof(std::uint64_t))
                        throw std::logic_error("binary codec: extended-width atomic requires an explicit portable wire form");
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
                        converter.write_ = &write_row_columns;
                        converter.read_ = &read_row_columns;
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
            if (profile == BinaryProfile::Fast) { select_fast_forms(*raw); }
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
        return bind_binary_converter(meta, BinaryProfile::Compact);
    }

    BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta, BinaryProfile profile)
    {
        // Revision 0 of either profile is the field-wise encoding, so the two
        // synthesize alike until RFC 0040's later stages give each its own.
        auto plan = std::make_shared<BoundBinaryConverter::Impl>();
        plan->profile = profile;
        const auto *active = active_type_realization();
        plan->realization = active != nullptr ? active->shared_from_this()
                                             : TypeRealizationSnapshot::capture(TypeRegistry::instance());
        plan->root = plan->build(meta);
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
    }

    Value BinaryConverter::read(BinaryReader &reader) const
    {
        auto depth = reader.enter();
        return read_(*this, reader);
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
