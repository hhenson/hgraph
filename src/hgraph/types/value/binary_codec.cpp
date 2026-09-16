#include <hgraph/types/value/binary_codec.h>

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[noreturn]] void short_buffer()
        {
            throw std::runtime_error("binary codec: truncated buffer");
        }

        // --- composites ----------------------------------------------------
        // A presence bitmap precedes the present fields. It is not an
        // optimisation: a Bundle field may be UNSET rather than defaulted, and
        // without the bitmap a decoder can tell neither UNSET from a value nor
        // where the next variable-width field begins.

        [[nodiscard]] std::size_t bitmap_bytes(std::size_t fields) noexcept
        {
            return (fields + 7) / 8;
        }

        void write_composite(const BinaryConverter &self, const ValueView &view, std::string &out)
        {
            const std::size_t fields = self.children.size();
            const std::size_t bitmap_at = out.size();
            out.append(bitmap_bytes(fields), '\0');

            const auto indexed = view.as_indexed_view();
            for (std::size_t i = 0; i < fields; ++i)
            {
                const auto field = indexed.at(i);
                if (!field.has_value()) { continue; }
                out[bitmap_at + (i / 8)] =
                    static_cast<char>(static_cast<unsigned char>(out[bitmap_at + (i / 8)]) |
                                      (1u << (i % 8)));
                self.children[i]->write(field, out);
            }
        }

        Value read_composite(const BinaryConverter &self, BinaryReader &reader)
        {
            const std::size_t fields = self.children.size();
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

        void write_atom(const BinaryConverter &self, const ValueView &view, std::string &out)
        {
            // Trivially copyable and fixed width, so the canonical wire form is
            // the storage image on a little-endian host. That covers every
            // numeric and temporal atom without enumerating the taxonomy.
            const auto *bytes = static_cast<const char *>(view.data());
            out.append(bytes, self.atom_size);
        }

        Value read_atom(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto *bytes = reader.take(self.atom_size);
            return Value{self.binding, bytes};
        }

        void write_string(const BinaryConverter &, const ValueView &view, std::string &out)
        {
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
        void write_bytes(const BinaryConverter &, const ValueView &view, std::string &out)
        {
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

        // --- sequences -----------------------------------------------------

        void write_list(const BinaryConverter &self, const ValueView &view, std::string &out)
        {
            const auto list = view.as_list();
            write_varint(list.size(), out);
            for (std::size_t i = 0; i < list.size(); ++i) { self.children[0]->write(list.at(i), out); }
        }

        Value read_list(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto  count   = static_cast<std::size_t>(read_varint(reader));
            const auto  element = self.children[0]->binding;
            ListBuilder builder{element, *self.meta};
            for (std::size_t i = 0; i < count; ++i)
            {
                Value item = self.children[0]->read(reader);
                builder.push_back(item.view());
            }
            ListStorage storage = builder.build_storage();
            return Value{compact_list_type(element, *self.meta), &storage};
        }

        void write_set(const BinaryConverter &self, const ValueView &view, std::string &out)
        {
            const auto set = view.as_set();
            std::string elements;
            std::size_t count = 0;
            for (const auto element : set)
            {
                self.children[0]->write(element, elements);
                ++count;
            }
            // The count precedes the elements, and a set has no size() to ask
            // for up front, so the elements are built first.
            write_varint(count, out);
            out.append(elements);
        }

        Value read_set(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count   = static_cast<std::size_t>(read_varint(reader));
            const auto element = self.children[0]->binding;
            SetBuilder builder{element};
            for (std::size_t i = 0; i < count; ++i)
            {
                Value item = self.children[0]->read(reader);
                (void)builder.insert(item.view());
            }
            SetStorage storage = builder.build_storage();
            return Value{compact_set_type(element), &storage};
        }

        void write_map(const BinaryConverter &self, const ValueView &view, std::string &out)
        {
            const auto  map = view.as_map();
            std::string entries;
            std::size_t count = 0;
            for (const auto entry : map)
            {
                self.children[0]->write(entry.first, entries);
                self.children[1]->write(entry.second, entries);
                ++count;
            }
            write_varint(count, out);
            out.append(entries);
        }

        Value read_map(const BinaryConverter &self, BinaryReader &reader)
        {
            const auto count = static_cast<std::size_t>(read_varint(reader));
            const auto key   = self.children[0]->binding;
            const auto value = self.children[1]->binding;
            MapBuilder builder{key, value};
            for (std::size_t i = 0; i < count; ++i)
            {
                Value k = self.children[0]->read(reader);
                Value v = self.children[1]->read(reader);
                builder.set_item(k.view(), v.view());
            }
            MapStorage storage = builder.build_storage();
            return Value{compact_map_type(key, value), &storage};
        }

        // --- synthesis -----------------------------------------------------

        TypeSystemMutex                                                          converters_mutex;
        ankerl::unordered_dense::map<const ValueTypeMetaData *,
                                     std::unique_ptr<BinaryConverter>>           converters;

        const BinaryConverter *converter_for_locked(const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { throw std::logic_error("binary codec: null schema"); }
            if (const auto found = converters.find(meta); found != converters.end())
            {
                return found->second.get();
            }

            auto  owned = std::make_unique<BinaryConverter>();
            auto *raw   = owned.get();
            converters.emplace(meta, std::move(owned));
            auto unwind = make_scope_exit([&] { converters.erase(meta); });

            raw->meta    = meta;
            raw->binding = ts_data_detail::canonical_value_binding_for(meta);

            switch (meta->value_kind())
            {
                case ValueTypeKind::Atomic: {
                    if (meta == scalar_descriptor<Str>::value_meta())
                    {
                        raw->write_ = &write_string;
                        raw->read_  = &read_string;
                        break;
                    }
                    if (meta == scalar_descriptor<Bytes>::value_meta())
                    {
                        raw->write_ = &write_bytes;
                        raw->read_  = &read_bytes;
                        break;
                    }
                    if (!meta->has(ValueTypeFlags::TriviallyCopyable))
                    {
                        throw std::logic_error(
                            fmt::format("binary codec: atomic '{}' is not trivially copyable and "
                                        "has no wire form yet",
                                        meta->name()));
                    }
                    const auto *plan = raw->binding.plan();
                    if (plan == nullptr)
                    {
                        throw std::logic_error(
                            fmt::format("binary codec: atomic '{}' has no storage plan", meta->name()));
                    }
                    raw->atom_size = plan->layout.size;
                    raw->write_    = &write_atom;
                    raw->read_     = &read_atom;
                    break;
                }
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle: {
                    for (std::size_t i = 0; i < meta->field_count; ++i)
                    {
                        raw->children.push_back(converter_for_locked(meta->fields[i].type));
                    }
                    raw->write_ = &write_composite;
                    raw->read_  = &read_composite;
                    break;
                }
                case ValueTypeKind::List: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_list;
                    raw->read_  = &read_list;
                    break;
                }
                case ValueTypeKind::Set: {
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_set;
                    raw->read_  = &read_set;
                    break;
                }
                case ValueTypeKind::Map: {
                    raw->children.push_back(converter_for_locked(meta->key_type));
                    raw->children.push_back(converter_for_locked(meta->element_type));
                    raw->write_ = &write_map;
                    raw->read_  = &read_map;
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

    const std::byte *BinaryReader::take(std::size_t count)
    {
        if (remaining() < count) { short_buffer(); }
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
        return *converter_for_locked(meta);
    }

    void clear_binary_converters() noexcept
    {
        const std::lock_guard guard{converters_mutex};
        converters.clear();
    }

    void to_binary_string(const ValueView &view, std::string &out)
    {
        binary_converter(view.schema()).write(view, out);
    }

    std::string to_binary_string(const ValueView &view)
    {
        std::string out;
        to_binary_string(view, out);
        return out;
    }

    Value from_binary_string(const ValueTypeMetaData *meta, std::string_view bytes)
    {
        BinaryReader reader{bytes, 0};
        Value        result = binary_converter(meta).read(reader);
        if (reader.remaining() != 0)
        {
            throw std::runtime_error("binary codec: trailing bytes after one value");
        }
        return result;
    }
}  // namespace hgraph
