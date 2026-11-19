#pragma once
#include <hgraph/python/bridge_state.h>
#include <nanobind/nanobind.h>
#include <optional>
#include <stdexcept>

namespace hgraph::python_bridge
{
    namespace
    {
        inline nb::dict python_list_delta(nb::dict modified, nb::object removed, nb::object size) {
            nb::dict delta;
            delta["modified"]      = std::move(modified);
            delta["removed"]       = std::move(removed);
            delta["size_modified"] = std::move(size);
            return delta;
        }

        template <typename ChildDelta> nb::dict python_list_snapshot(std::size_t count, bool dynamic, ChildDelta read) {
            nb::dict modified;
            for (std::size_t index = 0; index < count; ++index) {
                auto item = read(index);
                if (!item.is_none()) modified[nb::int_(index)] = std::move(item);
            }
            return python_list_delta(std::move(modified), nb::set(), dynamic ? nb::object(nb::int_(count)) : nb::none());
        }

        /** Decode the Python spelling once, before resizing or updating any child. */
        struct ListDeltaSource
        {
            nb::dict                   modified;
            nb::set                    removed;
            std::optional<std::size_t> size;

            ListDeltaSource(nb::handle source, bool dynamic, std::size_t bound) {
                const auto &strict  = removed_sentinel_slot();
                const auto &lenient = remove_if_exists_sentinel_slot();
                auto        add     = [&](nb::handle key, nb::handle item) {
                    if (item.is_none()) return;
                    const auto index = nb::cast<std::size_t>(key);
                    if ((strict.is_valid() && item.is(strict)) || (lenient.is_valid() && item.is(lenient))) {
                        removed.add(key);
                    } else {
                        if ((!dynamic && index >= bound) || (size && index >= *size))
                            throw std::out_of_range("TSL update outside the declared size");
                        modified[key] = item;
                    }
                };
                if (nb::isinstance<nb::dict>(source)) {
                    auto       mapping = nb::borrow<nb::dict>(source);
                    const bool canonical =
                        mapping.contains("size_modified") || mapping.contains("removed") || mapping.contains("modified");
                    nb::object length =
                        canonical ? mapping.attr("get")("size_modified") : nb::getattr(source, "size_modified", nb::none());
                    if (!length.is_none()) {
                        if (!dynamic) throw std::invalid_argument("a bounded TSL cannot resize");
                        size = nb::cast<std::size_t>(length);
                    }
                    if (canonical) {
                        for (auto [key, item] : mapping) {
                            if (!key.equal(nb::str("modified")) && !key.equal(nb::str("removed")) &&
                                !key.equal(nb::str("size_modified")))
                                throw std::invalid_argument("unknown TSL delta field");
                        }
                        if (mapping.contains("removed")) {
                            for (auto key : mapping["removed"]) {
                                static_cast<void>(nb::cast<std::size_t>(key));
                                removed.add(key);
                            }
                        }
                        mapping = nb::cast<nb::dict>(mapping.attr("get")("modified", nb::dict()));
                    }
                    for (auto [key, item] : mapping) add(key, item);
                } else {
                    if (nb::isinstance<nb::str>(source) || !nb::isinstance<nb::sequence>(source))
                        throw std::invalid_argument("a TSL update requires a sequence or mapping");
                    if (dynamic) size = nb::len(source);
                    std::size_t index = 0;
                    for (auto item : source) add(nb::int_(index++), item);
                }
                for (auto key : removed) {
                    if (!dynamic && nb::cast<std::size_t>(key) >= bound)
                        throw std::out_of_range("bounded TSL removal outside its size");
                    if (modified.contains(key))
                        throw std::invalid_argument("a TSL position cannot be modified and removed together");
                }
            }
        };
    }  // namespace
}  // namespace hgraph::python_bridge
