#include <hgraph/types/ref_value.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ts.h>
#include <hgraph/types/ts_indexed.h>
#include <algorithm>

namespace hgraph
{
    ref_value_tp TimeSeriesReference::make() { return {new EmptyTimeSeriesReference()}; }

    ref_value_tp TimeSeriesReference::make(time_series_output_ptr output) {
        if (output.get() == nullptr) {
            return make();
        } else {
            return {new BoundTimeSeriesReference(output)};
        }
    }

    ref_value_tp TimeSeriesReference::make(std::vector<ptr> items) {
        if (items.empty()) { return make(); }
        return {new UnBoundTimeSeriesReference(std::move(items))};
    }

    ref_value_tp TimeSeriesReference::make(const std::vector<nb::ref<TimeSeriesReferenceInput>> &items) {
        if (items.empty()) { return make(); }
        std::vector<ref_value_tp> refs;
        refs.reserve(items.size());
        for (auto item : items) {
            // Call value() instead of accessing _value directly, so bound items return their output's value
            refs.emplace_back(item->value());
        }
        return {new UnBoundTimeSeriesReference(refs)};
    }

    void TimeSeriesReference::register_with_nanobind(nb::module_ &m) {
        nb::class_<TimeSeriesReference, nb::intrusive_base>(m, "TimeSeriesReference")
            .def("__str__", &TimeSeriesReference::to_string)
            .def("__repr__", &TimeSeriesReference::to_string)
            .def("bind_input", &TimeSeriesReference::bind_input)
            .def_prop_ro("has_output", &TimeSeriesReference::has_output)
            .def_prop_ro("is_empty", &TimeSeriesReference::is_empty)
            .def_prop_ro("is_valid", &TimeSeriesReference::is_valid)
            .def_static("make", static_cast<ptr (*)()>(&TimeSeriesReference::make))
            .def_static("make", static_cast<ptr (*)(TimeSeriesOutput::ptr)>(&TimeSeriesReference::make))
            .def_static("make", static_cast<ptr (*)(std::vector<ptr>)>(&TimeSeriesReference::make))
            .def_static(
                "make",
                [](nb::object ts, nb::object items) -> ptr {
                    if (not ts.is_none()) {
                        if (nb::isinstance<TimeSeriesOutput>(ts)) return make(nb::cast<TimeSeriesOutput::ptr>(ts));
                        if (nb::isinstance<TimeSeriesReferenceInput>(ts))
                            return nb::cast<TimeSeriesReferenceInput::ptr>(ts)->value();
                        if (nb::isinstance<TimeSeriesInput>(ts)) {
                            auto ts_input = nb::cast<TimeSeriesInput::ptr>(ts);
                            if (ts_input->has_peer()) return make(ts_input->output());
                            // Deal with list of inputs
                            std::vector<ptr> items_list;
                            auto             ts_ndx{dynamic_cast<IndexedTimeSeriesInput *>(ts_input.get())};
                            items_list.reserve(ts_ndx->size());
                            for (auto &ts_ptr : ts_ndx->values()) {
                                auto ref_input{dynamic_cast<TimeSeriesReferenceInput *>(ts_ptr.get())};
                                items_list.emplace_back(ref_input ? ref_input->value() : nullptr);
                            }
                            return make(items_list);
                        }
                        // We may wish to raise an exception here?
                    } else if (not items.is_none()) {
                        auto items_list = nb::cast<std::vector<ptr>>(items);
                        return make(items_list);
                    }
                    return make();
                },
                "ts"_a = nb::none(), "from_items"_a = nb::none());

        nb::class_<EmptyTimeSeriesReference, TimeSeriesReference>(m, "EmptyTimeSeriesReference");

        nb::class_<BoundTimeSeriesReference, TimeSeriesReference>(m, "BoundTimeSeriesReference")
            .def_prop_ro("output", &BoundTimeSeriesReference::output);

        nb::class_<UnBoundTimeSeriesReference, TimeSeriesReference>(m, "UnBoundTimeSeriesReference")
            .def_prop_ro("items", &UnBoundTimeSeriesReference::items)
            .def("__getitem__", [](UnBoundTimeSeriesReference &self, size_t index) -> ref_value_tp {
                const auto &items = self.items();
                if (index >= items.size()) { throw std::out_of_range("Index out of range"); }
                return items[index];
            });
    }

    void EmptyTimeSeriesReference::bind_input(TimeSeriesInput &ts_input) const {
        try {
            ts_input.un_bind_output(false);
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("Error in EmptyTimeSeriesReference::bind_input: ") + e.what());
        } catch (...) { throw std::runtime_error("Unknown error in EmptyTimeSeriesReference::bind_input"); }
    }

    bool EmptyTimeSeriesReference::has_output() const { return false; }

    bool EmptyTimeSeriesReference::is_empty() const { return true; }

    bool EmptyTimeSeriesReference::is_valid() const { return false; }

    bool EmptyTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        return dynamic_cast<const EmptyTimeSeriesReference *>(&other) != nullptr;
    }

    std::string EmptyTimeSeriesReference::to_string() const { return "REF[<UnSet>]"; }

    BoundTimeSeriesReference::BoundTimeSeriesReference(const time_series_output_ptr &output) : _output{output} {}

    const TimeSeriesOutput::ptr &BoundTimeSeriesReference::output() const { return _output; }

    void BoundTimeSeriesReference::bind_input(TimeSeriesInput &input_) const {
        bool reactivate = false;
        // Treat inputs previously bound via a reference as bound, so we unbind to generate correct deltas
        if (input_.bound() && !input_.has_peer()) {
            reactivate = input_.active();
            input_.un_bind_output(false);
        }
        input_.bind_output(_output);
        if (reactivate) { input_.make_active(); }
    }

    bool BoundTimeSeriesReference::has_output() const { return true; }

    bool BoundTimeSeriesReference::is_empty() const { return false; }

    bool BoundTimeSeriesReference::is_valid() const { return _output->valid(); }

    bool BoundTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        auto bound_time_series_reference{dynamic_cast<const BoundTimeSeriesReference *>(&other)};
        return bound_time_series_reference != nullptr && bound_time_series_reference->output() == _output;
    }

    std::string BoundTimeSeriesReference::to_string() const {
        return fmt::format("REF[{}<{}>.output@{:p}]", _output->owning_node()->signature().name,
                           fmt::join(_output->owning_node()->node_id(), ", "),
                           const_cast<void *>(static_cast<const void *>(_output.get())));
    }

    UnBoundTimeSeriesReference::UnBoundTimeSeriesReference(std::vector<ptr> items) : _items{std::move(items)} {}

    const std::vector<ref_value_tp> &UnBoundTimeSeriesReference::items() const { return _items; }

    void UnBoundTimeSeriesReference::bind_input(TimeSeriesInput &input_) const {
        // Try to cast to supported input types

        bool reactivate = false;
        if (input_.bound() && input_.has_peer()) {
            reactivate = input_.active();
            input_.un_bind_output(false);
        }

        for (size_t i = 0; i < _items.size(); ++i) {
            // Get the child input (from REF, Indexed, or Signal input)
            TimeSeriesInput *item{input_.get_input(i)};

            auto &r{_items[i]};
            if (r != nullptr) {
                r->bind_input(*item);
            } else if (item->bound()) {
                item->un_bind_output(false);
            }
        }

        if (reactivate) { input_.make_active(); }
    }

    bool UnBoundTimeSeriesReference::has_output() const { return false; }

    bool UnBoundTimeSeriesReference::is_empty() const { return false; }

    bool UnBoundTimeSeriesReference::is_valid() const {
        return std::any_of(_items.begin(), _items.end(), [](const auto &item) { return item != nullptr && !item->is_empty(); });
    }

    bool UnBoundTimeSeriesReference::operator==(const TimeSeriesReferenceOutput &other) const {
        auto other_{dynamic_cast<const UnBoundTimeSeriesReference *>(&other)};
        return other_ != nullptr && other_->_items == _items;
    }

    const ref_value_tp &UnBoundTimeSeriesReference::operator[](size_t ndx) { return _items.at(ndx); }

    std::string UnBoundTimeSeriesReference::to_string() const {
        std::vector<std::string> string_items;
        string_items.reserve(_items.size());
        for (const auto &item : _items) { string_items.push_back(item->to_string()); }
        return fmt::format("REF[{}]", fmt::join(string_items, ", "));
    }
} // namespace hgraph
