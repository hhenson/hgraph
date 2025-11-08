#include <hgraph/types/graph.h>
#include <hgraph/types/ts_indexed.h>

#include <algorithm>
#include <ranges>

namespace hgraph {
    void IndexedTimeSeriesOutput::invalidate() {
        if (valid()) {
            for (auto &v: ts_values()) { v->invalidate(); }
        }
        mark_invalid();
    }

    void IndexedTimeSeriesOutput::copy_from_output(const TimeSeriesOutput &output) {
        if (auto *ndx_output = dynamic_cast<const IndexedTimeSeriesOutput *>(&output); ndx_output != nullptr) {
            if (ndx_output->size() == size()) {
                for (size_t i = 0; i < ts_values().size(); ++i) {
                    ts_values()[i]->copy_from_output(*ndx_output->ts_values()[i]);
                }
            } else {
                // We could do a full check, but that would be too much to do each time, and in theory the wiring should ensure
                //  we don't do that, but there should be a quick sanity check.
                //  Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format(
                    "Incorrect shape provided to copy_from_output, expected {} items got {}",
                    size(), ndx_output->size()));
            }
        } else {
            throw std::invalid_argument(std::format("Expected IndexedTimeSeriesOutput, got {}", typeid(output).name()));
        }
    }

    void IndexedTimeSeriesOutput::copy_from_input(const TimeSeriesInput &input) {
        if (auto *ndx_inputs = dynamic_cast<const IndexedTimeSeriesInput *>(&input); ndx_inputs != nullptr) {
            if (ndx_inputs->size() == size()) {
                for (size_t i = 0; i < ts_values().size(); ++i) {
                    auto &child{ndx_inputs->operator[](i)};
                    auto v{ts_values()[i]};
                    if (child->valid()) {
                        v->copy_from_input(*child);
                    } else {
                        if (v->valid()) { v->invalidate(); }
                    }
                }
            } else {
                // Simple validation at this level to ensure they are at least size compatible
                throw std::runtime_error(std::format(
                    "Incorrect shape provided to copy_from_input, expected {} items got {}",
                    size(), ndx_inputs->size()));
            }
        } else {
            throw std::invalid_argument(std::format("Expected TimeSeriesBundleOutput, got {}", typeid(input).name()));
        }
    }

    void IndexedTimeSeriesOutput::clear() {
        for (auto &v: ts_values()) { v->clear(); }
    }

    void IndexedTimeSeriesOutput::register_with_nanobind(nb::module_ &m) {
        using IndexedTimeSeries_Output = IndexedTimeSeries<BaseTimeSeriesOutput>;
        nb::class_<IndexedTimeSeries_Output, BaseTimeSeriesOutput>(m, "IndexedTimeSeries_Output")
                .def(
                    "__getitem__", [](const IndexedTimeSeries_Output &self, size_t idx) { return self[idx]; },
                    "index"_a)
                .def("values",
                     static_cast<collection_type (IndexedTimeSeries_Output::*)() const>(&
                         IndexedTimeSeries_Output::values))
                .def("valid_values", &IndexedTimeSeries_Output::py_valid_values)
                .def("modified_values", &IndexedTimeSeries_Output::py_modified_values)
                .def("__len__", &IndexedTimeSeries_Output::size)
                .def_prop_ro("empty", &IndexedTimeSeries_Output::empty);

        nb::class_<IndexedTimeSeriesOutput, IndexedTimeSeries_Output>(m, "IndexedTimeSeriesOutput")
                .def("copy_from_output", &IndexedTimeSeriesOutput::copy_from_output, "output"_a)
                .def("copy_from_input", &IndexedTimeSeriesOutput::copy_from_input, "input"_a);
    }

    bool IndexedTimeSeriesInput::modified() const {
        if (has_peer()) { return BaseTimeSeriesInput::modified(); }
        if (ts_values().empty()) { return false; }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->modified(); });
    }

    bool IndexedTimeSeriesInput::valid() const {
        if (has_peer()) { return BaseTimeSeriesInput::valid(); }
        // Empty bundles are considered valid (no invalid items)
        if (ts_values().empty()) { return true; }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->valid(); });
    }

    engine_time_t IndexedTimeSeriesInput::last_modified_time() const {
        if (has_peer()) { return BaseTimeSeriesInput::last_modified_time(); }
        if (ts_values().empty()) { return MIN_DT; }
        return std::ranges::max(ts_values() |
                                std::views::transform([](const time_series_input_ptr &ts) {
                                    return ts->last_modified_time();
                                }));
    }

    bool IndexedTimeSeriesInput::bound() const {
        return BaseTimeSeriesInput::bound() ||
               std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->bound(); });
    }

    bool IndexedTimeSeriesInput::active() const {
        if (has_peer()) { return BaseTimeSeriesInput::active(); }
        if (ts_values().empty()) { return false; }
        return std::ranges::any_of(ts_values(), [](const time_series_input_ptr &ts) { return ts->active(); });
    }

    void IndexedTimeSeriesInput::make_active() {
        if (has_peer()) {
            BaseTimeSeriesInput::make_active();
        } else {
            for (auto &ts: ts_values()) { ts->make_active(); }
        }
    }

    void IndexedTimeSeriesInput::make_passive() {
        if (has_peer()) {
            BaseTimeSeriesInput::make_passive();
        } else {
            for (auto &ts: ts_values()) { ts->make_passive(); }
        }
    }

    TimeSeriesInput *IndexedTimeSeriesInput::get_input(size_t index) { return (*this)[index].get(); }

    void IndexedTimeSeriesInput::register_with_nanobind(nb::module_ &m) {
        using IndexedTimeSeries_Input = IndexedTimeSeries<BaseTimeSeriesInput>;

        nb::class_<IndexedTimeSeries_Input, BaseTimeSeriesInput>(m, "IndexedTimeSeries_Input")
                .def(
                    "__getitem__", [](const IndexedTimeSeries_Input &self, size_t index) { return self[index]; },
                    "index"_a)
                .def("values",
                     static_cast<collection_type (IndexedTimeSeries_Input::*)() const>(&
                         IndexedTimeSeries_Input::values))
                .def("valid_values", &IndexedTimeSeries_Input::py_valid_values)
                .def("modified_values", &IndexedTimeSeries_Input::py_modified_values)
                .def("__len__", &IndexedTimeSeries_Input::size)
                .def_prop_ro("empty", &IndexedTimeSeries_Input::empty);

        nb::class_<IndexedTimeSeriesInput, IndexedTimeSeries_Input>(m, "IndexedTimeSeriesInput");
    }

    bool IndexedTimeSeriesInput::do_bind_output(time_series_output_ptr &value) {
        auto output_bundle = dynamic_cast<IndexedTimeSeriesOutput *>(value.get());
        if (output_bundle == nullptr) {
            throw std::runtime_error("IndexedTimeSeriesInput::do_bind_output: Expected IndexedTimeSeriesOutput");
        }

        bool peer = true;
        for (size_t i = 0; i < ts_values().size(); ++i) { peer &= ts_values()[i]->bind_output((*output_bundle)[i]); }

        time_series_output_ptr none{};
        BaseTimeSeriesInput::do_bind_output(peer ? value : none);
        return peer;
    }

    void IndexedTimeSeriesInput::do_un_bind_output(bool unbind_refs) {
        for (auto &ts: ts_values()) { ts->un_bind_output(unbind_refs); }
        if (has_peer()) { BaseTimeSeriesInput::do_un_bind_output(unbind_refs); }
    }
} // namespace hgraph