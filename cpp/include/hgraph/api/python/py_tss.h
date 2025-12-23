#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/feature_extension.h>
#include <memory>

namespace hgraph
{
    /**
     * PyTimeSeriesSetOutput - Wrapper for set outputs (TSS)
     *
     * Provides set operations via view.
     */
    struct PyTimeSeriesSetOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesSetOutput(PyTimeSeriesSetOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other))
            , _contains_extension(std::move(other._contains_extension))
            , _is_empty_output(std::move(other._is_empty_output)) {}

        PyTimeSeriesSetOutput& operator=(PyTimeSeriesSetOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
                _contains_extension = std::move(other._contains_extension);
                _is_empty_output = std::move(other._is_empty_output);
            }
            return *this;
        }

        PyTimeSeriesSetOutput(const PyTimeSeriesSetOutput&) = delete;
        PyTimeSeriesSetOutput& operator=(const PyTimeSeriesSetOutput&) = delete;

        // Override value() to filter out MAX_DT-marked elements (pending removal)
        [[nodiscard]] nb::object value() const;

        // Set operations (filter MAX_DT-marked elements)
        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;

        void add(const nb::object &item);
        void remove(const nb::object &item);

        // Override to handle SetDelta objects
        void set_value(nb::object py_value);
        void apply_result(nb::object value);

        // Value access
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;

        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;

        // Feature extensions (like Python's get_contains_output)
        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester);
        void release_contains_output(const nb::object &item, const nb::object &requester);
        [[nodiscard]] nb::object is_empty_output();

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    private:
        // Lazily initialized feature extensions
        std::unique_ptr<FeatureOutputExtension<nb::object>> _contains_extension;
        time_series_output_s_ptr _is_empty_output;

        // Helper to update contains extension after set changes
        void update_contains_for_keys(const nb::handle &keys);

        // Ensure contains extension is initialized
        void ensure_contains_extension();
    };

    /**
     * PyTimeSeriesSetInput - Wrapper for set inputs (TSS)
     *
     * Provides set operations via view.
     */
    struct PyTimeSeriesSetInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesSetInput(PyTimeSeriesSetInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesSetInput& operator=(PyTimeSeriesSetInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesSetInput(const PyTimeSeriesSetInput&) = delete;
        PyTimeSeriesSetInput& operator=(const PyTimeSeriesSetInput&) = delete;

        // Set operations
        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;

        // Value access
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;

        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
