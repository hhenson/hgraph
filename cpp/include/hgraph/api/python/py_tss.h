#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesSetOutput.
     *
     * Uses TSView/TSSView for operations.
     */
    struct PyTimeSeriesSetOutput: PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesSetOutput(TSMutableView view);

        // Move constructor
        PyTimeSeriesSetOutput(PyTimeSeriesSetOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesSetOutput& operator=(PyTimeSeriesSetOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesSetOutput(const PyTimeSeriesSetOutput&) = delete;
        PyTimeSeriesSetOutput& operator=(const PyTimeSeriesSetOutput&) = delete;

        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;
        void add(const nb::object &key) const;
        void remove(const nb::object &key) const;
        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester) const;
        void release_contains_output(const nb::object &item, const nb::object &requester) const;
        [[nodiscard]] nb::object is_empty_output() const;

        // value() and delta_value() are inherited from base - view layer handles TSS specifics

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    /**
     * @brief Python wrapper for TimeSeriesSetInput.
     *
     * Uses TSView/TSSView for operations.
     */
    struct PyTimeSeriesSetInput: PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesSetInput(TSView view);

        // Move constructor
        PyTimeSeriesSetInput(PyTimeSeriesSetInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesSetInput& operator=(PyTimeSeriesSetInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesSetInput(const PyTimeSeriesSetInput&) = delete;
        PyTimeSeriesSetInput& operator=(const PyTimeSeriesSetInput&) = delete;

        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;

        // value() and delta_value() are inherited from base - view layer handles TSS specifics

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
