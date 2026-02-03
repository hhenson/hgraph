#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesSetOutput.
     *
     * Non-templated wrapper that uses Python interop methods for all operations.
     */
    struct PyTimeSeriesSetOutput: PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesSetOutput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesSetOutput(api_ptr impl) : PyTimeSeriesOutput(std::move(impl)) {}

        // New view-based constructor
        explicit PyTimeSeriesSetOutput(TSOutputView view) : PyTimeSeriesOutput(std::move(view)) {}

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
        [[nodiscard]] nb::object value() const;
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
        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetOutput *impl() const { return static_cast_impl<TimeSeriesSetOutput>(); }
    };

    /**
     * @brief Python wrapper for TimeSeriesSetInput.
     *
     * Non-templated wrapper that uses Python interop methods for all operations.
     */
    struct PyTimeSeriesSetInput: PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesSetInput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesSetInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

        // New view-based constructor
        explicit PyTimeSeriesSetInput(TSInputView view) : PyTimeSeriesInput(std::move(view)) {}

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
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;
        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetInput *impl() const { return static_cast_impl<TimeSeriesSetInput>(); }
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
