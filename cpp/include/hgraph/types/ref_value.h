//
// Extracted TimeSeriesReference value types
//
#pragma once

#include <hgraph/types/time_series_type.h>
#include <hgraph/types/v2/ts_value.h>

#define HGRAPH_REF_VALUE_TYPES_DECLARED 1

namespace hgraph
{
    struct TimeSeriesReferenceOutput;  // fwd
    struct TimeSeriesReferenceInput;   // fwd

    struct HGRAPH_EXPORT TimeSeriesReference : nb::intrusive_base
    {
        using ptr = nb::ref<TimeSeriesReference>;

        virtual void bind_input(TimeSeriesInput &ts_input) const = 0;

        virtual bool has_output() const = 0;

        virtual bool is_empty() const = 0;

        virtual bool is_valid() const = 0;

        virtual bool operator==(const TimeSeriesReferenceOutput &other) const = 0;

        virtual std::string to_string() const = 0;

        static ptr make();

        static ptr make(time_series_output_ptr output);

        static ptr make(std::vector<ptr> items);

        static ptr make(const std::vector<nb::ref<TimeSeriesReferenceInput>>& items);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct EmptyTimeSeriesReference final : TimeSeriesReference
    {
        void bind_input(TimeSeriesInput &ts_input) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;
    };

    struct BoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit BoundTimeSeriesReference(const time_series_output_ptr &output);

        const TimeSeriesOutput::ptr &output() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;

      private:
        TimeSeriesOutput::ptr _output;
    };

    struct UnBoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit UnBoundTimeSeriesReference(std::vector<ptr> items);

        const std::vector<ptr> &items() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        const ptr &operator[](size_t ndx);

        std::string to_string() const override;

      private:
        std::vector<ptr> _items;
    };

    // Use this definition of the ref-value-type to make it easier to switch
    // out later when we turn the ref into a proper value type.
    using ref_value_tp = TimeSeriesReference::ptr;
}  // namespace hgraph
