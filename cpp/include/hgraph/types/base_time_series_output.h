#ifndef BASE_TIME_SERIES_OUTPUT_H
#define BASE_TIME_SERIES_OUTPUT_H

#include <hgraph/types/time_series_type.h>
#include <unordered_set>

namespace hgraph {
    /*
     * Concrete reusable base that implements the common state/behaviour for TimeSeriesOutput.
     * All concrete output types should inherit from this instead of directly from TimeSeriesOutput.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesOutput : TimeSeriesOutput {
        using ptr = nb::ref<BaseTimeSeriesOutput>;
        using TimeSeriesOutput::TimeSeriesOutput;

        static void register_with_nanobind(nb::module_ &m);

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        void mark_invalid() override;

        void mark_modified() override;

        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;

        [[nodiscard]] bool valid() const override;

        [[nodiscard]] bool all_valid() const override;

        [[nodiscard]] TimeSeriesOutput::ptr parent_output() const override;

        [[nodiscard]] TimeSeriesOutput::ptr parent_output() override;

        [[nodiscard]] bool has_parent_output() const override;

        void subscribe(Notifiable *node) override;

        void un_subscribe(Notifiable *node) override;

        // Minimal-teardown helper used by builders during release; must not access owning_node/graph
        void builder_release_cleanup() override;

        bool can_apply_result(nb::object value) override;

        void clear() override;

        void invalidate() override;

        void mark_modified(engine_time_t modified_time) override;

    protected:
        void _notify(engine_time_t modified_time);

        void _reset_last_modified_time();

    private:
        friend OutputBuilder;
        // I think we can change this to not reference count if we track the inputs, this should be one-to-one
        std::unordered_set<Notifiable *> _subscribers{};
        engine_time_t _last_modified_time{MIN_DT};
    };
} // namespace hgraph

#endif // BASE_TIME_SERIES_OUTPUT_H
