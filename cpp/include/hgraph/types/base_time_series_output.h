#ifndef BASE_TIME_SERIES_OUTPUT_H
#define BASE_TIME_SERIES_OUTPUT_H

#include <hgraph/types/time_series_type.h>

namespace hgraph {
    /*
     * Concrete reusable base that implements the common state/behaviour for TimeSeriesOutput.
     * All concrete output types should inherit from this instead of directly from TimeSeriesOutput.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesOutput : TimeSeriesOutput {
        using TimeSeriesOutput::TimeSeriesOutput;

        static void register_with_nanobind(nb::module_ &m);

        // Generic behaviour implementations
        [[nodiscard]] bool modified() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        virtual void mark_invalid() override;
        virtual void mark_modified() override;
        virtual void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        [[nodiscard]] bool valid() const override;
        [[nodiscard]] bool all_valid() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() override;
        [[nodiscard]] bool has_parent_output() const override;
        void subscribe(Notifiable *node) override;
        void un_subscribe(Notifiable *node) override;
        void builder_release_cleanup() override;
        virtual bool can_apply_result(nb::object value) override;
        virtual void clear() override;
        virtual void invalidate() override;
        virtual void mark_modified(engine_time_t modified_time) override;
        void notify(engine_time_t et) override;

    protected:
        void _notify(engine_time_t modified_time);
        void _reset_last_modified_time();

    private:
        std::unordered_set<Notifiable *> _subscribers{};
        engine_time_t _last_modified_time{MIN_DT};
    };
}

#endif // BASE_TIME_SERIES_OUTPUT_H
