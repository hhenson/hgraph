#ifndef BASE_TIME_SERIES_OUTPUT_H
#define BASE_TIME_SERIES_OUTPUT_H

#include <hgraph/types/time_series_type.h>
#include <optional>
#include <unordered_set>

namespace hgraph {
    /*
     * Concrete reusable base that implements the common state/behaviour for TimeSeriesOutput.
     * All concrete output types should inherit from this instead of directly from TimeSeriesOutput.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesOutput : TimeSeriesOutput {
        using ptr = nb::ref<BaseTimeSeriesOutput>;

        BaseTimeSeriesOutput() = default;
        explicit BaseTimeSeriesOutput(const node_ptr &parent) { re_parent(parent); }
        explicit BaseTimeSeriesOutput(const TimeSeriesType::ptr &parent) { re_parent(parent); }

        static void register_with_nanobind(nb::module_ &m);

        // Implement TimeSeriesType abstract interface
        [[nodiscard]] node_ptr owning_node() override;
        [[nodiscard]] node_ptr owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        void re_parent(const node_ptr &parent) override;
        void re_parent(const TimeSeriesType::ptr &parent) override;
        [[nodiscard]] bool is_reference() const override;
        [[nodiscard]] bool has_reference() const override;
        void reset_parent_or_node() override;

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

        [[nodiscard]] bool has_owning_node() const override;

    protected:
        void _notify(engine_time_t modified_time);
        void _reset_last_modified_time();

        // TimeSeriesType storage hooks
        [[nodiscard]] TimeSeriesType::ptr &_parent_time_series();
        [[nodiscard]] TimeSeriesType::ptr &_parent_time_series() const;
        [[nodiscard]] bool _has_parent_time_series() const;
        void _set_parent_time_series(TimeSeriesType *ts);
        void _set_parent(const node_ptr &parent);
        void _set_parent(const TimeSeriesType::ptr &parent);
        void _reset_parent_or_node();
        [[nodiscard]] bool has_parent_or_node() const;
        [[nodiscard]] node_ptr _owning_node() const;

    private:
        using TsOrNode = std::variant<time_series_type_ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};
        std::unordered_set<Notifiable *> _subscribers{};
        engine_time_t _last_modified_time{MIN_DT};
    };
}

#endif // BASE_TIME_SERIES_OUTPUT_H
