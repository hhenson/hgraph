#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/api/python/py_evaluation_clock.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    enum class EvaluationMode;
    struct EvaluationEngineApi;
    struct EvaluationLifeCycleObserver;

    struct HGRAPH_EXPORT PyEvaluationEngineApi
    {
        using api_ptr = ApiPtr<EvaluationEngineApi>;

        explicit PyEvaluationEngineApi(api_ptr engine);

        [[nodiscard]] EvaluationMode evaluation_mode() const;

        [[nodiscard]] engine_time_t start_time() const;

        [[nodiscard]] engine_time_t end_time() const;

        [[nodiscard]] PyEvaluationClock evaluation_clock() const;

        void request_engine_stop() const;

        [[nodiscard]] nb::bool_ is_stop_requested() const;

        void add_before_evaluation_notification(nb::callable fn) const;

        void add_after_evaluation_notification(nb::callable fn) const;

        void add_life_cycle_observer(nb::object observer) const;

        void remove_life_cycle_observer(nb::object observer) const;

        // ComponentLifeCycle methods (delegated to _impl)
        [[nodiscard]] nb::bool_ is_started() const;
        [[nodiscard]] nb::bool_ is_starting() const;
        [[nodiscard]] nb::bool_ is_stopping() const;

        [[nodiscard]] nb::str str() const;

        [[nodiscard]] nb::str repr() const;

        static void register_with_nanobind(nb::module_ &m);

      private:
        api_ptr _impl;
    };

}  // namespace hgraph

