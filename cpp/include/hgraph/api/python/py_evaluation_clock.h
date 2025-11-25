#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct EvaluationClock;

    struct HGRAPH_EXPORT PyEvaluationClock
    {
        using api_ptr = ApiPtr<EvaluationClock>;

        explicit PyEvaluationClock(api_ptr clock);

        [[nodiscard]] engine_time_t evaluation_time() const;

        [[nodiscard]] engine_time_t now() const;

        [[nodiscard]] engine_time_t next_cycle_evaluation_time() const;

        [[nodiscard]] engine_time_delta_t cycle_time() const;

        [[nodiscard]] nb::object key() const;

        [[nodiscard]] nb::str str() const;

        [[nodiscard]] nb::str repr() const;

        static void register_with_nanobind(nb::module_ &m);

      private:
        api_ptr _impl;
    };

}  // namespace hgraph

