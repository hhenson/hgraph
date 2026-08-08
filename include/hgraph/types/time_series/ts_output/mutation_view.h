#ifndef HGRAPH_CPP_ROOT_TS_OUTPUT_MUTATION_VIEW_H
#define HGRAPH_CPP_ROOT_TS_OUTPUT_MUTATION_VIEW_H

#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{
    class TSOutput;
    class TSOutputView;
    class TSOutputMutationView;
    class TSBOutputView;
    class TSLOutputView;
    class TSSOutputView;
    class TSDOutputView;
    class TSWOutputView;

    /** Root mutation view for a TSOutput. */
    class TSOutputMutationView
    {
      public:
        TSOutputMutationView(TSOutput &output, DateTime evaluation_time);

        TSOutputMutationView(const TSOutputMutationView &) = delete;
        TSOutputMutationView &operator=(const TSOutputMutationView &) = delete;
        TSOutputMutationView(TSOutputMutationView &&other) noexcept;
        TSOutputMutationView &operator=(TSOutputMutationView &&) = delete;
        ~TSOutputMutationView() noexcept;

        /** Root TSData mutation view. */
        [[nodiscard]] TSDataMutationView &data_mutation() noexcept;
        [[nodiscard]] const TSDataMutationView &data_mutation() const noexcept;

        /** Current and delta value projections. */
        [[nodiscard]] ValueView value() const;
        [[nodiscard]] ValueView delta_value() const;

        /** Mutation time and modification status. */
        [[nodiscard]] DateTime current_mutation_time() const;
        [[nodiscard]] bool modified() const;

        /** Mark the root output as modified. */
        void mark_modified();

        /** Copy a value-layer view into the root TSData. */
        [[nodiscard]] bool copy_value_from(const ValueView &source);

        /** Move an owned value into the root TSData. */
        [[nodiscard]] bool move_value_from(Value &&source);

        /** Move from externally owned writable storage without taking ownership. */
        [[nodiscard]] bool move_value_from(ValueView source);

      private:
        static TSDataMutationView begin_root_mutation(TSOutput &output, DateTime evaluation_time);

        TSDataMutationView mutation_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TS_OUTPUT_MUTATION_VIEW_H
