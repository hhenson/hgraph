#ifndef HGRAPH_CPP_ROOT_TIME_SERIES_TYPED_VIEW_H
#define HGRAPH_CPP_ROOT_TIME_SERIES_TYPED_VIEW_H

#include <hgraph/types/time_series/ts_data.h>

#include <utility>

namespace hgraph
{
    namespace detail
    {
        struct TSEndpointVisitorAccess;

        /**
         * Construction token used after endpoint visitation has already
         * checked the semantic TS kind.
         *
         * Only ``TSEndpointVisitorAccess`` can create the token, so public
         * shape-view construction remains checked.
         */
        class TrustedTSEndpointKind
        {
            constexpr TrustedTSEndpointKind() noexcept = default;
            friend struct TSEndpointVisitorAccess;
        };
    }  // namespace detail

    template <typename Derived, typename EndpointView>
    class TSTypedTimeSeriesView
    {
      public:
        [[nodiscard]] const EndpointView &base() const noexcept { return view_; }
        [[nodiscard]] EndpointView &base() noexcept { return view_; }

        [[nodiscard]] DateTime evaluation_time() const noexcept { return view_.evaluation_time(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return view_.schema(); }
        [[nodiscard]] bool bound() const noexcept { return view_.bound(); }
        [[nodiscard]] bool valid() const { return view_.valid(); }
        [[nodiscard]] bool all_valid() const { return view_.all_valid(); }
        [[nodiscard]] DateTime last_modified_time() const { return view_.last_modified_time(); }
        [[nodiscard]] bool modified() const { return view_.modified(); }
        [[nodiscard]] ValueView value() const { return view_.value(); }
        [[nodiscard]] ValueView delta_value() const { return view_.delta_value(); }

      protected:
        explicit TSTypedTimeSeriesView(EndpointView view)
            : view_(std::move(view))
        {
        }

        EndpointView view_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TIME_SERIES_TYPED_VIEW_H
