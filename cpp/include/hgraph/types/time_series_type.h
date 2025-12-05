#ifndef TIME_SERIES_TYPE_H
#define TIME_SERIES_TYPE_H

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/util/arena_enable_shared_from_this.h>
#include <ddv/visitable.h>
#include <memory>

namespace hgraph
{

    struct TimeSeriesVisitor;

    using ts_input_types = tp::tpack<BaseTimeSeriesInput, TimeSeriesValueInputBase, TimeSeriesSignalInput, IndexedTimeSeriesInput,
                                     TimeSeriesListInput, TimeSeriesSetInput, TimeSeriesDictInput, TimeSeriesBundleInput>;
    inline constexpr auto ts_input_types_v = ts_input_types{};

    using ts_reference_input_types =
        tp::tpack<TimeSeriesReferenceInput, TimeSeriesValueReferenceInput, TimeSeriesWindowReferenceInput,
                  TimeSeriesListReferenceInput, TimeSeriesSetReferenceInput, TimeSeriesDictReferenceInput,
                  TimeSeriesBundleReferenceInput>;
    inline constexpr auto ts_reference_input_types_v = ts_reference_input_types{};

    using ts_output_types                   = tp::tpack<BaseTimeSeriesOutput, TimeSeriesValueOutputBase, IndexedTimeSeriesOutput,
                                                        TimeSeriesListOutput, TimeSeriesSetOutput, TimeSeriesDictOutput, TimeSeriesBundleOutput>;
    inline constexpr auto ts_output_types_v = ts_output_types{};

    using ts_reference_output_types =
        tp::tpack<TimeSeriesReferenceOutput, TimeSeriesValueReferenceOutput, TimeSeriesWindowReferenceOutput,
                  TimeSeriesListReferenceOutput, TimeSeriesSetReferenceOutput, TimeSeriesDictReferenceOutput,
                  TimeSeriesBundleReferenceOutput>;
    inline constexpr auto ts_reference_output_types_v = ts_reference_output_types{};

    using TimeSeriesInputVisitor =
        decltype(tp::make_v<ddv::mux>(ts_input_types_v +
                                      ts_reference_input_types_v
                                      // payload specialized inputs
                                      + tp::transform<tp::meta::apply<TimeSeriesValueInput>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesSetInput_T>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesDictInput_T>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesWindowInput>::type>(ts_payload_types_v)))::type;

    using TimeSeriesOutputVisitor =
        decltype(tp::make_v<ddv::mux>(ts_output_types_v +
                                      ts_reference_output_types_v
                                      // payload specialized outputs
                                      + tp::transform<tp::meta::apply<TimeSeriesValueOutput>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesSetOutput_T>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesDictOutput_T>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesFixedWindowOutput>::type>(ts_payload_types_v) +
                                      tp::transform<tp::meta::apply<TimeSeriesTimeWindowOutput>::type>(ts_payload_types_v)))::type;

    struct HGRAPH_EXPORT TimeSeriesType
    {
        using ptr   = TimeSeriesType *;
        using s_ptr = std::shared_ptr<TimeSeriesType>;

        // Pure virtual interface - constructors in derived classes
        TimeSeriesType()                       = default;
        TimeSeriesType(const TimeSeriesType &) = default;
        TimeSeriesType(TimeSeriesType &&)      = default;

        TimeSeriesType &operator=(const TimeSeriesType &) = default;
        TimeSeriesType &operator=(TimeSeriesType &&)      = default;
        virtual ~TimeSeriesType()                         = default;

        // Pure virtual methods to be implemented in derived classes

        // Graph navigation methods. These may not be required
        // (Other than for debugging) if we used the context approach
        [[nodiscard]] virtual node_ptr  owning_node()        = 0;
        [[nodiscard]] virtual node_ptr  owning_node() const  = 0;
        [[nodiscard]] virtual graph_ptr owning_graph()       = 0;
        [[nodiscard]] virtual graph_ptr owning_graph() const = 0;
        // Helper methods can be removed now we use ptr return types?
        [[nodiscard]] virtual bool has_parent_or_node() const = 0;
        [[nodiscard]] virtual bool has_owning_node() const    = 0;

        // Value of this node - for python API
        [[nodiscard]] virtual nb::object py_value() const       = 0;
        [[nodiscard]] virtual nb::object py_delta_value() const = 0;

        // When was this time-series last modified?
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
        // State of the value checks (related to last_modified_time and event type)
        [[nodiscard]] virtual bool modified() const  = 0;
        [[nodiscard]] virtual bool valid() const     = 0;
        [[nodiscard]] virtual bool all_valid() const = 0;

        /**
        FOR USE IN LIBRARY CODE.

        Change the owning node / time-series container of this time-series.
        This is used when grafting a time-series input from one node / time-series container to another.
        For example, see use in map implementation.
        */
        virtual void re_parent(node_ptr parent)                   = 0;
        virtual void re_parent(const time_series_type_ptr parent) = 0;
        virtual void reset_parent_or_node()                       = 0;
        // Currently used by builders to reset the state of the output. This because the time-series
        // does not currently support the life-cycle methods, may be better to change to support
        // life-cycle management?
        virtual void builder_release_cleanup() = 0;

        /*
         * This is used to deal with the fact we are not tracking the type in the time-series value.
         * We need to deal with reference vs non-reference detection and the 3 methods below help with that.
         */
        [[nodiscard]] virtual bool is_same_type(const TimeSeriesType *other) const = 0;
        [[nodiscard]] virtual bool is_reference() const                            = 0;
        [[nodiscard]] virtual bool has_reference() const                           = 0;

        static inline time_series_type_s_ptr null_ptr{};
    };

    struct TimeSeriesInput;
    struct OutputBuilder;
    struct TimeSeriesReferenceOutput;
    struct TimeSeriesReferenceInput;

    struct HGRAPH_EXPORT TimeSeriesOutput : TimeSeriesType,
                                            ddv::visitable<TimeSeriesOutput, TimeSeriesOutputVisitor>,
                                            arena_enable_shared_from_this<TimeSeriesOutput>
    {
        using ptr          = TimeSeriesOutput *;
        using s_ptr        = std::shared_ptr<TimeSeriesOutput>;
        TimeSeriesOutput() = default;

        // Output-specific navigation of the graph structure.
        [[nodiscard]] virtual s_ptr parent_output() const     = 0;
        [[nodiscard]] virtual s_ptr parent_output()           = 0;
        [[nodiscard]] virtual bool  has_parent_output() const = 0;

        // This is the key characteristic of an output node, it creates a shared state that
        // can be shared with other nodes/inputs. This allows for change notification.
        virtual void subscribe(Notifiable *node)    = 0;
        virtual void un_subscribe(Notifiable *node) = 0;

        // Mutation operations
        // The apply_result is the core mechanism to apply a python value to the
        // output, this will call py_set_value - If this gets a None, it will just return
        virtual void apply_result(const nb::object &value) = 0;
        // This os the method that does most of the work - if this gets a None, it will call invalidate
        virtual void py_set_value(const nb::object &value) = 0;
        // These methods were designed to allow C++ to perform a more optimal copy of values given its
        // knowledge of the internal state of the input/output passed. A copy visitor.
        virtual void copy_from_output(const TimeSeriesOutput &output) = 0;
        virtual void copy_from_input(const TimeSeriesInput &input)    = 0;

        // These seem to have a lot of overlap in terms of behaviour.
        // Clear will remove the value, internal tracking etc.
        virtual void clear() = 0;
        // Will reset the state and put the state back to it's unset state.
        virtual void invalidate() = 0;
        // There is a lot of overlap here as well, this is more of an internal mechanism, whereas invalidate is the
        // request driver, we may be able to collapse this
        virtual void mark_invalid() = 0;

        // The mechanism to indicate the time-series was modified, not intended to be exposed, thus we should look
        // to see if the mark_xxx method can be safely moved to the base implementation struct and not at the top level.
        virtual void mark_modified()                            = 0;
        virtual void mark_modified(engine_time_t modified_time) = 0;
        // This feels like an internal implementation logic, need to see what can be done as I suspect
        // This is in place largely for supporting TSD modified optimization.
        virtual void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) = 0;

        // This is used by the dequeing logic to work out how much we can de-queue from a push queue.
        // It could be moved into the queuing logic and implemented as a visitor, this would allow us to peek
        // The queue and perform the change, if the change is successful, we then pop the queue.
        virtual bool can_apply_result(const nb::object &value) = 0;
    };

    struct HGRAPH_EXPORT TimeSeriesInput : TimeSeriesType,
                                           Notifiable,
                                           ddv::visitable<TimeSeriesInput, TimeSeriesInputVisitor>,
                                           arena_enable_shared_from_this<TimeSeriesInput>
    {
        using ptr         = TimeSeriesInput *;
        using s_ptr       = std::shared_ptr<TimeSeriesInput>;
        TimeSeriesInput() = default;

        // Graph navigation specific to the input
        [[nodiscard]] virtual s_ptr parent_input() const     = 0;
        [[nodiscard]] virtual bool  has_parent_input() const = 0;

        // This is used to indicate if the owner of this input is interested in being notified when
        // modifications are made to the value represented by this input.
        [[nodiscard]] virtual bool active() const = 0;
        virtual void               make_active()  = 0;
        virtual void               make_passive() = 0;

        // Dealing with the various states the time-series can be found in, for the most part this
        // should not need to be exposed as a client facing API, but is used for internal state management.
        [[nodiscard]] virtual bool                     bound() const                                 = 0;
        [[nodiscard]] virtual bool                     has_peer() const                              = 0;
        [[nodiscard]] virtual time_series_output_s_ptr output() const                                = 0;
        [[nodiscard]] virtual bool                     has_output() const                            = 0;
        virtual bool                                   bind_output(time_series_output_s_ptr output_) = 0;
        virtual void                                   un_bind_output(bool unbind_refs)              = 0;

        // This is a feature used by the BackTrace tooling, this is not something that is generally
        // Useful, it could be handled through a visitor, or some other means of extraction.
        // This exposes internal implementation logic.
        [[nodiscard]] virtual time_series_reference_output_s_ptr reference_output() const = 0;

        // This is a hack to support REF time-series binding, this definitely needs to be revisited.
        [[nodiscard]] virtual s_ptr get_input(size_t index) = 0;
    };
}  // namespace hgraph

#endif  // TIME_SERIES_TYPE_H