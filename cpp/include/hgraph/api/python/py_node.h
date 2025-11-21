#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    struct NodeScheduler;
    struct Node;

    struct PyNodeScheduler
    {
        using api_ptr = ApiPtr<NodeScheduler>;

        explicit PyNodeScheduler(api_ptr scheduler);

        [[nodiscard]] engine_time_t next_scheduled_time() const;

        [[nodiscard]] nb::bool_ requires_scheduling() const;

        [[nodiscard]] nb::bool_ is_scheduled() const;

        [[nodiscard]] nb::bool_ is_scheduled_now() const;

        [[nodiscard]] nb::bool_ has_tag(const std::string &tag) const;

        [[nodiscard]] engine_time_t pop_tag(const std::string &tag) const;

        [[nodiscard]] engine_time_t pop_tag(const std::string &tag, engine_time_t default_time) const;

        void schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock = false) const;

        void schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock = false) const;

        void un_schedule(const std::string &tag) const;

        void un_schedule() const;

        void reset();

        static void register_with_nanobind(nb::module_ &m);

      private:
        api_ptr _impl;
    };

    struct PyGraph;

    struct HGRAPH_EXPORT PyNode
    {
        using api_ptr = ApiPtr<Node>;

        explicit PyNode(api_ptr node);

        void notify(const nb::handle&) const;

        // void notify_next_cycle();

        [[nodiscard]] nb::int_ node_ndx() const;

        [[nodiscard]] nb::tuple owning_graph_id() const;

        [[nodiscard]] nb::tuple node_id() const;

        [[nodiscard]] const NodeSignature &signature() const;

        [[nodiscard]] const nb::dict &scalars() const;

        [[nodiscard]] PyGraph graph() const;

        [[nodiscard]] time_series_bundle_input_ptr input() const;

        [[nodiscard]] nb::dict inputs() const;

        [[nodiscard]] nb::tuple start_inputs() const;

        time_series_output_ptr output();

        nb::object recordable_state();

        [[nodiscard]] nb::bool_ has_recordable_state() const;

        [[nodiscard]] nb::object scheduler() const;  // PyNodeScheduler

        [[nodiscard]] nb::bool_ has_scheduler() const;

        time_series_output_ptr error_output();

        [[nodiscard]] nb::bool_ has_input() const;

        [[nodiscard]] nb::bool_ has_output() const;

        [[nodiscard]] nb::str repr() const;

        [[nodiscard]] nb::str str() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        [[nodiscard]] control_block_ptr control_block() const;

        template <typename U>
            requires std::is_base_of_v<Node, U>
        U *static_cast_impl() const {
            return _impl.static_cast_<U>();
        }

        template <typename U>
            requires std::is_base_of_v<Node, U>
        U *dynamic_cast_impl() const {
            return _impl.dynamic_cast_<U>();
        }

      private:
        friend Node *unwrap_node(const PyNode &obj);
        api_ptr      _impl;
    };
}  // namespace hgraph
