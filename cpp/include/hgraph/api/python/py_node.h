#pragma once

#include "hgraph/nodes/mesh_node.h"

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

        void notify(const nb::handle &) const;

        // void notify_next_cycle();

        [[nodiscard]] nb::int_ node_ndx() const;

        [[nodiscard]] nb::tuple owning_graph_id() const;

        [[nodiscard]] nb::tuple node_id() const;

        [[nodiscard]] const NodeSignature &signature() const;

        [[nodiscard]] const nb::dict &scalars() const;

        [[nodiscard]] PyGraph graph() const;

        [[nodiscard]] nb::object input() const;

        [[nodiscard]] nb::dict inputs() const;

        [[nodiscard]] nb::tuple start_inputs() const;

        [[nodiscard]] nb::object output();

        nb::object recordable_state();

        [[nodiscard]] nb::bool_ has_recordable_state() const;

        [[nodiscard]] nb::object scheduler() const;  // PyNodeScheduler

        [[nodiscard]] nb::bool_ has_scheduler() const;

        [[nodiscard]] nb::object error_output();

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
        friend node_s_ptr unwrap_node(const PyNode &obj);
        api_ptr      _impl;
    };

    struct PyPushQueueNode : PyNode
    {
        using PyNode::PyNode;

        nb::int_ messages_in_queue() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyNestedNode : PyNode
    {
        using PyNode::PyNode;

        engine_time_t last_evaluation_time() const;

        nb::dict nested_graphs() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyMapNestedNode : PyNestedNode
    {
        using PyNestedNode::PyNestedNode;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyMeshNestedNode : PyNestedNode
    {
        template <typename T> static PyMeshNestedNode make_mesh_node(api_ptr node);

        bool add_graph_dependency(const nb::handle &key, const nb::handle &depends_on) const {
            return _add_graph_dependency_fn(*this, key, depends_on);
        }
        void remove_graph_dependency(const nb::handle &key, const nb::handle &depends_on) const {
            _remove_graph_dependency_fn(*this, key, depends_on);
        }

        static void register_with_nanobind(nb::module_ &m);

      private:
        explicit PyMeshNestedNode(api_ptr ptr, auto add_fn, auto remove_fn)
            : PyNestedNode(std::move(ptr)), _add_graph_dependency_fn(add_fn), _remove_graph_dependency_fn(remove_fn) {};
        std::function<bool(const PyMeshNestedNode &, const nb::handle &, const nb::handle &)> _add_graph_dependency_fn;
        std::function<void(const PyMeshNestedNode &, const nb::handle &, const nb::handle &)> _remove_graph_dependency_fn;
    };

    template <typename T> PyMeshNestedNode PyMeshNestedNode::make_mesh_node(api_ptr node) {
        return PyMeshNestedNode(
            std::move(node),
            [](const PyMeshNestedNode &self, const nb::handle &key, const nb::handle &depends_on) {
                return self.static_cast_impl<MeshNode<T>>()->_add_graph_dependency(nb::cast<T>(key), nb::cast<T>(depends_on));
            },
            [](const PyMeshNestedNode &self, const nb::handle &key, const nb::handle &depends_on) {
                return self.static_cast_impl<MeshNode<T>>()->_remove_graph_dependency(nb::cast<T>(key), nb::cast<T>(depends_on));
            });
    }

}  // namespace hgraph
