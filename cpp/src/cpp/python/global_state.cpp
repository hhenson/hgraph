#include <hgraph/python/global_state.h>
#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph {
    nb::object GlobalState::get_global_state_class() {
        // Import the GlobalState class from hgraph._runtime._global_state
        static nb::object global_state_class = []() {
            nb::object module = nb::module_::import_("hgraph._runtime._global_state");
            return module.attr("GlobalState");
        }();
        return global_state_class;
    }

    nb::object GlobalState::instance() {
        nb::object gs_class = get_global_state_class();
        return gs_class.attr("instance")();
    }

    bool GlobalState::has_instance() {
        try {
            nb::object gs_class = get_global_state_class();
            nb::object result = gs_class.attr("has_instance")();
            return nb::cast<bool>(result);
        } catch (const nb::python_error &e) {
            return false;
        }
    }

    void GlobalState::set(const std::string &key, nb::object value) {
        nb::object gs = instance();
        gs[nb::str(key.c_str())] = value;
    }

    nb::object GlobalState::get(const std::string &key) {
        nb::object gs = instance();
        return gs[nb::str(key.c_str())];
    }

    nb::object GlobalState::get(const std::string &key, nb::object default_value) {
        try {
            nb::object gs = instance();
            return gs.attr("get")(nb::str(key.c_str()), default_value);
        } catch (const nb::python_error &e) {
            return default_value;
        }
    }

    void GlobalState::remove(const std::string &key) {
        try {
            nb::object gs = instance();
            // Python GlobalState uses __delitem__ for removal
            // Use the del keyword via Python's built-in del operation
            gs.attr("__delitem__")(nb::str(key.c_str()));
        } catch (const nb::python_error &e) {
            // Ignore errors if key doesn't exist
        }
    }

    bool GlobalState::contains(const std::string &key) {
        try {
            nb::object gs = instance();
            return nb::cast<bool>(gs.attr("__contains__")(nb::str(key.c_str())));
        } catch (const nb::python_error &e) {
            return false;
        }
    }
} // namespace hgraph