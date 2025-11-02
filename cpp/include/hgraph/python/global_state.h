#ifndef HGRAPH_GLOBAL_STATE_H
#define HGRAPH_GLOBAL_STATE_H

#include <nanobind/nanobind.h>
#include <string>
#include <optional>

namespace nb = nanobind;

namespace hgraph {
    /**
     * Lightweight C++ wrapper around Python's GlobalState singleton.
     * This class provides access to the Python GlobalState by calling into Python.
     *
     * Python GlobalState is a dict-like object that provides global state accessible
     * across all graph components, useful for debugging and directory services.
     */
    class GlobalState {
    public:
        /**
         * Get the GlobalState instance by calling Python's GlobalState.instance()
         * @return Python GlobalState object
         * @throws std::runtime_error if no GlobalState instance exists
         */
        static nb::object instance();

        /**
         * Check if a GlobalState instance exists
         * @return true if GlobalState.has_instance() returns true
         */
        static bool has_instance();

        /**
         * Set a value in the GlobalState
         * @param key The key to set
         * @param value The value to store (can be any Python object)
         */
        static void set(const std::string &key, nb::object value);

        /**
         * Get a value from the GlobalState
         * @param key The key to retrieve
         * @return The value associated with the key
         * @throws nb::python_error if key doesn't exist
         */
        static nb::object get(const std::string &key);

        /**
         * Get a value from the GlobalState with a default
         * @param key The key to retrieve
         * @param default_value The default value to return if key doesn't exist
         * @return The value associated with the key, or default_value if not found
         */
        static nb::object get(const std::string &key, nb::object default_value);

        /**
         * Remove a key from the GlobalState
         * @param key The key to remove
         */
        static void remove(const std::string &key);

        /**
         * Check if a key exists in the GlobalState
         * @param key The key to check
         * @return true if the key exists
         */
        static bool contains(const std::string &key);

    private:
        // Private constructor - this is a utility class with only static methods
        GlobalState() = delete;

        // Get the Python GlobalState class
        static nb::object get_global_state_class();
    };
} // namespace hgraph

#endif // HGRAPH_GLOBAL_STATE_H