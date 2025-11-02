/**
 * Support for hashing and equality comparison of nanobind::object
 */

#ifndef HASHABLE_H
#define HASHABLE_H

#include <nanobind/nanobind.h>
#include <nanobind/intrusive/ref.h>
#include <functional>

namespace nanobind {
    inline bool operator==(const object &a, const object &b) noexcept {
        return a.equal(b);
    }
}

namespace std {
    template<>
    struct equal_to<nanobind::object> {
        bool operator()(const nanobind::object &a, const nanobind::object &b) const noexcept {
            return a.equal(b); // nanobind::object::equal handles Python equality
        }
    };

    template<>
    struct hash<nanobind::object> {
        size_t operator()(const nanobind::object &obj) const noexcept {
            // nb::object has .hash() method that returns Py_hash_t
            return static_cast<size_t>(nanobind::hash(obj));
        }
    };

    template<typename T, typename U>
    struct hash<std::tuple<T *, U *> > {
        size_t operator()(const std::tuple<T *, U *> &t) const noexcept {
            return std::hash<T *>()(std::get < 0 > (t)) ^ (std::hash<U *>()(std::get < 1 > (t)) << 1);
        }
    };

    template<typename T>
    struct equal_to<nanobind::ref<T> > {
        size_t operator()(const nanobind::ref<T> &a, const nanobind::ref<T> &b) const noexcept {
            return a.get() == b.get();
        }
    };

    template<typename T>
    struct hash<nanobind::ref<T> > {
        size_t operator()(const nanobind::ref<T> &t) const noexcept {
            return std::hash<const void *>()(reinterpret_cast<const void *>(t.get()));
        }
    };
} // namespace std

#endif  // HASHABLE_H