//
// Created by Howard Henson on 04/04/2021.
//

#ifndef HGRAPH_SCALAR_VALUE_H
#define HGRAPH_SCALAR_VALUE_H

#include <cstddef>
#include <hgraph/hgraph_export.h>
#include <hgraph/types/schema.h>
#include <string>

#include <hgraph/util/date_time.h>
#include <boost/cast.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace hgraph {
    /*
     * Use the Type Erasure Patten (as described by Klaus Iglberger in 'Breaking Dependencies: Type Erasure - A Design Analysis)
     *
     * Other talks:
     * Pragmatic Type Erasure: Solving classic oop problems with an elegant design pattern
     * Back to Basics: Type Erasure
     * Dynamic Polymorphism with Code Injection and Metaclasses
     * Not leaving Performance on the Jump Table
     * Large Performance Grains by Data Orientation Design Principles Made Practical Through Generic Programming Components.
     *
     * Key ideas:
     * i. a class to represent the thing (in this case the ScalarValue)
     * ii. a concept class that provides a templated class who has the key behaviourism's as polymorphic methods.
     * iii. The implementation of the methods are delegated to polymorphic functions applied to the type supplied to ii.
     * iv. Create a polymorphic function for each behaviour that takes i. and delegates to the pimpl inside, which then delegates
     *     to the function of the actual type.
     *
     * Zoo - https://github.com/thecppzoo/zoo
     * Dyno - https://github.com/ldionne/dyno
     * Boost Type Erasure
     */

    /*
     * This implementation is a really basic first pass just to get the concept booted, ultimately I would like the scalar value
     * to support allocator aware construction to make the construction of types more efficient and also later to better support
     * serialisation.
     */
    using hg_byte = int8_t;
    using hg_int = int64_t;
    using hg_float = double;
    using hg_string = std::string;

    class ScalarValue;
    using scalar_type_cr = const ScalarValue &;

    namespace detail {
        struct ScalarTypeConcept {
            using u_ptr = std::unique_ptr<ScalarTypeConcept>;

            virtual ~ScalarTypeConcept() = default;

            [[nodiscard]] virtual bool operator==(const ScalarValue &other) const = 0;

            [[nodiscard]] virtual bool operator<(const ScalarValue &other) const = 0;

            [[nodiscard]] virtual size_t hashCode() const = 0;

            [[nodiscard]] virtual u_ptr clone() const = 0;

            [[nodiscard]] virtual u_ptr reference() const = 0;

            [[nodiscard]] virtual py::object py_object() const = 0;

            [[nodiscard]] virtual bool is_reference() const = 0;

            [[nodiscard]] virtual std::string to_string() const = 0;
        };

        // Extract the raw value from the scalar value type.
        template<typename T>
        T cast(scalar_type_cr);

        template<typename T>
        struct ScalarTypeModel : ScalarTypeConcept {
            explicit ScalarTypeModel(T value) : object{std::move(value)} {
            }

            [[nodiscard]] bool operator==(const ScalarValue &other) const override;

            [[nodiscard]] bool operator<(const ScalarValue &other) const override;

            [[nodiscard]] ScalarTypeConcept::u_ptr reference() const override;

            [[nodiscard]] ScalarTypeConcept::u_ptr clone() const override {
                return std::make_unique<ScalarTypeModel>(*this);
            }

            [[nodiscard]] size_t hashCode() const override {
                if constexpr (std::is_base_of_v<T, py::object>) {
                    return py::hash(object);
                } else {
                    return std::hash<T>()(object);
                }
            }

            [[nodiscard]] py::object py_object() const override {
                if constexpr (std::is_base_of_v<T, py::object>) {
                    return object;
                } else {
                    return py::cast(object);
                }
            }

            [[nodiscard]] bool is_reference() const final { return false; }
            [[nodiscard]] std::string to_string() const final { return fmt::format("{}", object); }
            T object;
        };

        template<typename T>
        struct ScalarTypeReference : ScalarTypeConcept {
            explicit ScalarTypeReference(ScalarTypeModel<T> *model) : referenced_model{model} {
            }

            explicit ScalarTypeReference(const ScalarTypeModel<T> *model)
                : ScalarTypeReference{const_cast<ScalarTypeModel<T> *>(model)} {
            }

            bool operator==(const ScalarValue &other) const override { return referenced_model->operator==(other); }
            bool operator<(const ScalarValue &other) const override { return referenced_model->operator<(other); }
            [[nodiscard]] size_t hashCode() const override { return referenced_model->hashCode(); }
            [[nodiscard]] u_ptr clone() const override { return referenced_model->clone(); }

            [[nodiscard]] u_ptr reference() const override {
                return std::make_unique<ScalarTypeReference<T> >(referenced_model);
            }

            [[nodiscard]] py::object py_object() const override { return referenced_model->py_object(); }
            [[nodiscard]] bool is_reference() const final { return true; }
            [[nodiscard]] std::string to_string() const final { return referenced_model->to_string(); }
            // There should be no way to retain a reference without making a copy (if we get it all correct)
            // So hopefully no way to leave a dangling pointer around, so we will just use a raw pointer for now.
            ScalarTypeModel<T> *referenced_model;
        };
    } // namespace detail

    /**
     * A generic scalar in hgraph represent a value that is a point-in-time representation of state.
     * Scalar values are intended to be immutable and as such should be safe to hash and use equality.
     */
    class HGRAPH_EXPORT ScalarValue {
        // If we can do a make_scalar_type we can try and put the instance of the piml just after the
        // scalar type in memory and also allocate the memory in one go to reduce the number of allocations.
        std::unique_ptr<detail::ScalarTypeConcept> m_pimpl;

    public:
        using s_ptr = std::shared_ptr<ScalarValue>;

        ScalarValue() : m_pimpl{} {
        }

        explicit ScalarValue(std::unique_ptr<detail::ScalarTypeConcept> value) : m_pimpl{std::move(value)} {
        }

        ScalarValue(const ScalarValue &); // Create a copy of the value
        explicit ScalarValue(const ScalarValue *); // Create a reference to the value.
        ScalarValue(ScalarValue &&) = default; // Move value
        [[nodiscard]] bool operator==(const ScalarValue &other) const { return m_pimpl->operator==(other); }
        [[nodiscard]] bool operator<(const ScalarValue &other) const { return m_pimpl->operator<(other); }

        friend std::hash<ScalarValue>;
        template<typename T>
        friend struct detail::ScalarTypeModel;

        ScalarValue &operator=(ScalarValue &&) = default;

        ScalarValue &operator=(const ScalarValue &);

        template<typename T>
        [[nodiscard]] const T &as() const {
            auto mdl{dynamic_cast<detail::ScalarTypeModel<T> *>(m_pimpl.get())};
            if (mdl) return mdl->object;
            auto ref_mdl{dynamic_cast<detail::ScalarTypeReference<T> *>(m_pimpl.get())};
            if (ref_mdl) return ref_mdl->referenced_model->object;
            throw std::runtime_error(
                fmt::format("ScalarValue does not contain a value of type: '{}'", typeid(T).name()));
        }

        [[nodiscard]] py::object py_object() const { return m_pimpl->py_object(); }

        template<typename T>
        [[nodiscard]] bool is() const {
            return dynamic_cast<detail::ScalarTypeModel<T> *>(m_pimpl.get()) ||
                   dynamic_cast<detail::ScalarTypeReference<T> *>(m_pimpl.get());
        }

        [[nodiscard]] bool is_reference() const { return m_pimpl->is_reference(); }

        [[nodiscard]] ScalarValue reference() const { return ScalarValue{m_pimpl->reference()}; }

        [[nodiscard]] ScalarValue clone() const { return ScalarValue{m_pimpl->clone()}; }

        [[nodiscard]] bool is_un_set() const { return !(bool) m_pimpl; }

        [[nodiscard]] std::string to_string() const { return m_pimpl->to_string(); }

        static void py_register(py::module_ &m);
    };

    // Create a stand-alone scalar type from a scalar value raw type.
    template<typename T>
    ScalarValue create_scalar_value(T value) {
        auto stm{std::make_unique<detail::ScalarTypeModel<T> >(std::move(value))};
        return ScalarValue(std::move(stm));
    }

    template<typename T>
    ScalarValue::s_ptr make_shared_scalar_value(T value) {
        auto stm{std::make_unique<detail::ScalarTypeModel<T> >(std::move(value))};
        return std::make_shared<ScalarValue>(std::move(stm));
    }

    namespace detail {
        template<typename T>
        bool ScalarTypeModel<T>::operator==(const ScalarValue &other) const {
            auto other_model{dynamic_cast<ScalarTypeModel<T> *>(other.m_pimpl.get())};
            if constexpr (std::is_base_of_v<T, py::object>) {
                return other_model && object.equal(other_model->object);
            } else {
                return other_model && object == other_model->object;
            }
        }

        template<typename T>
        bool ScalarTypeModel<T>::operator<(const ScalarValue &other) const {
            auto other_model{dynamic_cast<ScalarTypeModel<T> *>(other.m_pimpl.get())};
            return (other_model && object < other_model->object) || typeid(this).before(typeid(other.m_pimpl.get()));
        }

        template<typename T>
        ScalarTypeConcept::u_ptr ScalarTypeModel<T>::reference() const {
            return std::make_unique<ScalarTypeReference<T> >(this);
        }

        using ScalarTypeModelBool = ScalarTypeModel<bool>;
        using ScalarTypeModelByte = ScalarTypeModel<hg_byte>;
        using ScalarTypeModelInt = ScalarTypeModel<hg_int>;
        using ScalarTypeModelFloat = ScalarTypeModel<hg_float>;

        // TODO: In future these should be immutable data structures that have better memory layout support
        using ScalarTypeModelString = ScalarTypeModel<hg_string>;
        using ScalarTime = ScalarTypeModel<engine_time_t>;
        using ScalarDeltaTime = ScalarTypeModel<engine_time_delta_t>;
        using ScalarTypeModelList = ScalarTypeModel<std::vector<ScalarValue> >;
        using ScalarTypeModelSet = ScalarTypeModel<std::unordered_set<ScalarValue> >;
        using ScalarTypeModelDict = ScalarTypeModel<std::unordered_map<ScalarValue, ScalarValue> >;
        using ScalarTypeModelTuple = ScalarTypeModel<std::vector<ScalarValue> >;

        using ScalarTypeModelPython = ScalarTypeModel<py::object>;
    } // namespace detail

    using hg_set = std::unordered_set<ScalarValue>;
    using hg_dict = std::unordered_map<ScalarValue, ScalarValue>;
    using hg_tuple = std::vector<ScalarValue>;
} // namespace hgraph

namespace fmt {
    template<>
    struct formatter<hgraph::ScalarValue> : formatter<string_view> {
        // parse is inherited from formatter<string_view>.
        template<typename FormatContext>
        auto format(const hgraph::ScalarValue &c, FormatContext &ctx) const {
            return formatter<string_view>::format(c.to_string(), ctx);
        }
    };

    template<>
    struct formatter<py::object> : formatter<string_view> {
        // parse is inherited from formatter<string_view>.
        template<typename FormatContext>
        auto format(const py::object &c, FormatContext &ctx) const {
            return formatter<string_view>::format(c.cast<std::string>(), ctx);
        }
    };
} // namespace fmt

namespace std {
    template<>
    struct hash<hgraph::ScalarValue> {
        std::size_t operator()(const hgraph::ScalarValue &st) const { return st.m_pimpl->hashCode(); }
    };

    template<>
    struct hash<py::object> {
        std::size_t operator()(const py::object &obj) const { return obj.attr("__hash__")().cast<size_t>(); }
    };

    template<>
    struct hash<hgraph::engine_time_t> {
        std::size_t operator()(const hgraph::engine_time_t &obj) const { return obj.time_since_epoch().count(); }
    };

    template<>
    struct hash<std::vector<hgraph::ScalarValue> > {
        std::size_t operator()(const std::vector<hgraph::ScalarValue> &st) const {
            // Use a dumb hash function builder, rely on overflow to keep this going round
            return std::accumulate(st.begin(), st.end(), (size_t) 0,
                                   [](size_t h, const auto &v) { return hash<hgraph::ScalarValue>()(v) + h * 31; });
        }
    };
} // namespace std

#endif  // HGRAPH_SCALAR_VALUE_H
