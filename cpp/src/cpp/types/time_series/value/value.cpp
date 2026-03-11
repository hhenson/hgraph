#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>

namespace hgraph {

Value::Value(const value::TypeMeta &schema)
    : m_schema(schema)
{
    ValueStateFactory::construct(m_state, &m_schema.get());
}

Value::Value(const Value &other)
    : m_schema(other.m_schema)
{
    ValueStateFactory::copy_construct(m_state, other.m_state, &m_schema.get());
}

Value::Value(Value &&other) noexcept
    : m_schema(other.m_schema)
{
    ValueStateFactory::move_construct(m_state, other.m_state, &m_schema.get());
    ValueStateFactory::construct(other.m_state, &other.m_schema.get());
}

Value &Value::operator=(const Value &other)
{
    if (this != &other) {
        if (schema() != other.schema()) {
            throw std::invalid_argument("Value copy assignment requires matching schema");
        }
        reset();
        ValueStateFactory::copy_construct(m_state, other.m_state, &m_schema.get());
    }
    return *this;
}

Value &Value::operator=(Value &&other)
{
    if (this != &other) {
        if (schema() != other.schema()) {
            throw std::invalid_argument("Value move assignment requires matching schema");
        }
        reset();
        ValueStateFactory::move_construct(m_state, other.m_state, &m_schema.get());
        ValueStateFactory::construct(other.m_state, &other.m_schema.get());
    }
    return *this;
}

Value::~Value()
{
    reset();
}

bool Value::valid() const noexcept
{
    return true;
}

Value::operator bool() const noexcept
{
    return valid();
}

const value::TypeMeta *Value::schema() const noexcept
{
    return &m_schema.get();
}

View Value::view() noexcept
{
    return ValueStateFactory::view_of(m_state, &m_schema.get());
}

View Value::view() const noexcept
{
    return ValueStateFactory::view_of(m_state, &m_schema.get());
}

void Value::reset() noexcept
{
    ValueStateFactory::destroy(m_state, &m_schema.get());
}

}  // namespace hgraph
