#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/state.h>

namespace hgraph {

Value::Value(const value::TypeMeta &schema)
    : m_builder(ValueBuilderFactory::checked_builder_for(&schema))
{
    allocate_and_construct();
}

Value::Value(const Value &other)
    : m_builder(other.m_builder)
{
    if (other.m_dispatch == nullptr) { return; }

    void *memory = m_builder.get().allocate();
    try {
        m_builder.get().copy_construct(memory, other.m_dispatch, other.m_builder);
        m_dispatch = reinterpret_cast<detail::ViewDispatch *>(memory);
    } catch (...) {
        m_builder.get().deallocate(memory);
        throw;
    }
}

Value::Value(Value &&other)
    : m_builder(other.m_builder)
{
    m_dispatch       = other.m_dispatch;
    other.m_dispatch = nullptr;
}

Value &Value::operator=(const Value &other)
{
    if (this != &other) {
        if (&m_builder.get() != &other.m_builder.get()) {
            throw std::invalid_argument("Value copy assignment requires matching builder");
        }

        if (other.m_dispatch == nullptr) {
            reset();
            return *this;
        }

        void *replacement = m_builder.get().allocate();
        try {
            m_builder.get().copy_construct(replacement, other.m_dispatch, other.m_builder);
        } catch (...) {
            m_builder.get().deallocate(replacement);
            throw;
        }

        reset();
        m_dispatch = reinterpret_cast<detail::ViewDispatch *>(replacement);
    }
    return *this;
}

Value &Value::operator=(Value &&other)
{
    if (this != &other) {
        if (&m_builder.get() != &other.m_builder.get()) {
            throw std::invalid_argument("Value move assignment requires matching builder");
        }

        reset();
        m_dispatch       = other.m_dispatch;
        other.m_dispatch = nullptr;
    }
    return *this;
}

Value::~Value()
{
    reset();
}

bool Value::valid() const noexcept
{
    return m_dispatch != nullptr;
}

Value::operator bool() const noexcept
{
    return valid();
}

const value::TypeMeta *Value::schema() const noexcept
{
    return &m_builder.get().schema();
}

View Value::view() noexcept
{
    return View{m_dispatch, schema()};
}

View Value::view() const noexcept
{
    return View{m_dispatch, schema()};
}

void Value::allocate_and_construct()
{
    void *memory = m_builder.get().allocate();
    try {
        m_builder.get().construct(memory);
        m_dispatch = reinterpret_cast<detail::ViewDispatch *>(memory);
    } catch (...) {
        m_builder.get().deallocate(memory);
        throw;
    }
}

void Value::reset() noexcept
{
    if (m_dispatch != nullptr) {
        m_builder.get().destroy(m_dispatch);
        m_builder.get().deallocate(m_dispatch);
        m_dispatch = nullptr;
    }
}

}  // namespace hgraph
