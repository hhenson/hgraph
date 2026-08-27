#include <hgraph/types/time_series/endpoint_schema.h>

#include <stdexcept>
#include <string_view>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const TSValueTypeMetaData *child_schema_at(const TSValueTypeMetaData &schema,
                                                                 std::size_t                index)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    if (index >= schema.field_count())
                    {
                        throw std::out_of_range("TSEndpointSchema child index is out of range for TSB");
                    }
                    return schema.fields()[index].type;

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        if (index != 0)
                        {
                            throw std::out_of_range(
                                "TSEndpointSchema child index is out of range for dynamic TSL");
                        }
                        return schema.element_ts();
                    }
                    if (index >= schema.fixed_size())
                    {
                        throw std::out_of_range("TSEndpointSchema child index is out of range for TSL");
                    }
                    return schema.element_ts();

                case TSTypeKind::TSD:
                    if (index != 0)
                    {
                        throw std::out_of_range("TSEndpointSchema child index is out of range for TSD");
                    }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument(
                        "TSEndpointSchema non-peered annotations require TSB, fixed TSL, or TSD");
            }
        }

        [[nodiscard]] std::size_t non_peered_child_count(const TSValueTypeMetaData &schema)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    return schema.field_count();

                case TSTypeKind::TSL:
                    return schema.fixed_size() == 0 ? 1 : schema.fixed_size();

                case TSTypeKind::TSD:
                    return 1;

                default:
                    throw std::invalid_argument(
                        "TSEndpointSchema non-peered annotations require TSB, fixed TSL, or TSD");
            }
        }

        void validate_schema(const TSValueTypeMetaData *schema, const char *what)
        {
            if (schema == nullptr) { throw std::invalid_argument(what); }
        }

        void validate_non_peered_children(const TSValueTypeMetaData           &schema,
                                          const std::vector<TSEndpointSchema> &children)
        {
            const auto expected = non_peered_child_count(schema);
            if (children.size() != expected)
            {
                throw std::invalid_argument("TSEndpointSchema non-peered annotation has the wrong child count");
            }

            for (std::size_t index = 0; index < children.size(); ++index)
            {
                const auto *expected_schema = child_schema_at(schema, index);
                const auto *actual_schema   = children[index].schema();
                if (!time_series_schema_equivalent(expected_schema, actual_schema))
                {
                    throw std::invalid_argument(
                        "TSEndpointSchema child annotation schema does not match the parent schema");
                }
            }
        }
    }  // namespace

    const ValueTypeMetaData *value_schema_without_storage(const ValueTypeMetaData *schema) noexcept
    {
        // A loop, not one unwrap: a storage category may sit over another, and
        // every layer is invisible to the type system.
        while (schema != nullptr && schema->is_indirect() && schema->element_type != nullptr)
        {
            schema = schema->element_type;
        }
        return schema;
    }

    bool time_series_schema_equivalent(const TSValueTypeMetaData *lhs,
                                       const TSValueTypeMetaData *rhs) noexcept
    {
        if (lhs == rhs) { return true; }
        if (lhs == nullptr || rhs == nullptr || lhs->kind != rhs->kind) { return false; }

        switch (lhs->kind)
        {
            case TSTypeKind::TS:
            case TSTypeKind::TSS:
            case TSTypeKind::TSW:
            case TSTypeKind::SIGNAL:
                return value_schema_without_storage(lhs->value_type) ==
                           value_schema_without_storage(rhs->value_type) &&
                       lhs->period() == rhs->period() &&
                       lhs->min_period() == rhs->min_period() &&
                       lhs->time_range() == rhs->time_range() &&
                       lhs->min_time_range() == rhs->min_time_range();

            case TSTypeKind::TSD:
                return lhs->key_type() == rhs->key_type() &&
                       time_series_schema_equivalent(lhs->element_ts(), rhs->element_ts());

            case TSTypeKind::TSL:
                return lhs->fixed_size() == rhs->fixed_size() &&
                       time_series_schema_equivalent(lhs->element_ts(), rhs->element_ts());

            case TSTypeKind::TSB:
                if (lhs->field_count() != rhs->field_count()) { return false; }
                for (std::size_t index = 0; index < lhs->field_count(); ++index)
                {
                    const auto &l = lhs->fields()[index];
                    const auto &r = rhs->fields()[index];
                    const std::string_view lname = l.name != nullptr ? std::string_view{l.name} : std::string_view{};
                    const std::string_view rname = r.name != nullptr ? std::string_view{r.name} : std::string_view{};
                    if (lname != rname || !time_series_schema_equivalent(l.type, r.type)) { return false; }
                }
                return true;

            case TSTypeKind::REF:
                return time_series_schema_equivalent(lhs->element_ts(), rhs->element_ts());
        }
        return false;
    }

    TSEndpointSchema::TSEndpointSchema() noexcept = default;

    TSEndpointSchema::TSEndpointSchema(TSEndpointRole             role,
                                       const TSValueTypeMetaData *schema,
                                       std::vector<TSEndpointSchema> children)
        : role_(role),
          schema_(schema),
          children_(std::move(children))
    {
    }

    TSEndpointSchema TSEndpointSchema::peered(const TSValueTypeMetaData *schema)
    {
        validate_schema(schema, "TSEndpointSchema::peered requires a schema");
        return TSEndpointSchema{TSEndpointRole::Peered, schema, {}};
    }

    TSEndpointSchema TSEndpointSchema::local(const TSValueTypeMetaData *schema)
    {
        validate_schema(schema, "TSEndpointSchema::owned requires a schema");
        return TSEndpointSchema{TSEndpointRole::Local, schema, {}};
    }

    TSEndpointSchema TSEndpointSchema::non_peered(
        const TSValueTypeMetaData     *schema,
        std::vector<TSEndpointSchema> children)
    {
        validate_schema(schema, "TSEndpointSchema::non_peered requires a schema");
        validate_non_peered_children(*schema, children);
        return TSEndpointSchema{TSEndpointRole::NonPeered, schema, std::move(children)};
    }

    TSEndpointSchema TSEndpointSchema::non_peered_list(
        const TSValueTypeMetaData *schema,
        const TSEndpointSchema    &element)
    {
        validate_schema(schema, "TSEndpointSchema::non_peered_list requires a schema");
        if (schema->kind != TSTypeKind::TSL)
        {
            throw std::invalid_argument("TSEndpointSchema::non_peered_list requires a TSL schema");
        }
        const auto size = non_peered_child_count(*schema);
        if (!time_series_schema_equivalent(schema->element_ts(), element.schema()))
        {
            throw std::invalid_argument("TSEndpointSchema list element annotation schema does not match the TSL element");
        }
        return non_peered(schema, std::vector<TSEndpointSchema>(size, element));
    }

    TSEndpointSchema TSEndpointSchema::non_peered_dict(
        const TSValueTypeMetaData *schema,
        const TSEndpointSchema    &element)
    {
        validate_schema(schema, "TSEndpointSchema::non_peered_dict requires a schema");
        if (schema->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSEndpointSchema::non_peered_dict requires a TSD schema");
        }
        if (!time_series_schema_equivalent(schema->element_ts(), element.schema()))
        {
            throw std::invalid_argument("TSEndpointSchema dict element annotation schema does not match the TSD element");
        }
        return non_peered(schema, std::vector<TSEndpointSchema>{element});
    }

    bool TSEndpointSchema::empty() const noexcept
    {
        return schema_ == nullptr;
    }

    TSEndpointRole TSEndpointSchema::role() const noexcept
    {
        return role_;
    }

    const TSValueTypeMetaData *TSEndpointSchema::schema() const noexcept
    {
        return schema_;
    }

    bool TSEndpointSchema::is_peered() const noexcept
    {
        return role_ == TSEndpointRole::Peered && schema_ != nullptr;
    }

    bool TSEndpointSchema::is_non_peered() const noexcept
    {
        return role_ == TSEndpointRole::NonPeered && schema_ != nullptr;
    }

    bool TSEndpointSchema::is_local() const noexcept
    {
        return role_ == TSEndpointRole::Local && schema_ != nullptr;
    }

    std::size_t TSEndpointSchema::child_count() const noexcept
    {
        return children_.size();
    }

    const TSEndpointSchema &TSEndpointSchema::child(std::size_t index) const
    {
        return children_.at(index);
    }

    const std::vector<TSEndpointSchema> &TSEndpointSchema::children() const noexcept
    {
        return children_;
    }
}  // namespace hgraph
