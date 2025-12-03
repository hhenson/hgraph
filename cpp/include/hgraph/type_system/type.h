#ifndef HGRAPH_TYPE_H
#define HGRAPH_TYPE_H

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct Type {
        explicit Type(const std::string &name);

        virtual ~Type() = default;

        [[nodiscard]] std::string get_name() const;

        virtual bool is_scalar() const;
        virtual bool is_time_series() const;

    private:
        std::string _name;
    };

    template<typename T>
    struct type_t {
        
    };
} // namespace hgraph


#endif // HGRAPH_TYPE_H