//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
#define HGRAPH_CPP_ROOT_TYPE_REGISTRY_H

namespace hgraph::v2
{
    /**
     * Holds the type and schema information to ensure all "type" data is cached and can be used as interned values.
     * Thus, pointer comparison is equality.
     * This trades off some space for the benefit of performance in actual use and given we expect types to be re-used a lot
     * this is a good tradeoff.
     */
    struct TypeRegistry {};
}

#endif //HGRAPH_CPP_ROOT_TYPE_REGISTRY_H
