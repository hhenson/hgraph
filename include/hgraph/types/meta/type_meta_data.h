//
// Created by Howard Henson on 04/05/2024.
//

#ifndef TYPE_META_DATA_H
#define TYPE_META_DATA_H

#include <hgraph/python/pyb.h>
#include <hgraph/hgraph_export.h>

namespace hgraph {

    struct HGRAPH_EXPORT HgTypeMetaData {
        virtual bool is_resolved() const;
        virtual bool is_scalar() const;
        virtual bool is_atomic() const;
        virtual bool is_generic() const;
        virtual bool is_injectable() const;
        virtual bool is_reference() const;
        virtual bool is_context() const;
        virtual py::type_ is_context() const;
    };

}

#endif //TYPE_META_DATA_H
