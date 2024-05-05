//
// Created by Howard Henson on 05/05/2024.
//

#ifndef GRAPH_H
#define GRAPH_H

#include<hgraph/util/lifecycle.h>
#include<optional>
#include<vector>

namespace hgraph {
    class Node;

    struct HGRAPH_EXPORT Graph: ComponentLifeCycle {
        std::optional<Node*> parent_node;
        std::vector<int64_t> graph_id;
        std::string label;
        std::vector<Node*> nodes;

    };

}

#endif //GRAPH_H
