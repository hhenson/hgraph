//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_EVALUATION_ENGINE_H
#define HGRAPH_CPP_ROOT_EVALUATION_ENGINE_H

#include <hgraph/v2/runtime/graph_executor_ops.h>

namespace hgraph::v2
{
    struct GraphExecutor
    {
        GraphExecutor(GraphExecutorOps *ops, void *data);
        ~GraphExecutor();

        void run();
    private:
        GraphExecutorOps *_ops;
        void *_data;
    };
}

#endif //HGRAPH_CPP_ROOT_EVALUATION_ENGINE_H
