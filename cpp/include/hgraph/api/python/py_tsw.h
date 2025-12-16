// Version redirect header - includes from v1/ or v2/ based on build configuration
#ifndef HGRAPH_PY_TSW_REDIRECT_H
#define HGRAPH_PY_TSW_REDIRECT_H

#ifdef HGRAPH_API_V2
#include <hgraph/api/python/v2/py_tsw.h>
#else
#include <hgraph/api/python/v1/py_tsw.h>
#endif

#endif // HGRAPH_PY_TSW_REDIRECT_H
