// Version redirect header - includes from v1/ or v2/ based on build configuration
#ifndef HGRAPH_WRAPPER_FACTORY_REDIRECT_H
#define HGRAPH_WRAPPER_FACTORY_REDIRECT_H

#ifdef HGRAPH_API_V2
#include <hgraph/api/python/v2/wrapper_factory.h>
#else
#include <hgraph/api/python/v1/wrapper_factory.h>
#endif

#endif // HGRAPH_WRAPPER_FACTORY_REDIRECT_H
