#ifndef HGRAPH_CPP_ROOT_CONFIG_H
#define HGRAPH_CPP_ROOT_CONFIG_H

/*
 * CLion's code model can grey out Python bridge code when the active
 * configure profile has HGRAPH_ENABLE_PYTHON_USER_NODES=0. Keep real
 * compiler builds controlled by CMake, but let JetBrains code insight
 * parse the bridge surface.
 */
#if defined(__JETBRAINS_IDE__)
#undef HGRAPH_ENABLE_PYTHON_USER_NODES
#define HGRAPH_ENABLE_PYTHON_USER_NODES 1
#elif !defined(HGRAPH_ENABLE_PYTHON_USER_NODES)
#define HGRAPH_ENABLE_PYTHON_USER_NODES 0
#endif

#if defined(_MSC_VER)
#define HGRAPH_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define HGRAPH_NOINLINE __attribute__((noinline))
#else
#define HGRAPH_NOINLINE
#endif

#endif  // HGRAPH_CPP_ROOT_CONFIG_H
