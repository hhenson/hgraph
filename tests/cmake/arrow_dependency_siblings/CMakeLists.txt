cmake_minimum_required(VERSION 3.25)
project(hgraph_arrow_dependency_siblings LANGUAGES NONE)

add_subdirectory(../arrow_dependencies first)
add_subdirectory(../arrow_dependencies second)
