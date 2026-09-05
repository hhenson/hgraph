# syntax=docker/dockerfile:1.7
# Container image for hgl (RFC 0032): `hgl`, the installed SDK with every
# dependency it names, and a C++ toolchain, so runtime (`state`/`when`)
# functions compile inside the container and a CMake project can build an
# `hgl_add_module()` package against `find_package(hgraph)` in it.
# .github/workflows/packaging.yml builds the image on pull requests and runs
# the smoke and the installed-SDK consumer inside it; pushing to
# ghcr.io/hhenson/hgl:<release> is a release-switch step and happens nowhere
# yet.
#
#   docker build -f packaging/docker/Dockerfile \
#       --build-arg HGRAPH_RELEASE_VERSION=0.8.23 -t hgl:0.8.23 .
#   docker run --rm -v "$PWD:/work" hgl:0.8.23 hgl test smoke.hgl
#
# The image runs as the unprivileged user `hgl` (uid 1000, home /home/hgl,
# where compiled runtime functions are cached). Pass --user root to install
# extra packages or to write into a mounted directory owned by root.

ARG DEBIAN_CODENAME=trixie

# Sources of the SDK dependencies Debian ships below the floors in
# CMakeLists.txt. Keep the tags in step with the FetchContent pins there.
ARG FMT_TAG=12.2.0
ARG SPDLOG_TAG=v1.15.3
ARG SIMDJSON_TAG=v4.6.4
ARG DATE_TAG=v3.0.4

# ---------------------------------------------------------------------------
# Runtime dependencies shared by the build and the final image. Arrow (with
# compute and Acero) comes from the Apache apt repository because Debian's
# Arrow predates the compute/acero split hgraph links. The -dev packages
# stay in the final image: runtime functions compile against the SDK
# headers, which include Arrow's.
FROM debian:${DEBIAN_CODENAME}-slim AS base
ARG DEBIAN_CODENAME
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl -fsS --proto '=https' -o "apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
      "https://packages.apache.org/artifactory/arrow/debian/apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && apt-get install -y --no-install-recommends "./apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && rm "./apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      clang-format cmake g++ libarrow-acero-dev libarrow-compute-dev libarrow-dev ninja-build tzdata \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# The SDK dependencies the installed hgraphConfig.cmake resolves with
# find_dependency (fmt, spdlog, simdjson) plus date/tz, built as shared
# libraries into one prefix that both the build and the final image take as
# /usr/local. Installing them, rather than letting the tree fetch them, is
# what makes `find_package(hgraph)` work inside the image.
FROM base AS deps
ARG FMT_TAG
ARG SPDLOG_TAG
ARG SIMDJSON_TAG
ARG DATE_TAG
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /deps
RUN git clone --depth 1 --branch "${FMT_TAG}" https://github.com/fmtlib/fmt.git fmt \
 && cmake -S fmt -B fmt/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/deps -DBUILD_SHARED_LIBS=ON \
      -DFMT_DOC=OFF -DFMT_TEST=OFF \
 && cmake --build fmt/build && cmake --install fmt/build \
 && git clone --depth 1 --branch "${SPDLOG_TAG}" https://github.com/gabime/spdlog.git spdlog \
 && cmake -S spdlog -B spdlog/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/deps -DCMAKE_PREFIX_PATH=/opt/deps -DBUILD_SHARED_LIBS=ON \
      -DSPDLOG_FMT_EXTERNAL=ON -DSPDLOG_BUILD_EXAMPLE=OFF -DSPDLOG_BUILD_TESTS=OFF \
 && cmake --build spdlog/build && cmake --install spdlog/build \
 && git clone --depth 1 --branch "${SIMDJSON_TAG}" https://github.com/simdjson/simdjson.git simdjson \
 && cmake -S simdjson -B simdjson/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/deps -DBUILD_SHARED_LIBS=ON \
      -DSIMDJSON_DEVELOPER_MODE=OFF \
 && cmake --build simdjson/build && cmake --install simdjson/build \
 && git clone --depth 1 --branch "${DATE_TAG}" https://github.com/HowardHinnant/date.git date \
 && cmake -S date -B date/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/deps -DBUILD_SHARED_LIBS=ON \
      -DBUILD_TZ_LIB=ON -DUSE_SYSTEM_TZ_DB=ON -DENABLE_DATE_TESTING=OFF \
 && cmake --build date/build && cmake --install date/build \
 && rm -rf /deps

# ---------------------------------------------------------------------------
# Build against the packages above (HGRAPH_FETCH_*=OFF, no pyarrow).
# Boost.Math is still fetched: it is a private, header-only build input of
# the analytics kernels and no consumer surface names it.
FROM base AS build
ARG HGRAPH_RELEASE_VERSION=
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
COPY --from=deps /opt/deps/ /usr/local/
RUN ldconfig
WORKDIR /src
COPY . .
RUN cmake -S . -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DHGRAPH_RELEASE_VERSION="${HGRAPH_RELEASE_VERSION}" \
      -DHGRAPH_BUILD_LANGUAGE=ON \
      -DHGRAPH_BUILD_ANALYTICS_EXTENSION=ON \
      -DHGRAPH_BUILD_SHARED=ON \
      -DHGRAPH_BUILD_PYTHON_BINDINGS=OFF \
      -DHGRAPH_ENABLE_PYTHON_USER_NODES=OFF \
      -DHGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS=OFF \
      -DHGRAPH_USE_PYARROW_ARROW=OFF \
      -DHGRAPH_FETCH_SIMDJSON=OFF \
      -DHGRAPH_FETCH_DATE=OFF \
      -DHGRAPH_TIME_ZONE_BACKEND=date \
      -DHGRAPH_ENABLE_COMPILER_CACHE=OFF \
      -DBUILD_TESTING=OFF \
 && cmake --build build \
 && cmake --install build --prefix /opt/hgraph

# ---------------------------------------------------------------------------
# Final image: the dependency prefix and the install prefix on top of the
# runtime dependencies. /usr/local keeps hgl and the shared libraries on the
# default search paths, so no rpath or environment is needed, and
# find_package(hgraph) resolves fmt, spdlog and simdjson from the same
# prefix. cmake and ninja stay so an hgl_add_module() project builds here.
FROM base AS runtime
COPY --from=deps /opt/deps/ /usr/local/
COPY --from=build /opt/hgraph/ /usr/local/
RUN ldconfig \
 && useradd --create-home --uid 1000 --shell /bin/sh hgl \
 && mkdir -p /work \
 && chown hgl:hgl /work
USER hgl
WORKDIR /work
CMD ["hgl", "--help"]
