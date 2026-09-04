# syntax=docker/dockerfile:1.7
# Container image for hgl (RFC 0032): `hgl`, the installed SDK, and a C++
# compiler so runtime (`state`/`when`) functions compile inside the
# container. .github/workflows/packaging.yml builds it on pull requests and
# runs the smoke inside it; pushing to ghcr.io/hhenson/hgl:<release> is a
# release-switch step and happens nowhere yet.
#
#   docker build -f packaging/docker/Dockerfile \
#       --build-arg HGRAPH_RELEASE_VERSION=0.8.23 -t hgl:0.8.23 .
#   docker run --rm -v "$PWD:/work" hgl:0.8.23 hgl test smoke.hgl

ARG DEBIAN_CODENAME=trixie

# ---------------------------------------------------------------------------
# Runtime dependencies shared by the build and the final image. Arrow (with
# compute and Acero) comes from the Apache apt repository because Debian's
# Arrow predates the compute/acero split hgraph links. The -dev packages
# stay in the final image: runtime functions compile against the SDK
# headers, which include Arrow's.
FROM debian:${DEBIAN_CODENAME}-slim AS base
ARG DEBIAN_CODENAME
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates wget \
 && wget -q "https://packages.apache.org/artifactory/arrow/debian/apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && apt-get install -y --no-install-recommends "./apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && rm "./apache-arrow-apt-source-latest-${DEBIAN_CODENAME}.deb" \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      g++ libarrow-dev libarrow-compute-dev libarrow-acero-dev tzdata \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Build. fmt, spdlog, simdjson, date and Boost.Math are fetched at configure
# time (Debian's versions sit below the floors in CMakeLists.txt); the
# install carries the headers of the fetched ones under include/, so a
# consumer inside the image builds against exactly what hgl was built with.
FROM base AS build
ARG HGRAPH_RELEASE_VERSION=
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake ninja-build git \
 && rm -rf /var/lib/apt/lists/*
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
      -DHGRAPH_FETCH_SIMDJSON=ON \
      -DHGRAPH_FETCH_DATE=ON \
      -DHGRAPH_TIME_ZONE_BACKEND=date \
      -DHGRAPH_ENABLE_COMPILER_CACHE=OFF \
      -DBUILD_TESTING=OFF \
 && cmake --build build \
 && cmake --install build --prefix /opt/hgraph

# ---------------------------------------------------------------------------
# Final image: the install prefix on top of the runtime dependencies. /usr/local
# keeps hgl and the shared libraries on the default search paths, so no rpath
# or environment is needed.
FROM base AS runtime
COPY --from=build /opt/hgraph/ /usr/local/
RUN ldconfig
WORKDIR /work
CMD ["hgl", "--help"]
