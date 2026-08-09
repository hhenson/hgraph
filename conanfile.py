"""Conan recipe for the hgraph C++ shared-library SDK.

Packages the native runtime exactly as the installed SDK ships it: the
shared hgraph libraries, public headers, debugger support, and the
project's own ``hgraphConfig.cmake`` (consumers use ``find_package(hgraph)``
against that config — Conan does not generate a competing one). The Python
bridge is out of scope here; wheels remain the Python distribution channel.

Local development flow::

    conan create . --build=missing

Consumers add ``hgraph/<version>`` to their requires; the C++ API is
source-provisional (see docs/source/developer_guide/release_readiness.rst),
so native consumers pin the exact version and rebuild per release.
"""

import re
import subprocess

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout

MINIMUM_RELEASE_VERSION = (0, 8, 0)


class HgraphConan(ConanFile):
    name = "hgraph"
    package_type = "shared-library"
    license = "MIT"
    url = "https://github.com/hhenson/hgraph"
    homepage = "https://github.com/hhenson/hgraph"
    description = (
        "C++-first hgraph runtime: forward-propagating graph engine with "
        "time-series types, operator library, and extension SDK"
    )
    topics = ("reactive", "graph", "time-series", "frp")

    settings = "os", "arch", "compiler", "build_type"

    exports_sources = (
        "CMakeLists.txt",
        "include/*",
        "src/*",
        "tools/debugger/*",
    )

    def set_version(self):
        if self.version:
            return
        # Git tags are the version authority (release_readiness.rst: tagged
        # releases restamp metadata to the ``v_<version>`` tag). An exact tag
        # yields that version; commits past the tag yield ``<tag>.dev<n>`` so
        # a cache entry never impersonates a release. pyproject.toml is only
        # the no-git fallback.
        try:
            described = subprocess.check_output(
                ["git", "describe", "--tags", "--match", "v_*"],
                cwd=self.recipe_folder, text=True,
                stderr=subprocess.DEVNULL).strip()
            match = re.fullmatch(r"v_(.+?)(?:-(\d+)-g[0-9a-f]+)?", described)
            version, ahead = match.group(1), match.group(2)
            numeric = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
            if numeric and tuple(map(int, numeric.groups())) < MINIMUM_RELEASE_VERSION:
                # The merge preserves the 0.5 history and its reachable tags,
                # but those tags identify the maintained Python-first line.
                # Pre-tag Conan packages from C++-first main belong to 0.8.
                version = ".".join(map(str, MINIMUM_RELEASE_VERSION))
            self.version = f"{version}.dev{ahead}" if ahead else version
        except (subprocess.CalledProcessError, FileNotFoundError):
            pyproject = open(f"{self.recipe_folder}/pyproject.toml").read()
            self.version = re.search(
                r'^version\s*=\s*"([^"]+)"', pyproject, re.M).group(1)

    def requirements(self):
        # The floors match CMakeLists.txt; fmt is forced because spdlog's
        # recipe carries its own fmt pin.
        self.requires("fmt/11.2.0", force=True)
        self.requires("spdlog/1.15.3")
        self.requires("simdjson/4.6.3")
        self.requires("date/3.0.4")
        self.requires("arrow/25.0.0")

    def configure(self):
        # The SDK links Arrow's shared targets and needs compute + acero.
        self.options["arrow"].shared = True
        self.options["arrow"].compute = True
        self.options["arrow"].acero = True
        self.options["arrow"].parquet = False
        # date/tz as a compiled library reading the OS TZDB, matching the
        # project's FetchContent configuration.
        self.options["date"].header_only = False
        self.options["date"].use_system_tz_db = True
        self.options["fmt"].shared = True
        self.options["spdlog"].shared = True
        self.options["simdjson"].shared = True

    def layout(self):
        cmake_layout(self)

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.cache_variables["HGRAPH_BUILD_SHARED"] = True
        tc.cache_variables["HGRAPH_BUILD_PYTHON_BINDINGS"] = False
        tc.cache_variables["HGRAPH_ENABLE_PYTHON_USER_NODES"] = False
        tc.cache_variables["HGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS"] = False
        tc.cache_variables["BUILD_TESTING"] = False
        tc.cache_variables["HGRAPH_FETCH_SIMDJSON"] = False
        tc.cache_variables["HGRAPH_FETCH_DATE"] = False
        # Deterministic named-zone backend across platforms: the packaged
        # SDK always uses date/tz rather than probing the standard library.
        tc.cache_variables["HGRAPH_TIME_ZONE_BACKEND"] = "date"
        tc.cache_variables["HGRAPH_ENABLE_COMPILER_CACHE"] = False
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        # Consumers use the project's installed hgraphConfig.cmake — the
        # authoritative config carrying the SDK helpers
        # (hgraph_add_python_module, exact-version machinery) — rather than
        # a Conan-generated one.
        self.cpp_info.set_property("cmake_find_mode", "none")
        self.cpp_info.builddirs = ["lib/cmake/hgraph"]
