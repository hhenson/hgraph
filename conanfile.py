from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class HgraphCppConan(ConanFile):
    name = "hgraph-cpp"
    version = "0.0.0"
    # We don't produce a library package here; this recipe is for dependency resolution + toolchain files
    package_type = "application"

    settings = "os", "arch", "compiler", "build_type"

    options = {
        "shared": [True, False],
    }
    default_options = {
        "shared": False,
    }

    # Dependencies used by the C++ engine. Nanobind is discovered via the selected Python interpreter
    # and is NOT provided by Conan (it is a Python package).
    requires = (
        "fmt/10.1.0",
        "backward-cpp/1.6",
    )

    def layout(self):
        # Produce the canonical CMake layout so files are generated under the build dir
        cmake_layout(self)

    def generate(self):
        # Export CMake dependency and toolchain files for our build
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.generate()
