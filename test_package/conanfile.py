import os

from conan import ConanFile
from conan.tools.build import can_run
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.env import VirtualRunEnv


class HgraphTestPackage(ConanFile):
    settings = "os", "arch", "compiler", "build_type"

    def requirements(self):
        self.requires(self.tested_reference_str)

    def layout(self):
        cmake_layout(self)

    @property
    def _language(self):
        return bool(self.dependencies["hgraph"].options.get_safe("language"))

    def generate(self):
        CMakeDeps(self).generate()
        tc = CMakeToolchain(self)
        tc.cache_variables["HGRAPH_TEST_LANGUAGE"] = self._language
        tc.generate()
        if self._language:
            # hgl_add_module() runs the packaged ``hgl`` while building, so
            # the run environment (hgraph and its shared dependencies) has to
            # be active for the build scope as well as for test().
            run_env = VirtualRunEnv(self)
            run_env.generate()
            run_env.generate(scope="build")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def test(self):
        if can_run(self):
            self.run(os.path.join(self.cpp.build.bindir, "hgraph_conan_consumer"),
                     env="conanrun")
            if self._language:
                # RFC 0032: the language option ships ``bin/hgl`` reporting
                # the package's release version, and an HGL package builds
                # against the installed SDK.
                self.run("hgl --version", env="conanrun")
                self.run(os.path.join(self.cpp.build.bindir, "hgraph_conan_hgl_consumer"),
                         env="conanrun")
