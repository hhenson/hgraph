# Homebrew formula for hgraph: the native runtime SDK and the hgl toolchain.
#
# Source of truth for the tap `hhenson/homebrew-hgraph` (RFC 0032). The tap
# is created at the release switch by copying packaging/homebrew/ into it;
# until then this file is exercised by .github/workflows/packaging.yml,
# which runs the same CMake arguments against the checked-out tree and
# audits the formula in a throwaway local tap.
#
# RELEASE SWITCH: set `url` and `sha256` to the first release tag that
# contains language/ (0.8.22 predates it), then let `brew test-bot` add the
# bottle block.
class Hgraph < Formula
  desc "Reactive time-series runtime SDK and the hgl language toolchain"
  homepage "https://github.com/hhenson/hgraph"
  url "https://github.com/hhenson/hgraph/archive/refs/tags/0.8.23.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "MIT"
  head "https://github.com/hhenson/hgraph.git", branch: "main"

  depends_on "boost" => :build # Boost.Math, header-only, analytics kernels
  depends_on "cmake" => [:build, :test]
  depends_on "ninja" => [:build, :test]
  depends_on "apache-arrow"
  depends_on "fmt"
  depends_on "howard-hinnant-date"
  depends_on "clang-format" # runtime dependency of hgl code generation
  depends_on "simdjson"
  depends_on "spdlog"

  on_linux do
    # Runtime (`state`/`when`) functions compile at run time; macOS has
    # the Xcode command-line tools, Linux needs a C++23 compiler installed.
    depends_on "gcc"
  end

  # The REPL's line editor has no Homebrew package and the build sandbox has
  # no network, so it is staged as a resource and handed to FetchContent.
  # Keep the tag in step with language/CMakeLists.txt and conanfile.py.
  resource "isocline" do
    url "https://github.com/daanx/isocline/archive/refs/tags/v1.1.0.tar.gz"
    sha256 "1e5f0efa2b719c3e1d292f501e5329e141a039deefc801099f8bbb9a50255531"
  end

  def install
    (buildpath/"isocline").install resource("isocline")

    # Mirrors the "Channels: Homebrew" arguments in RFC 0032. Every
    # dependency comes from Homebrew (HGRAPH_FETCH_*=OFF, no pyarrow); the
    # named-zone backend is pinned to date/tz as the Conan recipe does.
    args = %W[
      -DHGRAPH_RELEASE_VERSION=#{version}
      -DHGRAPH_BUILD_LANGUAGE=ON
      -DHGRAPH_BUILD_ANALYTICS_EXTENSION=ON
      -DHGRAPH_BUILD_SHARED=ON
      -DHGRAPH_BUILD_PYTHON_BINDINGS=OFF
      -DHGRAPH_ENABLE_PYTHON_USER_NODES=OFF
      -DHGRAPH_ENABLE_IDE_PYTHON_HEADER_HINTS=OFF
      -DHGRAPH_USE_PYARROW_ARROW=OFF
      -DHGRAPH_FETCH_SIMDJSON=OFF
      -DHGRAPH_FETCH_DATE=OFF
      -DHGRAPH_TIME_ZONE_BACKEND=date
      -DHGRAPH_ENABLE_COMPILER_CACHE=OFF
      -DBUILD_TESTING=OFF
      -DFETCHCONTENT_FULLY_DISCONNECTED=ON
      -DFETCHCONTENT_SOURCE_DIR_ISOCLINE=#{buildpath}/isocline
    ]

    system "cmake", "-S", ".", "-B", "build", "-G", "Ninja", *args, *std_cmake_args
    # Ninja defaults to cores+2 jobs; keep the build inside HOMEBREW_MAKE_JOBS
    # because the largest std translation units need several GB each.
    system "cmake", "--build", "build", "--parallel", ENV.make_jobs.to_s
    system "cmake", "--install", "build"
    # The smoke program and its installed-SDK consumer, shared with the
    # container and Conan channels; `brew test` builds them from here.
    pkgshare.install "packaging/smoke"
  end

  test do
    assert_match "twice_ticks ... ok", shell_output("#{bin}/hgl test #{pkgshare}/smoke/smoke.hgl")
    assert_match version.to_s, shell_output("#{bin}/hgl --version")

    # A downstream package: find_package(hgraph) + hgl_add_module() against
    # the installed SDK, evaluated through the public harness.
    system "cmake", "-S", pkgshare/"smoke/consumer", "-B", "consumer", "-G", "Ninja",
           "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_PREFIX_PATH=#{HOMEBREW_PREFIX}"
    system "cmake", "--build", "consumer"
    assert_match "hgl smoke consumer ok", shell_output("consumer/hgl_smoke_consumer")
  end
end
