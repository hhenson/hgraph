Debugging
=========

The repository includes opt-in LLDB and GDB summaries plus expandable child
navigation for the type-erased runtime structures in ``tools/debugger``.

Load LLDB support with:

.. code-block:: text

   (lldb) command script import /path/to/hgraph/tools/debugger/hgraph_lldb.py

Load GDB support with:

.. code-block:: text

   (gdb) source /path/to/hgraph/tools/debugger/hgraph_gdb.py

When installed through CMake, the scripts are copied to
``share/hgraph/debugger`` under the install prefix.

The printers cover the common ``SchemaHeader`` and ``TypeRecord`` structures,
``AnyPtr``, and every ``TypedPtr<Family, Role>`` specialization (including the
public value, time-series, node, graph, executor, and clock pointer aliases).
Pointer and type-reference summaries use the same compact notation as public
views, for example ``ValuePtr{int value=42}``, ``TSOutputPtr{TS[int]}``, and
``ValueTypeRef{int}``. Typed null and unbound carriers append ``null`` and
``unbound``. Expanding a pointer exposes the canonical record, where family,
role, implementation, ABI, plan, ops, debug descriptor, and addresses remain
available when low-level representation details are needed.

The public authoring/debugging wrappers are covered as well: ``Value`` and
``ValueView``; value and time-series type references; ``TSData``, ``TSInput``,
``TSOutput`` and their views; and ``NodeView`` / ``GraphView``.  Time-series
summaries show their current scalar value and raw last-modified microsecond
count when available.  Output views also expose an atomic delta at the view's
evaluation time.  Nodes expand to their input, output, state, and scalar
storage, while graphs expand to nodes and per-node schedule entries. Registered
``std::string`` scalars display their contents through an explicit string debug
kind. Time-series outputs expose their current ``subscriber[N]`` observer
pointers; these are borrowed ``Notifiable*`` links and may represent nodes or
internal routing observers. GDB resolves their runtime type, and ``hg-p``
shallowly dereferences the selected subscriber so its concrete routing or node
fields can be inspected without recursively printing the graph::

   (gdb) hg-p out subscriber[0]
   (gdb) hg-p out subscriber[0] notifies
   (gdb) hg-p out subscriber[0] notifies node

``subscriber[N]`` is the immediate observer. ``notifies`` follows an input
routing observer's scheduling notifier to its eventual runtime recipient. If
that recipient is node runtime storage, ``node`` selects the concrete graph
node and ``graph`` selects its graph handle. ``notification_target`` is an
alias for ``notifies``.

The same names are synthetic children in GDB's normal pretty-printer tree.
IDE variable inspectors can therefore expand ``out -> subscriber[N] ->
notifies -> node`` (or ``graph``) without enabling recursive printing or using
the command line. Expanding that node exposes its ``input`` and ``output``
endpoints when present in the node schema.

Public wrapper summaries are intentionally compact. For example,
``TSOutputView{TS[int] value=11 modified=1us}`` leaves family, role, record,
implementation, and address details on its expandable ``data`` child. TS data
also exposes its TS parent or owning node from the stable parent-link metadata;
the node exposes its owning graph, and the graph exposes nodes and schedules.
For a bound input, ``source_data`` is the resolved output-side TS data cursor,
so the normal navigation path is ``TSInputView -> source_data -> owner_node ->
graph -> nodes``.

GDB shallow navigation
----------------------

Use ``hg-p`` when normal recursive ``print`` either expands the whole graph or
hides the link of interest behind ``set print max-depth``. It resolves a path
and prints only the selected object plus one level of links::

   (gdb) hg-p out owning_node graph nodes 4 output

``nodes N`` selects graph child ``N`` and ``schedule N`` selects its schedule
entry. Common aliases include ``owning_node``/``owner_node`` and
``source``/``source_data``. Quoted GDB expressions can be used as the first
argument when the expression itself contains spaces.

``hg-v`` stores the selected real debugger value in a GDB convenience
variable, allowing a long prefix to be retained and explored repeatedly::

   (gdb) hg-v $g out owning_node graph
   $g = AnyPtr{...debugger_graph...}
   (gdb) hg-p $g nodes 6

Omit ``$g`` to use the default variable ``$hg``. The command form is
``hg-v $g ...`` rather than ``set $g = hg-v ...`` because GDB commands do not
return expression values.

They only inspect debug-info fields and memory; they do not call methods in the
stopped process. Records carrying a stable data-only debug descriptor expose
bool, signed/unsigned integer, and 32/64-bit floating-point payloads directly.
Fixed tuples and bundles expand into child ``AnyPtr`` values using descriptor
offsets; unset fields appear typed-null through the published validity bitmap.
Supported sequences expand in logical order, including fixed arrays, dense
compact lists/sets, ring buffers, queues, and mutable slot-backed lists/sets.
Mutable maps expose live key/value pairs. Graphs expose their in-place nodes;
nodes expose state and scalar owners; single, switch, map, and mesh nested
nodes expose retained child graph owners. Slot navigation reads the stable
pointer table and ``SlotBitmap`` state, so erased slots are omitted while
constructed stopped entries remain inspectable.

Structured summaries include ``fields=N`` for bundles and ``size=N`` for
sequences, sets, and dictionaries. Structured time series retain an expandable
``value`` child. TSB promotes named child TSData objects directly (the scalar
bundle remains under ``value``), while TSS promotes ``[N]`` elements and TSD
promotes ``key[N]`` / ``value[N]`` pairs.
Their ``last_modified`` child and compact ``modified=Nus`` summary come from
the TS tracking record rather than the value-layer container.

Opaque atomic storage and nullable dynamic-list/compact-map validity remain
explicitly opaque. The printers do not infer a payload
layout from semantic labels, C++ template names, or private container fields.

Build with debug information enabled for reliable output. Optimized builds may
hide or fold the private fields that the summaries read.

Interactive printer validation
------------------------------

The deterministic fixture and batch validators exercise real debugger child
navigation.  Configure with ``HGRAPH_ENABLE_DEBUGGER_SMOKE_TESTS=ON`` to add
the platform debugger to CTest, or run the same check directly from the
repository root.

On macOS:

.. code-block:: console

   lldb --batch \
       -o 'command script import tools/debugger/hgraph_lldb.py' \
       -o 'command script import tests/debugger/hgraph_lldb_smoke.py' \
       -o 'breakpoint set --name hgraph_debugger_fixture_stop' \
       -o run -o hgraph-smoke \
       build/tests/cpp/hgraph_debugger_fixture

On Linux:

.. code-block:: console

   gdb --nx --batch \
       -ex 'set pagination off' \
       -ex 'source tools/debugger/hgraph_gdb.py' \
       -ex 'source tests/debugger/hgraph_gdb_smoke.py' \
       -ex 'break hgraph_debugger_fixture_stop' \
       -ex run -ex hgraph-smoke \
       build/tests/cpp/hgraph_debugger_fixture

A successful run ends with ``hgraph LLDB type-erasure smoke test passed`` or
``hgraph GDB type-erasure smoke test passed``.  The validator checks summaries
such as ``semantic="debugger_fixture_graph"`` with
``implementation="hgraph.graph.root"``, then expands the graph's ``[0]`` child
and verifies ``semantic="debugger_fixture_graph_node"``.  It also checks
bundle fields, mutable-map key/value pairs, typed-null pointers, malformed
pointers, and unsupported ABI versions.  The fixture also runs real switch,
map, and mesh nodes to the point where both switch banks are populated and
keyed slots contain one live and one stopped/pending child.  The validators
expand those children into their nested graph nodes, require keys 22 and 33,
and verify that the physically erased key 11 is no longer visible.

Some VM security configurations deny ``ptrace``.  In that environment GDB may
load both scripts successfully but fail with ``Couldn't get registers`` before
the fixture breakpoint.  Use a native Linux host or the required Linux CI gate
for the interactive smoke test; this failure does not exercise the printers.

Linux Python/ASan debugging from macOS
--------------------------------------

Some lifetime failures only reproduce when the Python bridge is loaded into a
Linux process. The project's preferred Linux system is the native host exposed
through the ``hg-linux`` SSH alias. Use it whenever it is available. It
exercises the Linux/GCC build on separate hardware without modifying the macOS
toolchain. An Ubuntu 24.04 OrbStack machine named ``ubuntu`` is the fallback
when ``hg-linux`` cannot be reached.

Keep the build directory and virtual environment on the Linux filesystem, not
in a macOS-shared source tree. The examples use ``/tmp`` for both.

Prepare the Linux host
~~~~~~~~~~~~~~~~~~~~~~

First check the preferred host:

.. code-block:: bash

   ssh -o BatchMode=yes -o ConnectTimeout=10 hg-linux true

When it is available, copy the current checkout, including uncommitted source
changes, into a disposable directory on that host and open a shell there:

.. code-block:: bash

   remote_root="$(ssh hg-linux 'mktemp -d /tmp/hgraph-linux.XXXXXX')"
   rsync -a \
       --exclude .git --exclude .venv --exclude '.parity' \
       --exclude 'cmake-build-*' --exclude '._*' --exclude '.DS_Store' \
       ./ "hg-linux:${remote_root}/repo/"
   ssh -t hg-linux "cd '${remote_root}/repo' && exec bash"
   export REPO="$PWD"

The copy intentionally excludes ``.venv``. Before running the native
acceptance preset, recreate that repository-local environment because
``CMakePresets.json`` selects ``.venv/bin/python`` for PyArrow discovery:

.. code-block:: bash

   uv venv --python 3.14 .venv
   uv pip install --python .venv/bin/python "pyarrow>=25,<26"

This bootstrap is required even when a separate disposable environment is
created below for Python bridge or sanitizer testing.

If ``hg-linux`` is unavailable, use the OrbStack VM instead. Install a current
GCC plus Python development support there when needed. Replace the example
checkout path below with the macOS path to the checkout; OrbStack mounts that
path at the same location in the guest:

.. code-block:: bash

   orb -m ubuntu bash
   sudo apt update
   sudo apt install -y build-essential gcc-14 g++-14 git python3-dev python3-venv
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 140
   sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 140
   sudo update-alternatives --install /usr/bin/cc cc /usr/bin/gcc-14 140
   sudo update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-14 140
   export REPO=/Users/<mac-user>/src/hgraph

Inside the selected Linux shell, create a disposable Python environment
containing the binding and test dependencies:

.. code-block:: bash

   export VENV=/tmp/hgraph-asan-venv
   export BUILD=/tmp/hgraph-linux-asan

   python3 -m venv "$VENV"
   source "$VENV/bin/activate"
   python -m pip install --upgrade pip cmake ninja nanobind \
       "pyarrow>=25,<26" "numpy>=2" "pytest>=8" "frozendict>=2.4" \
       "polars[rtcompat]>=1.32"

The ``rtcompat`` extra matters on virtualized x86_64 hosts that do not expose
AVX/AVX2. The default Polars runtime warns and may crash on those guests before
the hgraph extension is called.

Confirm the compiler before configuring. hgraph's supported Linux toolchain is
GCC 14 or newer; GCC 13 is not a supported validation compiler:

.. code-block:: bash

   g++-14 --version
   python --version

Configure and build the Python extension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure a Debug build with Python user nodes and the stable ABI enabled.
Warnings remain errors in this configuration because GCC diagnostics have
identified real emitted-code problems in the past.

.. code-block:: bash

   cmake -S "$REPO" -B "$BUILD" -GNinja \
       -DCMAKE_BUILD_TYPE=Debug \
       -DCMAKE_C_COMPILER=/usr/bin/gcc-14 \
       -DCMAKE_CXX_COMPILER=/usr/bin/g++-14 \
       -DPython_EXECUTABLE="$VENV/bin/python" \
       -DHGRAPH_BUILD_PYTHON_BINDINGS=ON \
       -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON \
       -DHGRAPH_PYTHON_STABLE_ABI=ON \
       -DHGRAPH_ENABLE_ASAN=ON \
       -DHGRAPH_WARNINGS_AS_ERRORS=ON \
       -DBUILD_TESTING=OFF
   cmake --build "$BUILD" --target _hgraph --parallel 4

Do not start pytest from another shell until the build command has exited
successfully. A header change can rebuild most of the extension; running tests
while that build is active can silently exercise the previous ``_hgraph``
binary.

Run under ASan
~~~~~~~~~~~~~~

The Python executable is not itself linked with ASan, so the sanitizer runtime
must be loaded before the instrumented extension. Resolve both runtime paths
through the same compiler used for the extension:

.. code-block:: bash

   export LD_PRELOAD="$(c++ -print-file-name=libasan.so):$(c++ -print-file-name=libstdc++.so)"
   export ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1
   export PYTHONPATH="$BUILD/python:$REPO/python"

Preloading ``libstdc++`` as well as ``libasan`` prevents Python or another
extension from selecting an older C++ runtime first. Leak detection is disabled
for this workflow because process-wide Python and third-party shutdown state is
too noisy for the ownership errors being investigated; use a dedicated native
leak run when leaks are the target.

First confirm that Python imports the extension from the Linux build tree:

.. code-block:: bash

   python -c "import _hgraph; print(_hgraph.__file__)"

Then minimize the failure to one test before running the complete compatibility
suite:

.. code-block:: bash

   python -m pytest -q \
       "$REPO/python/tests/ported/_operators/test_control_operators.py::test_race_tsd_of_bundles_switch_bundle_types"
   python -m pytest -q "$REPO/python/tests" -m "not wip"

Change the focused node id to the failing test. ``-s -vv`` and
``PYTHONFAULTHANDLER=1`` are useful when a native abort occurs before pytest
flushes its captured output.

Capture and interpret reports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For failures with a large amount of pytest output, direct ASan reports to a
separate file:

.. code-block:: bash

   rm -f /tmp/hgraph-asan.*
   export ASAN_OPTIONS=detect_leaks=0:halt_on_error=1:abort_on_error=1:log_path=/tmp/hgraph-asan
   python -m pytest -s -vv "$REPO/python/tests/path_to_test.py::test_name"
   grep -n -A100 -B10 -E \
       "ERROR: AddressSanitizer|freed by thread|previously allocated|SUMMARY:" \
       /tmp/hgraph-asan.*

Read the report in ownership order: the first stack is the invalid access, the
``freed by`` stack identifies the premature teardown, and the allocation stack
identifies the owner that should have controlled the lifetime. Debug builds and
``-fno-omit-frame-pointer`` (added by ``HGRAPH_ENABLE_ASAN``) keep those native
stacks usable.

An ``assert`` abort is not an ASan violation and may not create an ASan report.
Prefer GDB inside the VM for those failures. Some VM security configurations
deny ``ptrace`` and GDB then fails while reading registers. In that case,
temporarily adding a Linux ``backtrace``/``backtrace_symbols_fd`` call at the
failing invariant can identify the native caller. Remove that diagnostic as
soon as the call path is known.

Common problems
~~~~~~~~~~~~~~~

``ASan runtime does not come first``
   ``LD_PRELOAD`` was omitted or points at a different compiler's runtime. Use
   the ``c++ -print-file-name`` form above in the same shell that runs Python.

Python imports the wrong extension
   Check ``_hgraph.__file__`` and put ``$BUILD/python`` before ``$REPO/python``
   in ``PYTHONPATH``. Also make sure the extension build has completed.

The stack does not match the latest source
   Rebuild ``_hgraph`` and wait for a successful exit. For a difficult case,
   use ``nm -C "$BUILD/python/_hgraph.abi3.so"`` to confirm that an expected
   symbol is present in the binary under test.

GDB reports ``Couldn't get registers``
   The VM is denying ``ptrace`` across its process boundary. Run GDB entirely
   inside a suitably configured VM, or use the ASan log and temporary native
   backtrace approach above.

``rg`` is unavailable in the guest
   Install ripgrep or use ``grep`` for the ASan log. This does not affect the
   build or the report itself.
