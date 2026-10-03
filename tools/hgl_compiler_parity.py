"""Run every shared HGL library test, independently, on C++ and/or Rust.

Expected behavior remains in the shared HGL assertions. A successful process
must also report every test (and each assertion for Rust); matching failures are not
conformance. Reports identify working files, not only Git HEAD.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULE = re.compile(r'^\s*module\s+([A-Za-z_][\w.]*)\b', re.M)
TEST = re.compile(r'^\s*test\s+([A-Za-z_]\w*)\s*\{', re.M)
TOKEN = re.compile(r'#[^\n]*|"(?:\\.|[^"\\])*"|[A-Za-z_]\w*|[{}()\[\]]|\S')
RESULT = re.compile(r'^([\w.:]+) \.\.\. (.+)$', re.M)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(root):
    """Include staged and unstaged working contents, without private paths."""
    names = subprocess.check_output(['git', '-C', str(root), 'ls-files', '--cached', '--others', '--exclude-standard', '-z']).decode().split('\0')
    files = {name: digest(root / name) for name in sorted(set(names)) if name and (root / name).is_file()}
    return {'revision': subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip(),
            'dirty': bool(subprocess.check_output(['git', '-C', str(root), 'status', '--porcelain'])),
            'files': files}


def discover(library):
    groups, expected = defaultdict(list), defaultdict(list)
    for path in sorted(library.rglob('*.hgl')):
        source = path.read_text()
        match = MODULE.search(source)
        if not match:
            raise ValueError(f'missing module declaration: {path.name}')
        module = match[1]
        groups[module].append(path)
        expected[module].extend(TEST.findall(source))
    if not groups or not any(expected.values()):
        raise ValueError('shared HGL test corpus is empty')
    for module, names in expected.items():
        duplicates = [name for name, count in Counter(names).items() if count > 1]
        if duplicates:
            raise ValueError(f'duplicate shared tests in {module}: {duplicates}')
    # Unknown new modules must be handled explicitly, never silently skipped.
    unknown = set(groups) - {'hgraph.std', 'hgraph.operators', 'hgraph.native'}
    if unknown:
        raise ValueError(f'new shared modules need an adapter: {sorted(unknown)}')
    return dict(groups), dict(expected)


def assertion_counts(files):
    """Count shared test statements for adapters reporting once per assertion.

    This only inventories braces and statement markers, never interprets an
    assertion. New test statement forms fail closed until explicitly supported.
    """
    counts = Counter()
    for path in files:
        source = path.read_text()
        module = MODULE.search(source)[1]
        tokens = [m[0] for m in TOKEN.finditer(source) if not m[0].startswith('#')]
        i = 0
        while i < len(tokens):
            if tokens[i] != 'test' or i + 2 >= len(tokens) or tokens[i + 1] == '{':
                i += 1
                continue
            name = tokens[i + 1]
            if tokens[i + 2] != '{':
                raise ValueError(f'unrecognized test declaration in {path.name}')
            i += 3
            depth, previous, count = 1, '', 0
            while i < len(tokens) and depth:
                token = tokens[i]
                if depth == 1 and (token == 'assert' or (token == 'eval' and previous != 'assert')):
                    count += 1
                if token in ('{', '(', '['):
                    depth += 1
                elif token in ('}', ')', ']'):
                    depth -= 1
                previous = token
                i += 1
            if depth or not count:
                raise ValueError(f'cannot inventory statements in {module}::{name}')
            counts[f'{module}::{name}'] += count
    return counts


def assess(stdout, returncode, module, expected):
    observed = []
    for name, result in RESULT.findall(stdout):
        identity = name if '::' in name else f'{module}::{name}'
        observed.append({'test': identity, 'result': result})
    identities = Counter(row['test'] for row in observed)
    wanted = (Counter({f'{module}::{name}': 1 for name in expected})
              if not isinstance(expected, Counter) else expected)
    problems = []
    if returncode:
        problems.append(f'process exited {returncode}')
    if missing := wanted.keys() - identities.keys():
        problems.append(f'missing tests: {sorted(missing)}')
    if extra := identities.keys() - wanted.keys():
        problems.append(f'unexpected tests: {sorted(extra)}')
    if duplicates := sorted(name for name, count in identities.items() if count != wanted.get(name, 0)):
        problems.append(f'incorrect result counts: {duplicates}')
    if any(row['result'] != 'ok' for row in observed):
        problems.append('one or more test assertions failed')
    return {'module': module, 'passed': not problems, 'tests': observed, 'errors': problems}


def cpp_command(source, build, module, files):
    generated = build / 'language/generated/hgl_core_native'
    extra, native = [], []
    if module == 'hgraph.native':
        base = source / 'language/stdlib/hgl/hgraph'
        # The shared native declarations each have a target provider part.
        native = [base / 'native.hgl'] + sorted((base / 'native/impl').glob('cpp_*.hgl'))
        if len(native) == 1:
            raise ValueError('C++ native provider parts are missing')
        files = native[:1] + files + native[1:]
        extra = ['--native-provider-header', 'native_scalar.h', '--native-provider', 'hgl::stdlib::scalar_native']
    else:
        anchor = 'standard.hgl' if module == 'hgraph.std' else 'operators.hgl'
        files = sorted(files, key=lambda p: (p.name != anchor, str(p)))
        extra = ['--module-descriptor', str(generated / 'src/native.hgl-module.json')]
    driver = build / 'language/tests/hgl_stdlib_test_driver'
    command = [str(driver), 'test', str(files[0])]
    for file in files[1:]:
        command += ['--part', str(file)]
    env = dict(os.environ)
    env['CPLUS_INCLUDE_PATH'] = os.pathsep.join(filter(None, [str(generated / 'include'),
        str(source / 'language/stdlib/cpp'), env.get('CPLUS_INCLUDE_PATH')]))
    env['HGL_CACHE_DIR'] = str(build / 'language/tests/parity-cache')
    return command + extra, env, native


def rust_command(rust, library, module, files, build):
    # Reuse the Rust implementation's public test adapter, with this exact
    # corpus, rather than its independently pinned --stdlib shortcut.
    tests = [p for p in files if p.parent.name == 'tests' and TEST.search(p.read_text())]
    if not tests:
        raise ValueError(f'no shared tests in {module}')
    command = [sys.executable, str(rust / 'tools/test_hgl.py'), str(tests[0])]
    for file in tests[1:] + [rust / 'native/stdlib/rust.hgl']:
        command += ['--part', str(file)]
    command += ['--library', str(library), '--native-rust', str(rust / 'native/stdlib/native.rs'),
                '--build-dir', str(build / module)]
    return command, dict(os.environ)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=['cpp', 'rust', 'both'], default='both')
    parser.add_argument('--source', type=Path, default=ROOT)
    parser.add_argument('--build', type=Path, help='C++ CMake build, already rebuilt from --source')
    parser.add_argument('--rust-root', type=Path, help='explicit private Rust checkout; never downloaded')
    parser.add_argument('--output', type=Path, required=True, help='new local report directory')
    parser.add_argument('--timeout', type=int, default=900, help='seconds per module')
    parser.add_argument('--list', action='store_true', help='list corpus without invoking compilers')
    args = parser.parse_args()
    source = args.source.resolve()
    library = source / 'external/hgraph_std/hgl/hgraph'
    groups, expected = discover(library)
    if args.list:
        print(json.dumps(expected, indent=2))
        return 0
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    backends = ['cpp', 'rust'] if args.backend == 'both' else [args.backend]
    if 'cpp' in backends and not args.build:
        parser.error('--build is required for C++')
    if 'rust' in backends and not args.rust_root:
        parser.error('--rust-root is required for Rust')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shared = {name: snapshot(source / 'external' / name) for name in ('hgraph_spec', 'hgraph_std')}
    report = {'shared': shared, 'harness_sha256': digest(Path(__file__)), 'backends': {}}
    for backend in backends:
        implementation = source if backend == 'cpp' else args.rust_root.resolve()
        before = snapshot(implementation)
        rows = []
        identity = {'source': before}
        if backend == 'cpp':
            build = args.build.resolve()
            cache = (build / 'CMakeCache.txt').read_text()
            origin = re.search(r'^CMAKE_HOME_DIRECTORY:INTERNAL=(.+)$', cache, re.M)
            if not origin or Path(origin[1]).resolve() != source:
                parser.error('C++ build belongs to a different source checkout')
            identity['driver_sha256'] = digest(build / 'language/tests/hgl_stdlib_test_driver')
            identity['descriptor_sha256'] = digest(build / 'language/generated/hgl_core_native/src/native.hgl-module.json')
        runs = sorted(groups.items()) if backend == 'cpp' else [('shared', [p for paths in groups.values() for p in paths])]
        for module, files in runs:
            wanted = list(expected[module]) if backend == 'cpp' else assertion_counts(files)
            if backend == 'cpp':
                command, env, native = cpp_command(source, build, module, files)
                for file in native:
                    wanted.extend(TEST.findall(file.read_text()))
            else:
                command, env = rust_command(implementation, library, module, files, output / 'rust-build')
            log = output / f'{backend}-{module}.log'
            print(f'{backend}: {module} ({len(wanted)} tests)', flush=True)
            try:
                result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=args.timeout)
                log.write_text(result.stdout + result.stderr)
                row = assess(result.stdout, result.returncode, module, wanted)
            except (OSError, subprocess.TimeoutExpired) as error:
                log.write_text(str(error))
                row = {'module': module, 'passed': False, 'tests': [], 'errors': [type(error).__name__]}
            rows.append(row)
            for problem in row['errors']:
                print(f'{backend}: {module}: {problem}', file=sys.stderr)
        identity['unchanged_during_run'] = before == snapshot(implementation)
        report['backends'][backend] = {'identity': identity, 'groups': rows,
            'passed': identity['unchanged_during_run'] and all(row['passed'] for row in rows)}
    report['shared_unchanged'] = shared == {name: snapshot(source / 'external' / name) for name in shared}
    report['passed'] = report['shared_unchanged'] and all(b['passed'] for b in report['backends'].values())
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f"Compiler conformance: {'passed' if report['passed'] else 'FAILED'}; report: {output / 'report.json'}")
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
