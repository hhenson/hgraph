"""Embed the probe source and actual SDK header fingerprints at build time."""
import hashlib
import json
from pathlib import Path
import sys


def headers_digest(include, names):
    # Other installed extensions may share this include root. Fingerprint every
    # core SDK header named by the candidate's distribution manifest.
    files = {name: hashlib.sha256((include / name.removeprefix('include/')).read_bytes()).hexdigest()
             for name in names if name.startswith('include/')}
    assert files, 'Core SDK header manifest is empty'
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def main():
    include, source, output = map(Path, sys.argv[1:])
    artifacts = json.loads((source / 'observed.json').read_text())['provenance']['candidate_python_identity']['artifacts_sha256']
    fields = {'source_sha256': hashlib.sha256((source / 'native_nested_probe.cpp').read_bytes()).hexdigest(),
              'loader_sha256': hashlib.sha256((source / 'native_loaded_libraries.h').read_bytes()).hexdigest(),
              'headers_sha256': headers_digest(include, artifacts)}
    output.write_text('\n'.join(f'constexpr auto probe_{key} = "{value}";' for key, value in fields.items()) + '\n')


if __name__ == '__main__':
    main()
