"""Initialize pinned shared repositories and materialize compatibility paths."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check', action='store_true')
parser.add_argument('--offline', action='store_true')
args = parser.parse_args()
if not args.check and not args.offline:
    subprocess.run(['git', 'submodule', 'update', '--init', '--recursive'], cwd=ROOT, check=True)
script = ROOT / 'external/hgraph_spec_audit/tools/shared_artifacts.py'
if not script.is_file():
    parser.error('shared repositories are missing; run without --offline to initialize them')
subprocess.run([sys.executable, str(script), '--root', str(ROOT), '--offline'] +
               (['--check'] if args.check else []), check=True)
