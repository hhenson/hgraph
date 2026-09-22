"""Replay the bounded recipes three times in independent runtime processes."""
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from evidence import render
from native_identity import source_head

ROOT = Path(__file__).absolute().parent


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def decode(value):
    if isinstance(value, list):
        return [decode(child) for child in value]
    if isinstance(value, dict):
        if '$map' in value:
            return {key: decode(child) for key, child in value['$map']}
        return {key: decode(child) for key, child in value.items()}
    return value


def trusted_directory(value):
    path = Path(value).absolute()
    if not path.is_dir():
        raise argparse.ArgumentTypeError('Expected an existing harness directory')
    return path


def trusted_interpreter(value):
    # Resolving a venv's interpreter symlink would select the base environment.
    path = Path(value).absolute()
    if not path.is_file():
        raise argparse.ArgumentTypeError('Expected an existing Python executable file')
    return path


def run_json(interpreter, script):
    return json.loads(subprocess.check_output(
        [str(interpreter), '-I', str(script)], text=True, shell=False, timeout=30))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--harness', type=trusted_directory, required=True)
    parser.add_argument('--reference-python', type=trusted_interpreter, required=True)
    parser.add_argument('--candidate-python', type=trusted_interpreter, required=True)
    parser.add_argument('--raw-results', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.harness.resolve()))
    from tools.parity.catalog import validate_recipe
    from tools.parity.model import Recipe
    from tools.parity.process import run_recipe

    recipes = [Recipe.load(p) for p in sorted((ROOT / 'recipes').glob('*.json'))]
    for recipe in recipes:
        validate_recipe(recipe)
    args.raw_results.mkdir(parents=True, exist_ok=True)
    interpreters = {'python': args.reference_python, 'cpp': args.candidate_python}

    def run(recipe, side):
        case = recipe.parameters['mode']
        results = [run_recipe(interpreters[side], recipe, timeout=60) for _ in range(3)]
        (args.raw_results / f'{case}-{side}.json').write_text(json.dumps(results, indent=2))
        compact = []
        for result in results:
            observation = decode(result.get('trace'))
            status = 'error' if observation and observation.get('failure') else result['status']
            compact.append({'status': status, 'observation': observation,
                            'exception': result.get('exception')})
        fingerprints = [digest(result) for result in compact]
        out = {**compact[0], 'implementation': results[0].get('implementation'),
               'recipe_fingerprint': recipe.fingerprint,
               'stable': len(set(fingerprints)) == 1, 'replay_digests': fingerprints}
        print(case, side, out['status'], flush=True)
        return case, side, out

    cases = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = [pool.submit(run, recipe, side) for recipe in recipes for side in interpreters]
        for future in as_completed(pending):
            case, side, result = future.result()
            cases.setdefault(case, {})[side] = result
    provenance = run_json(args.candidate_python, ROOT / 'native_identity.py')
    harness_head = source_head(args.harness)
    if harness_head is None:
        raise RuntimeError('Harness is not a Git checkout')
    provenance.update(harness_base=harness_head,
                      adapter_sha256=hashlib.sha256((ROOT / 'adapter.patch').read_bytes()).hexdigest(),
                      reasoning_sha256=hashlib.sha256((ROOT / 'reasoned.json').read_bytes()).hexdigest(),
                      reference='Python reference environment; version is recorded per case.',
                      candidate='Installed C++ runtime with Python authoring surface; native hashes identify the binaries, source HEAD is context.',
                      replays=3, recorded=datetime.now(timezone.utc).date().isoformat())
    output = ROOT / 'observed.json'
    previous = json.loads(output.read_text()) if output.exists() else {}
    if 'native_probe' in previous.get('provenance', {}):
        provenance['native_probe'] = previous['provenance']['native_probe']
    output.write_text(render({'provenance': provenance, 'cases': dict(sorted(cases.items()))}) + '\n')


if __name__ == '__main__':
    main()
