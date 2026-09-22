"""Replay and assess public C++ wiring without treating it as another runtime vote."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess

from check import apply_decisions, at, equal
from evidence import atomic_write, render
from expand import assertions
from replay import candidate_identity, digest, trusted_interpreter

ROOT = Path(__file__).parent
CASES = {f'{outer}_{inner}_{role}_invalidate'
         for outer in ('tsl', 'tsb') for inner in ('tsl', 'tsb') for role in ('owned', 'assembled')}


def runtime_identity(identity):
    return {key: identity[key] for key in ('candidate_native_sha256', 'candidate_python_identity')}


def verify(recorded, bridge):
    provenance = recorded['provenance']
    assert provenance['source_sha256'] == hashlib.sha256((ROOT / 'native_nested_probe.cpp').read_bytes()).hexdigest(), 'Native wiring source changed'
    assert provenance['candidate_identity_sha256'] == digest(runtime_identity(bridge['provenance'])), 'Native wiring candidate differs'
    assert provenance['graph_evidence_sha256'] == digest({case: bridge['cases'][case] for case in sorted(CASES)}), 'Graph evidence changed'
    assert bridge['provenance']['reasoning_sha256'] == hashlib.sha256((ROOT / 'reasoned.json').read_bytes()).hexdigest(), 'Reasoning changed after graph replay'
    assert set(recorded['cases']) == CASES, 'Native wiring case coverage differs'
    assert all(len(case['ticks']) == 9 for case in recorded['cases'].values()), 'Missing native wiring ticks'
    assert provenance['stable'] is True and provenance['replay_digests'] == [digest(recorded['cases'])] * 3, 'Native wiring traces changed or unstable'


def assess(recorded, bridge):
    expected = [item for item in assertions() if item['case'] in CASES]
    decisions = [{**item, 'assertions': [a for a in item['assertions'] if a['case'] in CASES]}
                 for item in json.loads((ROOT / 'decisions.json').read_text())]
    apply_decisions(expected, decisions)
    results = []
    for assertion in expected:
        case = assertion['case']
        if case not in CASES:
            continue
        value = at(recorded['cases'][case], assertion['path'])
        graph_value = at(bridge['cases'][case]['cpp']['observation'], assertion['path'])
        if assertion.get('projection') == 'keys':
            value, graph_value = sorted(value), sorted(graph_value)
        elif assertion.get('projection') == 'length':
            value, graph_value = len(value), len(graph_value)
        results.append({**assertion, 'native_cpp': value, 'python_authored_cpp': graph_value,
                        'status': 'matches-contract' if equal(value, assertion['expected']) else 'variation',
                        'same_authoring_result': equal(value, graph_value)})
    return {'counts': dict(Counter(item['status'] for item in results)),
            'authoring_differences': sum(not item['same_authoring_result'] for item in results),
            'differences': [item for item in results if item['status'] == 'variation' or not item['same_authoring_result']]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--executable', type=Path)
    parser.add_argument('--record', action='store_true')
    parser.add_argument('--candidate-python', type=trusted_interpreter)
    args = parser.parse_args()
    if args.record and (args.executable is None or args.candidate_python is None):
        parser.error('--record requires --executable and --candidate-python')
    bridge = json.loads((ROOT / 'observed.json').read_text())
    destination = ROOT / 'native_nested_observed.json'
    if args.executable:
        executable = args.executable.absolute()
        before = hashlib.sha256(executable.read_bytes()).hexdigest()
        source_before = hashlib.sha256((ROOT / 'native_nested_probe.cpp').read_bytes()).hexdigest()
        if args.record:
            installation = runtime_identity(candidate_identity(args.candidate_python))
            assert installation == runtime_identity(bridge['provenance']), 'Candidate differs from graph evidence'
        runs = [json.loads(subprocess.check_output([str(executable)], timeout=60)) for _ in range(3)]
        assert runs[0] == runs[1] == runs[2], 'Native wiring replay is unstable'
        assert before == hashlib.sha256(executable.read_bytes()).hexdigest(), 'Native executable changed during replay'
        assert source_before == hashlib.sha256((ROOT / 'native_nested_probe.cpp').read_bytes()).hexdigest(), 'Native source changed during replay'
        if args.record:
            assert installation == runtime_identity(candidate_identity(args.candidate_python)), 'Candidate changed during replay'
            recorded = {'provenance': {'source_sha256': source_before,
                                      'candidate_identity_sha256': digest(installation), 'executable_sha256': before,
                                      'graph_evidence_sha256': digest({case: bridge['cases'][case] for case in sorted(CASES)}),
                                      'stable': True, 'replay_digests': [digest(run) for run in runs]},
                        'cases': runs[0]}
            verify(recorded, bridge)
            assess(recorded, bridge)
            atomic_write(destination, render(recorded) + '\n')
        else:
            recorded = json.loads(destination.read_text())
            assert runs[0] == recorded['cases'], 'Native wiring observations differ; review the variation before recording'
    else:
        recorded = json.loads(destination.read_text())
    verify(recorded, bridge)
    report = assess(recorded, bridge)
    summary = {key: value for key, value in report.items() if key != 'differences'}
    atomic_write(ROOT / 'native_nested_assessment.json', json.dumps(summary, indent=2)[:-2] +
                 ',\n  "differences": [\n' + ',\n'.join('    ' + json.dumps(item) for item in report['differences']) + '\n  ]\n}\n')
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
