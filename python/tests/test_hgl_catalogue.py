"""Ratchet the reviewed migration inventory against source and HGL evidence."""
import importlib.util
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("hgl_catalogue", ROOT / "tools/hgl_catalogue.py")
catalogue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catalogue)


def test_cpp_inventory_ignores_comments_and_keeps_nested_arguments():
    source = '// register_overload<Fake, Nope>();\nregister_overload<Op, lift<kernel<int, double>>>();'
    stripped = catalogue.without_comments(source)
    start = stripped.index('register_overload<') + len('register_overload<')
    args, _ = catalogue.template_arguments(stripped, start)
    assert args == ['Op', 'lift<kernel<int, double>>']
    assert stripped.count('\n') == source.count('\n')
    assert catalogue.without_comments('"// quoted" /* hidden */').startswith('"// quoted"')


def test_hgl_catalogue_is_current_and_every_identity_has_a_disposition():
    subprocess.run([sys.executable, str(ROOT / 'tools/hgl_catalogue.py'), '--check'], check=True)
    data = catalogue.build_catalogue()
    assert data['operators']
    for entry in data['operators']:
        review = entry['review']
        assert review['status'] != 'unreviewed', entry['name']
        assert review['completed_domains'] or review['blockers'], entry['name']
        assert review['evidence'], entry['name']
        assert entry['core_cutover'] == 'deferred'
        if review['status'].startswith('implemented'):
            assert any(x['kind'] == 'implementation' and x['form'] != 'native-delegation' for x in entry['hgl'])
