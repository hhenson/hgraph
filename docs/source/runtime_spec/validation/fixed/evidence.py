"""Keep complete tick records on single lines without altering their JSON."""
import json


def render(value, depth=0, key=''):
    indent = '  ' * depth
    child_indent = indent + '  '
    if isinstance(value, dict) and value:
        fields = [child_indent + json.dumps(k) + ': ' + render(v, depth + 1, k)
                  for k, v in value.items()]
        return '{\n' + ',\n'.join(fields) + '\n' + indent + '}'
    if isinstance(value, list) and value:
        rows = [json.dumps(v) if key in ('ticks', 'expected') else render(v, depth + 1)
                for v in value]
        return '[\n' + ',\n'.join(child_indent + row for row in rows) + '\n' + indent + ']'
    return json.dumps(value)
