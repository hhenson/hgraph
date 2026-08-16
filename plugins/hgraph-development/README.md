# hgraph Development plugin

<img src="assets/logo.png" alt="hgraph Development plugin logo" width="160">

This skills-only plugin packages the hgraph development guidance for:

- high-performance C++ compute and sink nodes;
- composable C++ and Python graphs;
- operator contracts, overloads, and cross-language registration.

The plugin is useful in downstream projects that build on hgraph and do not
otherwise inherit the repository-local skills from the hgraph source tree.

## Install from the open-source marketplace

Add the hgraph repository as a Codex marketplace, then install the plugin:

```sh
codex plugin marketplace add hhenson/hgraph
codex plugin add hgraph-development@hgraph
```

Start a new Codex session after installation so the bundled skills are loaded.
The plugin can also be found and installed through the Codex plugin browser
after the marketplace is added.

Codex IDE releases that do not support plugins can use the skill directories
directly. Copy or link the required directories from `skills/` into the
downstream repository's `.agents/skills/` directory. Claude users can expose
the same directories under `.claude/skills/`.

## Maintaining the bundle

The canonical in-repository skills remain under `.agents/skills/`. Refresh the
plugin copies after changing one of them:

```sh
python3 plugins/hgraph-development/scripts/sync_skills.py --write
python3 plugins/hgraph-development/scripts/sync_skills.py --check
```

The check exits unsuccessfully when a bundled skill differs from its canonical
source.

## Public directory

The repository marketplace makes the plugin installable directly from GitHub.
Publishing it in the universal ChatGPT and Codex plugin directory additionally
requires a skills-only submission through the OpenAI Platform plugin portal.
That submission is performed by a verified hgraph publisher and is reviewed by
OpenAI. [`SUBMISSION.md`](SUBMISSION.md) contains the prepared listing copy,
test cases, release notes, and remaining maintainer checklist.
