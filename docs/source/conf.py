import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath("."))

project = "hgraph"
author = "Howard Henson"
copyright = f"{datetime.now().year}, {author}"
release = "0.8.0"

extensions = [
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinxcontrib.bibtex",
    "sphinxcontrib.mermaid",
    "myst_parser",
]

# Autodoc is deliberately absent. The operator surface resolves lazily through
# `hgraph.__getattr__` over the native operator registry rather than through
# module-level definitions, so an autodoc-based API reference needs a
# generation strategy of its own and a docs build that imports the compiled
# extension. Both are tracked as the API-reference work; until then no page
# here uses an `auto*` directive, and the docs build needs no compiler.

templates_path = ["_templates"]
exclude_patterns = ["_build", "build", "Thumbs.db", ".DS_Store"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
}
intersphinx_disabled_domains = ["std"]

# Only run examples explicitly marked with `testcode`/`doctest` directives.
# Without this, `sphinx-build -b doctest` also harvests bare `>>>` blocks,
# including third-party doctests embedded in re-exported numpy docstrings.
doctest_test_doctest_blocks = ""

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_theme_options = {
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": -1,
    "includehidden": True,
    "titles_only": False,
}

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "tasklist",
]
myst_fence_as_directive = ["mermaid"]
# The specification pages use `##`-level headings under a `#` title; without
# this MyST refuses to build a section hierarchy deeper than the toctree depth.
myst_heading_anchors = 3

bibtex_bibfiles = ["references.bib"]
bibtex_default_style = "alpha"
bibtex_reference_style = "author_year"

latex_engine = "xelatex"
epub_show_urls = "footnote"
