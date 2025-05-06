# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath(".."))


import wiring_autodoc_extension

project = "hgraph"
author = "Howard Henson"
copyright = f"{datetime.now().year}, {author}"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinxcontrib.plantuml",
    "sphinx.ext.graphviz",
    "sphinxcontrib.bibtex",
    "sphinx.ext.mathjax",  # For HTML
    "sphinx.ext.imgmath",  # For PDF
    "wiring_autodoc_extension",
]

intersphinx_mapping = {
    "rtd": ("https://docs.readthedocs.io/en/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

autodoc_typehints_format = "short"
autodoc_member_order = "groupwise"
add_module_names = False

templates_path = ["_templates"]
exclude_patterns = ["build", "_build", "Thumbs.db", ".DS_Store"]


epub_show_urls = "footnote"

# This is to support Unicode characters in pdf and latex documentation generation
latex_engine = "xelatex"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": -1,
    "includehidden": True,
    "titles_only": False,
}

bibtex_bibfiles = ["references.bib"]  # List of your `.bib` files
bibtex_default_style = "alpha"
bibtex_reference_style = "author_year"
