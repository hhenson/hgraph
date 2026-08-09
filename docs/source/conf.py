from datetime import datetime

project = "hgraph"
author = "Howard Henson"
copyright = f"{datetime.now().year}, {author}"
release = "0.8.0"

extensions = [
    "myst_parser",
    "sphinx.ext.mathjax",
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": -1,
}

myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
]

myst_fence_as_directive = ["mermaid"]
latex_engine = "xelatex"
