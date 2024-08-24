# HGraph Orders and Pricing Library

Provides a library, based on the hgraph functional reactive framework to
support creating order and pricing logic.

The core components of the library include:

* instruments
* positions
* portfolios
* orders
* pricing

This library is currently very green and is expected to have significant changes.


## Development

The project is currently configured to make use of [Poetry](https://python-poetry.org) for dependency management. 
Take a look at the website to see how best to install the tool.
Once you have checked out the project, you can install the project for development using the following command:

This is optional, but you can ensure python uses the version of python you require.

```bash
poetry env use 3.11
```

Then use the following command to install the project and it's depenencies:

```bash
poetry install
```

Then you can find the location of the installation using:

```bash
poetry env info
```

PyCharm can make use of poetry to ``setup`` the project.

### Run Tests

```bash
# No Coverage
poetry run pytest
```

```bash
# Generate Coverage Report
poetry run pytest --cov=your_package_name --cov-report=xml
```