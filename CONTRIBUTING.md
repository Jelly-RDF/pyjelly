# Contributing to pyjelly

Hi! This guide explains how to get started with developing pyjelly and contributing to it.

## Quick start

1. Clone the project with `git clone git@github.com:Jelly-RDF/pyjelly.git`.

2. We use `uv` for package management. If you don't already have it, [install uv](https://github.com/astral-sh/uv).
    * On Linux this is: `curl -LsSf https://astral.sh/uv/install.sh | sh`

3. Run `uv sync` to install the project.
    * If you use an IDE, make sure that it uses the Python interpreter from the environment that will be created in the `.venv` directory.
    * If you get an error about the uv version being incorrect, run `uv self update`

4. [Activate the environment](https://docs.python.org/3/library/venv.html#how-venvs-work) or use [`uv run` to run commands and code](https://docs.astral.sh/uv/guides/projects/). 

## Submit Feedback

The best way to send feedback is to file an issue at [https://github.com/Jelly-RDF/pyjelly/issues](https://github.com/Jelly-RDF/pyjelly/issues)

If you are proposing a feature:

1. Explain how it would work.
2. Keep the scope as narrow as possible, to make it easier to implement.
3. Contributions are always welcome! Consider if you can help with implementing the feature.

## Contributions

1. Every major pull request should be connected to an issue. If you see a problem, first create an issue.
    * For minor issues (typos, small fixes) make sure you describe your problem well in the PR.
2. When opening a pull request:
    * Use a descriptive title.
    * Reference the related issue in the description.
3. Please make sure your code passes all the checks:
    * Tests (`pytest`)
    * Type safety (`mypy`)
    * Formatting and linting (`ruff` or via `pre-commit`)
    This helps us follow best practices and keep the codebase in shape.
