# Contributing to pyjelly

Hello! If you want to contribute to Jelly and help us make it go fast (in Python), please follow the rules described below.

## Quick start

1. Clone the project with `git clone https://github.com/Jelly-RDF/pyjelly.git`.

2. If it's not already installed on your machine, [install uv](https://github.com/astral-sh/uv). uv is the project manager of choice for pyjelly. 
    * Make sure you have the correct version of uv installed. You can find it in `pyproject.toml` under `[tool.uv]` as `required-version`. If you have installed uv through the dedicated standalone installer, you can change the version through `uv self update`:
        ```
        uv self update 0.6.17
        ```

3. Install the project to a virtual environment (typically `.venv` in the project directory) with `uv sync`.
    * If you use an IDE, make sure that it employs the Python interpreter from that environment.

4. [Activate the environment](https://docs.python.org/3/library/venv.html#how-venvs-work) or use [`uv run` to run commands and code](https://docs.astral.sh/uv/guides/projects/). 

## Submit Feedback

The best way to send feedback is to file an issue at https://github.com/Jelly-RDF/pyjelly/issues.

If you are proposing a feature:

1. Explain in detail how it would work.
2. Keep the scope as narrow as possible, to make it easier to implement.
3. Remember that this is a volunteer-driven project, and that contributions are welcome. ;)

## Contributions

1. Every major pull request should be connected to an issue. If you see a problem, first create an issue.
    * For minor issues (typos, small fixes) make sure you describe your problem well in the PR.
2. Every new branch should follow the naming convention of `GH-<issue-number>-<description>`.
    * For minor issues name the branch `GH-minor-<description>`.
3. When opening a pull request:
   * Use a descriptive title.
   * Reference the related issue in the description.
4. Please make sure your code passes all the checks:
   1. Tests (`pytest`)
   2. Type safety (`mypy`)
   3. Formatting and linting (`ruff` or via `pre-commit`)
   This helps us follow best practices and keep the codebase in shape.
