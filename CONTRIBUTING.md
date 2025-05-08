# Contributing to pyjelly

Hello! If you want to contribute to Jelly and help us make it go fast (in Python), please follow the rules described below.

## Quick start

1. Clone the project with `git clone https://github.com/Jelly-RDF/pyjelly.git`.

2. If it's not already installed on your machine, install [uv](https://github.com/astral-sh/uv). uv is the project manager of choice for pyjelly. 
    * Make sure you have the correct version of uv installed. You can find it in `pyproject.toml` under `[tool.uv]` as `required-version`. If you have installed uv through the dedicated standalone installer, you can change the version through `uv self.update`:
        ```
        uv self.update version==0.6.17
        ```

3. Now, when you call `uv run` (for example by calling `uv run mypy pyjelly`), uv will automatically download all necessary dependencies. Run `uv run tests/conftest.py` to check if everything works fine.
    * If you're using an IDE for development, make sure the Python interpreter path you're using points to the Python binary created by uv.

## Contributions

1. Every pull request should be connected to an issue. If you see a problem, first create an issue.
2. Every new branch should follow the naming convention of `GH-<issue-number>-<description>`.
3. If you submit a PR, make sure it has a descriptive title and references the issue it touches on in its description.
4. Before submitting a PR, make sure your code is clean by:
    1. Running 
        ```
        uv run mypy pyjelly
        uv run mypy tests
        ```
    2. Running all tests with `pytest`.