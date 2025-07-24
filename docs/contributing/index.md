# Contributing to pyjelly

Hi! This guide explains how to get started with developing pyjelly and contributing to it.

## Quick start

Clone the project: 

```shell
git clone --recursive git@github.com:Jelly-RDF/pyjelly.git
```

We use `uv` for package management. If you don't already have it, [install uv](https://github.com/astral-sh/uv).

On Linux this is: 

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Install the project:

```shell
uv sync
```

!!! note 
    
    If you use an IDE, make sure that it uses the Python interpreter from the environment that will be created in the `.venv` directory.

    If you get an error about the uv version being incorrect, run `uv self update`

[Activate the environment](https://docs.python.org/3/library/venv.html#how-venvs-work) or use [`uv run` to run commands and code](https://docs.astral.sh/uv/guides/projects/). 

## Giving feedback

The best way to send feedback is to file an issue at [https://github.com/Jelly-RDF/pyjelly/issues](https://github.com/Jelly-RDF/pyjelly/issues)

If you are proposing a feature:

1. Explain how it would work.
2. Keep the scope as narrow as possible, to make it easier to implement.
3. Contributions are always welcome! Consider if you can help with implementing the feature.

## Contributing code

If you've found a major issue (not just a typo) or have a feature request, please [open an issue](https://github.com/Jelly-RDF/pyjelly/issues/new/choose) first. This helps us keep track of what needs to be done and allows others to contribute as well.

**When opening a pull request:**

- Use a descriptive title.
- Clearly describe what the pull request does and why it is needed.
- Reference the related issue in the description.

The CI will check if your code passes all checks. You can run these checks locally:

- Tests: `uv run pytest`
- Type safety: `uv run mypy .`
- Linter: `uv run ruff check --fix`
- Formatter: `uv run ruff format`

This helps us follow best practices and keep the codebase in shape.

## Contributing documentation

The documentation is written in Markdown and built using [MkDocs](https://www.mkdocs.org/), using [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

To edit a documentation page, simply click the :material-file-edit-outline: button in the top-right of the page.

It will take you to GitHub, where you can edit the Markdown file and submit a pull request. You can also clone [the repository](https://github.com/Jelly-RDF/pyjelly) and edit the files locally. The source files are in the `docs` directory.

### Previewing documentation locally

Install dependencies for docs:

```shell
uv sync --group docs
```

Then, start a local docs server:

```shell
uv run mkdocs serve
```

### Further reading

- [More information on editing Jelly docs]({{ proto_link("contributing/#editing-documentation") }})
- [Material for MkDocs reference](https://squidfunk.github.io/mkdocs-material/reference/)
- [MkDocs documentation](https://www.mkdocs.org/user-guide/writing-your-docs/)
- [Macro plugin documentation](https://mkdocs-macros-plugin.readthedocs.io/en/latest/)

## Making releases

1. Make sure you are on the `main` branch and that it is up-to-date: `git checkout main && git pull`.
2. Checkout a new branch.
3. Update the version in `pyproject.toml` with either:
    - `uv version X.Y.Z`
    - `uv version --bump major|minor|patch`
4. Commit the changes, push the branch, and open a pull request.
5. Once the pull request is merged, go back to the `main` branch: `git checkout main && git pull`.
6. Create a new tag for the release. For example, for version 1.2.3: `git tag v1.2.3`. **The tag must start with `v`!**
7. Push the tag to GitHub: `git push origin v1.2.3`.
8. The release will be automatically built and published to PyPI.
