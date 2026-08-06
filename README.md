Neurocontainers repository is used for automatic building and testing of containers available in Neurodesktop

_A Tutorial on how to add new applications to NeuroDesk is here: [How to Add new tools](https://www.neurodesk.org/developers/new_tools/)_

_Information on the **Neurodesk** project is available at [neurodesk.org](https://neurodesk.org)_

_Information on **Neurocontainers** is available at [neurodesk.org/docs/neurocontainers](https://www.neurodesk.org/docs/getting-started/neurocontainers/)_

## Working in this repository

```bash
python3.13 -m venv env && source env/bin/activate
pip install -r requirements.txt && pip install -e .

sf-init <name> <version>    # Scaffold a new recipe in recipes/<name>/
sf-build <name>             # Generate the Dockerfile and build the image
sf-test <name>              # Run the recipe's fulltest.yaml

pytest builder/tests        # Builder and workflow unit tests
./workflows/test_all.sh     # Validate and generate every recipe
```

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to propose a change
- [AGENTS.md](AGENTS.md) — recipe format, conventions, and common tasks
- [builder/README.md](builder/README.md) — the build system and full recipe syntax
- [workflows/TESTING.md](workflows/TESTING.md) — how container tests run locally and in CI
