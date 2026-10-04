"""Check bundled notebook JSON and imports against the installed PCNtoolkit API."""

import ast
import json
from pathlib import Path

from IPython.core.inputtransformer2 import TransformerManager


def main() -> None:
    root = Path(__file__).resolve().parent
    notebooks = sorted(root.glob("*.ipynb"))
    assert notebooks, "no tutorial notebooks installed"
    assert (root / "LICENSE").is_file(), "upstream license missing"
    assert (root / "README.md").is_file(), "workflow index missing"
    transformer = TransformerManager()
    imports = set()
    for path in notebooks:
        notebook = json.loads(path.read_text())
        assert notebook["nbformat"] == 4, f"unsupported notebook format in {path.name}"
        assert isinstance(notebook["cells"], list), f"invalid cells in {path.name}"
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            source = cell["source"]
            code = source if isinstance(source, str) else "".join(source)
            tree = ast.parse(transformer.transform_cell(code), filename=path.name)
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("pcntoolkit"):
                    imports.add(ast.unparse(node))
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("pcntoolkit"):
                            imports.add(f"import {alias.name}")
    for statement in sorted(imports):
        exec(statement, {})
    print(f"Tutorial JSON and PCNtoolkit imports passed for {len(notebooks)} notebooks")


if __name__ == "__main__":
    main()
