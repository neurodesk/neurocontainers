"""Compare compiled downloads and build commands with a pre-migration git ref."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from builder.config import default_config
from builder.dockerfile import render_dockerfile
from builder.recipe import RecipeFile, compile_recipe
from builder.variants import concrete_variant_specs


def verify(ref: str) -> list[dict]:
    paths = subprocess.check_output(['git', 'diff', '--name-only', ref, '--', 'recipes/*/build.yaml'], cwd=ROOT, text=True).splitlines()
    results = []
    for relative in paths:
        path = ROOT / relative
        old = yaml.safe_load(subprocess.check_output(['git', 'show', f'{ref}:{relative}'], cwd=ROOT, text=True))
        new = yaml.safe_load(path.read_text())
        for spec in concrete_variant_specs(new):
            compiled = []
            for recipe in (old, new):
                with patch('builder.recipe.load_recipe_file', return_value=RecipeFile(path, recipe)):
                    compiled.append(compile_recipe(path.parent, architecture=spec['architecture'], variant=spec['variant'], include_dirs=default_config().include_dirs))
            before, after = compiled
            # Cache mount IDs hash recipe expressions, not downloaded bytes.
            dockerfiles = [re.sub(r'\bh[0-9a-f]{8}\b', 'CACHE_ID', render_dockerfile(c.definition)) for c in compiled]
            old_files, new_files = before.staging_plan.files, after.staging_plan.files
            file_changes = [name for name in sorted(old_files.keys() | new_files.keys())
                            if old_files.get(name) != new_files.get(name)]
            result = {'recipe': path.parent.name, 'variant': spec['variant'], 'architecture': spec['architecture'],
                      'downloads_unchanged': not file_changes, 'changed_files': file_changes,
                      'commands_unchanged': dockerfiles[0] == dockerfiles[1]}
            if not result['commands_unchanged']:
                import difflib
                result['diff'] = '\n'.join(difflib.unified_diff(dockerfiles[0].splitlines(), dockerfiles[1].splitlines(), n=1))
            results.append(result)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ref', default='HEAD')
    args = parser.parse_args()
    results = verify(args.ref)
    differences = [r for r in results if not (r['downloads_unchanged'] and r['commands_unchanged'])]
    print(json.dumps({'compiled_variants': len(results), 'differences': differences}, indent=2))
    return bool(differences)


if __name__ == '__main__':
    raise SystemExit(main())
