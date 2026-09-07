import pytest
import yaml

from workflows import migrate_local_image_sources as migration


@pytest.fixture
def migrated_recipe(tmp_path, monkeypatch):
    monkeypatch.setattr(migration, 'RECIPES', tmp_path)
    path = tmp_path / 'example' / 'build.yaml'
    path.parent.mkdir()
    path.write_text('''name: example
version: 1.0.0
auto_update:
  method: manual
build:
  base-image: ubuntu:22.04
  directives:
            - run:
              - git clone https://github.com/Bostrix/FSL-BET2
''')
    migration.migrate_local('example', 'ubuntu22', True)
    return path


def test_rerun_preserves_sources_and_local_inputs_added_later(migrated_recipe):
    recipe = yaml.safe_load(migrated_recipe.read_text())
    recipe['auto_update']['local'] = ['macros/shared']
    recipe['auto_update']['sources'].append({
        'id': 'shared_source', 'method': 'pypi', 'package': 'shared',
        'target': {'variable': 'shared_version'},
    })
    migrated_recipe.write_text(yaml.safe_dump(recipe, sort_keys=False))
    before = migrated_recipe.read_text()
    migration.migrate_local('example', 'ubuntu22', True)
    migration.migrate_local('example', 'ubuntu22', True)
    assert migrated_recipe.read_text() == before


@pytest.mark.parametrize('change', ['missing_source', 'changed_source', 'duplicate_checkout'])
def test_conflicting_migration_state_is_rejected_without_writing(migrated_recipe, change):
    recipe = yaml.safe_load(migrated_recipe.read_text())
    if change == 'missing_source':
        recipe['auto_update']['sources'].pop()
    elif change == 'changed_source':
        recipe['auto_update']['sources'][0]['tag'] = '24.04'
    else:
        commands = recipe['build']['directives'][0]['run']
        commands.append(commands[-1])
    migrated_recipe.write_text(yaml.safe_dump(recipe, sort_keys=False))
    before = migrated_recipe.read_text()
    with pytest.raises(ValueError):
        migration.migrate_local('example', 'ubuntu22', True)
    assert migrated_recipe.read_text() == before
