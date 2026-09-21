"""Edit YAML scalars without reformatting surrounding recipe content."""

from __future__ import annotations

import json

import yaml


def mapping_nodes(text: str) -> dict[str, tuple[yaml.Node, yaml.Node]]:
    root = yaml.compose(text)
    if not isinstance(root, yaml.MappingNode):
        raise ValueError("recipe must contain a YAML mapping")
    return {key.value: (key, value) for key, value in root.value}


def set_scalar(text: str, parent: str | None, key: str, value: str) -> str:
    nodes = mapping_nodes(text)
    if parent is None:
        if key not in nodes:
            return text.rstrip() + "\n\n" + yaml.safe_dump({key: value}, sort_keys=False)
        node = nodes[key][1]
    elif parent not in nodes:
        # Variables must precede expressions that reference them during rendering.
        return yaml.safe_dump({parent: {key: value}}, sort_keys=False) + "\n" + text
    else:
        node = nodes[parent][1]
        if not isinstance(node, yaml.MappingNode):
            raise ValueError(f"{parent} must be a mapping")
        match = next((v for k, v in node.value if k.value == key), None)
        if match is None:
            if node.flow_style:
                mapping = yaml.safe_load(text)[parent]
                mapping[key] = value
                rendered = yaml.safe_dump(mapping, default_flow_style=True, width=100000).strip()
                return text[:node.start_mark.index] + rendered + text[node.end_mark.index:]
            start = text.rfind("\n", 0, node.start_mark.index) + 1
            line = " " * node.start_mark.column + key + ": " + json.dumps(value) + "\n"
            return text[:start] + line + text[start:]
        node = match
    if not isinstance(node, yaml.ScalarNode):
        raise ValueError(f"{parent}.{key} must be a scalar")
    return text[:node.start_mark.index] + json.dumps(value) + text[node.end_mark.index:]
