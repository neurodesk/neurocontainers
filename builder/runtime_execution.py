"""Test configuration and process assertions shared by runtime test execution."""

from dataclasses import dataclass
from typing import Any, Mapping


def substitute_variables(text: str, variables: dict[str, str]) -> str:
    """Substitute ${var} placeholders, including nested variable values."""
    if not text:
        return text

    result = text
    for _ in range(10):
        previous = result
        for key, value in variables.items():
            result = result.replace(f"${{{key}}}", str(value))
            result = result.replace(f"${key}", str(value))
        if result == previous:
            break
    return result


@dataclass(frozen=True)
class TestSpec:
    """Execution fields from a fulltest entry; unrelated metadata is permitted."""

    name: str
    command: str
    script: str
    env_setup: str | None
    timeout: int | float | None
    ignore_exit_code: bool
    expected_exit_code: int
    expected_exit_code_not: int | None
    expected_output: tuple[str, ...]
    validations: tuple[Any, ...]

    @classmethod
    def from_mapping(
        cls,
        test: Mapping[str, Any],
        global_env_setup: str | None = None,
        default_timeout: int = 120,
    ) -> "TestSpec":
        expected_output = test.get("expected_output_contains") or ()
        if isinstance(expected_output, str):
            expected_output = (expected_output,)
        return cls(
            name=test.get("name", "Unnamed test"),
            command=test.get("command", ""),
            script=test.get("script", ""),
            env_setup=test.get("env_setup", global_env_setup),
            timeout=test.get("timeout", default_timeout),
            ignore_exit_code=test.get("ignore_exit_code", False),
            expected_exit_code=test.get("expected_exit_code", 0),
            expected_exit_code_not=test.get("expected_exit_code_not"),
            expected_output=tuple(str(value) for value in expected_output),
            validations=tuple(test.get("validate", [])),
        )


def evaluate_process_assertions(
    spec: TestSpec,
    exit_code: int,
    stdout: str,
    stderr: str,
    variables: dict[str, str],
) -> str | None:
    """Return the first exit/output failure, or None when assertions pass."""
    if not spec.ignore_exit_code:
        if spec.expected_exit_code_not is not None:
            if exit_code == spec.expected_exit_code_not:
                return f"Exit code should not be {spec.expected_exit_code_not}"
        elif exit_code != spec.expected_exit_code:
            return f"Expected exit code {spec.expected_exit_code}, got {exit_code}"

    combined_output = stdout + stderr
    for expected in spec.expected_output:
        expected = substitute_variables(expected, variables)
        if expected and expected not in combined_output:
            return f"Expected output not found: '{expected[:50]}...'"
    return None
