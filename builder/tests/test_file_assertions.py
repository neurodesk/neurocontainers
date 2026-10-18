"""Exercise fulltest file assertions using real host commands and tiny files."""

import nibabel as nib
import numpy as np
import pytest

from builder.run_tests import run_single_test


def run_assertions(tmp_path, validations, **fields):
    return run_single_test(
        {"command": "true", "validate": validations, **fields},
        None,
        {"output": "image.nii.gz", "directory": "output"},
        tmp_path,
    )


@pytest.mark.parametrize("shape", [(2, 3, 4), (2, 3), (2, 3, 4, 1), (2, 3, 4, 5)])
def test_is_3d_checks_actual_rank(tmp_path, shape):
    nib.save(nib.Nifti1Image(np.zeros(shape), np.eye(4)), tmp_path / "image.nii.gz")

    result = run_assertions(
        tmp_path, [{"output_exists": "${output}"}, {"is_3d": "${output}"}]
    )

    assert result.passed == (len(shape) == 3)
    if not result.passed:
        assert "Expected 3D image" in result.message
        assert str(shape) in result.message


@pytest.mark.parametrize("kind", ["missing", "invalid"])
@pytest.mark.parametrize("assertion", ["is_3d", "same_dimensions"])
def test_image_read_failures_are_reported(tmp_path, kind, assertion):
    if kind == "invalid":
        (tmp_path / "image.nii.gz").write_text("not an image")
    argument = "${output}" if assertion == "is_3d" else ["${output}", "${output}"]

    result = run_assertions(tmp_path, [{assertion: argument}])

    assert not result.passed
    assert "Error" in result.message
    assert "image.nii.gz" in result.message


def test_matching_dimensions_and_absolute_paths(tmp_path):
    image = tmp_path / "image.nii.gz"
    nib.save(nib.Nifti1Image(np.zeros((2, 3, 4)), np.eye(4)), image)

    result = run_assertions(
        tmp_path,
        [{"output_exists": str(image)}, {"same_dimensions": [str(image), "${output}"]}],
    )

    assert result.passed, result.message


@pytest.mark.parametrize("kind", ["missing", "file", "empty", "nonempty"])
def test_output_directory_must_exist_and_have_an_entry(tmp_path, kind):
    directory = tmp_path / "output"
    if kind == "file":
        directory.write_text("not a directory")
    elif kind != "missing":
        directory.mkdir()
        if kind == "nonempty":
            (directory / "entry").touch()

    result = run_assertions(tmp_path, [{"output_dir_not_empty": "${directory}"}])

    assert result.passed == (kind == "nonempty")
    if not result.passed:
        assert "directory" in result.message
        assert str(directory) in result.message


@pytest.mark.parametrize("validations", [None, "", "is_3d", {}, {"is_3d": "image"}, 0])
def test_validate_requires_a_list(tmp_path, validations):
    result = run_assertions(tmp_path, validations)

    assert not result.passed
    assert "validate must be a list" in result.message


@pytest.mark.parametrize("validation", [None, "is_3d", [], {}, 42, False])
def test_assertion_requires_a_nonempty_mapping(tmp_path, validation):
    result = run_assertions(tmp_path, [validation])

    assert not result.passed
    assert "non-empty mapping" in result.message


@pytest.mark.parametrize(
    "assertion", ["output_exists", "is_3d", "output_dir_not_empty"]
)
@pytest.mark.parametrize("argument", [None, "", "  ", 0, True, [], {}])
def test_path_assertions_require_nonempty_strings(tmp_path, assertion, argument):
    result = run_assertions(tmp_path, [{assertion: argument}])

    assert not result.passed
    assert assertion in result.message
    assert "non-empty path string" in result.message


@pytest.mark.parametrize(
    "argument", [None, "ab", [], ["a"], ["a", "b", "c"], ["a", 0], ["", "b"]]
)
def test_same_dimensions_requires_two_paths(tmp_path, argument):
    result = run_assertions(tmp_path, [{"same_dimensions": argument}])

    assert not result.passed
    assert "same_dimensions requires two non-empty path strings" in result.message


def test_unknown_assertion_cannot_hide_after_a_passing_assertion(tmp_path):
    (tmp_path / "image.nii.gz").touch()
    result = run_assertions(
        tmp_path, [{"output_exists": "${output}", "unknown_assertion": True}]
    )

    assert not result.passed
    assert result.message == "Unsupported file assertion: unknown_assertion"


@pytest.mark.parametrize("validation", ["invalid", {"unknown_assertion": True}])
def test_file_declaration_errors_follow_process_and_earlier_file_failures(
    tmp_path, validation
):
    process_result = run_assertions(tmp_path, [validation], command="exit 7")
    file_result = run_assertions(tmp_path, [{"output_exists": "missing"}, validation])

    assert process_result.message == "Expected exit code 0, got 7"
    assert file_result.message == f"Output file not found: {tmp_path / 'missing'}"


def test_empty_validation_list_is_allowed(tmp_path):
    assert run_assertions(tmp_path, []).passed


@pytest.mark.parametrize(
    "assertion", ["output_exists", "is_3d", "output_dir_not_empty", "same_dimensions"]
)
@pytest.mark.parametrize("empty", ["", "  "])
def test_empty_expanded_paths_are_rejected(tmp_path, assertion, empty):
    (tmp_path / "entry").touch()
    argument = (
        ["${nested}", "valid.nii"] if assertion == "same_dimensions" else "${nested}"
    )
    result = run_single_test(
        {"command": "true", "validate": [{assertion: argument}]},
        None,
        {"nested": "${output}", "output": empty},
        tmp_path,
    )

    assert not result.passed
    assert assertion in result.message
    assert "non-empty path string" in result.message
