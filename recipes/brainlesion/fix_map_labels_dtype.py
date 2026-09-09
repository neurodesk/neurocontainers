"""Stop instance relabelling overflowing the connected-components dtype.

panoptica._functionals._map_labels builds its lookup table with the dtype of the
connected-components array. cc3d returns the narrowest dtype that fits the
component count (uint8 for <= 255 components), but the instance matcher then
allocates new labels up to max(ref_labels) + 1 + n_unmatched_predictions, which
can exceed it. Ordinary multi-lesion evaluations therefore fail with either

    OverflowError: Python integer 256 out of bounds for uint8
    IndexError: index N is out of bounds for axis 0 with size 0

the second because max_value wraps to 0 and np.arange returns an empty array.

Casting the *input* does not help: the dtype comes from panoptica's internal CCA
output, not the user's array. The dtype is now chosen from the values being
written, matching the fix on upstream main. Still present in 2.1.6, so the image
carries it until a release includes it.

Do not "fix" this by pinning numpy<2: numpy 2 raises on the out-of-range cast,
while numpy 1 wraps silently (label 256 -> 0) and corrupts instance matching with
no error at all.
"""

import pathlib
from importlib.util import find_spec

MARKER = "neurocontainers: choose the lookup dtype from the values written"

OLD = """    k = np.array(list(label_map.keys()), dtype=arr.dtype)
    v = np.array(list(label_map.values()), dtype=arr.dtype)

    max_value = max(arr.max(), max(k), max(v)) + 1

    mapping_ar = np.arange(max_value, dtype=arr.dtype)
"""

NEW = f"""    # {MARKER}
    _keys = list(label_map.keys())
    _values = list(label_map.values())
    # int() throughout: max(...) + 1 on a uint8 array wraps to 0 otherwise
    max_value = int(max(int(arr.max()), int(max(_keys)), int(max(_values)))) + 1
    _target_dtype = np.promote_types(arr.dtype, np.min_scalar_type(max_value))
    k = np.array(_keys, dtype=_target_dtype)
    v = np.array(_values, dtype=_target_dtype)

    mapping_ar = np.arange(max_value, dtype=_target_dtype)
"""


def main() -> None:
    spec = find_spec("panoptica")
    if spec is None or not spec.submodule_search_locations:
        raise SystemExit("panoptica is not installed")
    target = pathlib.Path(next(iter(spec.submodule_search_locations))) / "_functionals.py"
    src = target.read_text()
    if MARKER in src:
        return
    if src.count(OLD) != 1:
        raise SystemExit(
            f"expected exactly one _map_labels dtype block in {target}, "
            f"found {src.count(OLD)}; upstream changed and this patch needs revisiting"
        )
    target.write_text(src.replace(OLD, NEW, 1))
    print(f"patched {target}")


if __name__ == "__main__":
    main()
