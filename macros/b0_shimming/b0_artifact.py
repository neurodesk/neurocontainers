from dataclasses import dataclass
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid

import nibabel as nib
import numpy as np
from scipy.ndimage import map_coordinates

from b0mapromeo_analytic import _validate_model, _number
from b0mapromeo_shim import compute_shim, read_calibration, ShimResult


@dataclass(frozen=True)
class SavedMap:
    id: str
    field_hz: np.ndarray
    support: np.ndarray
    affine: np.ndarray
    context: dict
    identity: dict
    path: Path
    digest: str


@dataclass(frozen=True)
class TargetEvaluation:
    field_hz: np.ndarray
    support: np.ndarray
    fit_roi: np.ndarray
    shim: object
    comment: str


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value
    ):
        raise ValueError(
            "b0mapid must be 1-64 ASCII letters, digits, underscore or hyphen"
        )
    return value


def _store(store):
    return Path(
        store
        if store is not None
        else os.environ.get("B0_MAP_STORE", "/tmp/share/b0maps")
    )


def _measurement(field, support, affine):
    field = np.array(field, dtype=float, copy=True)
    support = np.array(support, copy=True)
    affine = np.array(affine, dtype=float, copy=True)
    if (
        field.ndim != 3
        or any(n == 0 for n in field.shape)
        or not np.isfinite(field).all()
        or support.shape != field.shape
        or not np.isin(support, [0, 1]).all()
        or not support.any()
        or affine.shape != (4, 4)
        or not np.isfinite(affine).all()
        or not np.allclose(affine[3], [0, 0, 0, 1], rtol=0, atol=1e-8)
        or abs(np.linalg.det(affine[:3, :3])) < 1e-12
    ):
        raise ValueError(
            "Field requires finite XYZ Hz, binary nonempty support and nonsingular RAS mm affine"
        )
    support = support.astype(bool)
    for array in (field, support, affine):
        array.setflags(write=False)
    return field, support, affine


def source_context(settings):
    calibration = settings.get("shim_calibration")
    currents = settings.get("shim_current_a")
    model = settings.get("shim_analytical_model")
    native = settings.get("shim_native_settings")
    measured = calibration is not None or currents is not None
    analytical = model is not None or native is not None
    if measured and analytical:
        raise ValueError("Measured and analytical shim inputs cannot be combined")
    if analytical:
        if model is None or native is None:
            raise ValueError(
                "Analytical model and native acquisition settings must both be supplied"
            )
        raw_model = (
            model if isinstance(model, dict) else json.loads(Path(model).read_text())
        )
        parsed = _validate_model(raw_model)
        baseline = json.loads(native) if isinstance(native, str) else native
        if not isinstance(baseline, dict) or set(baseline) != {
            c.label for c in parsed.channels
        }:
            raise ValueError(
                "Native acquisition settings must name exactly the selected channels"
            )
        baseline = {key: _number(value) for key, value in baseline.items()}
        values = np.array([baseline[c.label] for c in parsed.channels])
        bounds = np.array(parsed.absolute_native_bounds)
        if np.any(values < bounds[:, 0]) or np.any(values > bounds[:, 1]):
            raise ValueError("Acquisition native settings exceed absolute bounds")
        return {
            "kind": "analytical",
            "model": raw_model,
            "baseline": baseline,
            "provenance": "source acquisition explicit inputs",
        }
    if measured:
        if calibration is None or currents is None:
            raise ValueError(
                "Shim calibration and acquisition currents must both be supplied"
            )
        config, baseline = read_calibration(calibration, currents)
        profile = config.get("coil_profiles")
        if not isinstance(profile, str) or not profile:
            raise ValueError("coil_profiles must identify a registered NIfTI file")
        image = nib.load(Path(calibration).parent / profile)
        profiles = image.get_fdata(dtype=np.float64)
        if (
            profiles.ndim != 4
            or profiles.shape[3] != len(baseline)
            or not np.isfinite(profiles).all()
        ):
            raise ValueError("Calibration profiles must be finite XYZ-channel Hz/A")
        _measurement(profiles[..., 0], np.ones(profiles.shape[:3]), image.affine)
        return {
            "kind": "measured",
            "model": config,
            "baseline": dict(zip(config["channels"], baseline.tolist())),
            "profiles": profiles,
            "profile_affine": image.affine,
            "provenance": "source acquisition explicit inputs",
        }
    return {
        "kind": "unknown",
        "model": None,
        "baseline": None,
        "provenance": "source acquisition settings not supplied",
    }


def _json(value):
    return json.dumps(
        value, sort_keys=True, allow_nan=False, separators=(",", ":")
    ).encode()


def _write(path, data):
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fchmod(stream.fileno(), 0o644)
        os.fsync(stream.fileno())


def _nifti_bytes(array, affine):
    image = nib.Nifti1Image(array, affine)
    image.header.set_xyzt_units("mm")
    return gzip.compress(image.to_bytes(), mtime=0)


def publish_map(
    field, support, affine, context, identity=None, *, requested_id=None, store=None
):
    field, support, affine = _measurement(field, support, affine)
    name = _id(requested_id or uuid.uuid4().hex)
    root = _store(store)
    root.mkdir(parents=True, exist_ok=True)
    destination = root / name
    if destination.exists() or destination.is_symlink():
        raise ValueError("b0mapid already exists")
    staging = Path(tempfile.mkdtemp(prefix=".pending-", dir=root))
    try:
        context = dict(context)
        payloads = {
            "b0_hz.nii.gz": _nifti_bytes(field, affine),
            "support.nii.gz": _nifti_bytes(support.astype(np.uint8), affine),
        }
        if context["kind"] == "measured":
            profiles = context.pop("profiles")
            profile_affine = context.pop("profile_affine")
            nib.save(
                nib.Nifti1Image(profiles, profile_affine), staging / "profiles.nii"
            )
            payloads["profiles.nii"] = (staging / "profiles.nii").read_bytes()
            (staging / "profiles.nii").unlink()
            context["model"] = {**context["model"], "coil_profiles": "profiles.nii"}
            payloads["calibration.json"] = _json(context["model"])
        manifest = {
            "schema": 1,
            "id": name,
            "units": "Hz",
            "coordinates": "RAS_mm_XYZ_voxel_centers",
            "affine": affine.tolist(),
            "shape": list(field.shape),
            "context": context,
            "identity": identity or {},
            "payloads": {
                key: hashlib.sha256(data).hexdigest() for key, data in payloads.items()
            },
        }
        for key, data in payloads.items():
            _write(staging / key, data)
        _write(staging / "manifest.json", _json(manifest))
        _read_bundle(staging, name)
        staging.chmod(0o755)
        descriptor = os.open(staging, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        # Completed destinations are nonempty, so Linux rename cannot replace them.
        if destination.exists() or destination.is_symlink():
            raise ValueError("b0mapid already exists")
        try:
            staging.rename(destination)
        except OSError as exc:
            raise ValueError(
                "Map publication failed or b0mapid already exists"
            ) from exc
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return read_map(name, store=root)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _safe_bytes(directory, name):
    if not isinstance(name, str) or Path(name).name != name or name in (".", ".."):
        raise ValueError("Artifact payload paths must be simple relative filenames")
    descriptor = os.open(directory / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        import stat

        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("Artifact payload must be a regular file")
        return stream.read()


def _read_bundle(directory, name):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("Map must be a completed nonsymlink directory")
    raw = _safe_bytes(directory, "manifest.json")
    manifest = json.loads(raw)
    if (
        manifest.get("schema") != 1
        or manifest.get("id") != name
        or manifest.get("units") != "Hz"
        or manifest.get("coordinates") != "RAS_mm_XYZ_voxel_centers"
    ):
        raise ValueError("Unsupported or inconsistent map manifest")
    payloads = manifest.get("payloads")
    if (
        not isinstance(payloads, dict)
        or not {"b0_hz.nii.gz", "support.nii.gz"} <= payloads.keys()
    ):
        raise ValueError("Map payloads are incomplete")
    loaded = {}
    for key, digest in payloads.items():
        data = _safe_bytes(directory, key)
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Map payload digest mismatch")
        loaded[key] = data
    images = [
        nib.Nifti1Image.from_bytes(gzip.decompress(loaded[key]))
        for key in ("b0_hz.nii.gz", "support.nii.gz")
    ]
    for image in images:
        if image.header.get_xyzt_units()[0] != "mm" or not np.allclose(
            image.affine, manifest["affine"], rtol=0, atol=1e-4
        ):
            raise ValueError("NIfTI geometry or units disagree with manifest")
    if images[1].get_data_dtype() != np.dtype("uint8"):
        raise ValueError("Support NIfTI must use uint8")
    field, support, affine = _measurement(
        images[0].get_fdata(), images[1].get_fdata(), manifest["affine"]
    )
    if list(field.shape) != manifest.get("shape"):
        raise ValueError("Map shape disagrees with manifest")
    context = manifest["context"]
    if context.get("kind") == "analytical":
        source_context(
            {
                "shim_analytical_model": context["model"],
                "shim_native_settings": context["baseline"],
            }
        )
    elif context.get("kind") == "measured":
        if not {"calibration.json", "profiles.nii"} <= loaded.keys():
            raise ValueError("Measured snapshot is incomplete")
        if (
            json.loads(loaded["calibration.json"]) != context["model"]
            or context["model"].get("coil_profiles") != "profiles.nii"
        ):
            raise ValueError("Measured snapshot model mismatch")
        source_context(
            {
                "shim_calibration": directory / "calibration.json",
                "shim_current_a": context["baseline"],
            }
        )
    elif context != {
        "kind": "unknown",
        "model": None,
        "baseline": None,
        "provenance": "source acquisition settings not supplied",
    }:
        raise ValueError("Invalid acquisition context")
    identity = manifest.get("identity")
    if not isinstance(identity, dict) or any(
        not isinstance(v, str) for v in identity.values()
    ):
        raise ValueError("Invalid source identity")
    return SavedMap(
        name,
        field,
        support,
        affine,
        context,
        identity,
        directory,
        hashlib.sha256(raw).hexdigest(),
    )


def read_map(selected_id, *, store=None):
    return _read_bundle(_store(store) / _id(selected_id), selected_id)


def _resolve_context(saved, settings):
    later = source_context(settings or {})
    known = saved.context
    if later["kind"] == "unknown":
        return known, saved.path / "calibration.json"
    if known["kind"] != "unknown":
        if known["kind"] != later["kind"] or known["baseline"] != later["baseline"]:
            raise ValueError(
                "Explicit source acquisition baseline conflicts with selected map"
            )
        if known["kind"] == "analytical" and known["model"] != later["model"]:
            raise ValueError(
                "Explicit source analytical model conflicts with selected map"
            )
        if (
            known["kind"] == "measured"
            and known["model"]["channels"] != later["model"]["channels"]
        ):
            raise ValueError(
                "Explicit source calibration channels conflict with selected map"
            )
    return {**later, "provenance": "operator supplied for selected source map"}, (
        settings or {}
    ).get("shim_calibration")


def evaluate_target(saved, target, source_inputs=None):
    for key in ("StudyInstanceUID", "FrameOfReferenceUID"):
        if (
            saved.identity.get(key)
            and target.identity.get(key)
            and saved.identity[key] != target.identity[key]
        ):
            raise ValueError(f"Selected map and target have different {key}")
    transform = np.linalg.solve(saved.affine, target.affine)
    coords = (
        transform[:3, :3] @ np.indices(target.magnitude.shape).reshape(3, -1)
        + transform[:3, 3, None]
    )
    # Snap roundoff only at lattice coordinates; exact boundary centres remain valid.
    near = np.abs(coords - np.rint(coords)) < 1e-10
    coords[near] = np.rint(coords[near])
    field = map_coordinates(
        saved.field_hz, coords, order=1, mode="constant", cval=0, prefilter=False
    ).reshape(target.magnitude.shape)
    support = (
        map_coordinates(
            saved.support.astype(float),
            coords,
            order=1,
            mode="constant",
            cval=0,
            prefilter=False,
        ).reshape(target.magnitude.shape)
        >= 1 - 1e-12
    )
    field = np.where(support, field, 0)
    requested = np.isfinite(target.magnitude) & (target.magnitude > 0)
    roi = requested & support
    if not roi.any():
        raise ValueError(
            "Selected field map has no supported positive target magnitude overlap"
        )
    context, calibration = _resolve_context(saved, source_inputs)
    reason = "source acquisition settings unavailable"
    shim = None
    if context["kind"] == "measured":
        image = nib.load(
            calibration.parent / context["model"]["coil_profiles"]
            if isinstance(calibration, Path)
            else Path(calibration).parent / context["model"]["coil_profiles"]
        )
        if image.shape[:3] != field.shape or not np.allclose(
            image.affine, target.affine, rtol=0, atol=1e-4
        ):
            reason = "unsupported calibration geometry: profiles must match target grid exactly"
        else:
            try:
                shim = compute_shim(
                    field, roi, target.affine, calibration, context["baseline"]
                )
            except ValueError as exc:
                if (
                    str(exc)
                    != "Coil profiles must have independent spatial effects in the ROI"
                ):
                    raise
                reason = "rank-unidentifiable target ROI"
    elif context["kind"] == "analytical":
        try:
            shim = compute_shim(
                field,
                roi,
                target.affine,
                analytical_model_path=context["model"],
                acquisition_native=context["baseline"],
            )
        except ValueError as exc:
            if (
                str(exc)
                != "Free native channels lack independent spatial effects in ROI"
            ):
                raise
            reason = "rank-unidentifiable target ROI"
    if shim is None:
        shim = ShimResult("unavailable", "shim unavailable: " + reason)
    pairing = (
        "matched study/frame identifiers"
        if all(
            saved.identity.get(k) and target.identity.get(k)
            for k in ("StudyInstanceUID", "FrameOfReferenceUID")
        )
        else "operator map pairing; study/frame identifiers incomplete; motion not checked"
    )
    coverage = f"supported ROI {int(roi.sum())}/{int(requested.sum())} ({100 * roi.sum() / requested.sum():.1f}%); source support {int(support.sum())}/{support.size}"
    comment = f'{shim.comment}; B0MapId={saved.id}; {coverage}; {context["provenance"]}; {pairing}; source digest={saved.digest}'
    return TargetEvaluation(field, support, roi, shim, comment)
