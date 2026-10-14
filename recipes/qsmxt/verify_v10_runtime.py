import argparse
import ctypes
import errno
import json
import platform
import socket
import subprocess
import tempfile
from pathlib import Path

import nibabel as nib
import numpy as np
from scipy.ndimage import shift


def run(binary, arguments, succeeds=True):
    result = subprocess.run([binary, *map(str, arguments)], text=True, capture_output=True)
    if (result.returncode == 0) != succeeds:
        raise AssertionError(f"Native command returned {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result.stdout + result.stderr


def fixture(root, spacing=(1, 1, 1), displaced=False, thickness=None):
    bids = root / "bids"
    anat = bids / "sub-01" / "anat"
    anat.mkdir(parents=True)
    (bids / "dataset_description.json").write_text(json.dumps({
        "Name": "QSMxT native feature fixture", "BIDSVersion": "1.9.0",
    }))
    shape = (32, 32, 24)
    xyz = np.indices(shape) - np.array([15.5, 15.5, 11.5])[:, None, None, None]
    radius = np.sum((xyz / np.array([13, 12, 10])[:, None, None, None]) ** 2, axis=0)
    brain = radius < 1
    chi = np.where(np.sum(xyz ** 2, axis=0) < 20, 0.12, 0.01) * brain
    frequencies = np.meshgrid(*(np.fft.fftfreq(n, d=s) for n, s in zip(shape, spacing)), indexing="ij")
    k2 = sum(axis ** 2 for axis in frequencies)
    dipole = 1 / 3 - np.divide(frequencies[2] ** 2, k2, out=np.zeros(shape), where=k2 > 0)
    dipole[0, 0, 0] = 0
    field = np.fft.ifftn(np.fft.fftn(chi) * dipole).real
    affine = np.diag([*spacing, 1.0])
    texture = 1000 * (1 + 0.25 * np.sin(xyz[0] * 0.7) + 0.15 * np.cos(xyz[1] * 0.5))
    for echo, te in enumerate([0.006, 0.012, 0.018], 1):
        magnitude = brain * texture * np.exp(-30 * te)
        phase = np.angle(np.exp(1j * field * 2 * np.pi * 42.57747892 * 3 * te))
        phase[13:17, 13:17, 9:13] += np.random.default_rng(echo).uniform(-np.pi, np.pi, (4, 4, 4))
        if displaced and echo > 1:
            complex_signal = shift(magnitude * np.exp(1j * phase), (echo - 1, 0, 0), order=1)
            magnitude, phase = np.abs(complex_signal), np.angle(complex_signal)
        for part, data in [("mag", magnitude), ("phase", (phase + np.pi) * 4096 / (2 * np.pi))]:
            stem = f"sub-01_echo-{echo}_part-{part}_MEGRE"
            nib.save(nib.Nifti1Image(data.astype(np.float32), affine), anat / f"{stem}.nii.gz")
            sidecar = {"EchoTime": te, "MagneticFieldStrength": 3.0, "B0_dir": [0, 0, 1]}
            if thickness is not None:
                sidecar.update(SliceThickness=thickness, MRAcquisitionType="2D", SliceEncodingDirection="k")
            (anat / f"{stem}.json").write_text(json.dumps(sidecar))
    return bids, brain, affine


def reconstruct(binary, bids, output, *options, succeeds=True):
    command = ["run", bids, output, "--force", "--n-procs", "2",
               "--unwrapping-algorithm", "romeo", "--bf-algorithm", "vsharp",
               "--mask", "magnitude,threshold:otsu,close:1,fill-holes:0",
               "--no-inhomogeneity-correction"]
    if "--qsm-algorithm" not in options:
        command.extend(["--qsm-algorithm", "tkd"])
    if "--qsm-reference" not in options:
        command.extend(["--qsm-reference", "none"])
    return run(binary, [*command, *options], succeeds)


def primary(output):
    paths = [p for p in output.rglob("*_Chimap.nii*") if "_desc-" not in p.name]
    assert len(paths) == 1, paths
    image = nib.load(paths[0])
    data = image.get_fdata()
    assert np.isfinite(data).all() and np.any(data != 0), paths[0]
    return data, image


def verify(binary, root):
    bids, brain, affine = fixture(root / "ordinary")
    reconstruct(binary, bids, root / "ilsqr", "--qsm-algorithm", "ilsqr")
    primary(root / "ilsqr")
    print("iLSQR produced a finite nonzero susceptibility map")
    reconstruct(binary, bids, root / "two-pass", "--two-pass")
    result, _ = primary(root / "two-pass")
    single = list((root / "two-pass").rglob("*_desc-singlepass_Chimap.nii*"))
    reliable = list((root / "two-pass").rglob("*_desc-reliable_mask.nii*"))
    assert len(single) == len(reliable) == 1
    mask = nib.load(reliable[0]).get_fdata() > 0
    assert mask.any() and np.any(brain & ~mask)
    assert np.any(result[brain & ~mask] != 0), "Ordinary pass must fill unreliable regions"
    single_data = nib.load(single[0]).get_fdata()
    np.testing.assert_allclose(result[~mask], single_data[~mask], atol=1e-6)
    print("Two-pass produced reliable-mask and single-pass derivatives and filled unreliable regions")
    moved, _, _ = fixture(root / "displaced", displaced=True)
    reconstruct(binary, moved, root / "motion", "--motion-correction")
    primary(root / "motion")
    reports = [json.loads(p.read_text()) for p in (root / "motion").rglob("*.json")]
    reports = [r for r in reports if "max_displacement_mm" in r and "echoes" in r]
    assert len(reports) == 1 and reports[0]["max_displacement_mm"] > 0.5, reports
    for echo, expected in zip(reports[0]["echoes"], [0, 1, 2]):
        assert abs(echo["translation_mm"] - expected) < 0.1, echo
        assert echo["rotation_deg"] < 0.1 and echo["ncc"] > 0.99, echo
    print("Motion measurements", json.dumps(reports[0]["echoes"]))
    coverage = list((root / "motion").rglob("*_desc-motioncoverage_mask.nii*"))
    assert len(coverage) == 1 and nib.load(coverage[0]).get_fdata().any()
    print("Motion correction estimated nonidentity transforms for displaced echoes")
    anisotropic, _, _ = fixture(root / "anisotropic", spacing=(1, 1, 2), thickness=2)
    reconstruct(binary, anisotropic, root / "slice", "--vsharp-grid", "finest", "--do-swi", "--swi-unwrapping", "laplacian-slice")
    _, image = primary(root / "slice")
    np.testing.assert_allclose(image.header.get_zooms(), (1, 1, 2))
    swi = list((root / "slice").rglob("*_swi.nii*"))
    assert len(swi) == 1 and np.isfinite(nib.load(swi[0]).get_fdata()).all()
    print("Anisotropic V-SHARP and slice Laplacian SWI preserved acquired grid")
    gapped, _, _ = fixture(root / "gapped", spacing=(1, 1, 3), thickness=2)
    log = reconstruct(binary, gapped, root / "gap-refused", succeeds=False)
    assert "--ignore-slice-gap" in log and not list((root / "gap-refused").rglob("*_Chimap.nii*")), log
    print("Native QSM refused slice gaps before reconstruction")
    reconstruct(binary, bids, root / "unreferenced")
    raw, image = primary(root / "unreferenced")
    dseg = np.zeros(brain.shape, dtype=np.int16)
    dseg[brain & (np.indices(brain.shape)[0] < 16)] = 4
    dseg[brain & (np.indices(brain.shape)[0] >= 16)] = 43
    custom = bids / "derivatives" / "fixture" / "sub-01" / "anat"
    custom.mkdir(parents=True)
    nib.save(nib.Nifti1Image(dseg, affine), custom / "sub-01_dseg.nii.gz")
    for region in ("ventricles", "4,43", "left-lateral-ventricle"):
        output = root / f"reference-{region}"
        reconstruct(binary, bids, output, "--qsm-reference", region, "--use-custom-dseg", "fixture")
        referenced, _ = primary(output)
        roi = (dseg == 4) if region == "left-lateral-ventricle" else dseg > 0
        support = raw != 0
        published = list((root / "unreferenced").rglob("*_desc-qsm_mask.nii*"))
        assert len(published) == 1, published
        assert np.all(nib.load(published[0]).get_fdata()[support] > 0)
        offset = raw[roi & support].mean()
        expected = np.where(support, raw - offset, 0)
        np.testing.assert_allclose(referenced, expected, atol=1e-6)
    print("Native named ventricular and numeric union references subtract the actual ROI mean")
    log = reconstruct(binary, bids, root / "bad-region", "--qsm-reference", "not-a-region", succeeds=False)
    assert "not-a-region" in log
    nib.save(nib.Nifti1Image(np.full(brain.shape, 2, dtype=np.int16), affine), custom / "sub-01_dseg.nii.gz")
    log = reconstruct(binary, bids, root / "empty-region", "--qsm-reference", "4",
                      "--use-custom-dseg", "fixture", succeeds=False)
    assert "no voxels" in log, log


def verify_segmentation(binary, template, root):
    deny_internet_sockets()
    output = root / "synthseg.nii.gz"
    lookup = root / "synthseg.tsv"
    run(binary, ["segment", template, "--output", output, "--lookup", lookup,
                 "--synthseg-crop", "128", "--no-synthseg-flip-averaging"])
    labels = nib.load(output).get_fdata()
    expected = {0, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 26, 28, 41, 42, 43, 44, 46, 47, 49, 50, 51, 52, 53, 54, 58, 60}
    assert set(np.unique(labels)) == expected
    assert labels.shape == nib.load(template).shape
    np.testing.assert_allclose(nib.load(output).affine, nib.load(template).affine)
    table = lookup.read_text()
    assert "left lateral ventricle" in table and "right lateral ventricle" in table, table
    assert np.isin(labels, [4, 5, 14, 15, 43, 44]).sum() > 50
    print("Packaged offline SynthSeg v1 produced all 32 labels including ventricles on MNI anatomy")


def deny_internet_sockets():
    assert platform.system() == "Linux" and platform.machine() == "x86_64"

    class Filter(ctypes.Structure):
        _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte),
                    ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint)]

    class Program(ctypes.Structure):
        _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]

    instructions = (Filter * 10)(
        Filter(0x20, 0, 0, 4), Filter(0x15, 1, 0, 0xc000003e),
        Filter(0x06, 0, 0, 0x80000000), Filter(0x20, 0, 0, 0),
        Filter(0x15, 0, 4, 41), Filter(0x20, 0, 0, 16),
        Filter(0x15, 1, 0, socket.AF_INET), Filter(0x15, 0, 1, socket.AF_INET6),
        Filter(0x06, 0, 0, 0x00050000 | errno.ENETUNREACH),
        Filter(0x06, 0, 0, 0x7fff0000),
    )
    libc = ctypes.CDLL(None, use_errno=True)
    program = Program(len(instructions), instructions)
    if libc.prctl(38, 1, 0, 0, 0) or libc.prctl(22, 2, ctypes.byref(program), 0, 0):
        raise OSError(ctypes.get_errno(), "Cannot install Internet socket restriction")
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.socket(family, socket.SOCK_STREAM)
        except OSError as error:
            assert error.errno == errno.ENETUNREACH
        else:
            raise AssertionError("Internet socket restriction failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="/opt/qsmxt/qsmxt")
    parser.add_argument("--template", default="/opt/qsmxt/test-data/MNI152_T1_2mm.nii.gz")
    parser.add_argument("--segmentation", action="store_true")
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        if arguments.segmentation:
            verify_segmentation(arguments.binary, arguments.template, root)
        else:
            verify(arguments.binary, root)
