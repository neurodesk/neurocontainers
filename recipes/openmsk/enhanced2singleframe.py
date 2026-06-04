#!/usr/bin/env python3
"""Split Siemens qDESS *Enhanced* (multi-frame) MR DICOM into classic
single-frame MR Image Storage DICOMs.

DOSMA's ``QDess.from_dicom`` (used by the KneePipeline segmentation/T2 steps)
expects classic single-frame DICOM with top-level ``EchoTime`` and geometry
tags. Siemens qDESS often exports as two Enhanced multi-frame files (one per
echo, N frames each), which DOSMA cannot parse directly. This utility expands
each frame into a single-frame DICOM, lifting the per-frame functional-group
geometry (position/orientation/spacing) and echo timing to the top level so the
direct-DICOM pipeline works:

    python enhanced2singleframe.py /path/to/qDESS_enhanced_dir /path/to/out_dir
    python /opt/KneePipeline/run_pipeline.py /path/to/out_dir /path/to/results --config /opt/KneePipeline/config.json

Notes
-----
* qDESS **T2 mapping** additionally requires the Siemens private tags
  ``GL_AREA (0019,10B6)`` and ``TG (0019,10B7)``. These are propagated when
  present; if the source export stripped them, DOSMA skips T2 with a warning
  (segmentation + thickness still run from the qDESS RSS).
"""
import argparse
import os

import pydicom
from pydicom.dataset import FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage, generate_uid

# Siemens qDESS private tags required for T2 (propagated if present).
GL_AREA_TAG = 0x001910B6
TG_TAG = 0x001910B7


def _fg_item(ds, frame, name):
    """First item of functional-group sequence `name` (per-frame, then shared)."""
    for src in (
        getattr(ds, "PerFrameFunctionalGroupsSequence", [None] * (frame + 1))[frame]
        if "PerFrameFunctionalGroupsSequence" in ds
        else None,
        ds.SharedFunctionalGroupsSequence[0]
        if "SharedFunctionalGroupsSequence" in ds
        else None,
    ):
        if src is not None and name in src:
            seq = src[name].value
            if len(seq):
                return seq[0]
    return None


def split_file(path, out_dir, echo_index, series_uid):
    ds = pydicom.dcmread(path)
    n_frames = int(getattr(ds, "NumberOfFrames", 1))
    arr = ds.pixel_array
    if arr.ndim == 2:
        arr = arr[None]

    drop = {
        "NumberOfFrames",
        "PerFrameFunctionalGroupsSequence",
        "SharedFunctionalGroupsSequence",
        "DimensionOrganizationSequence",
        "DimensionIndexSequence",
    }
    written = 0
    for i in range(n_frames):
        sf = pydicom.Dataset()
        for elem in ds:
            if elem.tag == 0x7FE00010 or elem.keyword in drop:
                continue
            sf.add(elem)

        pp = _fg_item(ds, i, "PlanePositionSequence")
        po = _fg_item(ds, i, "PlaneOrientationSequence")
        pm = _fg_item(ds, i, "PixelMeasuresSequence")
        fc = _fg_item(ds, i, "FrameContentSequence")
        ec = _fg_item(ds, i, "MREchoSequence")
        tm = _fg_item(ds, i, "MRTimingAndRelatedParametersSequence")

        if pp is not None and "ImagePositionPatient" in pp:
            sf.ImagePositionPatient = list(pp.ImagePositionPatient)
        if po is not None and "ImageOrientationPatient" in po:
            sf.ImageOrientationPatient = list(po.ImageOrientationPatient)
        if pm is not None:
            if "PixelSpacing" in pm:
                sf.PixelSpacing = list(pm.PixelSpacing)
            if "SliceThickness" in pm:
                sf.SliceThickness = pm.SliceThickness
            if "SpacingBetweenSlices" in pm:
                sf.SpacingBetweenSlices = pm.SpacingBetweenSlices
        if tm is not None:
            if "RepetitionTime" in tm:
                sf.RepetitionTime = tm.RepetitionTime
            if "FlipAngle" in tm:
                sf.FlipAngle = tm.FlipAngle

        # Echo time: per-frame effective echo time if present, else synthesize a
        # distinct value per echo so DOSMA groups the two volumes correctly.
        te = None
        if ec is not None and "EffectiveEchoTime" in ec:
            te = float(ec.EffectiveEchoTime)
        if te is None:
            te = 5.0 + 10.0 * echo_index
        sf.EchoTime = te
        sf.EchoNumbers = echo_index + 1

        in_stack = None
        if fc is not None and "InStackPositionNumber" in fc:
            in_stack = int(fc.InStackPositionNumber)
        slice_no = in_stack if in_stack is not None else (i + 1)
        sf.InstanceNumber = echo_index * n_frames + slice_no
        if "ImagePositionPatient" in sf:
            sf.SliceLocation = float(sf.ImagePositionPatient[2])

        # Propagate qDESS private tags needed for T2 if present in the source.
        for tag in (GL_AREA_TAG, TG_TAG):
            if tag in ds:
                sf[tag] = ds[tag]

        sf.SOPClassUID = MRImageStorage
        sf.SOPInstanceUID = generate_uid()
        sf.SeriesInstanceUID = series_uid
        sf.SeriesNumber = int(getattr(ds, "SeriesNumber", 1))
        if "ImageType" not in sf:
            sf.ImageType = ["ORIGINAL", "PRIMARY", "M", "NONE"]

        sf.Rows = int(ds.Rows)
        sf.Columns = int(ds.Columns)
        sf.SamplesPerPixel = int(getattr(ds, "SamplesPerPixel", 1))
        sf.PhotometricInterpretation = getattr(ds, "PhotometricInterpretation", "MONOCHROME2")
        sf.BitsAllocated = int(ds.BitsAllocated)
        sf.BitsStored = int(ds.BitsStored)
        sf.HighBit = int(ds.HighBit)
        sf.PixelRepresentation = int(ds.PixelRepresentation)
        sf.PixelData = arr[i].astype(arr.dtype).tobytes()

        fm = FileMetaDataset()
        fm.MediaStorageSOPClassUID = MRImageStorage
        fm.MediaStorageSOPInstanceUID = sf.SOPInstanceUID
        fm.TransferSyntaxUID = ExplicitVRLittleEndian
        sf.file_meta = fm
        sf.is_little_endian = True
        sf.is_implicit_VR = False

        sf.save_as(
            os.path.join(out_dir, "e%d_%03d.dcm" % (echo_index + 1, slice_no)),
            write_like_original=False,
        )
        written += 1
    return written


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input_dir", help="Folder of Siemens qDESS Enhanced (multi-frame) DICOMs")
    ap.add_argument("output_dir", help="Folder to write single-frame DICOMs into")
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.lower().endswith((".dcm", ".ima"))
    ]
    # Order echoes by InstanceNumber (echo 1 then echo 2 ...).
    files.sort(key=lambda p: int(getattr(pydicom.dcmread(p, stop_before_pixels=True), "InstanceNumber", 0)))
    if not files:
        raise SystemExit("No DICOM files found in %s" % args.input_dir)

    series_uid = generate_uid()
    total = 0
    for echo_index, path in enumerate(files):
        n = split_file(path, args.output_dir, echo_index, series_uid)
        print("echo %d  %s  ->  %d single-frame DICOMs" % (echo_index + 1, os.path.basename(path), n))
        total += n
    print("Wrote %d single-frame DICOMs to %s" % (total, args.output_dir))


if __name__ == "__main__":
    main()
