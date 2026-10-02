#!/usr/bin/env python3
"""Offline OpenRecon simulation helpers for the frisgo container.

nifti2mrd  converts a 4D NIfTI time series into scanner-like MRD images
           (one uint16 2D image per slice and repetition).
mrd2nifti  converts every returned image series of an MRD file back into a
           4D NIfTI, using only the returned slice and repetition counters.
compare    compares a returned NIfTI with a reference NIfTI.
"""
import argparse
import os
import sys
import uuid

import ismrmrd
import ismrmrd.xsd
import nibabel as nib
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from frisgo import nifti_affine  # noqa: E402


def nifti2mrd(args):
    nifti = nib.load(args.input)
    data = np.asarray(nifti.dataobj)
    if data.ndim != 4:
        raise SystemExit(f"expected a 4D NIfTI, got shape {data.shape}")
    if data.min() < 0 or data.max() > np.iinfo(np.uint16).max:
        raise SystemExit("input values do not fit scanner uint16 magnitude images")
    n_x, n_y, n_z, n_t = data.shape
    d_x, d_y, d_z = (float(v) for v in nifti.header.get_zooms()[:3])
    tr_ms = float(nifti.header.get_zooms()[3]) * 1000.0
    if nifti.header.get_xyzt_units()[1] == "msec":
        tr_ms /= 1000.0

    ras_to_lps = np.diag([-1.0, -1.0, 1.0])
    columns = ras_to_lps @ nifti.affine[:3, :3]
    read_dir = columns[:, 0] / np.linalg.norm(columns[:, 0])
    phase_dir = columns[:, 1] / np.linalg.norm(columns[:, 1])
    slice_dir = columns[:, 2] / np.linalg.norm(columns[:, 2])
    name = args.series_description or os.path.basename(args.input).split(".")[0]
    seed = f"frisgo-simulation|{os.path.abspath(args.input)}"
    series_uid = f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"

    if os.path.exists(args.output):
        os.remove(args.output)
    dataset = ismrmrd.Dataset(args.output, "dataset", create_if_needed=True)
    header = ismrmrd.xsd.ismrmrdHeader()
    header.measurementInformation = ismrmrd.xsd.measurementInformationType(
        protocolName=name, patientPosition="HFS"
    )
    header.acquisitionSystemInformation = ismrmrd.xsd.acquisitionSystemInformationType(
        systemVendor="SIMULATION", systemModel="frisgo_simulate"
    )
    header.experimentalConditions = ismrmrd.xsd.experimentalConditionsType(
        H1resonanceFrequency_Hz=297000000
    )
    space = ismrmrd.xsd.encodingSpaceType(
        matrixSize=ismrmrd.xsd.matrixSizeType(x=n_x, y=n_y, z=n_z),
        fieldOfView_mm=ismrmrd.xsd.fieldOfViewMm(x=n_x * d_x, y=n_y * d_y, z=n_z * d_z),
    )
    limits = ismrmrd.xsd.encodingLimitsType(
        slice=ismrmrd.xsd.limitType(minimum=0, maximum=n_z - 1, center=n_z // 2),
        repetition=ismrmrd.xsd.limitType(minimum=0, maximum=n_t - 1, center=0),
    )
    header.encoding.append(
        ismrmrd.xsd.encodingType(
            encodedSpace=space,
            reconSpace=space,
            encodingLimits=limits,
            trajectory=ismrmrd.xsd.trajectoryType.CARTESIAN,
        )
    )
    header.sequenceParameters = ismrmrd.xsd.sequenceParametersType(TR=[tr_ms])
    dataset.write_xml_header(header.toXML("utf-8"))

    for t in range(n_t):
        for z in range(n_z):
            plane = np.ascontiguousarray(data[:, :, z, t].T).astype(np.uint16)
            image = ismrmrd.Image.from_array(plane, transpose=False)
            image.image_type = ismrmrd.IMTYPE_MAGNITUDE
            image.image_series_index = 1
            image.image_index = t * n_z + z + 1
            image.slice = z
            image.repetition = t
            image.acquisition_time_stamp = int(round(t * tr_ms / 2.5))
            image.field_of_view[:] = (n_x * d_x, n_y * d_y, d_z)
            centre_voxel = nifti.affine @ np.array([(n_x - 1) / 2, (n_y - 1) / 2, z, 1.0])
            image.position[:] = ras_to_lps @ centre_voxel[:3]
            image.read_dir[:] = read_dir
            image.phase_dir[:] = phase_dir
            image.slice_dir[:] = slice_dir

            meta = ismrmrd.Meta()
            for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
                meta[key] = name
            meta["DataRole"] = "Image"
            meta["ImageType"] = "ORIGINAL\\PRIMARY\\M\\ND"
            meta["ImageTypeValue3"] = "M"
            meta["ImageProcessingHistory"] = ["NIFTI_SIMULATION"]
            meta["RepetitionTime"] = f"{tr_ms:g}"
            meta["PixelSpacing"] = [f"{d_x:g}", f"{d_y:g}"]
            meta["SliceThickness"] = f"{d_z:g}"
            meta["SeriesInstanceUID"] = series_uid
            meta["SOPInstanceUID"] = f"{series_uid}.{t * n_z + z + 1}"
            meta["Keep_image_geometry"] = "1"
            image.attribute_string = meta.serialize()
            dataset.append_image("image_0", image)
    dataset.close()
    print(f"Wrote {n_t * n_z} MRD image(s) ({n_t} repetitions x {n_z} slices) to {args.output}")


def mrd2nifti(args):
    dataset = ismrmrd.Dataset(args.input, args.group, create_if_needed=False)
    os.makedirs(args.output_dir, exist_ok=True)
    groups = sorted(
        name for name in dataset.list() if name.startswith("image_")
    )
    for group in groups:
        images = [dataset.read_image(group, i) for i in range(dataset.number_of_images(group))]
        meta = ismrmrd.Meta.deserialize(images[0].attribute_string)
        slices = sorted({int(image.slice) for image in images})
        repetitions = sorted({int(image.repetition) for image in images})
        n_y, n_x = np.asarray(images[0].data).shape[-2:]
        volume = np.zeros((n_x, n_y, len(slices), len(repetitions)), np.asarray(images[0].data).dtype)
        filled = np.zeros(volume.shape[2:], bool)
        for image in images:
            z = slices.index(int(image.slice))
            t = repetitions.index(int(image.repetition))
            if filled[z, t]:
                raise SystemExit(f"{group}: duplicate slice {z} repetition {t}")
            volume[:, :, z, t] = np.asarray(image.data).reshape(n_y, n_x).T
            filled[z, t] = True
        if not filled.all():
            raise SystemExit(f"{group}: {np.count_nonzero(~filled)} image(s) missing")
        first_volume = sorted(
            (image for image in images if int(image.repetition) == repetitions[0]),
            key=lambda image: int(image.slice),
        )
        nifti = nib.Nifti1Image(volume, nifti_affine(first_volume))
        name = meta.get("SeriesDescription", group)
        path = os.path.join(args.output_dir, f"{args.prefix}{group}.nii")
        nib.save(nifti, path)
        print(
            f"{group}: series_index={int(images[0].image_series_index)} "
            f"SeriesDescription={name!r} ImageType={meta.get('ImageType')!r} "
            f"images={len(images)} shape={volume.shape} dtype={volume.dtype} -> {path}"
        )


def compare(args):
    returned = np.asarray(nib.load(args.returned).dataobj, dtype=np.float64)
    reference = np.asarray(nib.load(args.reference).dataobj, dtype=np.float64)
    if returned.shape != reference.shape:
        raise SystemExit(f"shape mismatch: {returned.shape} vs {reference.shape}")
    expected = np.clip(np.rint(reference), 0, np.iinfo(np.uint16).max)
    difference = np.abs(returned - reference)
    in_range = reference >= 0
    exact = np.array_equal(returned, expected)
    print(f"returned  : {args.returned}")
    print(f"reference : {args.reference}")
    print(f"shape {returned.shape}, voxels {returned.size}")
    print(f"max |returned - reference|              : {difference.max():.4f}")
    print(f"max |returned - reference| (ref >= 0)   : {difference[in_range].max():.4f}")
    print(f"mean |returned - reference|             : {difference.mean():.6f}")
    print(f"reference voxels < 0 (clipped to 0)     : {np.count_nonzero(~in_range)}")
    print(f"correlation                             : {np.corrcoef(returned.ravel(), reference.ravel())[0, 1]:.9f}")
    print(f"identical to uint16(round(clip(reference))): {exact}")
    if not exact:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    to_mrd = commands.add_parser("nifti2mrd")
    to_mrd.add_argument("input")
    to_mrd.add_argument("output")
    to_mrd.add_argument("--series-description")
    to_mrd.set_defaults(handler=nifti2mrd)

    to_nifti = commands.add_parser("mrd2nifti")
    to_nifti.add_argument("input")
    to_nifti.add_argument("output_dir")
    to_nifti.add_argument("--group", default="dataset")
    to_nifti.add_argument("--prefix", default="")
    to_nifti.set_defaults(handler=mrd2nifti)

    check = commands.add_parser("compare")
    check.add_argument("returned")
    check.add_argument("reference")
    check.set_defaults(handler=compare)

    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
