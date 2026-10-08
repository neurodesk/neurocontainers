import copy

import ismrmrd
import numpy as np
import openreconi2iexample as helpers
from b0_geometry import _image_axes
from b0mapromeo_analytic import AnalyticalShimResult
from b0mapromeo_shim import ShimResult, unavailable


def output_images(
    field: np.ndarray,
    anchors: list[ismrmrd.Image],
    series_index: int,
    shim_result: ShimResult | AnalyticalShimResult | None = None,
    *,
    support=None,
    name="ROMEO B0 Hz",
    comment=None,
    map_id=None,
) -> list[ismrmrd.Image]:
    """Return unsigned scanner pixels with a reversible Hz rescale."""
    shim_result = shim_result or unavailable()
    comment = comment or shim_result.comment
    if map_id and f"B0MapId={map_id}" not in comment:
        comment += f"; B0MapId={map_id}"
    if support is not None:
        field = np.where(support, field, 0)
    product = {
        "ROMEO B0 Hz": ("B0MAP_ROMEO", ["ROMEO", "B0_HZ"]),
        "Target B0 Hz": ("B0MAP_TARGET", ["TARGET", "B0_HZ"]),
        "Predicted B0 Hz": ("B0MAP_PREDICTED", ["PREDICTED", "B0_HZ"]),
    }[name]
    maximum = float(np.max(np.abs(field)))
    scale = min(1.0, 2046.0 / maximum) if maximum else 1.0
    display = np.clip(np.rint(field * scale + 2048), 1, 4095).astype(np.uint16)
    if support is not None:
        display = np.where(support, display, 0).astype(np.uint16)
    identity = helpers._build_output_series_identity_from_name(
        anchors[0], series_index, name
    )
    outputs = []
    for z, source in enumerate(anchors):
        output = ismrmrd.Image.from_array(
            display[:, :, z].T[None, None], transpose=False
        )
        header = copy.deepcopy(source.getHead())
        header.data_type = output.data_type
        output.setHead(header)
        helpers._stamp_output_image(
            output,
            source,
            series_index,
            z,
            name,
            ["Image", "Quantitative"],
            product[0],
            product[1],
            series_identity=identity,
            extra_meta={
                "ImageComment": comment,
                "ImageComments": comment,
                "B0ShimStatus": shim_result.status,
                "RescaleSlope": str(1 / scale),
                "RescaleIntercept": str(-2048 / scale),
                "RescaleType": "Hz",
                "WindowCenter": "0",
                "WindowWidth": "400",
                "B0MapUnits": "Hz",
                **({"B0MapId": map_id} if map_id else {}),
                **(
                    {"PixelPaddingValue": "0", "PixelPaddingRangeLimit": "0"}
                    if support is not None
                    else {}
                ),
                "B0MapDisplayFormula": f"Hz = (stored - 2048) / {scale}",
                "NumberOfSlices": str(len(anchors)),
                "ImagesInAcquisition": str(len(anchors)),
                "slice_count": str(len(anchors)),
                "NumberInSeries": str(z + 1),
            },
        )
        source_meta = ismrmrd.Meta.deserialize(source.attribute_string)
        if "ImageRowDir" in source_meta or "ImageColumnDir" in source_meta:
            axes = _image_axes(source)
            meta = ismrmrd.Meta.deserialize(output.attribute_string)
            meta["ImageSliceDir"] = axes[:, 2].tolist()
            meta["ImageSliceNormDir"] = axes[:, 2].tolist()
            output.attribute_string = meta.serialize()
        outputs.append(output)
    return outputs
