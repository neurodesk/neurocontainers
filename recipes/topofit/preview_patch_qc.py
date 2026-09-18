"""Render a ranked cortical-patch contact sheet from actual run geometry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import nibabel as nib
import numpy as np
from PIL import Image, ImageDraw, ImageFont

from topofit_geometry import triangle_voxel_mask


def render_preview(input_path: Path, run_dir: Path, output_path: Path) -> None:
    image = nib.as_closest_canonical(nib.load(input_path))
    data = np.asarray(image.dataobj, dtype=float)
    lo, hi = np.percentile(data[data > 0], (1, 99.5))
    gray = np.uint8(np.clip((data - lo) / max(hi - lo, 1), 0, 1) * 210)
    manifest = json.loads((run_dir / "topofit_manifest.json").read_text())
    patches = manifest["flat_patches"]
    geometry = np.load(run_dir / "topofit_patch_geometry.npz")
    rows = max(1, (len(patches) + 2) // 3)
    canvas = Image.new("RGB", (1500, 125 + rows * 520), (12, 15, 21))
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default(size=18)
    small = ImageFont.load_default(size=15)
    draw.text((20, 12), f"TopoFit: {len(patches)} distinct cortical patches at 50% depth", font=font)
    draw.text((20, 42), "Green: white boundary. Red: pial boundary. Yellow: mid-cortex. Cyan: projected normal, 12 mm.", font=font)
    draw.text((20, 72), "Actual scan, source-grid views with cortex zooms. Research candidates, not matched controls or prescription coordinates.", font=font)
    inverse = np.linalg.inv(image.affine)
    masks = {name: np.zeros(image.shape, dtype=bool) for name in ("white", "pial", "mid")}
    centers = {}
    for patch_id, patch in patches.items():
        centers[patch_id] = nib.affines.apply_affine(inverse, patch["center_ras_mm"])
        for boundary in masks:
            masks[boundary] |= triangle_voxel_mask(
                image.shape, image.affine, geometry[f"{patch_id}_{boundary}_ras_mm"],
                geometry[f"{patch_id}_faces"],
            )
    if not patches:
        draw.text((30, 160), "No cortical patches passed the quality criteria.", font=font)
    for number, (patch_id, patch) in enumerate(patches.items()):
        center = centers[patch_id]
        normal = np.asarray(patch["normal_ras"])
        voxel_normal = inverse[:3, :3] @ normal
        axis = int(np.argmin(abs(voxel_normal / np.linalg.norm(voxel_normal))))
        remaining = [a for a in range(3) if a != axis]
        index = int(np.clip(round(center[axis]), 0, image.shape[axis] - 1))
        plane = np.repeat(np.take(gray, index, axis=axis)[..., None], 3, axis=-1)
        for boundary, color in (("white", (40, 230, 100)), ("pial", (255, 80, 80)), ("mid", (255, 225, 40))):
            plane[np.take(masks[boundary], index, axis=axis)] = color
        plane = Image.fromarray(np.rot90(plane))
        spacing = nib.affines.voxel_sizes(image.affine)[remaining]
        scale = min(470 / (plane.width * spacing[0]), 410 / (plane.height * spacing[1]))
        size = (round(plane.width * spacing[0] * scale), round(plane.height * spacing[1] * scale))
        plane = plane.resize(size, Image.Resampling.NEAREST)
        overlay = ImageDraw.Draw(plane)

        def point(voxel):
            return np.asarray((
                voxel[remaining[0]] * size[0] / image.shape[remaining[0]],
                (image.shape[remaining[1]] - 1 - voxel[remaining[1]]) * size[1] / image.shape[remaining[1]],
            ))

        for other_id, other in patches.items():
            other_center = centers[other_id]
            if other_id != patch_id and abs(other_center[axis] - index) > 0.75:
                continue
            endpoint = nib.affines.apply_affine(inverse,
                np.asarray(other["center_ras_mm"]) + 12 * np.asarray(other["normal_ras"]))
            start, end = point(other_center), point(endpoint)
            delta = end - start
            if np.linalg.norm(delta) > 3:
                direction = delta / np.linalg.norm(delta)
                side = np.asarray((-direction[1], direction[0]))
                overlay.line((tuple(start), tuple(end)), fill=(40, 220, 255), width=2)
                overlay.polygon([tuple(end), tuple(end - 7 * direction + 3 * side),
                                 tuple(end - 7 * direction - 3 * side)], fill=(40, 220, 255))
        col, row = number % 3, number // 3
        xbase, ybase = col * 500, 120 + row * 520
        draw.text((xbase + 16, ybase),
                  f"{patch_id}   {patch['area_mm2']:.0f} mm^2   RMS {patch['rms_distance_mm']:.3f} mm", font=font)
        draw.text((xbase + 16, ybase + 26),
                  f"{('Sagittal', 'Coronal', 'Axial')[axis]} view; ribbon {patch['median_ribbon_separation_mm']:.2f} mm", font=small)
        xoff, yoff = xbase + (500 - size[0]) // 2, ybase + 55
        canvas.paste(plane, (xoff, yoff))
        cx, cy = point(center)
        half = max(24, round(15 * scale))
        crop = plane.crop((round(cx - half), round(cy - half), round(cx + half), round(cy + half)))
        crop = crop.resize((150, 150), Image.Resampling.NEAREST)
        zx, zy = xbase + 335, ybase + 337
        canvas.paste(crop, (zx, zy))
        draw.rectangle((zx, zy, zx + 150, zy + 150), outline=(150, 160, 180), width=1)
        draw.text((zx, zy - 20), f"{patch_id} cortex zoom", font=small)
    canvas.save(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    render_preview(args.input, args.run_dir, args.output)
