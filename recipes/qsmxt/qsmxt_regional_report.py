from __future__ import annotations

import csv
import io
import logging
import math
import textwrap
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass(frozen=True)
class _RegionRow:
    index: int
    name: str
    n_voxels: int
    n_valid: int
    mean: float
    sd: float
    median: float


@dataclass(frozen=True)
class _RegionalTable:
    rows: tuple[_RegionRow, ...]
    input_variant: str
    requested_reference: str
    reference: str | None
    reference_offset_ppm: float | None


def build_pages(
    output_dir: Path, *, input_variant: str, requested_reference: str,
) -> tuple[np.ndarray, ...]:
    """Read and log one native run's TSV, then render its Chimap ppm rows."""
    candidates = sorted(
        path for path in (output_dir / "derivatives/qsmxt").glob(
            "sub-*/**/anat/*_desc-segmentation_stats.tsv"
        ) if path.is_file()
    )
    if len(candidates) != 1:
        raise ValueError(
            "Regional QSM table requires exactly one per-run segmentation TSV; "
            f"found {len(candidates)} in {output_dir}"
        )
    with candidates[0].open(encoding="utf-8", newline="") as stream:
        raw = stream.read()
    logging.info(
        "QSMxT regional QSM raw TSV input_variant=%s requested_reference=%s source=%s\n%s",
        input_variant, requested_reference, candidates[0], raw,
    )
    table = _parse_table(raw, input_variant, requested_reference)
    return _render_pages(table)


def _parse_table(raw: str, input_variant: str, requested_reference: str) -> _RegionalTable:
    reader = csv.reader(io.StringIO(raw), delimiter="\t", quoting=csv.QUOTE_NONE)
    header = next(reader, [])
    required = {
        "map", "unit", "index", "name", "n_voxels", "n_valid",
        "mean", "sd", "median", "reference", "reference_offset_ppm",
    }
    if len(set(header)) != len(header) or not required <= set(header):
        raise ValueError("Regional QSM TSV has missing or duplicate columns")
    columns = {name: index for index, name in enumerate(header)}
    rows = []
    labels = set()
    provenance = set()
    for line_number, fields in enumerate(reader, start=2):
        if not fields:
            continue
        if len(fields) <= columns["map"]:
            raise ValueError(f"Regional QSM TSV row {line_number} is incomplete")
        if fields[columns["map"]] != "Chimap":
            continue
        if len(fields) != len(header):
            raise ValueError(f"Regional QSM TSV row {line_number} is incomplete")
        values = dict(zip(header, fields))
        try:
            row = _RegionRow(
                index=int(values["index"]), name=values["name"],
                n_voxels=int(values["n_voxels"]), n_valid=int(values["n_valid"]),
                mean=float(values["mean"]), sd=float(values["sd"]),
                median=float(values["median"]),
            )
            reference = values["reference"] or None
            offset_text = values["reference_offset_ppm"]
            offset = float(offset_text) if offset_text else None
        except ValueError as error:
            raise ValueError(f"Regional QSM TSV row {line_number} has invalid numbers") from error
        if values["unit"] != "ppm":
            raise ValueError("Regional QSM Chimap rows must use ppm")
        if not all(math.isfinite(value) for value in (row.mean, row.sd, row.median)):
            raise ValueError("Regional QSM statistics must be finite")
        if offset is not None and not math.isfinite(offset):
            raise ValueError("Regional QSM reference offset must be finite when recorded")
        if row.index <= 0 or not row.name.strip() or row.index in labels:
            raise ValueError("Regional QSM labels must be named, positive and unique")
        if not 0 < row.n_valid <= row.n_voxels or row.sd < 0:
            raise ValueError("Regional QSM counts or standard deviation are invalid")
        labels.add(row.index)
        provenance.add((reference, offset))
        rows.append(row)
    if not rows:
        raise ValueError("Regional QSM TSV has no Chimap ppm rows")
    if len(provenance) != 1:
        raise ValueError("Regional QSM TSV mixes native reference provenance")
    reference, offset = provenance.pop()
    return _RegionalTable(tuple(rows), input_variant, requested_reference, reference, offset)


def _render_pages(table: _RegionalTable) -> tuple[np.ndarray, ...]:
    from matplotlib import font_manager
    from PIL import Image, ImageDraw, ImageFont

    width, height = 1600, 1800
    margin = 60
    font_path = font_manager.findfont("DejaVu Sans Mono")
    font = ImageFont.truetype(font_path, 24)
    title_font = ImageFont.truetype(font_path, 36)
    line_height = 34

    def wrap(text, available_width):
        return textwrap.wrap(
            text, width=max(1, int(available_width / font.getlength("M"))),
        ) or [""]

    offset = (
        f"{table.reference_offset_ppm:.6f} ppm"
        if table.reference_offset_ppm is not None else "not recorded"
    )
    captions = [
        f"Input variant: {table.input_variant}",
        f"Requested QSM reference: {table.requested_reference}",
        f"Native QSM reference: {table.reference or 'not recorded'}",
        f"Native reference offset: {offset}",
    ]
    caption_lines = [line for caption in captions for line in wrap(caption, width - 2 * margin)]
    table_top = 132 + len(caption_lines) * line_height + 30
    body_top = table_top + 54
    body_bottom = height - 180
    row_layouts = []
    for row in table.rows:
        lines = wrap(f"{row.index}  {row.name}", 640)
        row_height = max(54, len(lines) * line_height + 18)
        if body_top + row_height > body_bottom:
            raise ValueError("Regional QSM label or reference is too long for a report page")
        row_layouts.append((row, lines, row_height))

    groups = [[]]
    y = body_top
    for layout in row_layouts:
        if y + layout[2] > body_bottom:
            groups.append([])
            y = body_top
        groups[-1].append(layout)
        y += layout[2]

    pages = []
    for page_index, group in enumerate(groups, start=1):
        image = Image.new("L", (width, height), 255)
        draw = ImageDraw.Draw(image)
        draw.text((margin, 50), "Regional QSM table", font=title_font, fill=0)
        for index, line in enumerate(caption_lines):
            draw.text((margin, 132 + index * line_height), line, font=font, fill=0)
        draw.rectangle((margin, table_top, width - margin, body_top), fill=225)
        draw.text((margin + 8, table_top + 10), "Label / SynthSeg region", font=font, fill=0)
        for x, title in (
            (900, "Mean ppm"), (1090, "SD ppm"),
            (1300, "Median ppm"), (1532, "Valid/total"),
        ):
            draw.text((x, table_top + 10), title, anchor="ra", font=font, fill=0)
        y = body_top
        for row, lines, row_height in group:
            for index, line in enumerate(lines):
                draw.text((margin + 8, y + 10 + index * line_height), line, font=font, fill=0)
            for x, value in (
                (900, f"{row.mean:.6f}"), (1090, f"{row.sd:.6f}"),
                (1300, f"{row.median:.6f}"), (1532, f"{row.n_valid}/{row.n_voxels}"),
            ):
                draw.text((x, y + 10), value, anchor="ra", font=font, fill=0)
            y += row_height
            draw.line((margin, y, width - margin, y), fill=210, width=1)
        draw.text(
            (margin, height - 144), "Native Chimap statistics; no values recomputed.",
            font=font, fill=0,
        )
        draw.text(
            (margin, height - 108),
            "QSMxT excludes zero/nonfinite voxels and regions without valid values.",
            font=font, fill=0,
        )
        draw.text((margin, height - 72), f"Page {page_index}/{len(groups)}", font=font, fill=0)
        pages.append((np.asarray(image, dtype=np.uint32) * 4095 // 255).astype(np.uint16))
    return tuple(pages)
