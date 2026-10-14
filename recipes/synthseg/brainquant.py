"""Age-normative SynthSeg brain volume report (research use only).

Norms are parsed once from JSON, volumes once from the mri_synthseg ``--vol``
CSV; everything downstream works on the typed structures below.
"""

import csv
from dataclasses import dataclass
from datetime import date
import json
import math
from pathlib import Path
from typing import Literal

import numpy as np
from scipy.special import ndtr, ndtri

NORMS_SCHEMA = "neurodesk.synthseg.brainquant.norms/1"
NORMS_PATH = Path(__file__).with_name("brainquant_norms.json")
MODEL_KEYS = ("female", "male", "all")
PAGE_PIXELS = 1024
PIXEL_MAX = 4095
LOW_PERCENTILE = 5.0
HIGH_PERCENTILE = 95.0

Sex = Literal["female", "male"]
HEADER_SEX = {"F": "female", "M": "male"}


@dataclass(frozen=True)
class NormModel:
    """Log-volume model at the reference ICV, in centred and scaled age ``a``."""

    mu: tuple[float, float, float]
    icv_slope: float
    log_sigma: tuple[float, float]

    def location(self, a):
        b0, b1, b2 = self.mu
        return b0 + b1 * a + b2 * np.square(a)

    def scale(self, a):
        s0, s1 = self.log_sigma
        return np.exp(s0 + s1 * a)

    def evaluate(self, a: float, adjusted_log_volume: float) -> float:
        return float((adjusted_log_volume - self.location(a)) / self.scale(a))

    def centile(self, a, quantile: float):
        """Volume in mm3 at the reference ICV for a quantile in (0, 1)."""
        return np.exp(self.location(a) + float(ndtri(quantile)) * self.scale(a))


@dataclass(frozen=True)
class Structure:
    id: str
    title: str
    columns: tuple[str, ...]
    models: dict[str, NormModel]


@dataclass(frozen=True)
class Reference:
    cohort: str
    url: str
    license: str
    subjects: int
    female: int
    male: int
    age_min: float
    age_max: float
    synthseg: str


@dataclass(frozen=True)
class Norms:
    reference: Reference
    age_center: float
    age_scale: float
    icv_column: str
    reference_icv_mm3: float
    structures: tuple[Structure, ...]

    def scaled_age(self, age):
        return (np.asarray(age, dtype=float) - self.age_center) / self.age_scale

    @property
    def columns(self) -> set[str]:
        return {self.icv_column}.union(*(structure.columns for structure in self.structures))


@dataclass(frozen=True)
class Subject:
    age: int | None
    sex: Sex | None
    age_source: str
    sex_source: str

    @property
    def model_key(self) -> str:
        return self.sex or "all"

    def describe(self) -> str:
        age = f"{self.age} y" if self.age is not None else "unknown age"
        sex = self.sex or "unknown sex"
        return f"{age} ({self.age_source}), {sex} ({self.sex_source})"


@dataclass(frozen=True)
class StructureResult:
    structure: Structure
    volume_mm3: float
    adjusted_mm3: float
    z: float | None

    @property
    def percentile(self) -> float | None:
        return None if self.z is None else float(100.0 * ndtr(self.z))

    @property
    def flag(self) -> str:
        percentile = self.percentile
        if percentile is None:
            return ""
        if percentile < LOW_PERCENTILE:
            return "<5th"
        if percentile > HIGH_PERCENTILE:
            return ">95th"
        return ""


@dataclass(frozen=True)
class Report:
    norms: Norms
    subject: Subject
    icv_mm3: float
    results: tuple[StructureResult, ...]
    footnotes: tuple[str, ...] = ()

    @property
    def outside_reference_age(self) -> bool:
        reference = self.norms.reference
        return self.subject.age is not None and not (
            reference.age_min <= self.subject.age <= reference.age_max
        )


def _number(value, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"Brain volume norms: {label} must be a finite number, got {value!r}")
    return float(value)


def _numbers(value, count: int, label: str) -> tuple[float, ...]:
    if not isinstance(value, list) or len(value) != count:
        raise ValueError(f"Brain volume norms: {label} must list {count} numbers, got {value!r}")
    return tuple(_number(item, f"{label}[{index}]") for index, item in enumerate(value))


def _model(raw, label: str) -> NormModel:
    return NormModel(
        mu=_numbers(raw["mu"], 3, f"{label}.mu"),
        icv_slope=_number(raw["icv_slope"], f"{label}.icv_slope"),
        log_sigma=_numbers(raw["log_sigma"], 2, f"{label}.log_sigma"),
    )


def parse_norms(raw: dict) -> Norms:
    if raw.get("schema") != NORMS_SCHEMA:
        raise ValueError(f"Brain volume norms schema {raw.get('schema')!r}, expected {NORMS_SCHEMA}")
    try:
        reference = raw["reference"]
        structures = []
        for entry in raw["structures"]:
            columns = tuple(entry["columns"])
            if not columns or not all(isinstance(column, str) and column for column in columns):
                raise ValueError(f"Brain volume norms: structure {entry['id']} needs CSV columns")
            if set(entry["models"]) != set(MODEL_KEYS):
                raise ValueError(
                    f"Brain volume norms: structure {entry['id']} models "
                    f"{sorted(entry['models'])}, expected {list(MODEL_KEYS)}"
                )
            structures.append(Structure(
                id=str(entry["id"]),
                title=str(entry["title"]),
                columns=columns,
                models={key: _model(entry["models"][key], f"{entry['id']}.{key}") for key in MODEL_KEYS},
            ))
        norms = Norms(
            reference=Reference(
                cohort=str(reference["cohort"]),
                url=str(reference["url"]),
                license=str(reference["license"]),
                subjects=int(reference["subjects"]),
                female=int(reference["female"]),
                male=int(reference["male"]),
                age_min=_number(reference["age_min"], "reference.age_min"),
                age_max=_number(reference["age_max"], "reference.age_max"),
                synthseg=str(reference["synthseg"]),
            ),
            age_center=_number(raw["age_center"], "age_center"),
            age_scale=_number(raw["age_scale"], "age_scale"),
            icv_column=str(raw["icv_column"]),
            reference_icv_mm3=_number(raw["reference_icv_mm3"], "reference_icv_mm3"),
            structures=tuple(structures),
        )
    except KeyError as missing:
        raise ValueError(f"Brain volume norms are missing field {missing}") from None
    if not structures or norms.age_scale <= 0 or norms.reference_icv_mm3 <= 0:
        raise ValueError("Brain volume norms need structures and positive age_scale and reference ICV")
    return norms


def load_norms(path: str | Path) -> Norms:
    return parse_norms(json.loads(Path(path).read_text(encoding="utf-8")))


def read_volumes(path: str | Path) -> dict[str, float]:
    """Read the single-subject mri_synthseg --vol CSV as {column name: mm3}.

    The first column is the subject name; parcels from --parc follow the
    tissue columns, so callers select by name.
    """
    with Path(path).open(newline="", encoding="utf-8") as handle:
        rows = [row for row in csv.reader(handle) if row]
    if len(rows) != 2 or len(rows[0]) != len(rows[1]) or len(rows[0]) < 2:
        raise ValueError(f"Expected one header and one subject row in SynthSeg volume CSV {path}")
    volumes = {}
    for name, text in zip(rows[0][1:], rows[1][1:]):
        value = float(text)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"SynthSeg volume CSV {path} has invalid {name!r} volume {text!r}")
        volumes[name.strip()] = value
    return volumes


def _header_date(value) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def whole_years(birth: date, on: date) -> int:
    return on.year - birth.year - ((on.month, on.day) < (birth.month, birth.day))


def resolve_subject(metadata, gui_age: int, gui_sex: str) -> Subject:
    """Resolve age and sex from the MRD XML header, overridden by GUI values.

    ``gui_age`` 0 and ``gui_sex`` "header" defer to the header. The header may
    be absent or partial (raw-data paths, anonymised exams).
    """
    subject_info = getattr(metadata, "subjectInformation", None)
    study_info = getattr(metadata, "studyInformation", None)
    if gui_age > 0:
        age, age_source = int(gui_age), "GUI"
    else:
        birth = _header_date(getattr(subject_info, "patientBirthdate", None))
        # Without a study date the acquisition age is unknown; the processing
        # date would overstate it whenever an old exam is reprocessed.
        on = _header_date(getattr(study_info, "studyDate", None))
        age = whole_years(birth, on) if birth is not None and on is not None and birth <= on else None
        age_source = "header" if age is not None else "unknown"
    if gui_sex in ("female", "male"):
        sex, sex_source = gui_sex, "GUI"
    else:
        gender = getattr(subject_info, "patientGender", None)
        sex = HEADER_SEX.get(str(getattr(gender, "value", gender) or "").strip().upper())
        sex_source = "header" if sex is not None else "unknown"
    return Subject(age=age, sex=sex, age_source=age_source, sex_source=sex_source)


def assess(norms: Norms, volumes: dict[str, float], subject: Subject, footnotes=()) -> Report:
    missing = sorted(norms.columns - volumes.keys())
    if missing:
        raise ValueError(f"SynthSeg volume CSV is missing columns required by the norms: {missing}")
    icv = volumes[norms.icv_column]
    if icv <= 0:
        raise ValueError("SynthSeg reported a non-positive total intracranial volume")
    log_icv_ratio = math.log(icv) - math.log(norms.reference_icv_mm3)
    results = []
    for structure in norms.structures:
        model = structure.models[subject.model_key]
        volume = sum(volumes[column] for column in structure.columns)
        if volume <= 0:
            raise ValueError(f"SynthSeg reported no volume for {structure.title}")
        adjusted_log = math.log(volume) - model.icv_slope * log_icv_ratio
        z = (
            None if subject.age is None
            else model.evaluate(float(norms.scaled_age(subject.age)), adjusted_log)
        )
        results.append(StructureResult(structure, volume, math.exp(adjusted_log), z))
    return Report(norms, subject, icv, tuple(results), tuple(footnotes))


def _ml(mm3: float) -> float:
    return mm3 / 1000.0


def _percentile_text(result: StructureResult) -> str:
    percentile = result.percentile
    if percentile is None:
        return "n/a"
    if percentile < 1:
        return "<1st"
    if percentile > 99:
        return ">99th"
    value = int(round(percentile))
    suffix = "th" if 10 <= value % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def log_fields(result: StructureResult) -> dict[str, str]:
    return {
        "structure": result.structure.id,
        "volume_ml": f"{_ml(result.volume_mm3):.3f}",
        "adjusted_ml": f"{_ml(result.adjusted_mm3):.3f}",
        "z": "n/a" if result.z is None else f"{result.z:.2f}",
        "percentile": "n/a" if result.percentile is None else f"{result.percentile:.1f}",
    }


def _license_text(spdx: str) -> str:
    parts = spdx.split("-")
    if parts[0] == "CC" and len(parts) >= 3:
        return f"CC {'-'.join(parts[1:-1])} {parts[-1]}"
    return spdx


def _header_lines(report: Report, page_title: str) -> list[str]:
    reference = report.norms.reference
    lines = [
        f"SynthSeg brain volume report - {page_title}",
        f"Patient: {report.subject.describe()}.  Total intracranial volume {_ml(report.icv_mm3):.0f} mL.",
    ]
    if report.subject.sex is None:
        lines.append("Sex unknown: compared with the combined female and male reference.")
    return lines + [
        (
            f"Reference: {reference.cohort} (n={reference.subjects}, {reference.female} F / {reference.male} M, "
            f"age {reference.age_min:g}-{reference.age_max:g} y, {_license_text(reference.license)}), "
            f"{reference.synthseg}."
        ),
        "Research use only. Not for diagnostic use.",
    ]


def _footer_lines(report: Report) -> list[str]:
    lines = list(report.footnotes)
    if report.outside_reference_age:
        reference = report.norms.reference
        lines.append(
            f"Patient age {report.subject.age} y is outside the reference age range "
            f"{reference.age_min:g}-{reference.age_max:g} y; the curves are extrapolated."
        )
    lines.append(
        f"Adjusted volume: scaled to the reference total intracranial volume of "
        f"{_ml(report.norms.reference_icv_mm3):.0f} mL. Bands: 5th-95th and 25th-75th percentiles; line: median."
    )
    return lines


def _figure():
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure

    figure = Figure(figsize=(PAGE_PIXELS / 100, PAGE_PIXELS / 100), dpi=100, facecolor="white")
    FigureCanvasAgg(figure)
    return figure


def _write_text(figure, report: Report, page_title: str) -> tuple[float, float]:
    """Draw header and footer; return the free vertical band (bottom, top)."""
    header = _header_lines(report, page_title)
    y = 0.985
    for index, line in enumerate(header):
        figure.text(
            0.02, y, line, ha="left", va="top",
            fontsize=15 if index == 0 else 10.5,
            fontweight="bold" if index in (0, len(header) - 1) else "normal",
        )
        y -= 0.032 if index == 0 else 0.024
    footer = _footer_lines(report)
    for index, line in enumerate(footer):
        figure.text(0.02, 0.012 + 0.022 * (len(footer) - 1 - index), line, ha="left", va="bottom", fontsize=9.5)
    return 0.012 + 0.022 * len(footer), y


def _to_pixels(figure) -> np.ndarray:
    figure.canvas.draw()
    rgba = np.asarray(figure.canvas.buffer_rgba(), dtype=np.float64)
    luminance = rgba[..., :3] @ np.array([0.299, 0.587, 0.114])
    return np.rint(luminance * (PIXEL_MAX / 255.0)).astype(np.uint16)


def _chart_page(report: Report) -> np.ndarray:
    figure = _figure()
    bottom, top = _write_text(figure, report, "volume by age")
    norms = report.norms
    reference = norms.reference
    columns = 3
    rows = math.ceil((len(report.results) + 1) / columns)
    grid = figure.add_gridspec(
        rows, columns, left=0.075, right=0.985, top=top - 0.035,
        bottom=bottom + 0.045, hspace=0.55, wspace=0.28,
    )
    age = report.subject.age
    span_min = min(reference.age_min, age) if age is not None else reference.age_min
    span_max = max(reference.age_max, age) if age is not None else reference.age_max
    ages = np.linspace(span_min, span_max, 200)
    a = norms.scaled_age(ages)
    for index, result in enumerate(report.results):
        model = result.structure.models[report.subject.model_key]
        axis = figure.add_subplot(grid[index // columns, index % columns])
        outer = [_ml(model.centile(a, q)) for q in (0.05, 0.95)]
        inner = [_ml(model.centile(a, q)) for q in (0.25, 0.75)]
        axis.fill_between(ages, *outer, color="0.86", linewidth=0)
        axis.fill_between(ages, *inner, color="0.70", linewidth=0)
        axis.plot(ages, _ml(model.centile(a, 0.5)), color="0.25", linewidth=1.2)
        adjusted = _ml(result.adjusted_mm3)
        if age is None:
            axis.axhline(adjusted, color="black", linewidth=2.0, linestyle="--")
        else:
            axis.plot([age], [adjusted], marker="o", markersize=9, color="black",
                      markeredgecolor="white", markeredgewidth=1.5, zorder=3)
        if age is not None and report.outside_reference_age:
            axis.axvspan(reference.age_min, reference.age_max, fill=False, edgecolor="0.4",
                         linestyle=":", linewidth=1.0)
            axis.text(0.98, 0.04, "outside reference age range", transform=axis.transAxes,
                      ha="right", va="bottom", fontsize=8.5, fontweight="bold")
        axis.set_title(f"{result.structure.title}: {_percentile_text(result)}",
                       fontsize=11.5, fontweight="bold" if result.flag else "normal", loc="left")
        axis.set_xlim(span_min - 2, span_max + 2)
        axis.tick_params(labelsize=9, color="0.5")
        axis.grid(True, color="0.9", linewidth=0.6)
        axis.set_axisbelow(True)
        for side in ("top", "right"):
            axis.spines[side].set_visible(False)
        axis.set_xlabel("age (years)", fontsize=9)
        axis.set_ylabel("adjusted volume (mL)", fontsize=9)
    _legend(figure.add_subplot(grid[len(report.results) // columns, len(report.results) % columns]),
            known_age=age is not None)
    return _to_pixels(figure)


def _legend(axis, known_age: bool) -> None:
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    patient = (
        Line2D([], [], marker="o", markersize=9, color="black", markeredgecolor="white", linestyle="none")
        if known_age else Line2D([], [], color="black", linewidth=2.0, linestyle="--")
    )
    axis.axis("off")
    axis.legend(
        [patient, Line2D([], [], color="0.25", linewidth=1.2), Patch(color="0.70"), Patch(color="0.86")],
        ["patient (adjusted volume)" if known_age else "patient (age unknown)",
         "median", "25th-75th percentile", "5th-95th percentile"],
        loc="center left", frameon=False, fontsize=10.5,
    )


def _table_page(report: Report) -> np.ndarray:
    figure = _figure()
    _, top = _write_text(figure, report, "percentile table")
    headings = ("Structure", "Volume (mL)", "Adjusted (mL)", "z", "Percentile", "Flag")
    cells = [
        (
            result.structure.title,
            f"{_ml(result.volume_mm3):.2f}",
            f"{_ml(result.adjusted_mm3):.2f}",
            "n/a" if result.z is None else f"{result.z:+.2f}",
            _percentile_text(result),
            result.flag,
        )
        for result in report.results
    ]
    positions = (0.04, 0.50, 0.67, 0.77, 0.90, 0.92)
    alignments = ("left", "right", "right", "right", "right", "left")
    top -= 0.08
    step = min(0.05, 0.62 / (len(cells) + 1))
    for x, heading, alignment in zip(positions, headings, alignments):
        figure.text(x, top, heading, ha=alignment,
                    va="center", fontsize=13, fontweight="bold")
    figure.add_artist(_rule(top - step / 2))
    for row_index, row in enumerate(cells, start=1):
        y = top - row_index * step
        bold = bool(row[-1])
        for x, text, alignment in zip(positions, row, alignments):
            figure.text(x, y, text, ha=alignment, va="center",
                        fontsize=13, fontweight="bold" if bold else "normal")
    figure.add_artist(_rule(top - (len(cells) + 0.5) * step))
    return _to_pixels(figure)


def _rule(y: float):
    from matplotlib.lines import Line2D

    return Line2D([0.03, 0.97], [y, y], color="0.4", linewidth=1.0)


def render_pages(report: Report) -> list[np.ndarray]:
    """Return the chart and table pages as uint16 grayscale arrays (0..4095)."""
    return [_chart_page(report), _table_page(report)]
