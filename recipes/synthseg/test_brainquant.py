import copy
import json
import math
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import brainquant  # noqa: E402

# Tissue columns in the order FreeSurfer 8.2.0 mri_synthseg writes them
# (synthseg_segmentation_names_2.0.npy sorted by label, ICV moved first).
SYNTHSEG_VOL_COLUMNS = [
    "total intracranial", "left cerebral white matter", "left cerebral cortex",
    "left lateral ventricle", "left inferior lateral ventricle",
    "left cerebellum white matter", "left cerebellum cortex", "left thalamus",
    "left caudate", "left putamen", "left pallidum", "3rd ventricle", "4th ventricle",
    "brain-stem", "left hippocampus", "left amygdala", "csf", "left accumbens area",
    "left ventral DC", "right cerebral white matter", "right cerebral cortex",
    "right lateral ventricle", "right inferior lateral ventricle",
    "right cerebellum white matter", "right cerebellum cortex", "right thalamus",
    "right caudate", "right putamen", "right pallidum", "right hippocampus",
    "right amygdala", "right accumbens area", "right ventral DC",
]
PARCEL_COLUMNS = ["ctx-lh-bankssts", "ctx-lh-caudalanteriorcingulate", "ctx-rh-insula"]
SAMPLE_VOLUMES_MM3 = {
    "total intracranial": 1450000.0, "left cerebral white matter": 230000.0,
    "left cerebral cortex": 245000.0, "left lateral ventricle": 9000.0,
    "left inferior lateral ventricle": 500.0, "left cerebellum white matter": 14000.0,
    "left cerebellum cortex": 52000.0, "left thalamus": 7600.0, "left caudate": 3500.0,
    "left putamen": 4800.0, "left pallidum": 1900.0, "3rd ventricle": 1000.0,
    "4th ventricle": 1600.0, "brain-stem": 21000.0, "left hippocampus": 3900.0,
    "left amygdala": 1600.0, "csf": 1200.0, "left accumbens area": 550.0,
    "left ventral DC": 4000.0, "right cerebral white matter": 229000.0,
    "right cerebral cortex": 246000.0, "right lateral ventricle": 8500.0,
    "right inferior lateral ventricle": 450.0, "right cerebellum white matter": 13800.0,
    "right cerebellum cortex": 52500.0, "right thalamus": 7400.0, "right caudate": 3600.0,
    "right putamen": 4900.0, "right pallidum": 1850.0, "right hippocampus": 4000.0,
    "right amygdala": 1700.0, "right accumbens area": 560.0, "right ventral DC": 3950.0,
}
BRAIN_TISSUE = [
    "cerebral white matter", "cerebral cortex", "cerebellum white matter",
    "cerebellum cortex", "thalamus", "caudate", "putamen", "pallidum", "hippocampus",
    "amygdala", "accumbens area", "ventral DC",
]
PANEL = [
    ("hippocampus_left", "Left hippocampus", ["left hippocampus"], 3900, -0.05),
    ("hippocampus_right", "Right hippocampus", ["right hippocampus"], 4000, -0.05),
    ("amygdala_left", "Left amygdala", ["left amygdala"], 1600, -0.04),
    ("amygdala_right", "Right amygdala", ["right amygdala"], 1700, -0.04),
    ("thalamus_left", "Left thalamus", ["left thalamus"], 7600, -0.04),
    ("thalamus_right", "Right thalamus", ["right thalamus"], 7400, -0.04),
    ("lateral_ventricles", "Lateral ventricles", [
        "left lateral ventricle", "left inferior lateral ventricle",
        "right lateral ventricle", "right inferior lateral ventricle",
    ], 15000, 0.25),
    ("cortical_grey_matter", "Cortical grey matter",
     ["left cerebral cortex", "right cerebral cortex"], 480000, -0.04),
    ("cerebral_white_matter", "Cerebral white matter",
     ["left cerebral white matter", "right cerebral white matter"], 460000, -0.02),
    ("whole_brain", "Whole brain",
     [f"{side} {tissue}" for side in ("left", "right") for tissue in BRAIN_TISSUE] + ["brain-stem"],
     1150000, -0.025),
]


def _fixture_norms() -> dict:
    def model(median, slope, offset):
        return {"mu": [math.log(median) + offset, slope, -0.004], "icv_slope": 0.8,
                "log_sigma": [math.log(0.6 if slope > 0.1 else 0.09), 0.02]}

    return {
        "schema": "neurodesk.synthseg.brainquant.norms/1",
        "reference": {
            "cohort": "IXI", "url": "https://brain-development.org/ixi-dataset/",
            "license": "CC-BY-SA-3.0", "subjects": 560, "female": 310, "male": 250,
            "age_min": 20.0, "age_max": 86.0,
            "synthseg": "SynthSeg 2.0, fast mode, FreeSurfer 8.2.0 mri_synthseg --vol",
        },
        "age_center": 50.0,
        "age_scale": 10.0,
        "icv_column": "total intracranial",
        "reference_icv_mm3": 1500000.0,
        "structures": [
            {"id": sid, "title": title, "columns": columns,
             "models": {"female": model(median, slope, -0.04), "male": model(median, slope, 0.04),
                        "all": model(median, slope, 0.0)}}
            for sid, title, columns, median, slope in PANEL
        ],
    }


def _write_vol_csv(path: Path, volumes: dict, parcels: bool = False) -> Path:
    header = ["subject"] + SYNTHSEG_VOL_COLUMNS + (PARCEL_COLUMNS if parcels else [])
    row = ["input"] + [str(volumes[name]) for name in SYNTHSEG_VOL_COLUMNS]
    row += ["2500.0"] * (len(PARCEL_COLUMNS) if parcels else 0)
    path.write_text(",".join(header) + "\n" + ",".join(row) + "\n")
    return path


def _norm(name="all", **changes):
    raw = _fixture_norms()
    for structure in raw["structures"]:
        structure["models"][name].update(changes)
    return brainquant.parse_norms(raw)


def _subject(age=50, sex="female"):
    return brainquant.Subject(age=age, sex=sex, age_source="GUI", sex_source="GUI")


def _only(norms, structure_id):
    norms_one = copy.copy(norms)
    object.__setattr__(norms_one, "structures", tuple(s for s in norms.structures if s.id == structure_id))
    return norms_one


@pytest.fixture
def norms():
    return brainquant.parse_norms(_fixture_norms())


def _percentile(norms, volume_mm3, icv_mm3, age=50, sex="female", structure="hippocampus_left"):
    report = brainquant.assess(
        _only(norms, structure),
        {"total intracranial": icv_mm3, "left hippocampus": volume_mm3},
        _subject(age, sex),
    )
    return report.results[0].percentile


def test_median_volume_at_reference_icv_is_50th_percentile(norms):
    model = norms.structures[0].models["female"]
    for age in (25, 50, 80):
        median = float(model.centile(norms.scaled_age(age), 0.5))
        assert _percentile(norms, median, norms.reference_icv_mm3, age=age) == pytest.approx(50.0, abs=1e-6)


def test_one_point_645_sigma_above_median_is_95th_percentile(norms):
    model = norms.structures[0].models["female"]
    a = norms.scaled_age(70)
    volume = math.exp(float(model.location(a)) + 1.645 * float(model.scale(a)))
    assert _percentile(norms, volume, norms.reference_icv_mm3, age=70) == pytest.approx(95.0, abs=0.01)
    assert float(model.centile(a, 0.95)) == pytest.approx(volume, rel=1e-3)


def test_icv_adjustment_cancels_proportional_scaling():
    norms = _norm("female", icv_slope=1.0)
    base = _percentile(norms, 3300.0, 1400000.0)
    assert _percentile(norms, 6600.0, 2800000.0) == pytest.approx(base, abs=1e-9)
    assert _percentile(norms, 6600.0, 1400000.0) > base


def test_unknown_sex_uses_combined_model_and_sex_changes_percentile(norms):
    volumes = {"total intracranial": 1500000.0, "left hippocampus": 3900.0}
    one = _only(norms, "hippocampus_left")
    female, male, unknown = (
        brainquant.assess(one, volumes, _subject(sex=sex)).results[0].percentile
        for sex in ("female", "male", None)
    )
    assert female > unknown > male


def test_unknown_age_reports_no_percentile_but_keeps_adjusted_volume(norms):
    report = brainquant.assess(norms, SAMPLE_VOLUMES_MM3, _subject(age=None))
    for result in report.results:
        assert result.z is None and result.percentile is None and result.flag == ""
        assert result.adjusted_mm3 > 0
    assert not report.outside_reference_age


def test_flags_mark_volumes_outside_the_5th_to_95th_band(norms):
    model = norms.structures[0].models["female"]
    low = float(model.centile(norms.scaled_age(50), 0.02))
    high = float(model.centile(norms.scaled_age(50), 0.98))
    one = _only(norms, "hippocampus_left")
    flags = [
        brainquant.assess(one, {"total intracranial": 1500000.0, "left hippocampus": v}, _subject()).results[0].flag
        for v in (low, high, float(model.centile(norms.scaled_age(50), 0.5)))
    ]
    assert flags == ["<5th", ">95th", ""]


@pytest.mark.parametrize("parcels", [False, True])
def test_structure_volumes_sum_named_csv_columns(tmp_path, norms, parcels):
    volumes = brainquant.read_volumes(_write_vol_csv(tmp_path / "vol.csv", SAMPLE_VOLUMES_MM3, parcels))
    assert ("ctx-lh-bankssts" in volumes) is parcels
    report = brainquant.assess(norms, volumes, _subject())
    by_id = {result.structure.id: result.volume_mm3 for result in report.results}
    assert by_id["hippocampus_left"] == 3900.0
    assert by_id["lateral_ventricles"] == 9000.0 + 500.0 + 8500.0 + 450.0
    assert by_id["cortical_grey_matter"] == 245000.0 + 246000.0
    whole_brain = sum(
        value for name, value in SAMPLE_VOLUMES_MM3.items()
        if name not in {"total intracranial", "csf", "3rd ventricle", "4th ventricle"}
        and "ventricle" not in name
    )
    assert by_id["whole_brain"] == pytest.approx(whole_brain)
    assert report.icv_mm3 == 1450000.0


def test_missing_csv_column_is_rejected(tmp_path, norms):
    volumes = dict(SAMPLE_VOLUMES_MM3)
    del volumes["right amygdala"]
    with pytest.raises(ValueError, match="right amygdala"):
        brainquant.assess(norms, volumes, _subject())


def test_norms_with_wrong_schema_or_missing_model_are_rejected():
    raw = _fixture_norms()
    raw["schema"] = "other"
    with pytest.raises(ValueError, match="schema"):
        brainquant.parse_norms(raw)
    raw = _fixture_norms()
    del raw["structures"][0]["models"]["all"]
    with pytest.raises(ValueError, match="models"):
        brainquant.parse_norms(raw)


def _header(birth=None, gender=None, study=None):
    return SimpleNamespace(
        subjectInformation=SimpleNamespace(patientBirthdate=birth, patientGender=gender),
        studyInformation=SimpleNamespace(studyDate=study),
    )


@pytest.mark.parametrize("header, gui_age, gui_sex, expected", [
    (_header("1960-05-17", "F", "2024-05-16"), 0, "header", (63, "female", "header", "header")),
    (_header("1960-05-17", "M", "2024-05-17"), 0, "header", (64, "male", "header", "header")),
    (_header("1960-05-17", "O", "2024-05-17"), 0, "header", (64, None, "header", "unknown")),
    (_header("1960-05-17", "F", None), 0, "header", (None, "female", "unknown", "header")),
    (_header("1960-05-17", "F", "2024-05-17"), 30, "male", (30, "male", "GUI", "GUI")),
    (_header(None, None, "2024-05-17"), 0, "header", (None, None, "unknown", "unknown")),
    (None, 0, "header", (None, None, "unknown", "unknown")),
    (None, 41, "female", (41, "female", "GUI", "GUI")),
])
def test_subject_resolution_from_header_and_gui(header, gui_age, gui_sex, expected):
    subject = brainquant.resolve_subject(header, gui_age, gui_sex)
    assert (subject.age, subject.sex, subject.age_source, subject.sex_source) == expected


def test_subject_resolution_reads_real_mrd_header_types():
    xsd = pytest.importorskip("ismrmrd.xsd")
    from xsdata.models.datatype import XmlDate

    header = SimpleNamespace(
        subjectInformation=xsd.subjectInformationType(
            patientBirthdate=XmlDate(1958, 2, 28), patientGender="F",
        ),
        studyInformation=xsd.studyInformationType(studyDate=XmlDate(2026, 2, 27)),
    )
    subject = brainquant.resolve_subject(header, 0, "header")
    assert (subject.age, subject.sex) == (67, "female")


def _ink(page: np.ndarray) -> float:
    return float(np.mean(page < brainquant.PIXEL_MAX * 0.5))


@pytest.mark.parametrize("age", [62, None, 12])
def test_report_pages_render_legible_grayscale(norms, age):
    report = brainquant.assess(norms, SAMPLE_VOLUMES_MM3, _subject(age=age, sex=None),
                               footnotes=("Footnote shown on the page.",))
    pages = brainquant.render_pages(report)
    assert len(pages) == 2
    for page in pages:
        assert page.dtype == np.uint16
        assert page.shape[0] >= 1024 and page.shape[1] >= 1024
        assert page.max() <= brainquant.PIXEL_MAX
        assert np.median(page) == brainquant.PIXEL_MAX
        assert 0.005 < _ink(page) < 0.5
    preview = os.environ.get("BRAINQUANT_PREVIEW")
    if preview and age == 62:
        from matplotlib.image import imsave

        imsave(preview, np.hstack(pages), cmap="gray", vmin=0, vmax=brainquant.PIXEL_MAX)


@pytest.mark.parametrize("field_strength, fast, expected", [
    (None, True, "brainquant_norms.json"),
    (1.5, True, "brainquant_norms.json"),
    (2.89, True, "brainquant_norms.json"),
    (6.98, True, "brainquant_norms_7t.json"),
    (9.4, True, "brainquant_norms_7t.json"),
    (None, False, "brainquant_norms_normal.json"),
    (2.89, False, "brainquant_norms_normal.json"),
    (5.0, False, "brainquant_norms_7t_normal.json"),
])
def test_reference_is_selected_by_field_strength_and_mode(field_strength, fast, expected):
    path, reason = brainquant.select_norms(field_strength, fast)
    assert path.name == expected
    assert ("not in the header" in reason) == (field_strength is None)
    assert reason.endswith("fast mode" if fast else "normal mode")


def test_field_strength_reads_real_mrd_header_types():
    xsd = pytest.importorskip("ismrmrd.xsd")
    header = SimpleNamespace(acquisitionSystemInformation=xsd.acquisitionSystemInformationType(systemFieldStrength_T=6.98))
    assert brainquant.header_field_strength(header) == pytest.approx(6.98)
    assert brainquant.header_field_strength(SimpleNamespace(acquisitionSystemInformation=None)) is None
    assert brainquant.header_field_strength(None) is None


def test_shipped_references_share_structures_and_match_their_cohort_and_mode():
    shipped = {key: brainquant.load_norms(brainquant.norms_path(*key)) for key in brainquant.NORMS_FILES}
    layouts = {tuple((s.id, s.columns) for s in norms.structures) for norms in shipped.values()}
    assert len(layouts) == 1
    for (cohort, fast), norms in shipped.items():
        assert norms.reference.cohort == {"ixi": "IXI", "ahead": "AHEAD"}[cohort]
        assert norms.reference.field_strength == {"ixi": "1.5 T and 3 T", "ahead": "7 T"}[cohort]
        assert ("fast mode" if fast else "normal mode") in norms.reference.synthseg


@pytest.mark.parametrize("key", list(brainquant.NORMS_FILES))
def test_report_text_stays_inside_the_page(key):
    norms = brainquant.load_norms(brainquant.norms_path(*key))
    volumes = dict(SAMPLE_VOLUMES_MM3)
    footnotes = (
        f"The {norms.reference.cohort} reference was chosen because the scanner field strength is 6.98 T.",
        "This run used ssmodel=robust with fast mode on. The reference was fitted with SynthSeg 2.0 in "
        "fast mode, so volumes may be shifted relative to it.",
    )
    report = brainquant.assess(norms, volumes, brainquant.resolve_subject(None, 90, "header"), footnotes)
    for page in brainquant.render_pages(report):
        # Clipped text leaves ink in the rightmost columns of the header and footer bands.
        rows = page.shape[0]
        bands = np.concatenate([page[: rows // 6], page[-rows // 8:]])
        assert not np.any(bands[:, -6:] < brainquant.PIXEL_MAX * 0.5)
