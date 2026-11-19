"""Named cortical search regions for the flat-patch search.

The regions and their ids are those of the TopoFit web app
(@neurodesk/topofit/regions): Wang 2015 visual areas, FreeSurfer's
--lobesStrict lobes of the Desikan-Killiany atlas plus the insula, and the
Desikan-Killiany regions. Both atlases are fsaverage label maps, packed in
fsaverage-regions.bin with their colour tables in cortex-regions.manifest.json.
They are atlas estimates, not an individual's retinotopy or parcellation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

REGIONS_ASSET = "fsaverage-regions.bin"
REGIONS_MANIFEST = "cortex-regions.manifest.json"

# FEF is a Wang 2015 topographic area but frontal, so it is not part of the visual cortex union.
_VISUAL_AREAS = (
    ("V1", ("V1v", "V1d")),
    ("V2", ("V2v", "V2d")),
    ("V3", ("V3v", "V3d")),
    *((name, (name,)) for name in (
        "hV4", "VO1", "VO2", "PHC1", "PHC2", "TO1", "TO2", "LO1", "LO2", "V3A", "V3B",
        "IPS0", "IPS1", "IPS2", "IPS3", "IPS4", "IPS5", "SPL1",
    )),
)
_HALVES = (
    ("V1v", "V1 ventral (V1v)"), ("V1d", "V1 dorsal (V1d)"),
    ("V2v", "V2 ventral (V2v)"), ("V2d", "V2 dorsal (V2d)"),
    ("V3v", "V3 ventral (V3v)"), ("V3d", "V3 dorsal (V3d)"),
)
# FreeSurfer's mri_annotation2label --lobesStrict (MRISaparc2lobes); insula stays its own lobe.
_LOBES = (
    ("frontal", "Frontal lobe", (
        "caudalmiddlefrontal", "superiorfrontal", "rostralmiddlefrontal", "parsopercularis",
        "parstriangularis", "parsorbitalis", "lateralorbitofrontal", "medialorbitofrontal",
        "paracentral", "frontalpole", "precentral",
    )),
    ("parietal", "Parietal lobe", (
        "supramarginal", "inferiorparietal", "superiorparietal", "precuneus", "postcentral",
    )),
    ("temporal", "Temporal lobe", (
        "superiortemporal", "entorhinal", "temporalpole", "fusiform", "inferiortemporal",
        "middletemporal", "parahippocampal", "bankssts", "transversetemporal",
    )),
    ("occipital", "Occipital lobe", ("pericalcarine", "cuneus", "lingual", "lateraloccipital")),
    ("cingulate", "Cingulate cortex", (
        "isthmuscingulate", "posteriorcingulate", "caudalanteriorcingulate", "rostralanteriorcingulate",
    )),
    ("insula", "Insula", ("insula",)),
)
_DK_NAMES = {
    "bankssts": "Banks of the superior temporal sulcus",
    "caudalanteriorcingulate": "Caudal anterior cingulate",
    "caudalmiddlefrontal": "Caudal middle frontal",
    "cuneus": "Cuneus",
    "entorhinal": "Entorhinal",
    "frontalpole": "Frontal pole",
    "fusiform": "Fusiform",
    "inferiorparietal": "Inferior parietal",
    "inferiortemporal": "Inferior temporal",
    "insula": "Insula",
    "isthmuscingulate": "Isthmus cingulate",
    "lateraloccipital": "Lateral occipital",
    "lateralorbitofrontal": "Lateral orbitofrontal",
    "lingual": "Lingual",
    "medialorbitofrontal": "Medial orbitofrontal",
    "middletemporal": "Middle temporal",
    "paracentral": "Paracentral",
    "parahippocampal": "Parahippocampal",
    "parsopercularis": "Pars opercularis",
    "parsorbitalis": "Pars orbitalis",
    "parstriangularis": "Pars triangularis",
    "pericalcarine": "Pericalcarine",
    "postcentral": "Postcentral",
    "posteriorcingulate": "Posterior cingulate",
    "precentral": "Precentral",
    "precuneus": "Precuneus",
    "rostralanteriorcingulate": "Rostral anterior cingulate",
    "rostralmiddlefrontal": "Rostral middle frontal",
    "superiorfrontal": "Superior frontal",
    "superiorparietal": "Superior parietal",
    "superiortemporal": "Superior temporal",
    "supramarginal": "Supramarginal",
    "temporalpole": "Temporal pole",
    "transversetemporal": "Transverse temporal",
}


@dataclass(frozen=True)
class Region:
    """A named search region: a set of labels of one fsaverage atlas."""

    id: str
    name: str
    atlas: str
    label_names: tuple[str, ...]


def _region(key: str, name: str, atlas: str, label_names) -> Region:
    return Region(f"{atlas}:{key}", name, atlas, tuple(label_names))


# The selectable regions, in menu order. A region id never changes once released.
REGIONS: dict[str, Region] = {region.id: region for region in (
    _region("visual", "Visual cortex (all mapped areas)", "wang2015",
            [label for _, labels in _VISUAL_AREAS for label in labels]),
    *(_region(name, name, "wang2015", labels) for name, labels in _VISUAL_AREAS),
    _region("FEF", "FEF", "wang2015", ["FEF"]),
    *(_region(name, label, "wang2015", [name]) for name, label in _HALVES),
    *(_region(f"lobe-{lobe}", name, "aparc", labels) for lobe, name, labels in _LOBES),
    *(_region(label, name, "aparc", [label])
      for label, name in sorted(_DK_NAMES.items(), key=lambda item: item[1])),
)}


@dataclass(frozen=True)
class RegionAtlas:
    """One atlas of fsaverage-regions.bin: a label per fsaverage vertex and hemisphere."""

    id: str
    name: str
    labels: dict[str, np.ndarray]
    table: list[dict]

    def values(self, region: Region) -> np.ndarray:
        by_name = {entry["name"]: entry["value"] for entry in self.table}
        missing = [name for name in region.label_names if name not in by_name]
        if missing:
            raise ValueError(f"{self.id} has no label {', '.join(missing)}")
        return np.asarray([by_name[name] for name in region.label_names])


def read_region_atlas(atlas_dir: Path, atlas: str, vertex_count: int) -> RegionAtlas:
    """Read one atlas of fsaverage-regions.bin, checked against fsaverage's vertex count."""

    manifest = json.loads((atlas_dir / REGIONS_MANIFEST).read_text(encoding="utf-8"))
    data = (atlas_dir / REGIONS_ASSET).read_bytes()
    atlases = [entry["id"] for entry in manifest["atlases"]]
    count = int.from_bytes(data[:4], "little") if len(data) >= 4 else -1
    if count != vertex_count or len(data) != 4 + 2 * count * len(atlases):
        raise ValueError("invalid cortical region atlas")
    index = atlases.index(atlas)
    labels = np.frombuffer(data, dtype=np.uint8, offset=4).reshape(len(atlases), 2, count)[index]
    entry = manifest["atlases"][index]
    if labels.max() >= len(entry["labels"]):
        raise ValueError(f"{atlas} has labels outside its colour table")
    return RegionAtlas(atlas, entry["name"], {"lh": labels[0], "rh": labels[1]}, entry["labels"])
