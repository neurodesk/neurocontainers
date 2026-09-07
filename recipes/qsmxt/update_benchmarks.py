#!/usr/bin/env python3
"""Refresh the QSM-CI snapshot and regenerate the OpenRecon benchmark tables."""

import argparse
import ast
import hashlib
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent
BASE_URL = "https://qsmxt.github.io/QSM-CI/"
SNAPSHOT = ROOT / "qsmci-in-silico-2019.json"
START = "<!-- BEGIN QSM-CI benchmarks -->"
END = "<!-- END QSM-CI benchmarks -->"
SLUGS = {
    "romeo": "romeo-qsmrs",
    "laplacian": "laplacian-qsmci",
    "fansi": "fansi-nltv-qsmrs",
    "fansi-tgv": "fansi-nltgv-qsmrs",
    **{name: f"{name}-qsmrs" for name in (
        "rts", "tv", "tkd", "tsvd", "tgv", "tikhonov", "nltv", "medi",
        "tfi", "ilsqr", "qsmart", "ndi", "l1qsm", "whqsm", "hdqsm",
        "amp-pe", "vsharp", "pdf", "lbv", "ismv", "sharp", "resharp",
        "harperella", "iharperella",
    )},
    **{name: name for name in (
        "xqsm", "qsmnet", "qsmnet-plus", "autoqsm", "qsmgan", "ir2qsm",
        "lpcnn", "modl-qsm", "nextqsm", "iqsm", "iqsm-plus", "bfrnet", "iqfm",
    )},
}
STAGES = {
    "dipole": "Inversion from true local field",
    "bfr": "Background removal from true total field",
    "field-mapping": "Field mapping from phase",
    "bfr+dipole": "QSM from true total field",
    "unwrap+bfr": "Local field from phase",
    "end-to-end": "QSM from phase",
}


def presets() -> dict[str, dict[str, str | None]]:
    tree = ast.parse((ROOT / "qsmxt.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "ALGORITHM_PIPELINE_PRESETS"
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError("Cannot find OpenRecon pipeline presets")


def read_source(path: str | None, url: str) -> bytes:
    if path:
        return Path(path).read_bytes()
    with urlopen(url, timeout=120) as response:
        return response.read()


def select_one(
    runs: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool]
) -> dict[str, Any] | None:
    matches = [run for run in runs if predicate(run)]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous benchmark selection: {[r['id'] for r in matches]}")
    if not matches:
        return None
    run = matches[0]
    selected = {key: run.get(key) for key in (
        "id", "slug", "stage", "mode", "variant", "combo", "phantom",
        "status", "runtime_s",
    )}
    selected["metrics"] = {key: run.get("metrics", {}).get(key) for key in ("xsim", "nrmse")}
    return selected


def refresh(args: argparse.Namespace, parameters: dict[str, Any]) -> None:
    index = read_source(args.index, BASE_URL + "results/index.json")
    manifest = read_source(args.algorithms, BASE_URL + "algorithms.json")
    hidden = {a["slug"] for a in json.loads(manifest)["algorithms"] if a.get("hidden")}
    runs = [r for r in json.loads(index)["runs"] if
            r["track"] == "sim" and r.get("phantom", "sim") == "sim"
            and r["slug"] not in hidden
            and not (set((r.get("combo") or {}).values()) & hidden)]
    algorithms = {}
    for key in ("qsmalgorithm", "unwrappingalgorithm", "bfalgorithm"):
        for choice in parameters[key]["values"]:
            name = choice["id"]
            if name == "default":
                continue
            slug = SLUGS[name]
            algorithms[name] = select_one(runs, lambda r:
                r["slug"] == slug and r["mode"] == "isolated"
                and r.get("variant", "default") == "default")
    pipelines = {}
    for name, settings in presets().items():
        qsm = SLUGS[settings["qsm_algorithm"]]
        unwrap = SLUGS.get(settings["unwrapping_algorithm"])
        bfr = SLUGS.get(settings["bf_algorithm"])
        combo = {"field_mapping": unwrap, "bfr": bfr, "dipole": qsm} if bfr else (
            {"field_mapping": unwrap} if unwrap else {})
        pipelines[name] = select_one(runs, lambda r:
            r["mode"] == "composed" and (r.get("combo") or {}) == combo
            and (bfr is not None or r["slug"] == qsm))
    snapshot = {
        "retrieved": date.today().isoformat(),
        "index_url": BASE_URL + "results/index.json",
        "index_sha256": hashlib.sha256(index).hexdigest(),
        "algorithms_url": BASE_URL + "algorithms.json",
        "algorithms_sha256": hashlib.sha256(manifest).hexdigest(),
        "algorithms": algorithms,
        "pipelines": pipelines,
    }
    SNAPSHOT.write_text(json.dumps(snapshot, indent=2) + "\n")


def result_cells(run: dict[str, Any] | None) -> str:
    if run is None:
        return "Not reported | Not reported | Not reported | No matching run"
    link = f"[run]({BASE_URL}submission.html?run={run['id']})"
    if run["status"] != "ok":
        return f"Not completed | Not completed | Not completed | {link}"
    seconds = run["runtime_s"]
    runtime = f"~{seconds:.0f} s" if seconds < 60 else f"~{seconds / 60:.1f} min"
    metrics = run["metrics"]
    return f"{metrics['xsim']:.3f} | {metrics['nrmse']:.1f}% | {runtime} | {link}"


def render(snapshot: dict[str, Any], parameters: dict[str, Any]) -> str:
    lines = [START, "", "### QSM-CI in silico 2019 benchmarks", "",
        f"Snapshot retrieved {snapshot['retrieved']} from the [QSM-CI results page]({BASE_URL}results.html).",
        "The tables use the default isolated run for each packaged implementation;",
        "tuned variants and retired implementations are excluded. The pipeline table",
        "uses the matching composed run starting from phase. Higher xSIM and lower",
        "NRMSE are better. Compare scores within the same input/stage, not across",
        "different reconstruction tasks.", "",
        "Times are rounded QSM-CI wall-clock measurements, not OpenRecon scanner",
        "estimates. Hardware, threading, acquisition size, masking, echo handling,",
        "and algorithm versions can differ. These are results on one simulated",
        "head phantom, not evidence of clinical accuracy. Standalone stage timings",
        "exclude the rest of the reconstruction and must not be added to predict",
        "scanner turnaround. Read each linked run for its benchmark details.", ""]
    for key, title in (("qsmalgorithm", "QSM algorithms"),
                       ("unwrappingalgorithm", "Phase unwrapping and field mapping"),
                       ("bfalgorithm", "Background removal")):
        lines += [f"#### {title}", "",
            "| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |",
            "| --- | --- | ---: | ---: | ---: | --- |"]
        for choice in parameters[key]["values"]:
            name = choice["id"]
            if name == "default":
                continue
            run = snapshot["algorithms"][name]
            stage = STAGES[run["stage"]] if run else "Not reported"
            lines.append(f"| `{name}` | {stage} | {result_cells(run)} |")
        lines += [""]
    lines += ["#### Full pipeline presets", "",
        "These measurements include the upstream reconstruction stages. They are",
        "separate from the scanner measurements in the earlier preset table.", "",
        "| Preset | xSIM ↑ | NRMSE ↓ | Runtime | Source |",
        "| --- | ---: | ---: | ---: | --- |"]
    for name in presets():
        lines.append(f"| `{name}` | {result_cells(snapshot['pipelines'][name])} |")
    lines += ["", "ROMEO and Laplacian rows measure field mapping, including echo handling,",
        "rather than unwrapping alone. HARPERELLA, iHARPERELLA, and iQFM benchmark",
        "joint unwrapping/background removal. QSMART refers to QSM.rs, not MATLAB.", "",
        "TFI is benchmarked from the true total field. The packaged QSMxT `run` dispatcher",
        "still applies the selected background-removal stage before TFI; its GUI",
        "selection therefore does not reproduce that isolated TFI benchmark.", "",
        "Regenerate these tables with `python3 recipes/qsmxt/update_benchmarks.py`.",
        "Use `--refresh` to fetch new results or `--check` to check the committed",
        "tables against `qsmci-in-silico-2019.json`. The snapshot records source hashes.",
        "", END]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--index", help="Local QSM-CI index for an offline refresh")
    parser.add_argument("--algorithms", help="Local algorithm manifest for an offline refresh")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check and args.refresh:
        parser.error("--check and --refresh cannot be combined")
    parameters = {p["id"]: p for p in json.loads((ROOT / "OpenReconLabel.json").read_text())["parameters"]}
    if args.refresh:
        refresh(args, parameters)
    generated = render(json.loads(SNAPSHOT.read_text()), parameters)
    readme = ROOT / "OpenReconREADME.md"
    text = readme.read_text()
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    updated = before + generated + after
    if args.check:
        if updated != text:
            raise SystemExit("Benchmark tables are stale; run update_benchmarks.py")
        print("Benchmark tables match the snapshot and GUI choices")
    else:
        readme.write_text(updated)


if __name__ == "__main__":
    main()
