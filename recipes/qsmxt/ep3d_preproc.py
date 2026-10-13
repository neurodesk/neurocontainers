"""Run the ep3d-highres-preprocessing complex workflow on one 4D magnitude/phase pair.

Wraps ``preproc_complex`` from
https://github.com/mrphysics-bonn/ep3d-highres-preprocessing (basic-preprocessing):
MCFLIRT motion correction on magnitude applied to real/imaginary, temporal
phase matching, complex averaging and optional ANTs NLM denoising. Writes the
averaged magnitude/phase paths to a JSON file.
"""

import argparse
import json
import os
import sys
from pathlib import Path

DEFAULT_EP3D_DIR = "/opt/ep3d-highres-preprocessing/basic-preprocessing"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("magnitude")
    parser.add_argument("phase")
    parser.add_argument("work_dir")
    parser.add_argument("outputs_json")
    parser.add_argument("--denoise", action="store_true")
    parser.add_argument("--n-procs", type=int, default=1)
    args = parser.parse_args()

    sys.path.insert(0, os.environ.get("EP3D_PREPROC_DIR", DEFAULT_EP3D_DIR))
    from complex_nifti_preproc import preproc_complex

    workflow = preproc_complex(base_dir=str(Path(args.work_dir).resolve()))
    workflow.inputs.inputnode.magn_file = str(Path(args.magnitude).resolve())
    workflow.inputs.inputnode.phas_file = str(Path(args.phase).resolve())
    if args.denoise:
        result_node = "cart2pol_denoise"
    else:
        result_node = "cart2pol"
        workflow.remove_nodes(
            [workflow.get_node("denoise"), workflow.get_node("cart2pol_denoise")]
        )

    graph = workflow.run(plugin="MultiProc", plugin_args={"n_procs": args.n_procs})
    node = next(node for node in graph.nodes() if node.name == result_node)
    outputs = node.result.outputs
    Path(args.outputs_json).write_text(
        json.dumps({"magnitude": outputs.out_file1, "phase": outputs.out_file2})
    )


if __name__ == "__main__":
    main()
