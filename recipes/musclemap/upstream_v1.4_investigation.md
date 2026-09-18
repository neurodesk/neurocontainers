# MuscleMap whole-body model v1.4 update investigation

Research date: 2026-08-27

This note compares the current NeuroContainers MuscleMap recipe with the official upstream state that introduced the whole-body model v1.4. It uses the upstream GitHub repository and Zenodo deposits as primary sources.

## Finding

Upstream does not publish a MuscleMap source release or Git tag named `1.4`. The upstream version numbers now describe two different artifacts:

- `2.0` is the latest tagged MuscleMap software release. Upstream `setup.py`, `scripts.__version__`, and `version.txt` identify the current software as 2.0. [GitHub releases](https://github.com/MuscleMap/MuscleMap/releases), [setup.py at the audited commit](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/setup.py#L7-L23), [version.txt](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/version.txt)
- `1.4` is the whole-body model version. Pull request 88 added its immutable Zenodo record ID to the software and merged as commit `6e1e1eb6732337c13cab53bd5cc800c69024774f` on 2026-08-16. [Pull request 88](https://github.com/MuscleMap/MuscleMap/pull/88), [model-version map](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L54-L65), [Zenodo record 21929873](https://zenodo.org/records/21929873)

The container update therefore cannot be a tag substitution from `1.3` to `1.4`. It must package software 2.0 from an exact post-release commit and separately pin whole-body model 1.4.

## Audited baselines

The current recipe is `1.3.45`. It derives the source tag from the first two recipe-version components, so `1.3.45` resolves to upstream tag `1.3`. It installs that archive into `/opt/MuscleMap`. [Current build.yaml](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L1-L12), [install directives](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L54-L63)

That baseline already has an upstream asset mismatch. The PTH bundled by tag 1.3 has MD5 `472b4880647306dfeb9dbe290f51f1e2`, exactly matching the v1.3 Zenodo weight. The JSON bundled beside it reports `model.version` as 1.2. The immutable Zenodo v1.3 JSON differs only by reporting version 1.3. [Tag 1.3 config](https://github.com/MuscleMap/MuscleMap/blob/2a38b0712dd295186a060c9f49560660eb816d07/scripts/models/wholebody/contrast_agnostic_wholebody_model.json#L8-L17), [v1.3 Zenodo metadata and checksums](https://zenodo.org/api/records/19976940), [v1.3 Zenodo config](https://zenodo.org/records/19976940/files/contrast_agnostic_wholebody_model.json)

The v1.4 update must assert both file identities and the fields used to construct the network. Checking only a version string would not have caught the current mixed metadata.

The current upstream model support is commit `6e1e1eb6732337c13cab53bd5cc800c69024774f`. That commit is the merge of pull request 88 and was the upstream `main` head when this investigation ran. The latest source tag remains `2.0` at commit `9f569ea27de092e511e3e9f1960bf43e352c2182`. The official release list jumps from 1.3 to 2.0 and contains no 1.4 source release. [GitHub releases](https://github.com/MuscleMap/MuscleMap/releases), [tag 2.0](https://github.com/MuscleMap/MuscleMap/releases/tag/2.0), [merge commit](https://github.com/MuscleMap/MuscleMap/commit/6e1e1eb6732337c13cab53bd5cc800c69024774f)

Pin the source commit. Do not fetch `main`, because later upstream commits could change code without changing the recipe version.

## Model changes from v1.3 to v1.4

Zenodo publishes both versions under the same concept record. Model v1.3 is record `19976940`. Model v1.4 is record `21929873`. [v1.3 record](https://zenodo.org/records/19976940), [v1.4 record](https://zenodo.org/records/21929873)

| Property | Whole-body v1.3 | Whole-body v1.4 | Container impact |
| --- | ---: | ---: | --- |
| Published | 2026-05-02 | 2026-08-16 | Use the immutable v1.4 record, not the concept `latest` URL. |
| Weight size | 54,417,738 bytes | 104,884,148 bytes | The baked model grows by about 50.5 MB. |
| Weight MD5 | `472b4880647306dfeb9dbe290f51f1e2` | `910b722aeb641c380404c99ec6d1af97` | Assert the v1.4 file identity during the build or release test. |
| Config size and MD5 | 9,296 bytes, `d75bf2b324860c0188f80d2700e20a03` | 10,555 bytes, `b586ac488b2e40a4e8624a9a1c52d6b5` | Bake the config beside the weight. |
| Output channels | 100 | 114 | The v1.3 weights and v1.4 config cannot be mixed. |
| Residual units | 1 | 2 | Loading the state dictionary is the minimum useful compatibility test. |
| Label entries | 99 | 113 | Documentation, metrics lookup, and label tests must follow the v1.4 config. |

The record supplies `contrast_agnostic_wholebody_model.pth`, `contrast_agnostic_wholebody_model.json`, and `LICENSE`. The config sets `version` to 1.4, `out_channels` to 114, `num_res_units` to 2, and 113 label entries. [Zenodo API metadata](https://zenodo.org/api/records/21929873), [v1.4 model config](https://zenodo.org/records/21929873/files/contrast_agnostic_wholebody_model.json)

The label update is not append-only:

- v1.4 adds patella labels `7231` and `7232`, fibula labels `8171` and `8172`, and ten deep-leg muscle labels from `8181` through `8222`.
- Values `8151` and `8152` change meaning from tibia to lateral gastrocnemius.
- Values `8161` and `8162` change meaning from fibula to tibia.
- Values `8101` through `8122` change from compartment labels to individual muscles.
- Values `8141` and `8142` change from gastrocnemius to medial gastrocnemius.
- The config corrects the names of latissimus dorsi and tensor fasciae latae without changing their values.

These names and values come directly from the two immutable configs. [v1.3 config](https://zenodo.org/records/19976940/files/contrast_agnostic_wholebody_model.json), [v1.4 config](https://zenodo.org/records/21929873/files/contrast_agnostic_wholebody_model.json). The current upstream README also lists all 113 v1.4 labels. [Current label list](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L134-L252)

Treat the shifted leg values as a user-visible compatibility change. A segmentation with label `8151` means tibia in v1.3 and lateral gastrocnemius in v1.4. Any lookup table or downstream report that assumes the old meaning will be wrong without raising an error.

## Upstream software changes since tag 1.3

The full source comparison is [tag 1.3 to the audited commit](https://github.com/MuscleMap/MuscleMap/compare/1.3...6e1e1eb6732337c13cab53bd5cc800c69024774f). The update includes more than model weights.

### Model and template storage

Software 2.0 removes all model weights, model configs, and abdomen templates from Git. It downloads them from Zenodo into directories below the installed package. The source uses these paths:

- `scripts/models/<region>/v<version>/` for model weights and configs
- `scripts/templates/<region>/` for templates

The resolver contacts Zenodo when callers request `latest`. A caller that requests an explicit version uses an already cached pair before any network request. [Cache paths and record map](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L23-L85), [explicit-version resolution](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L168-L225), [latest-version resolution](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L227-L271)

This design does not work unchanged in a read-only SIF. The first-use download target is below `/opt/MuscleMap`, and the runtime cannot write there. A network-enabled run can also move to a later model because both `mm_segment` and `mm_extract_metrics` default to `--model_version latest`. [mm_segment option](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_segment.py#L72-L85), [mm_extract_metrics option](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_extract_metrics.py#L43-L53)

The recipe must declare the immutable Zenodo files, use `get_file()` in the build directives, and copy the pair to `/opt/MuscleMap/scripts/models/wholebody/v1.4/`. The OpenRecon bridge must pass an explicit model version to both upstream commands.

### Launchers and behavior

Upstream still installs four console scripts: `mm_segment`, `mm_extract_metrics`, `mm_gui`, and `mm_register_to_template`. [Entry points](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/setup.py#L15-L21)

The behavior changes that affect the recipe are:

- `mm_segment` adds `--model_version`, resolves the selected model through Zenodo, and logs the model version from the config. Existing `-i`, `-r`, `-c`, `-s`, and `-g` calls remain accepted. [Parser and startup](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_segment.py#L54-L110), [model loading](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_segment.py#L125-L167)
- `mm_extract_metrics` adds `--model_version` and `--qc`. A region-specific metrics run now resolves its model config through Zenodo. [Metrics parser and config loading](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_extract_metrics.py#L22-L81)
- `mm_extract_metrics --qc` loads the new `scripts/mm_qc_gui.py`. Upstream does not add a separate console entry point for that module. [v2.0 release notes](https://github.com/MuscleMap/MuscleMap/releases/tag/2.0), [QC call path](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L1097-L1128)
- `mm_gui` discovers cached versions for its segmentation panel and passes the chosen version there. Its metrics panel and chained workflow do not pass `--model_version`, so those paths still resolve `latest`. [Segmentation panel](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_gui.py#L585-L638), [metrics panel](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_gui.py#L640-L704), [chained workflow](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_gui.py#L739-L760)
- `mm_register_to_template` still expects Spinal Cord Toolbox 6.5. Upstream documents and checks that version. The registration command supports abdomen templates, not the `wholebody` example in the current recipe README. [Upstream installation note](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L117-L119), [registration parser](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_register_to_template.py#L11-L64), [current incorrect example](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L112-L123)

The `mm_extract_metrics` help text gives 2.0 as an example model version even though the configured whole-body model versions end at 1.4. This text confuses the software version with the model version. [Metrics option text](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_extract_metrics.py#L49-L53), [configured model versions](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L54-L65). Container documentation must use 1.4 for `--model_version`.

### Dependencies

The upstream source now requires Python 3.11 or newer. Its README asks for Python 3.11.8. [setup.py](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/setup.py#L23), [installation instructions](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L24-L47)

Compared with tag 1.3, the audited requirements make three changes:

- `torch` changes from 2.4.0 to 2.4.1.
- `customtkinter==5.2.2` is added.
- `Pillow==11.3.0` is added.

All other direct requirements keep their existing pins. [Current requirements](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/requirements.txt), [tag 1.3 requirements](https://github.com/MuscleMap/MuscleMap/blob/2a38b0712dd295186a060c9f49560660eb816d07/requirements.txt)

The current recipe uses a PyTorch 2.4.0 CUDA 11.8 base and then forces Pillow 10.4.0 after installing MuscleMap. Both pins conflict with the audited upstream requirements. [Current dependency directives](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L30-L35), [Pillow override](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L56-L63)

Upstream has one inconsistency. `requirements.txt` pins torch 2.4.1, but the README still tells GPU users to install PyTorch 2.4.0. [requirements.txt](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/requirements.txt#L1-L2), [README GPU instructions](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L85-L107). Use the machine-readable 2.4.1 pin for the update, then prove CUDA 11 compatibility in the built image.

### Platforms and architectures

Upstream documents CPU execution, NVIDIA CUDA, and AMD ROCm. It says ROCm support is Linux-only. Upstream does not publish an architecture matrix or CI workflow, and `setup.py` has no architecture markers. [GPU instructions](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L85-L115), [setup.py](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/setup.py)

The current recipe declares only `x86_64`. Keep that architecture for this update. The official `pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime` tag exists as a Linux `amd64` image, so the base can move with the upstream torch pin without changing the scanner CUDA 11 contract. [PyTorch image tag](https://hub.docker.com/r/pytorch/pytorch/tags?name=2.4.1-cuda11.8-cudnn9-runtime)

### Licenses

The audited GitHub source and the whole-body model v1.4 are MIT licensed. [Source LICENSE](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/LICENSE), [v1.4 Zenodo metadata](https://zenodo.org/api/records/21929873). Spinal Cord Toolbox 6.5 remains LGPL-3.0. [SCT 6.5 LICENSE](https://github.com/spinalcordtoolbox/spinalcordtoolbox/blob/6.5/LICENSE)

The source 2.0 move to Zenodo matters if the container preserves legacy regional models and registration templates. Zenodo marks the pelvis and leg records and the abdomen template record as CC-BY-4.0, while the `LICENSE` file uploaded inside those records contains the MIT text. [Pelvis record](https://zenodo.org/api/records/19632902), [leg record](https://zenodo.org/api/records/19633057), [abdomen template record](https://zenodo.org/api/records/20043148). Do not silently copy the old recipe's MIT-only declaration onto these separately published assets. Record CC-BY-4.0 in `copyright` if the update bakes them, or ask upstream to resolve the metadata conflict first.

## NeuroContainers impact

### Source and version variables

The existing `musclemap_source_tag` expression would turn a recipe version such as `1.4.x` into source tag `1.4`. That archive does not exist. [Current variables](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L6-L12), [upstream release list](https://github.com/MuscleMap/MuscleMap/releases)

Decouple these identities in the recipe:

- the NeuroContainers release version, proposed as `1.4.0` so the image continues to track the packaged whole-body model
- the exact MuscleMap source commit
- the upstream software version, 2.0
- the whole-body model version, derived from `context.version` where practical
- the immutable Zenodo record ID, 21929873

Use the source commit in the archive URL and extracted-directory name. Do not construct a source tag from the model version.

### Build assets and offline behavior

Declare both v1.4 whole-body files in `files:` and reference them with `get_file()` in `run:` directives. Install them at the cache path that upstream already understands:

```text
/opt/MuscleMap/scripts/models/wholebody/v1.4/
├── contrast_agnostic_wholebody_model.json
└── contrast_agnostic_wholebody_model.pth
```

The immutable file endpoints are listed in the [Zenodo API record](https://zenodo.org/api/records/21929873). Do not call Zenodo through `curl` or `wget` in a build directive.

Software 2.0 also removes the legacy regional weights and abdomen templates that tag 1.3 carried in the source tree. The current recipe exposes abdomen, pelvis, thigh, and leg in the OpenRecon configuration, and it deploys `mm_register_to_template`. [Current region choices](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/OpenReconLabel.json#L141-L197), [current deployed commands](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/build.yaml#L96-L101)

Preserve the existing commands and region choices. Bake all five upstream v0.0 regional model pairs for abdomen, forearm, leg, pelvis, and thigh. Bake the complete abdomen template record too. Install every asset in the cache path expected by upstream. This keeps the current OpenRecon choices, `mm_gui`, and `mm_register_to_template` usable without runtime writes or downloads. It also preserves forearm through the direct CLI, where upstream has a record but neither GUI currently lists it. The upstream record IDs are defined in `ZENODO_MODELS` and `ZENODO_TEMPLATES`. [Model record map](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L28-L66), [template record map](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L273-L333)

The six model pairs and ten abdomen template NIfTIs total 630,133,839 bytes before source code, configs, and license files. This is about 50.5 MB more model data than the current image because the v1.4 whole-body weight replaces the smaller v1.3 weight.

Pin `wholebody` to 1.4 and every regional model to 0.0. Make the label transform region-aware. Whole-body output uses the reversible transformed coding. Regional output keeps its native dense labels.

### OpenRecon bridge

The bridge currently runs `mm_segment` and `mm_extract_metrics` without `--model_version`. [Segmentation command](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/musclemap.py#L3924-L3942), [metrics command](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/musclemap.py#L3329-L3350). Under software 2.0, both commands would contact Zenodo for `latest` and could fail against a read-only `/opt` tree or select a later model.

Pass the pinned version on both paths. For `wholebody`, pass 1.4. If the image preserves legacy regional models, pass 0.0 for those regions. Derive the command value from one recipe-controlled constant so segmentation and metrics cannot use different configs.

Apply the same rule to every deployed launcher. Direct CLI calls default to `latest`, and upstream `mm_gui` omits a version on its metrics and chained-workflow paths. Either patch those paths to choose the baked version or stop deploying `mm_gui` until it can honor the container pin. A read-only image cannot promise model 1.4 while a launcher still asks Zenodo for `latest`.

The current bridge's label transform is reversible for all v1.4 whole-body values, including the new maximum `8222`. The existing unit test covers only values through `1122`. [Transform functions](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/musclemap.py#L3549-L3563), [current test](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/test_musclemap_openrecon.py#L260-L304). Extend the test through `8222` and include the shifted `8151`, `8161`, and `8171` values.

Legacy regional models use dense values such as 1 through 28, not the whole-body `xx0`, `xx1`, and `xx2` coding. The bridge applies the whole-body transform whenever `labeltransform` is true. A full preservation of regional OpenRecon choices therefore needs region-aware label handling. Baking the assets alone is not enough. The upstream configs define those dense values. [Legacy model records](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_util.py#L28-L53), [current unconditional transform](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/musclemap.py#L4495-L4521)

### Documentation and metadata

Update both `build.yaml` readme text and `OpenReconREADME.md`:

- Say that the image packages MuscleMap software 2.0 at the pinned commit and whole-body model 1.4 from Zenodo.
- Link whole-body labels to the immutable v1.4 config. The current GitHub `scripts/models` link cannot work with software 2.0 because upstream removed the file.
- Document the changed leg label meanings.
- Add `--model_version` to direct CLI examples.
- Change the registration example from `wholebody` to `abdomen`.
- Keep the scanner-specific spatial overlap default only if it is intentional. Upstream's code default is 90, the current README recommendation for large scans is 75, and the upstream parser help still says 50. [README recommendation](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/README.md#L254-L260), [parser defaults](https://github.com/MuscleMap/MuscleMap/blob/6e1e1eb6732337c13cab53bd5cc800c69024774f/scripts/mm_segment.py#L78-L85)
- Preserve the existing base64 icon and `body` category. They already satisfy recipe validation.
- Keep MIT and LGPL-3.0. Add CC-BY-4.0 for the Zenodo assets whose records declare it.
- Replace the hardcoded `1.3.0` component in the OpenRecon UDI with `VERSION_WILL_BE_REPLACED_BY_SCRIPT`, as used by the other versioned regulatory fields. [Current OpenRecon UDI](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/OpenReconLabel.json#L8-L19)

Update the recipe and `fulltest.yaml` versions together. The current fulltest still declares `1.3.45`. [Current fulltest header](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/fulltest.yaml#L1-L6)

Committed release metadata currently ends at `releases/musclemap/1.3.10.json`, while the recipe and fulltest are already 1.3.45. [Committed MuscleMap releases](https://github.com/neurodesk/neurocontainers/tree/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/releases/musclemap). Do not hand-edit an old release JSON. Build and test the new image first, then generate fresh release metadata through the normal release workflow.

## Recommended implementation sequence

1. Set the new recipe version and decouple the source commit, software version, model version, and Zenodo record variables.
2. Change the base to PyTorch 2.4.1 with CUDA 11.8. Remove the Pillow 10.4.0 override and assert Python 3.11 or newer, torch 2.4.1, CUDA 11.x, customtkinter 5.2.2, and Pillow 11.3.0 after installation.
3. Declare the source archive and every retained Zenodo asset in `files:`. Use only `get_file()` to unpack or copy them. Install the whole-body pair in `scripts/models/wholebody/v1.4/`.
4. Bake the five v0.0 regional model pairs and all abdomen template NIfTIs. Add the applicable Zenodo license metadata.
5. Pass the explicit model version in the OpenRecon segmentation and metrics subprocesses. Add region-aware version selection for the retained regional models. Make the direct CLI and `mm_gui` paths honor the same baked versions.
6. Make label validation and transformation region-aware. Whole-body uses the existing transformed coding. Regional models retain their native dense labels. Do not ship the current regional choices with software 2.0 until a real regional run passes.
7. Update the readmes, label links, CLI examples, model identity, and leg-label compatibility note.
8. Update focused unit tests before the image build. Then generate the Dockerfile, validate the recipe, build the x86_64 image, and run the release tests.
9. Measure a representative whole-body scanner run before retaining the current OpenRecon resource declaration. The v1.4 model doubles residual units and adds 14 output channels. Confirm that auto-chunking and retry behavior stay within `min_required_gpu_memory = 10048` and `min_required_memory = 40096`, or update those values from measured peaks.

## Verification contract

The current fulltest checks launcher help, CUDA 11, OpenRecon imports, and local assets. It does not prove which upstream software or model is installed. [Current fulltest](https://github.com/neurodesk/neurocontainers/blob/cf9a6e80aeac99a42f40ad1e344b8bdbed998dd9/recipes/musclemap/fulltest.yaml#L16-L68)

The local pre-update baseline is green when run with NumPy and Pillow available: all 25 tests in `test_musclemap_openrecon.py` pass, `builder/validation.py` accepts `build.yaml`, and `validate_openrecon_labels.py` accepts the MuscleMap label against the OpenRecon 1.1.0 schema. Preserve that baseline while adding the version and asset checks.

Upstream pull request 88 changed the README, one Zenodo record mapping, `version.txt`, `.gitignore`, and an image asset. It added no automated tests. [Pull request 88 files](https://github.com/MuscleMap/MuscleMap/pull/88/files). NeuroContainers must supply the packaging and runtime proof.

Add these checks:

- Assert `scripts.__version__ == "2.0"` and inspect installed package metadata.
- Assert Python is at least 3.11, torch is 2.4.1, and `torch.version.cuda` starts with 11.
- Resolve `get_model_and_config_paths("wholebody", version="1.4")` with outbound network disabled. Assert both paths are below `/opt/MuscleMap/scripts/models/wholebody/v1.4`.
- Assert the baked config reports version 1.4, 114 output channels, two residual units, 113 labels, and the exact values for `8151`, `8161`, `8171`, and `8222`.
- Load the v1.4 state dictionary into the UNet described by the config. This catches a mixed v1.3 weight and v1.4 config without requiring a full segmentation run.
- Run `mm_extract_metrics -m average` on a tiny generated NIfTI and segmentation with `--model_version 1.4`. Assert that the output CSV uses v1.4 anatomy names.
- Exercise `_build_mm_segment_command` and `_run_mm_extract_metrics` unit tests. Assert that both commands carry the same pinned version.
- Round-trip the label transform for all 113 v1.4 values, not a hand-picked prefix.
- Run one tiny real regional inference or a model-load test per retained region. Assert that regional labels bypass the whole-body transform.
- Assert that the baked abdomen template resolver works without network access.
- Run one scanner-format OpenRecon sample through segmentation, metrics, label transformation, and output composition. Confirm that the new leg labels survive the MRD and DICOM metadata paths.
- Record peak GPU and host memory for that scanner sample. Confirm that the declared resource minimums cover the measured peaks with operational headroom.

Run the repository checks after those focused tests:

```bash
python3 builder/validation.py recipes/musclemap/build.yaml
python -m builder generate musclemap --recreate
pytest recipes/musclemap/test_musclemap_openrecon.py
sf-build musclemap
sf-test musclemap
```

The final acceptance condition is specific: the image must complete whole-body segmentation and metrics with model v1.4 while Zenodo is unreachable and `/opt` is read-only. Launcher help and import tests do not prove that contract.
