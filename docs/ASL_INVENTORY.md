# ASL pipeline coverage

This tracks issue [#54](https://github.com/neurodesk/neurocontainers/issues/54)
against the [OSIPI Task Force 1.1 ASL pipeline inventory](https://docs.google.com/document/d/e/2PACX-1vQ-1GF2fmz6Q4IukuKP_-57H-xi872Xq_uBlX5P0Cwpj4RYd_t73pvZ64UqXegPaVpQJhQQrVRJRPro/pub),
retrieved on 2026-10-03. The 22 names below reproduce its first table. Links
identify distribution or publication evidence from that table; a listed source
is not an assertion that its license permits redistribution or that it works
in a container.

Existing recipes and release records establish availability. They do not prove
scientific accuracy or compatibility with every acquisition. Missing pipelines
need a source and license review, a pinned recipe, and a real runtime smoke test.
Do not close #54 when only this inventory or one new recipe lands.

| OSIPI pipeline | NeuroContainers coverage | Source or next packaging step |
| --- | --- | --- |
| ASAP | Blocked: registration, no licence | [ASAP 2.0 download](https://sites.google.com/site/asltoolbox/download_2_0) requires a contact form, and the site states no licence or redistribution terms (checked 2026-10-04). It needs MATLAB R2015+, SPM12 and FSL. Ask the authors (virginia.mato@udc.es) for redistribution terms. |
| ASLtbx | Blocked: mixed licence | The UPenn page did not resolve on 2026-10-04. The author's mirror, [zewangnew/ASLtbx](https://github.com/zewangnew/ASLtbx), has no top-level licence. Of its 165 `.m` files, five are GPL-3.0, others carry only "Copyright by Ze Wang", and 15 are MathWorks-copyrighted. Its README says the code needs updates for recent MATLAB. Ask the author for a single licence and remove the MathWorks files before packaging. |
| ASL-MRICloud | External service | [MRICloud](https://braingps.mricloud.org/). The inventory links a cloud service, not a redistributable local installer. Local source and licensing remain prerequisites. |
| aslm | Low priority: GPL-2.0, abandoned | [aslm-0.1.zip](https://sourceforge.net/projects/aslm/files/) (sha256 `63f318b0…dc72c`), GPL-2.0, last changed 2014. It is an interactive MATLAB class library built on SPM8, MarsBaR and GIFT, with no command-line entrypoint. Its ASL subtraction routine is the Federspiel/Jann (Bern) modification of the UPenn code. Packaging needs MATLAB with SPM8 or an Octave port, plus a scripted entrypoint. |
| ASLPrep | [aslprep](../recipes/aslprep/build.yaml) | Existing recipe and releases. [Upstream documentation](https://aslprep.readthedocs.io/). |
| BASIL | [fsl](../recipes/fsl/build.yaml) | FSL installs OXASL/ASL components. [BASIL documentation](https://asl-docs.readthedocs.io/en/latest/). FSL distribution terms apply. |
| CereFlow | Missing, vendor review | [Translational MRI](https://www.transmri.com/). No source or download is linked in the inventory. Obtain the vendor's installation and redistribution terms. |
| CLINICAL ASL-CVR | Blocked: proprietary QASL | [ClinicalASL v1.0.4](https://github.com/JSIERO/ClinicalASL), BSD-3-Clause. Both Python inference methods (`ssvb`, `vaby`) call the commercial `qasl` binary, which needs a [Quantified Imaging](https://quantified-imaging.com/) licence key, so the Python pipeline cannot run in a public image. The MATLAB version uses FSL BASIL, but it needs MATLAB and ships Windows-only binaries (an elastix DLL and a `.exe`). Possible routes: an open QASL-free inference path upstream, or a MATLAB Compiler build. |
| ENABLE | Not redistributable | Implemented as [oxasl_enable](https://github.com/physimals/oxasl_enable), an optional oxasl plugin. Its Oxford academic licence (§1.1(e)) forbids distribution in any form, so it cannot ship in a public image. It is deliberately excluded from the Quantiphyse image. |
| ExploreASL | [exploreasl](../recipes/exploreasl/build.yaml) | Existing recipe and releases. [Upstream](https://exploreasl.org/). |
| FASL, Functional ASL | Blocked: source withdrawn | The linked `HernandezGarciaLab/Matlab_ASL_repo` no longer exists (checked 2026-10-04). That account now hosts only `ASLvideos`, `umvsasl` (a pulse sequence) and `website`. Ask the lab for a public source and licence. |
| Iris pipeline | Missing, source needed | [Publication](https://onlinelibrary.wiley.com/doi/10.1002/hbm.22522/abstract). The inventory has no download link. Obtain source and redistribution terms. |
| LOFT-CBF | [loftcbf](../recipes/loftcbf/build.yaml) | [kayjann/LOFT-CBF](https://github.com/kayjann/LOFT-CBF), GPL-3.0, pinned by commit. Runs in GNU Octave with SPM12 r7771 (Octave patch) and the NIfTI toolbox. The smoke test checks mean CBF against the toolbox's pCASL model on synthetic data. The toolbox scales CBF by a fixed MoCSF constant and uses M0 only for masking. |
| milxASL | Missing, source needed | [Publication](https://doi.org/10.1016/j.neuroimage.2015.05.048). No installer or source is linked in the inventory. Obtain the software and license. |
| mjdASL | Blocked: no public source | The Donahue Lab MATLAB ASL toolkit has no public repository or stated licence, and the [lab software page](https://www.vumc.org/donahue-lab/software) returned HTTP 500 on 2026-10-04. Request source and licence from the lab. |
| nordicICE, nICE | Missing, vendor review | [Inventory publication](https://iopscience.iop.org/article/10.1088/1361-6560/aaf83b). No downloadable source is linked. Establish vendor permission, supported Linux runtime and license handling. |
| Philips 3D ASL | Scanner application | [Philips product](https://www.philips.co.uk/healthcare/product/HCNMRB967/3d-asl-mr-clinical-application). Establish whether a separate redistributable Linux analysis package exists. A scanner option cannot be assumed to be a container installer. |
| Quantiphyse, ASL | [quantiphyse](../recipes/quantiphyse/build.yaml) (#3354) | Quantiphyse with its ASL and Fabber plugins, OXASL, and a minimal FSL Fabber backend. OXASL_ENABLE is excluded because its licence forbids redistribution. `oxasl_mp` is built from the Apache-2.0 upstream commit, because the PyPI 2.0.1 release carries the Oxford licence and lacks `run()`. |
| SCRUB-ASL | [aslprep](../recipes/aslprep/build.yaml) | ASLPrep implements SCORE/SCRUB in `aslprep.interfaces.cbf.ScoreAndScrubCBF`. No standalone package is needed. |
| Super-selective pCASL CBF | Missing, source needed | [Publication](https://www.sciencedirect.com/science/article/abs/pii/S0730725X18301309). Obtain source, license and an example acquisition. |
| VANDPIRE | Not redistributable | [VANDPIRE on NITRC](https://www.nitrc.org/projects/vandpire) is a Python GUI distributed through the [VU e-Innovations](http://vandpire.vueinnovations.com/licensing) portal under a free, non-transferable academic licence that requires approval. A public image would bypass that approval. |
| Andrea Federspiel | Related to aslm | No versioned package is published under this name. The Bern subtraction code by Federspiel and Jann ships inside aslm, and Kay Jann's later code is LOFT-CBF. Treat this row as covered by those two unless the authors name another program. |

[HCP-ASL](../recipes/hcpasl/build.yaml) is also available, although it is not a
separate entry in this inventory.

## Category rollout

The category name is `arterial spin labelling`. The builder and
[NeuroContainers UI registry](https://github.com/neurodesk/neurocontainers-ui/blob/main/components/common.ts)
must accept that exact spelling before recipes emit it. Registry support alone
adds no category to release metadata. The UI change is deployed
(neurocontainers-ui#65), and `aslprep`, `exploreasl`, `hcpasl` and `fsl` carry
the category (#3356). The Quantiphyse recipe carries it from its first
release. Existing categories remain useful alongside ASL.
