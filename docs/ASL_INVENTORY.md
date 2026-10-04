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
| ASAP | Missing | [ASL toolbox site](https://sites.google.com/site/asltoolbox/). Establish a working download, version, license, and MATLAB requirements. |
| ASLtbx | Missing | [University of Pennsylvania distribution](https://cfn.upenn.edu/zewang/ASLtbx.php). Pin the downloadable MATLAB/SPM toolbox; check redistribution terms and required MATLAB runtime. |
| ASL-MRICloud | External service | [MRICloud](https://braingps.mricloud.org/). The inventory links a cloud service, not a redistributable local installer. Local source and licensing remain prerequisites. |
| aslm | Missing | [SourceForge project](https://aslm.sourceforge.net/). Review the released source and its license, and resolve MATLAB/SPM dependencies. |
| ASLPrep | [aslprep](../recipes/aslprep/build.yaml) | Existing recipe and releases. [Upstream documentation](https://aslprep.readthedocs.io/). |
| BASIL | [fsl](../recipes/fsl/build.yaml) | FSL installs OXASL/ASL components. [BASIL documentation](https://asl-docs.readthedocs.io/en/latest/). FSL distribution terms apply. |
| CereFlow | Missing, vendor review | [Translational MRI](https://www.transmri.com/). No source or download is linked in the inventory. Obtain the vendor's installation and redistribution terms. |
| CLINICAL ASL-CVR | Missing | [ClinicalASL source](https://github.com/JSIERO/ClinicalASL). Check license, MATLAB dependencies, pinned release or commit, and a supported entrypoint. |
| ENABLE | Missing, source needed | [Publication](https://pubmed.ncbi.nlm.nih.gov/28681479/). The inventory supplies publication/contact information but no download. Identify the standalone package or document an equivalent existing implementation. |
| ExploreASL | [exploreasl](../recipes/exploreasl/build.yaml) | Existing recipe and releases. [Upstream](https://exploreasl.org/). |
| FASL, Functional ASL | Missing | [Matlab_ASL_repo](https://github.com/HernandezGarciaLab/Matlab_ASL_repo). Select the pipeline's entrypoint, license, immutable source and MATLAB dependencies. |
| Iris pipeline | Missing, source needed | [Publication](https://onlinelibrary.wiley.com/doi/10.1002/hbm.22522/abstract). The inventory has no download link. Obtain source and redistribution terms. |
| LOFT-CBF | Missing, source needed | The inventory lists the USC LOFT group and an author contact, with no download or publication link. Obtain an identifiable release and license. |
| milxASL | Missing, source needed | [Publication](https://doi.org/10.1016/j.neuroimage.2015.05.048). No installer or source is linked in the inventory. Obtain the software and license. |
| mjdASL | Missing | [Donahue Lab software](https://www.vumc.org/donahue-lab/software). Identify the exact mjdASL artifact and its license; distinguish it from VANDPIRE on the same page. |
| nordicICE, nICE | Missing, vendor review | [Inventory publication](https://iopscience.iop.org/article/10.1088/1361-6560/aaf83b). No downloadable source is linked. Establish vendor permission, supported Linux runtime and license handling. |
| Philips 3D ASL | Scanner application | [Philips product](https://www.philips.co.uk/healthcare/product/HCNMRB967/3d-asl-mr-clinical-application). Establish whether a separate redistributable Linux analysis package exists. A scanner option cannot be assumed to be a container installer. |
| Quantiphyse, ASL | Recipe in development | [ASL documentation](https://quantiphyse.readthedocs.io/en/latest/asl/asl.html), [Quantiphyse](https://github.com/physimals/quantiphyse), [ASL plugin](https://github.com/physimals/quantiphyse-asl). Verify released GUI and ASL/Fabber packages together, including the numerical backend. |
| SCRUB-ASL | Missing, source needed | [Publication](https://onlinelibrary.wiley.com/doi/abs/10.1002/jmri.25436). No download is linked. Determine whether standalone source is available or the method is exposed by an existing pipeline. |
| Super-selective pCASL CBF | Missing, source needed | [Publication](https://www.sciencedirect.com/science/article/abs/pii/S0730725X18301309). Obtain source, license and an example acquisition. |
| VANDPIRE | Missing | [Donahue Lab software](https://www.vumc.org/donahue-lab/software). Select a licensed version and dependencies; test separately from mjdASL. |
| Andrea Federspiel | Missing, package identity needed | [University of Bern profile](https://www.upd.unibe.ch/about_us/personen/prof_dr_phil_nat_federspiel_andrea/index_eng.html). This row names a person, not a versioned package. Identify the intended program and obtain source/license. |

[HCP-ASL](../recipes/hcpasl/build.yaml) is also available, although it is not a
separate entry in this inventory.

## Category rollout

The category name is `arterial spin labelling`. The builder and
[NeuroContainers UI registry](https://github.com/neurodesk/neurocontainers-ui/blob/main/components/common.ts)
must accept that exact spelling before recipes emit it. Registry support alone
adds no category to release metadata. After the UI change is deployed, tag
`aslprep`, `exploreasl`, `hcpasl`, `fsl`, and a tested Quantiphyse recipe, then
verify catalogue filtering. Existing categories remain useful alongside ASL.
