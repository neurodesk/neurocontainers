# qDESS echo and spoiler metadata in OpenMSK

Research note, 2026-09-02. This note separates standard DICOM attributes from the private-element convention that DOSMA uses. The private creator matters. `(0019,10B6)` by itself is not a vendor-neutral identifier.

## Short answer

| Quantity | Standard or private representation | What OpenMSK currently does | Confidence |
| --- | --- | --- | --- |
| TE1 | Classic MR: Echo Time `(0018,0081)` on echo 1, with Echo Number(s) `(0018,0086)`. Enhanced MR: Effective Echo Time `(0018,9082)` in MR Echo Sequence `(0018,9114)`. | Reads the first MRD sequence or image-metadata TE. | High |
| TE2 | Use the same standard echo-time attribute on echo 2, or the Enhanced MR functional group. GE also documents `Second echo` `(0019,107D,"GEMS_ACQU_01")`, but its conformance statement does not state the units or qDESS-specific interpretation. | Does not read a second TE. It computes `TE2 = 2 * TR - TE1`, then writes that value as `(0018,0081)` on synthetic echo-2 instances. | High for the OpenMSK behavior; medium for the GE private fallback |
| GL_AREA | DOSMA convention: `(0019,xxB6,"GEMS_ACQU_01")`, normally physical tag `(0019,10B6)` when the creator occupies `(0019,0010)`. GE calls this `User data 15`; DOSMA calls it spoiler area. | Writes physical `(0019,10B6)` with creator `SIEMENS MR HEADER`. | High for GE/DOSMA; low for the Siemens meaning |
| TG | DOSMA convention: `(0019,xxB7,"GEMS_ACQU_01")`, normally physical tag `(0019,10B7)`. GE calls this `User data 16`; DOSMA treats it as spoiler duration in microseconds. | Writes physical `(0019,10B7)` with creator `SIEMENS MR HEADER`. | High for GE/DOSMA; low for the Siemens meaning |

The practical conclusion is that the `B6/B7` pair is a GE research-sequence convention consumed by DOSMA. I found no primary-source basis for calling it a Siemens qDESS private-tag pair. On Siemens data, use the standard echo attributes and obtain spoiler area and duration from the exact sequence implementation or exported protocol. Do not infer them from physical tags `0019,10B6` and `0019,10B7` without first checking the private creator.

## Standard echo timing

DICOM defines Echo Time `(0018,0081)` in milliseconds and Echo Number(s) `(0018,0086)` for classic MR images. For Enhanced MR, the frame-level equivalent is Effective Echo Time `(0018,9082)` inside MR Echo Sequence `(0018,9114)`. These definitions are vendor-neutral:

- [DICOM PS3.3, classic MR Image Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.3.html)
- [DICOM PS3.3, Enhanced MR Echo Macro](https://dicom.nema.org/Medical/Dicom/current/output/chtml/part03/sect_C.8.13.5.4.html)

For symmetric qDESS, the conventional effective timing of the second signal is `TE2 = 2 * TR - TE1`. The signal itself is acquired after the spoiler and before the following RF pulse. The qDESS literature explicitly uses that relationship, while also noting that the two acquisition times can be chosen independently in a modified DESS sequence:

- [Barbieri et al., qDESS B0 mapping, equation 2](https://pmc.ncbi.nlm.nih.gov/articles/PMC9712261/)
- [Staroswiecki et al., modified 3D DESS pulse-sequence description](https://pmc.ncbi.nlm.nih.gov/articles/PMC3306505/)

That relationship is a sequence assumption, not a replacement for measured metadata. If a non-symmetric or modified DESS protocol reports both effective echo times, preserve those values.

## GE private elements

GE's MR750/DV25.1 conformance statement identifies `(0019,0010)` as private creator `GEMS_ACQU_01`. In that block it defines:

- `(0019,107D)` as `Second echo`
- `(0019,107E)` as `Number of echoes`
- `(0019,10B6)` as `User data 15`
- `(0019,10B7)` as `User data 16`

Source: [GE MR DV25.1 DICOM Conformance Statement, DOC1708004 Rev. 2, sections 11.4.4 and table 11.4.4-1](https://www.gehealthcare.com/content/dam/gehc/sitecore-migrated-assets/widen/2018/01/25/0204/gehealthcarecom/migrated/2018/02/19/0841/ance-gehc-dicom-conformance_dv25-1-disoverymr750w-750-450-optimamr450w_doc1708004_rev2_pdf.pdf).

GE's document deliberately leaves User Data 15 and 16 generic. The qDESS semantics come from DOSMA, whose `QDess` class maps those physical tags to `__GL_AREA_TAG__` and `__TG_TAG__`. It describes `gl_area` as spoiler area and `tg` as spoiler duration in microseconds:

- [DOSMA `qdess.py` at commit `bd5efec`](https://github.com/ad12/DOSMA/blob/bd5efecbb944263c9a5d7853f154d9071c72ba62/dosma/scan_sequences/mri/qdess.py#L44-L45)
- [DOSMA T2 parameter handling and units](https://github.com/ad12/DOSMA/blob/bd5efecbb944263c9a5d7853f154d9071c72ba62/dosma/scan_sequences/mri/qdess.py#L124-L204)

The field name `TG` here does not mean GE transmit gain. GE documents Auto Prescan Transmit Gain at `(0019,1094,"GEMS_ACQU_01")` and Transmit gain at `(0019,10F9,"GEMS_ACQU_01")`. In the DOSMA qDESS calculation, `TG` is the spoiler duration.

DOSMA does not document the `GL_AREA` unit directly. Its conversion divides the value by `TG` in microseconds and converts G/cm to G/m. That arithmetic implies `GL_AREA` is in `G/cm * us`. Under that interpretation, OpenMSK's default `3132` equals `31.32 mT/m * ms`. This unit conclusion is an inference from the source code, so it should be checked against the pulse-sequence prescription or a known DICOM instance.

One DOSMA test fixture has comments beside `B6/B7` that reverse "gradient time" and "gradient area." Those comments conflict with the implementation and its public parameter documentation. Treat them as a test-comment error, not another mapping.

## Siemens private elements

The Siemens syngo MR E11E conformance statement supports the standard classic Echo Time and Echo Number attributes, and the standard Enhanced MR Effective Echo Time functional group. Its published `SIEMENS MR HEADER` module lists private diffusion offsets `xx0C`, `xx0D`, `xx0E`, and `xx27`. Its private-element registry does not define `xxB6` or `xxB7` as spoiler fields. It also documents a CSA Series Header blob at `(0029,xx20,"SIEMENS CSA HEADER")`, but does not publish a qDESS spoiler-area or duration key inside that blob.

Source: [Siemens syngo MR E11E DICOM Conformance Statement, sections 8.1.11, 8.1.12, 8.2.8, and 8.6](https://marketing.webassets.siemens-healthineers.com/3a12fc41dc0371ab/20bf18d900b4/Conformance_DC_VE11E.pdf).

This does not prove that no Siemens research sequence ever writes these values. It means there is no universal, published Siemens mapping for them. A Siemens WIP or custom sequence may place parameters in a CSA/Phoenix protocol structure or another private creator block. The sequence source, protocol export, or a vendor-provided tag specification is needed to name those fields safely.

### This Siemens WIP protocol

The supplied `wip_dess_CS_260902` export is from a MAGNETOM 3.0T X60 running Numaris/X VA61A-092T. It identifies the customer sequence as `%CustomerSeq%\wip_GRE_3D_DRB_dess` and records:

- `TR = 15.19 ms`
- `TE = 4.90 ms`
- `sWipMemBlock.adFree[15] = 31.33`
- three displayed `Diff Mom. (R/P/S)` values of `0.00`, `0.00`, and `31.33 ms*mT/m`

For this sequence build, `adFree[15]` is therefore the strongest candidate for the spoiler moment. The displayed R/P/S values indicate that the nonzero moment is on the slice component. In DOSMA's inferred `G/cm * us` convention, `31.33 mT/m * ms` corresponds to `GL_AREA = 3133`.

The export contains only `alTE[0] = 4900 us`, so it does not provide a separate TE2 field. Under the symmetric qDESS convention used by OpenMSK, the effective value is `2 * 15.19 - 4.90 = 25.48 ms`. The protocol does not expose spoiler duration `TG`. If OpenMSK's `TG = 1560 us` default is used with this moment, the implied rectangular-gradient amplitude is about `20.08 mT/m`, but that is an assumption, not a value recovered from the export. Confirm TG in the `wip_GRE_3D_DRB_dess` sequence source or with its author.

## Gradient-spoiling scheme used by the model

qDESS acquires one signal before and one after an unbalanced spoiler gradient. The spoiler separates the two echoes and contributes diffusion weighting. The model uses gradient amplitude `G`, duration `tau`, and area `G * tau`; the direction and area are properties of the pulse sequence, not patient metadata. The foundational sequence description shows two fully rewound readouts separated by the spoiler and states that spoiler amplitude and duration can be set independently on each axis:

- [Staroswiecki et al., modified 3D DESS](https://pmc.ncbi.nlm.nih.gov/articles/PMC3306505/)
- [Sveinsson et al., analytic single-scan DESS T2 model](https://pmc.ncbi.nlm.nih.gov/articles/PMC5360502/)

DOSMA reduces the gradient metadata to one scalar area and one scalar duration. It derives a scalar gradient amplitude and uses the analytic spoiled-DESS T2 expression. That is adequate for the GE qDESS convention for which it was written, but it does not describe an arbitrary three-axis or time-varying spoiler waveform.

Enhanced MR also has the standard Spoiling attribute `(0018,9016)` in the MR Modifier Sequence. Its values only say whether RF spoiling, gradient spoiling, both, or neither were applied. It does not encode gradient area, duration, direction, or waveform. See [DICOM PS3.3, MR Modifier Macro](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.13.5.5.html).

## What this means for OpenMSK

Current upstream OpenMSK accepts the tag pair when both values are numeric and nonzero. Otherwise it runs DOSMA's low-spoiling approximation and records `t2_method="low_spoiling"`. See [upstream OpenMSK `steps/t2_mapping.py` at commit `ea9938a`](https://github.com/gattia/OpenMSK/blob/ea9938a561120d55403bbece9939fec224ec9668/steps/t2_mapping.py#L82-L176). The older KneePipeline revision pinned by this recipe skips T2 mapping when either tag is absent. See [KneePipeline commit `61144f2`](https://github.com/gattia/KneePipeline/blob/61144f23d9001950a70f77cb70d628e1883da86d/steps/t2_mapping.py#L74-L100).

The current wrapper defines the DOSMA tag pair and defaults at [`openmsk.py` lines 106-113](https://github.com/NeuroDesk/neurocontainers/blob/9a55e4b6d7e07691b8ed21f0e5568bfb4fc5408f/recipes/openmsk/openmsk.py#L106-L113). It resolves only TE1, computes TE2, and writes synthetic per-echo standard Echo Time values at [`openmsk.py` lines 1134-1148](https://github.com/NeuroDesk/neurocontainers/blob/9a55e4b6d7e07691b8ed21f0e5568bfb4fc5408f/recipes/openmsk/openmsk.py#L1134-L1148). The synthetic DICOM writer then declares `SIEMENS MR HEADER` and writes the DOSMA `B6/B7` pair at [`openmsk.py` lines 1333-1357](https://github.com/NeuroDesk/neurocontainers/blob/9a55e4b6d7e07691b8ed21f0e5568bfb4fc5408f/recipes/openmsk/openmsk.py#L1333-L1357).

That output works as a local adapter because DOSMA reads the physical tags without validating their private creator. It should not be treated as evidence that Siemens defines those fields. The Enhanced-to-single-frame helper has the same creator-blind assumption when it propagates the two physical tags.

## Safe inspection checklist

Before assigning semantics to a real DICOM series, record:

1. Manufacturer `(0008,0070)`, model `(0008,1090)`, and software version `(0018,1020)`.
2. SOP Class UID, to distinguish classic from Enhanced MR.
3. Echo Time and Echo Number on every classic instance, or MR Echo Sequence and Effective Echo Time in shared and per-frame functional groups.
4. Every private creator in group `0019`, especially the creator that reserves the block containing offsets `B6` and `B7`.
5. The sequence name, protocol name, and the exact WIP or research PSD version.

DICOM requires private creator qualification because different implementers may use the same odd group and offset. It recommends the notation `(gggg,xxee,"private creator")`, not a bare physical tag. See [DICOM PS3.5 section 7.8](https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_7.8.html).
