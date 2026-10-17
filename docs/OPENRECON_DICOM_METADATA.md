# OpenRecon DICOM output identity

Research checked on 2026-10-17 against the official DICOM `current` HTML,
which identifies itself as edition 2026d. This note proposes a metadata
policy. It does not establish that every container or scanner export
already implements the policy.

`ImageType` can distinguish output classes, such as a denoised image and a
segmentation preview. It cannot uniquely identify a series, a processing
run, or all of its parameters. `SeriesInstanceUID (0020,000E)` and
`SOPInstanceUID (0008,0018)` identify series and instances. Processing
metadata records the run and its parameters.
See the [General Series Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.3.html)
and [SOP Common Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.12.html).

## Derived images and original reconstructions

For image postprocessing, value 1 of `ImageType (0008,0008)` describes
whether pixels are `ORIGINAL` or `DERIVED`. Denoising, segmentation,
quantitative calculations, and overlays burned into existing pixels
produce derived images. A changed image that can affect professional
interpretation must have a new SOP Instance UID.
See [PS3.3 C.7.6.1.1.2](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.6.html#sect_C.7.6.1.1.2).

An alternative reconstruction directly from acquired k-space can still
qualify as `ORIGINAL`. Enhanced MR defines original pixels as those
reconstructed directly from sensor data, with value 4 and Volume Based
Calculation Technique both `NONE`. Our software creating an image does
not, by itself, make that image `DERIVED`.
See [PS3.3 C.8.16.1.1](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.16.html#sect_C.8.16.1.1).

Value 2 describes the examination relationship. `PRIMARY` means a direct
result of the examination, and `SECONDARY` means an image created after
the initial examination. Derived pixels do not automatically imply
`SECONDARY`.
See [PS3.3 C.7.6.1.1.2](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.6.html#sect_C.7.6.1.1.2).

## The output SOP Class determines the allowed values

For classic MR Image Storage, value 3 has MR-specific Defined Terms,
including `OTHER`, `PHASE MAP`, and `T1 MAP`. Later values can carry
documented implementation identifiers. For an offline denoised classic
MR image, a proposed classification is
`DERIVED\SECONDARY\OTHER\OR_DENOISED`. An inline examination result may
use `PRIMARY` instead. A short product identifier in value 4 can identify
the output class, provided the implementation documents its meaning.
See [MR Image Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.3.html#sect_C.8.3.1.1.1)
and [general Image Type rules](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.6.html#sect_C.7.6.1.1.2).

Enhanced MR has exactly four values. Value 2 must be `PRIMARY`, value 3
is Image Flavor, and value 4 is Derived Pixel Contrast. For example,
`DERIVED\PRIMARY\VOLUME\RESAMPLED` describes a resampled volume. A fifth
product token violates that layout. Product names are not generally a
description of pixel contrast.
See [PS3.3 C.8.16.1](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.16.html).

Defined Terms permit documented extensions with new semantics, while
Enumerated Values do not. This permits considered extensions where an
IOD allows them, rather than treating every custom Enhanced MR value as
invalid. It does not remove the four-value limit or the meaning of each
position. See [PS3.5 6.3](https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.3.html).

Other output objects have different contracts:

- Native DICOM Segmentation has exactly `DERIVED\PRIMARY`, with no
  additional values. Segment descriptions and algorithm identification
  carry the detailed meaning. A segmentation preview exported as an MR
  image is a different representation.
  See [Segmentation Image Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.20.2.html).
- Parametric Map supports integer or floating point real-world values.
  Its Image Type starts with `DERIVED\PRIMARY`, followed by Image Flavor
  and Derived Pixel Contrast. `QUANTITY` and Quantity Definition Sequence
  describe quantitative outputs. Current Enhanced MR guidance replaces
  former value-4 terms such as `ADC` with that mechanism.
  See [Parametric Map IOD](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_A.75.html),
  [Parametric Map Image Module](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.32.2.html),
  and [Enhanced MR derived contrast](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.8.13.html#sect_C.8.13.1.1.1.4).
- Presentation States can display graphics, text, and overlays by referencing
  images. They do not require a replacement image with altered pixels.
  See [Softcopy Presentation State IODs](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_A.33.html).
- Structured Reports can encode text, numeric measurements, and image
  references. An MR image containing a report screenshot remains a
  rendered image, not a Structured Report.
  See [Comprehensive SR IOD](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_A.35.3.html).

Every `ImageType` component uses the CS representation: at most 16 bytes,
with uppercase letters, digits, spaces, and underscores. Lowercase names,
hyphens, dots in version numbers, and parameter JSON do not belong there.
See [PS3.5 6.2](https://dicom.nema.org/medical/dicom/current/output/chtml/part05/sect_6.2.html).

## Identity, provenance, and verification

The proposed OpenRecon policy gives each distinct output series a new
Series Instance UID and each new image instance a new SOP Instance UID.
Output descriptions identify the tool and product for readers. Processing
records retain the algorithm version, model identity, parameters, and
ordered operations. A reusable product code classifies outputs across
runs. UIDs distinguish the actual series and instances. When different
processing produces different images, it needs different instance UIDs.
Deterministic UID generation must account for the processing version,
model, and parameters that affect the output.

For classic images, `DerivationDescription (0008,2111)` describes the
operations, `DerivationCodeSequence (0008,9215)` gives coded operations,
and `SourceImageSequence (0008,2112)` references source SOP Class and
Instance UIDs. Multiple derivation codes can describe successive steps.
These attributes are optional in the General Reference Module, but are
appropriate requirements for our derived-image policy.
See [PS3.3 C.12.4](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.12.4.html).

Enhanced objects use the applicable functional groups, including
Derivation Image Sequence with source references and frame derivation.
Quantitative objects also need measurement units and the mapping from
stored pixels to physical values. Image Type does not encode either.
See [Derivation Image and Real World Value Mapping macros](https://dicom.nema.org/medical/dicom/current/output/chtml/part03/sect_C.7.6.16.2.html#sect_C.7.6.16.2.6).

Existing adapters need an export-level audit. For example,
[SynthSeg](../recipes/synthseg/synthseg.py) deliberately inherits source
Image Type in its source-image-header segmentation mode, and
[TopoFit](../recipes/topofit/topofit.py) copies source metadata without
setting Image Type in `_stamp_output`. New output UIDs alone do not fix
an inherited `ORIGINAL` classification. The shared
[`_derived_series_uid`](../macros/openrecon_i2i/openreconi2iexample.py)
uses source series identity and series index or name, without a complete
processing-version and parameter identity.

The pinned offline
[`mrd2dicom.py`](https://github.com/astewartau/python-ismrmrd-server/blob/33362f2139701fbf5ea855808a325eb59b7db2ae/mrd2dicom.py)
copies MRD `ImageType`, but reads series identity from the global MRD
measurement header. It does not copy per-image Series Instance UID,
SOP Instance UID, Derivation Description, or Source Image Sequence.
That converter and the scanner export path are separate implementations.
Verification must inspect exported DICOMs from each path, including SOP
Class, Image Type, UIDs, source references, and quantitative scaling.
MRD metadata assertions alone cannot establish the final DICOM contract.

## Initial adapter changes

QSMxT, FLAMeS white matter lesion segmentation, and ME-ICA use the shared
`macros/openrecon/openrecon_metadata.py` helper for processed output metadata.
It retains the existing four-value Siemens MRD layout,
`DERIVED\PRIMARY\M\<product>`. The MRD image header remains magnitude.
The standalone `ImageTypeValue3` field is omitted because some scanner
conversions reject it. These are scanner-facing metadata conventions, not
proof of the final Enhanced MR Image Type or Frame Type.

The product identifiers are implementation-defined descriptions of the
derived pixels. They are uppercase and fit within 16 characters:

- `OR_QSM_CHIMAP`, `OR_QSM_PARA`, `OR_QSM_DIA`, and `OR_QSM_TOTAL` identify
  susceptibility maps, including the separated susceptibility components.
- `OR_QSM_MASK` identifies a reconstruction mask; `OR_QSM_MAG` identifies
  combined magnitude pixels; `OR_QSM_SWI`, `OR_QSM_SMWI_PARA`, and
  `OR_QSM_SMWI_DIA` identify susceptibility-weighted image contrasts.
- `OR_QSM_T2STAR` and `OR_QSM_R2STAR` identify relaxation maps;
  `OR_QSM_REPORT` identifies rendered regional susceptibility tables.
- `OR_WML_OVERLAY`, `OR_WML_MASK`, `OR_WML_PROB`, and `OR_WML_REPORT`
  identify lesion-outline images, binary lesion labels, probability maps,
  and rendered lesion measurements.
- `OR_MEICA_DR2S` and `OR_MEICA_T2S` identify denoised apparent dR2* images
  and T2* maps.

The exported SOP Class determines whether these values are retained,
mapped to standard Image Flavor and Derived Pixel Contrast terms, or
represented elsewhere. In particular, Enhanced MR exports require their
four positional meanings and matching frame metadata. Native SEG does
not use these product tokens. Confirm both classic and Enhanced MR exports
on the target scanner before declaring conformance.

Each emitted output series gets a random Series Instance UID, and each
processed MRD message gets a separate `OpenReconImageUID`. QSMxT retains
its established mapping of that identifier to `SOPInstanceUID`, with fresh
identities on each run. ME-ICA and FLAMeS leave final SOP Instance UID
allocation to the scanner, preserving their existing behavior: several
MRD messages can become frames of one Enhanced MR instance. Original
copies retain their existing classification and storage handling.

On scanners exporting both classic and Enhanced MR, verify that the
writer allocates different SOP Instance UIDs for the two representations.
An MRD message identifier does not establish the final DICOM identity.

`OPENRECON_SOFTWARE_VERSION` and `OPENRECON_CONTAINER_VERSION` bind to
recipe variables. Outgoing metadata records those versions, the adapter
and helper SHA-256 values, parameters, and a parameter SHA-256. The short
Derivation Description contains identifiers and hashes; the complete
parameters stay in `OpenReconParameters`. QSMxT also records the model
manifest SHA-256; FLAMeS records the backend's reported model provenance.
Existing quantitative rescaling and warnings remain in the image comments.

`OpenReconGeometrySourceSeriesInstanceUID`,
`OpenReconGeometrySourceSOPInstanceUID`, and
`OpenReconGeometrySourceSOPClassUID` record available geometry-source
identifiers. They are scalar MRD metadata, not a complete list of every
image used by a multi-echo calculation. They do not create a DICOM Source
Image Sequence or Contributing Equipment Sequence. Those nested sequences
still require scanner-writer support and export-level verification.
