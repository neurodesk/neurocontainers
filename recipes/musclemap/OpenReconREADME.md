# musclemap VERSION_WILL_BE_REPLACED_BY_SCRIPT

This OpenRecon tool is based on the MuscleMap Toolbox (https://github.com/MuscleMap/MuscleMap) - A free and open-source software toolbox for whole-body muscle segmentation and analysis.

It runs on the selected image type from a Dixon sequence and generates muscle segmentations. Opposed Phase is selected by default.

We are currently developing the standardized acquisition protocol for whole-body quantitative MRI of muscle. You can access the Google doc here: https://docs.google.com/document/d/1q7AAnPEr7Rj5gb9d_mLrRnAiav1f32J-RPswvOPk5xE/edit?usp=sharing. To collaborate on the standardized acquisition protocol, please contact us: neuromuscularinsightlab@stanford.edu.

When citing MuscleMap, please cite the following publication:

McKay MJ, Weber KA 2nd, Wesselink EO, Smith ZA, Abbott R, Anderson DB, Ashton-James CE, Atyeo J,  
Beach AJ, Burns J, Clarke S, Collins NJ, Coppieters MW, Cornwall J, Crawford RJ, De Martino E,  
Dunn AG, Eyles JP, Feng HJ, Fortin M, Franettovich Smith MM, Galloway G, Gandomkar Z, Glastras S,  
Henderson LA, Hides JA, Hiller CE, Hilmer SN, Hoggarth MA, Kim B, Lal N, LaPorta L, Magnussen JS,  
Maloney S, March L, Nackley AG, O'Leary SP, Peolsson A, Perraton Z, Pool-Goudzwaard AL, Schnitzler M,  
Seitz AL, Semciw AI, Sheard PW, Smith AC, Snodgrass SJ, Sullivan J, Tran V, Valentin S, Walton DM,  
Wishart LR, Elliott JM.  
MuscleMap: An Open-Source, Community-Supported Consortium for Whole-Body Quantitative MRI of Muscle.  
J Imaging. 2024;10(11):262.  
https://doi.org/10.3390/jimaging10110262


# Parameters

| ID | Label | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `sendoriginal` | Send original images | boolean | `true` | Return source-native 2D original images before the MuscleMap-derived outputs, with fresh scanner storage identity |
| `composesegmentation` | Compose segmentation | boolean | `false` | Enable whole-body composing by replacing the Fat-Fraction series with MuscleMap labels. Uses the genuine Fat-Fraction images as carriers so the labels inherit the real ComposingGroup routing. Replaces the real Fat-Fraction series; re-threshold the Adaptive-blended labelmap offline |
| `segmentwater` | Segment Water | boolean | `false` | Run MuscleMap segmentation on the Dixon water image |
| `segmentinphase` | Segment Inphase | boolean | `false` | Run MuscleMap segmentation on the Dixon in-phase image |
| `segmentopposedphase` | Segment Opposed Phase | boolean | `true` | Run MuscleMap segmentation on the Dixon opposed-phase image |
| `segmentfat` | Segment Fat | boolean | `false` | Run MuscleMap segmentation on the Dixon fat image |
| `labeltransform` | Scale labels to lower integer range for DICOM 12BIT | boolean | `true` | Applying label transformation: 3 * (label_in // 10) + (label_in % 10) |
| `metricsoutput` | Metrics output | choice | `all` | Select where MuscleMap average metrics are returned |
| `bodyregion` | Body Region | choice | `wholebody, abdomen, pelvis, thigh, leg` | Select the body region for segmentation |
| `chunksize` | Chunk Size | string | `100` | Chunk size between 5 and 200 - change for memory optimization on GPU |
| `spatialoverlap` | Spatial Overlap | int | `50` | Spatial overlap percentage |

# Scanner output handling

MuscleMap buffers the incoming image stream before returning anything to the
scanner. When `sendoriginal` is enabled, all received source images are returned
first as source-native 2D passthrough images: source protocol, sequence,
image-type, slice, and per-image numbering fields are preserved while
series/storage UIDs are refreshed. Scanner partition counters such as
`Actual3DImagePartNumber` and `AnatomicalPartitionNo` are forced to the returned
single-partition stream (`0`) instead of copying source partition values. If
`sendoriginal` is disabled, non-processed input images are still returned with
the same source-native passthrough handling.

MuscleMap segmentation images are sent after the source passthrough stream as
source-geometry 2D masks with fresh series identity, fresh SOP identity,
`SequenceDescriptionAdditional` set to the segmented source Dixon contrast,
`Keep_image_geometry = 1`,
`DataRole = Segmentation`, and `SegmentSourceGeometry = 1`. This matches the
`openreconi2iexample` 2D + detach + after-originals path when
`segmentpostprocessingstamp` is disabled. Optional metrics report output follows
the `openreconi2iexample` metrics path: one standalone explicit-volume derived
image, preferring `image_series_index = 120` when available, with
`DataRole = Segmentation`, `Keep_image_geometry = 0`, and no `IceMiniHead`.

## Composing the segmentation across stations (`composesegmentation`)

For multi-station whole-body Dixon, the inline composer combines images per
*declared contrast channel* (Water, Fat, Fat-Fraction) and aborts a channel if it
receives more images than the protocol declares. A segmentation tagged with its
own `MUSCLEMAP` image type is claimed by no channel and is therefore never
composed; tagging it with a contrast that is *also* sent as an original overfills
that channel and switches composing off for the whole measurement.

When enabled, `composesegmentation` resolves this by **replacing** the most
expendable contrast (Fat-Fraction). MuscleMap suppresses the genuine
Fat-Fraction passthrough images, reuses them as carriers, and overwrites their
pixel data with the per-slice segmentation labels. Carriers are matched to each
station by measurement UID and to each label slice by nearest projected position
(Water and Fat-Fraction are co-registered). Because the carrier keeps the real
Fat-Fraction image type and storage identity, the ICE side assigns it the same
`ComposingGroup` it gives a real Fat-Fraction pass-through. MuscleMap patches
the display names and grouping to `Musclemap` so the returned series is not
scanner-visible as the source `2ptFF` series. These carrier images are marked
with `MuscleMapComposeNativeFF = 1` and are exempt from the derived-output
series/storage contract checks (they intentionally reuse the source storage
identity). Water and Fat originals continue to compose normally; the composed
segmentation is delivered in the Fat-Fraction "Composed" series. If no usable
carriers are found, MuscleMap falls back to the non-composing segmentation output
for that volume.

Caveats:

- **The real Fat-Fraction series is replaced** by the segmentation. Fat-Fraction
  is recomputable offline from the composed Water/Fat images.
- The Fat-Fraction channel composes with **Adaptive** intensity blending, which
  *averages* voxels in the station-overlap band. Label values in those overlap
  slices are blended and must be **re-thresholded to the nearest label** offline.
- Requires a Dixon protocol that actually produces a Fat-Fraction contrast. If no
  Fat-Fraction images are present, the labels are still stamped as `FAT_FRAC` but
  the composer has no matching channel to combine them (a warning is logged).
- **Only one segmentation contrast** is composed. The Fat-Fraction compose
  container is statically pre-sized to a single contrast's slot count, so if more
  than one segmentation contrast is selected (e.g. Water and Fat) compose mode
  restricts segmentation to the first one (display order Water, In-Phase,
  Opposed-Phase, Fat) — emitting two `FAT_FRAC` series would overfill the channel
  and switch composing off for the whole measurement.

### How the routing works (ICE composing model)

Composing is statically configured at protocol-prep time, not data-driven. Each
compose functor listens for a fixed `ComposingGroup` (the configurator-owned
`HEADER.ComposingGroup` routing key) and a `ComposeType` (ORIG/SUB/MIP/DIXON/…),
and is pre-sized to `m_iNumberTotal` slots. The only image-controllable lever is
the `ImageType` / `ImageTypeValue4` filter, which selects the `ComposeType`
channel. `ComposingGroup` is **not** present in the MRD/IceMiniHead data and
cannot be set from the OpenRecon module — it is assigned on the ICE side. Within
one Dixon acquisition the Water, Fat and Fat-Fraction channels all share the same
`ComposingGroup` and differ only by `ComposeType`. MuscleMap reuses the genuine
Fat-Fraction images as return carriers so the scanner assigns the labels to the
same Fat-Fraction compose container.

# Labels

Label values are model-specific and depend on the selected `bodyregion`.

## `wholebody`

For running this in Open Recon we need a reversible int12-safe mapping:

`mapped = 3 * (original // 10) + (original % 10)`

`original = 10 * (mapped // 3) + (mapped % 3)`

This transform is valid for labels ending in `0`, `1`, or `2`

Metrics extraction uses the original unscaled labels so MuscleMap can map label
IDs to anatomy names, even when the returned segmentation overlay is scaled for
DICOM.

| Region | Anatomy | Side | Value | Int12 Mapped |
| :--- | :--- | :--- | ---: | ---: |
| neck | levator scapulae | left | 1101 | 331 |
| neck | levator scapulae | right | 1102 | 332 |
| neck | semispinalis cervicis and multifidus | left | 1111 | 334 |
| neck | semispinalis cervicis and multifidus | right | 1112 | 335 |
| neck | semispinalis capitis | left | 1121 | 337 |
| neck | semispinalis capitis | right | 1122 | 338 |
| neck | splenius capitis | left | 1131 | 340 |
| neck | splenius capitis | right | 1132 | 341 |
| neck | sternocleidomastoid | left | 1141 | 343 |
| neck | sternocleidomastoid | right | 1142 | 344 |
| neck | longus colli | left | 1151 | 346 |
| neck | longus colli | right | 1152 | 347 |
| neck | trapezius | left | 1161 | 349 |
| neck | trapezius | right | 1162 | 350 |
| shoulder | supraspinatus | left | 2101 | 631 |
| shoulder | supraspinatus | right | 2102 | 632 |
| shoulder | subscapularis | left | 2111 | 634 |
| shoulder | subscapularis | right | 2112 | 635 |
| shoulder | infraspinatus | left | 2121 | 637 |
| shoulder | infraspinatus | right | 2122 | 638 |
| shoulder | deltoid | left | 2141 | 643 |
| shoulder | deltoid | right | 2142 | 644 |
| thorax | rhomboid | left | 4101 | 1231 |
| thorax | rhomboid | right | 4102 | 1232 |
| abdomen | thoracolumbar multifidus | left | 5101 | 1531 |
| abdomen | thoracolumbar multifidus | right | 5102 | 1532 |
| abdomen | erector spinae | left | 5111 | 1534 |
| abdomen | erector spinae | right | 5112 | 1535 |
| abdomen | psoas major | left | 5121 | 1537 |
| abdomen | psoas major | right | 5122 | 1538 |
| abdomen | quadratus lumborum | left | 5131 | 1540 |
| abdomen | quadratus lumborum | right | 5132 | 1541 |
| abdomen | lattisimus dorsi | left | 5141 | 1543 |
| abdomen | lattisimus dorsi | right | 5142 | 1544 |
| pelvis | gluteus minimus | left | 6101 | 1831 |
| pelvis | gluteus minimus | right | 6102 | 1832 |
| pelvis | gluteus medius | left | 6111 | 1834 |
| pelvis | gluteus medius | right | 6112 | 1835 |
| pelvis | gluteus maximus | left | 6121 | 1837 |
| pelvis | gluteus maximus | right | 6122 | 1838 |
| pelvis | tensor fascia latae | left | 6131 | 1840 |
| pelvis | tensor fascia latae | right | 6132 | 1841 |
| pelvis | iliacus | left | 6141 | 1843 |
| pelvis | iliacus | right | 6142 | 1844 |
| pelvis | ilium | left | 6151 | 1846 |
| pelvis | ilium | right | 6152 | 1847 |
| pelvis | sacrum | no side | 6160 | 1848 |
| pelvis | femur | left | 6171 | 1852 |
| pelvis | femur | right | 6172 | 1853 |
| pelvis | piriformis | left | 6181 | 1855 |
| pelvis | piriformis | right | 6182 | 1856 |
| pelvis | pectineus | left | 6191 | 1858 |
| pelvis | pectineus | right | 6192 | 1859 |
| pelvis | obturator internus | left | 6201 | 1861 |
| pelvis | obturator internus | right | 6202 | 1862 |
| pelvis | obturator externus | left | 6211 | 1864 |
| pelvis | obturator externus | right | 6212 | 1865 |
| pelvis | gemelli and quadratus femoris | left | 6221 | 1867 |
| pelvis | gemelli and quadratus femoris | right | 6222 | 1868 |
| thigh | vastus lateralis | left | 7101 | 2131 |
| thigh | vastus lateralis | right | 7102 | 2132 |
| thigh | vastus intermedius | left | 7111 | 2134 |
| thigh | vastus intermedius | right | 7112 | 2135 |
| thigh | vastus medialis | left | 7121 | 2137 |
| thigh | vastus medialis | right | 7122 | 2138 |
| thigh | rectus femoris | left | 7131 | 2140 |
| thigh | rectus femoris | right | 7132 | 2141 |
| thigh | sartorius | left | 7141 | 2143 |
| thigh | sartorius | right | 7142 | 2144 |
| thigh | gracilis | left | 7151 | 2146 |
| thigh | gracilis | right | 7152 | 2147 |
| thigh | semimembranosus | left | 7161 | 2149 |
| thigh | semimembranosus | right | 7162 | 2150 |
| thigh | semitendinosus | left | 7171 | 2152 |
| thigh | semitendinosus | right | 7172 | 2153 |
| thigh | biceps femoris long head | left | 7181 | 2155 |
| thigh | biceps femoris long head | right | 7182 | 2156 |
| thigh | biceps femoris short head | left | 7191 | 2158 |
| thigh | biceps femoris short head | right | 7192 | 2159 |
| thigh | adductor magnus | left | 7201 | 2161 |
| thigh | adductor magnus | right | 7202 | 2162 |
| thigh | adductor longus | left | 7211 | 2164 |
| thigh | adductor longus | right | 7212 | 2165 |
| thigh | adductor brevis | left | 7221 | 2167 |
| thigh | adductor brevis | right | 7222 | 2168 |
| leg | anterior compartment | left | 8101 | 2431 |
| leg | anterior compartment | right | 8102 | 2432 |
| leg | deep posterior compartment | left | 8111 | 2434 |
| leg | deep posterior compartment | right | 8112 | 2435 |
| leg | lateral compartment | left | 8121 | 2437 |
| leg | lateral compartment | right | 8122 | 2438 |
| leg | soleus | left | 8131 | 2440 |
| leg | soleus | right | 8132 | 2441 |
| leg | gastrocnemius | left | 8141 | 2443 |
| leg | gastrocnemius | right | 8142 | 2444 |
| leg | tibia | left | 8151 | 2446 |
| leg | tibia | right | 8152 | 2447 |
| leg | fibula | left | 8161 | 2449 |
| leg | fibula | right | 8162 | 2450 |

## `abdomen`

| Region | Anatomy | Side | Value |
| :--- | :--- | :--- | ---: |
| abdomen | multifidus | right | 1 |
| abdomen | multifidus | left | 2 |
| abdomen | erector spinae | right | 3 |
| abdomen | erector spinae | left | 4 |
| abdomen | psoas major | right | 5 |
| abdomen | psoas major | left | 6 |
| abdomen | quadratus lumborum | right | 7 |
| abdomen | quadratus lumborum | left | 8 |

## `forarm`

| Region | Anatomy | Side | Value |
| :--- | :--- | :--- | ---: |
| leg | other muscles | no side | 1 |
| leg | radius | no side | 2 |
| leg | ulna | no side | 3 |
| leg | extensor compartment | no side | 4 |
| leg | flexor compartment | no side | 5 |

## `leg`

| Region | Anatomy | Side | Value |
| :--- | :--- | :--- | ---: |
| leg | anterior compartment | left | 1 |
| leg | anterior compartment | right | 2 |
| leg | deep posterior compartment | left | 3 |
| leg | deep posterior compartment | right | 4 |
| leg | lateral compartment | left | 5 |
| leg | lateral compartment | right | 6 |
| leg | soleus | left | 7 |
| leg | soleus | right | 8 |
| leg | gastrocnemius | left | 9 |
| leg | gastrocnemius | right | 10 |
| leg | tibia | left | 11 |
| leg | tibia | right | 12 |
| leg | fibula | left | 13 |
| leg | fibula | right | 14 |

## `pelvis`

| Region | Anatomy | Side | Value |
| :--- | :--- | :--- | ---: |
| pelvis | gluteus minimus | left | 1 |
| pelvis | gluteus minimus | right | 2 |
| pelvis | gluteus medius | left | 3 |
| pelvis | gluteus medius | right | 4 |
| pelvis | gluteus maximus | left | 5 |
| pelvis | gluteus maximus | right | 6 |
| pelvis | tensor fasciae latae | left | 7 |
| pelvis | tensor fasciae latae | right | 8 |
| pelvis | femur | left | 9 |
| pelvis | femur | right | 10 |
| pelvis | pelvic girdle | left | 11 |
| pelvis | pelvic girdle | right | 12 |
| pelvis | sacrum | no side | 13 |

## `thigh`

| Region | Anatomy | Side | Value |
| :--- | :--- | :--- | ---: |
| thigh | vastus lateralis | left | 1 |
| thigh | vastus lateralis | right | 2 |
| thigh | vastus intermedius | left | 3 |
| thigh | vastus intermedius | right | 4 |
| thigh | vastus medialis | left | 5 |
| thigh | vastus medialis | right | 6 |
| thigh | rectus femoris | left | 7 |
| thigh | rectus femoris | right | 8 |
| thigh | sartorius | left | 9 |
| thigh | sartorius | right | 10 |
| thigh | gracilis | left | 11 |
| thigh | gracilis | right | 12 |
| thigh | semimembranosus | left | 13 |
| thigh | semimembranosus | right | 14 |
| thigh | semitendinosus | left | 15 |
| thigh | semitendinosus | right | 16 |
| thigh | biceps femoris long head | left | 17 |
| thigh | biceps femoris long head | right | 18 |
| thigh | biceps femoris short head | left | 19 |
| thigh | biceps femoris short head | right | 20 |
| thigh | adductor magnus | left | 21 |
| thigh | adductor magnus | right | 22 |
| thigh | adductor longus | left | 23 |
| thigh | adductor longus | right | 24 |
| thigh | adductor brevis | left | 25 |
| thigh | adductor brevis | right | 26 |
| thigh | femur | left | 27 |
| thigh | femur | right | 28 |

# Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/musclemap

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
