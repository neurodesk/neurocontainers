# musclemap VERSION_WILL_BE_REPLACED_BY_SCRIPT

This OpenRecon tool is based on the MuscleMap Toolbox (https://github.com/MuscleMap/MuscleMap) - A free and open-source software toolbox for whole-body muscle segmentation and analysis.

It runs on Water images of a Dixon sequence and generates muscle segmentations.

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
| `sendoriginal` | Send original images | boolean | `true` | Return restamped derived copies of the original source images with the MuscleMap output |
| `labeltransform` | Scale labels to lower integer range for DICOM 12BIT | boolean | `true` | Applying label transformation: 3 * (label_in // 10) + (label_in % 10) |
| `bodyregion` | Body Region | choice | `wholebody, abdomen, pelvis, thigh, leg` | Select the body region for segmentation |
| `chunksize` | Chunk Size | string | `100` | Chunk size between 5 and 200 - change for memory optimization on GPU |
| `spatialoverlap` | Spatial Overlap | int | `50` | Spatial overlap percentage |

# Scanner output handling

MuscleMap buffers the incoming image stream before returning anything to the
scanner. The segmentation, optional metrics report, optional original copies,
and non-processed passthrough images are all sent as derived output series with
fresh series identity, fresh SOP identity, and sequential scanner slice fields.
This avoids reusing the source scanner storage identity for returned images.

# Labels

Label values are model-specific and depend on the selected `bodyregion`.

## `wholebody`

For running this in Open Recon we need a reversible int16-safe mapping:

`mapped = 3 * (original // 10) + (original % 10)`

`original = 10 * (mapped // 3) + (mapped % 3)`

This transform is valid for labels ending in `0`, `1`, or `2`

| Region | Anatomy | Side | Value | Int16 Mapped |
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
