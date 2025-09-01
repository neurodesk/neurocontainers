 ----------------------------------
## niimath/{{ context.version }}##

## About

It is said that `imitation is the sincerest form of flattery`. This project emulates the popular [fslmaths](https://fsl.fmrib.ox.ac.uk/fslcourse/lectures/practicals/intro3/index.html) tool. fslmaths is advertised as a `general image calculator` and is not only one of the foundational tools for FSL's brain imaging pipelines (such as [FEAT](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FEAT)), but has also been widely adopted by many tools. This popularity suggests that it fulfills an important niche. While scientists are often encouraged to discover novel solutions, it sometimes seems that replication is undervalued. Here are some specific reasons for creating this tool:

1. While fslmaths is provided for without charge, it is not [open source](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/Licence). This limits its inclusion in other projects, in particular for commercial exploitation.
2. Using an open source license allows niimath to build with open source libraries that the FSL team can not use. Specifically, the CloudFlare zlib provides dramatically faster performance than the public domain library used by fslmaths. n.b. Subsequently, we helped update [CloudFlare zlib](https://github.com/cloudflare/zlib/pull/19) that allows recent FSL releases to use this library,  improving the speed for all FSL tools.
3. Minimal dependencies allow easy distribution, compilation and development. For example, it can be compiled for MacOS, Linux and Windows (fsl can not target Windows).
4. Designed from ground up to optionally use parallel processing (OpenMP and CloudFlare-enhanced [pigz](https://github.com/madler/pigz)).
5. Most programs are developed organically, with new features added as need arises. Cloning an existing tool provides a full specification, which can lead to optimization. niimath uses explicit single and double precision pipelines that allow the compiler to better use advanced instructions (every x86_64 CPU provides SSE, but high level code has trouble optimizing these routines). The result is that modern compilers are able to create operations that are limited by memory bandwidth, obviating the need for [hand tuning](https://github.com/neurolabusc/simd) the code.
6. Developing a robust regression testing dataset has allowed us to discover a few edge cases where fslmaths provides anomalous or unexpected answers (see below). Therefore, this can benefit the popular tool that is being cloned.
7. While the code is completely reverse engineered, the FSL team has been gracious to allow us to copy their error messages and help information. This allows true plug in compatibility. They have also provided pseudo code for poorly documented routines. This will allow the community to better understand the actual algorithms.
8. This project provides an open-source foundation to introduce new features that fill gaps with the current FSL tools (e.g. unsharp, sobel, resize functions). For future releases, Bob Cox has graciously provided permission to use code from [AFNI's](https://afni.nimh.nih.gov) 3dTshift and 3dBandpass tools that provide performance unavailable within [FSL](https://neurostars.org/t/bandpass-filtering-different-outputs-from-fsl-and-nipype-custom-function/824). Including them in this project ensures they work in a familiar manner to other FSL tools (and leverage the same environment variables).

The Reason to use fslmaths instead of niimath:

1. niimath is new and largely untested software. There may be unknown corner cases where produces poor results. fslmaths has been used for years and therefore has been battle tested. In the few instances where fslmaths generates results that bear no resemblance to its own documentation (as described below), one could argue it is the `correct` result (with comparison to itself). However, many tools may have been developed to assume this loss of high frequency signal and these tools may not perform well when provided with the result specified in the documentation.

## Usage

niimath provides the same commands as [fslmaths](https://mandymejia.com/fsl-maths-commands/), so you can use it just as you would fslmaths. If you are brave, you can even rename it fslmaths and use it as a drop in replacement. You can also modify your environment variables to unleash advanced features:

- Just like fslmaths, it uses your [`FSLOUTPUTTYPE` Environment Variable ](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FslEnvironmentVariables) to determine output file format. Unix users can specify `export NIFTI_GZ` or `export NIFTI` from the command line or profile to select between compressed (smaller) or uncompressed (faster) results. Windows users can use `set` instead of `export`.
- To turn on parallel processing and threading, you can either set the environment variable `export AFNI_COMPRESSOR=PIGZ`. If the environment variable `AFNI_COMPRESSOR` does not exist, or is set to any value other than `PIGZ` you will get single threaded compresson.

niimath has a few features not provided by fslmaths:

- `bandpass <hp> <lp> <tr>`: Butterworth filter, highpass and lowpass in Hz,TR in seconds (zero-phase 2*2nd order filtfilt)
- `bptfm <hp> <lp>`        : Same as bptf but does not remove mean (emulates fslmaths < 5.0.7)
- `bwlabel <conn>`         : Connected component labelling for non-zero voxels (conn sets neighbors: 6, 18, 26)
- `ceil`                   : round voxels upwards to the nearest integer
- `crop <tmin> <tsize>`    : remove volumes, starts with 0 not 1! Inputting -1 for a size will set it to the full range
- `dehaze <mode>`          : set dark voxels to zero (mode 1..5; higher yields more surviving voxels)
- `detrend`                : remove linear trend (and mean) from input
- `demean`                 : remove average signal across volumes (requires 4D input)
- `edt`                    : estimate Euler Distance Transform (distance field). Assumes isotropic input
- `floor`                  : round voxels downwards to the nearest integer
- `mod`                    : modulus fractional remainder - same as '-rem' but includes fractions
- `otsu <mode>`            : binarize image using Otsu''s method (mode 1..5; higher yields more bright voxels))
- `power <exponent>`       : raise the current image by following exponent
- `resize <X> <Y> <Z> <m>` : grow (>1) or shrink (<1) image. Method <m> (0=nearest,1=linear,2=spline,3=Lanczos,4=Mitchell)\n");
- `round`                  : round voxels to the nearest integer
- `sobel`                  : fast edge detection
- `sobel_binary`           : sobel creating binary edge
- `tensor_2lower`          : convert FSL style upper triangle image to NIfTI standard lower triangle order
- `tensor_2upper`          : convert NIfTI standard lower triangle image to FSL style upper triangle order
- `tensor_decomp_lower`    : as tensor_decomp except input stores lower diagonal (AFNI, ANTS, Camino convention)
- `trunc`                  : truncates the decimal value from floating point value and returns integer value
- `unsharp  <sigma> <scl>` : edge enhancing unsharp mask (sigma in mm, not voxels; 1.0 is typical for amount (scl))
- `dog <sPos> <sNeg>`      : difference of gaussian with zero-crossing edges (positive and negative sigma mm)
- `dogr <sPos> <sNeg>`     : as dog, without zero-crossing (raw rather than binarized data)
- `dogx <sPos> <sNeg>`    : as dog, zero-crossing for 2D sagittal slices
- `dogy <sPos> <sNeg>`    : as dog, zero-crossing for 2D coronal slices
- `dogz <sPos> <sNeg>`    : as dog, zero-crossing for 2D axial slices
- `mesh`                  : see separate section below
- `qform <code>`          : set qform code
- `sform <code>`          : set sform code
- `--compare <ref>`       : report if images are identical, terminates without saving new image\n");
- `filename.nii`          : mimic fslhd (can also export to a txt file: 'niimath T1.nii 2> T1.txt') report header and terminate without saving new image

## License

niimath is licensed under the 2-Clause BSD License. Except where noted, the code was written by Chris Rorden in 2020-2022. The code in `tensor.c` was written by Daniel Glen (2004) from the US National Institutes of Health and is not copyrighted (though it is included here with the permission of the author). The FSL team graciously allowed the text strings (help, warning and error messages) to be copied verbatim. The Butterworth Filter Coefficients in `bw.c` are from [Exstrom Labs](http://www.exstrom.com/journal/sigproc/) and the authors provided permission for it to be included in this project under the [LGPL](https://www.gnu.org/licenses/lgpl-3.0.en.html), the file provides additional details. Taylor Hanayik from the FSL group provided pseudo-code for some functions where there is little available documentation. The PolygoniseCube function comes from Cory Bloyd's public domain [Marching Cubes example](http://paulbourke.net/geometry/polygonise/) program described here. The bwlabel.cpp file was written by Jesper Andersson, who has explicitly allowed this to be shared using the BSD 2-Clause license. The [high performance](https://github.com/gaspardpetit/base64) base64.cpp was written by Jouni Malinen and is distributed under the BSD license. The mesh simplification was written by [Sven Forstmann](https://github.com/sp4cerat/Fast-Quadric-Mesh-Simplification) and distributed under the MIT license. It was ported from C++ to C by Chris Rorden.  The [radixsort.c](https://github.com/bitshifter/radixsort) was written by Cameron Hart (2014) using the zlib license.

## Links

  - [imbibe](https://github.com/jonclayden/imbibe) is a R wrapper for niimath, allowing the performance of tuned code with the convenience of a scripting language.
  - [3dcalc](https://afni.nimh.nih.gov/pub/dist/doc/program_help/3dcalc.html) is AFNI's tool for image arithmetic.
  - [c3d](https://sourceforge.net/p/c3d/git/ci/master/tree/doc/c3d.md) provides mathematical functions and format conversion for medical images.
  - [fslmaths](https://fsl.fmrib.ox.ac.uk/fslcourse/lectures/practicals/intro3/index.html) is the inspiration for niimath.

## Citation

  - Rorden C, Webster M, Drake C,  Jenkinson M, Clayden JD, Li N, Hanayik T ([2024](https://apertureneuro.org/article/94384-niimath-and-fslmaths-replication-as-a-method-to-enhance-popular-neuroimaging-tools)) niimath and fslmaths: replication as a method to enhance popular neuroimaging tools. Aperture Neuro.4. doi:10.52294/001c.94384

 ----------------------------------
