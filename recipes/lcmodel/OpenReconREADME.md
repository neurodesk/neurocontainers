# LCModel

LCModel 6.3 quantification of single-voxel proton MR spectra as an OpenRecon / FIRE
application. The analysis code is the
[LCModel MRD App](https://github.com/kspaceKelvin/LCModel-MRD-App) by Kelvin Chow.
LCModel is by Stephen Provencher.

## Workflow

1. Run the water-unsuppressed reference scan through the app first. The app stores it
   under `/tmp/share/store` and returns no images for it.
2. Run the water-suppressed scan. The app uses the most recent reference scan from the
   same study (same Study Instance UID, within 24 hours) for water scaling. Without a
   reference, LCModel runs without water scaling.

The app returns the LCModel report as an RGB image series (`LCMODEL`). The
concentration table is stored base64 encoded in the `LCModelTable` MetaAttribute of
that image.

## Input

The app expects pre-processed (coil-combined, averaged) time-domain spectra as one
complex MRD image per scan. It reads these header fields:

- `experimentalConditions.H1resonanceFrequency_Hz` or
  `acquisitionSystemInformation.systemFieldStrength_T`
- `sequenceParameters.TE[0]` in ms
- `userParameterDouble` `SpectralWidth` in Hz, or `DwellTime_0` in us
- `userParameterString` `FatWaterContrast`: `WATER_SATURATION` marks a
  water-suppressed scan; any other value or no value marks a reference scan
- `userParameterDouble` `SpecVoiThickness`, `SpecVoiPhaseFOV` and
  `SpecVoiReadoutFOV` for the voxel volume. Without them the app assumes 1 mL.
- `studyInformation` study date, time and instance UID to pair reference scans

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| Basis file (`filebas`) | `basis/3t/press_te30_3t_v3.basis` | Basis set relative to the server folder. Bundled: `basis/1.5t`, `basis/3t`, `basis/7t`, `basis/9.4t` from lcmodel.com. Match field strength, sequence and TE. |
| Analysis window start (`ppmst`) | 4.0 ppm | Upper end of the fitted spectral range |
| Analysis window end (`ppmend`) | 0.2 ppm | Lower end of the fitted spectral range |
| Report resolution (`pngdpi`) | 198 dpi | Resolution of the rendered report |
| Grayscale report (`grayscale`) | off | Return a grayscale instead of an RGB report |
| Send spectrum plot series (`plotSpectra`) | off | Also return the magnitude spectrum drawn as ROIs |
| Send original spectra (`sendOriginal`) | off | Also return the received spectroscopy data |

## Research use

This application is for research use only. Results are not intended for diagnosis.
