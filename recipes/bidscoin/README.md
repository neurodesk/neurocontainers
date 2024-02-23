
----------------------------------
## bidscoin/4.3.0 ##
Contains a GUI and CLI tools needed for DICOM to BIDS conversion, as well as MRS spectroscopy and physiological data to BIDS conversion

Tools included (NB: some tools that depend on FSL or FreeSurfer are not included):
```
dcm2niix: https://github.com/rordenlab/dcm2niix (v1.0.20240202)
spec2nii: https://github.com/wtclarke/spec2nii (v0.7.2)
bidscoin: https://bidscoin.readthedocs.io/en/4.3.0
    bidscoin
    bidscoiner
    bidseditor
    bidsmapper
    bidsparticipants
    dicomsort
    echocombine
    physio2tsv
    plotphysio
    rawmapper
    slicereport
```

Example converting dicom to BIDS: 
```
Getting started:
bidscoin -h

Convert DICOM to BIDS:
bidsmapper inputfolder bidsoutputfolder
bidscoiner inputfolder bidsoutputfolder
```

More documentation can be found here:
https://bidscoin.readthedocs.io/

Citation:
```
Zwiers MP, Moia S, Oostenveld R. BIDScoin: A User-Friendly Application to Convert Source Data to Brain Imaging Data Structure. Front Neuroinform. 2022 Jan 13;15:770608. doi: 10.3389/fninf.2021.770608. PMID: 35095452; PMCID: PMC8792932.
```

To run container outside of this environment: ml bidscoin/4.3.0

----------------------------------
