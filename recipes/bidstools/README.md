
----------------------------------
## bidstools/1.0.2 ##
Contains a collection of tools useful for DICOM to BIDS conversion

Tools included:
```
dcm2niix: https://github.com/rordenlab/dcm2niix
dcmtk: https://support.dcmtk.org/docs/pages.html
    dcmdump
    dump2dcm
    dcmconv
    dcm2json
    dcmodify
    img2dcm
    dcm2pnm
    dcmicmp
xmedcon: https://xmedcon.sourceforge.io/
heudiconv: https://heudiconv.readthedocs.io/en/latest/heuristics.html
Bru2Nii: https://github.com/neurolabusc/Bru2Nii
```

Example converting Bruker data to Nifti
```
Bru2 -o /Users/cr/dir2/out /Users/cr/dir/acqp
```

To run container outside of this environment: ml bidstools/toolVersion

----------------------------------
