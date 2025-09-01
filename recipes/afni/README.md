
----------------------------------
## afni/24.3.00 ##
AFNI (Analysis of Functional NeuroImages) is a leading software suite of C, Python, R programs and shell scripts primarily developed for the analysis and display of anatomical and functional MRI (FMRI) data. It is freely available (both in source code and in precompiled binaries) for research purposes. The software is made to run on virtually any Unix system with X11 and Motif displays. Binary Packages are provided for MacOS and Linux systems including Fedora, Ubuntu (including Ubuntu under the Windows Subsystem for Linux) 

To setup AFNI you need to run these commands on first use
```
cp /opt/afni-latest/AFNI.afnirc ~/.afnirc
suma -update_env
apsearch -update_all_afni_help
# but don't add the commands to your .bashrc because this will cause errors outside of the AFNI container
```

If you like to setup autocompletion for afni you need to run this any time you use the AFNI container:
```
ahdir=`apsearch -afni_help_dir`
if [ -f "$ahdir/all_progs.COMP.bash" ]
then
   . $ahdir/all_progs.COMP.bash
fi
``` 

Example:
```
suma
afni
```

More documentation can be found here: https://afni.nimh.nih.gov/pub/dist/doc/htmldoc/index.html

To run container outside of this environment: ml afni/24.3.00

Citation:
```
Cox RW (1996). AFNI: software for analysis and visualization of functional magnetic resonance neuroimages. Comput Biomed Res 29(3):162-173. doi:10.1006/cbmr.1996.0014
https://pubmed.ncbi.nlm.nih.gov/8812068/
RW Cox, JS Hyde (1997). Software tools for analysis and visualization of FMRI Data. NMR in Biomedicine, 10: 171-178.
https://pubmed.ncbi.nlm.nih.gov/9430344/
```

License: Gnu General Public License, https://afni.nimh.nih.gov/Legal_info

----------------------------------
