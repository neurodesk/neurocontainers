
----------------------------------
## mneextended/1.1.0 ##
Python MNE environment with VScode 

This environment contains MNE Python and some additional tools with dependencies (MNE BIDS Pipeline, MNE-connectivity, MNE-icalabel, Neurokit2)   

Example:
```
source /opt/miniconda/etc/profile.d/conda.sh
conda activate mne-extended 
```

More documentation can be found here: https://mne.tools/stable/index.html
To cite MNE Python see here: https://mne.tools/stable/overview/cite.html


To use the MNE BIDS Pipeline (https://mne.tools/mne-bids-pipeline/)
```
python /opt/mne-bids-pipeline-main/run.py --config=/path/to/your/custom_config.py 
```

To run applications outside of this container: ml mneextended/1.1.0
Note the use of the module system does not currently interface with MNE and conda environments in this container

----------------------------------
