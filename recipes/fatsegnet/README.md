
----------------------------------
## fatsegnet/1.0.gpu ##
This contains the tool designed for the Rhineland Study for segmenting visceral and subcuteneous adipose tissue on fat images from a two-point Dixon sequence.

If you use this tool please cite:

Estrada, Santiago, et al. "FatSegNet: A fully automated deep learning pipeline for adipose tissue segmentation on abdominal dixon MRI." Magnetic resonance in medicine 83.4 (2020): 1471-1483. https:// doi.org/10.1002/mrm.28022

Example:
```
python3 tool/run_FatSegNet.py \
-i /YOUR_INFPUT_FOLDER \
-outp /YOUR_OUTPUT_FOLDER\
-loc
```

Note: This container requires an NVIDIA GPU to run.

More documentation can be found here: https://github.com/Deep-MI/FatSegNet

To run container outside of this environment: ml fatsegnet/1.0.gpu

----------------------------------
