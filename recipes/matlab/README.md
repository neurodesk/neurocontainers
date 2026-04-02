----------------------------------
## matlab/R2025b ##
MATLAB with the Deep Learning Toolbox Converter for ONNX Model Format preinstalled.

Licensing
---------

MATLAB is commercial software and requires a valid MathWorks license.

Preferred options:
1. Set `MLM_LICENSE_FILE=port@hostname` to use a network license manager.
2. Place a `.lic` file in `~/Downloads`. The `matlab` wrapper automatically picks up the first license file it finds there.

Examples
--------

Launch the interactive IDE:

  matlab

Run a batch command:

  matlab -batch "ver"

Show the MEX tool help:

  mex -help

Check the module system:

  module avail

ONNX support
------------

The container includes the `Deep_Learning_Toolbox_Converter_for_ONNX_Model_Format`
support package and stores it under `/opt/matlab/support-packages/R2025b`, so
`importONNXFunction` does not require installing the add-on through Add-On Explorer.

More documentation
------------------

  https://www.mathworks.com/help/deeplearning/ref/importonnxfunction.html
  https://github.com/mathworks-ref-arch/matlab-dockerfile

To run applications outside of this container
---------------------------------------------

  ml matlab/R2025b

Citation
--------

  (MATLAB, MathWorks Inc.)

----------------------------------
