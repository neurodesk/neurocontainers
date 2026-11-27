# Third-Party Notices

SimCortex is licensed under the Apache License 2.0, except for third-party
components identified below, which remain subject to their respective terms.

## Topology lookup table

File distributed with SimCortex:

```text
src/simcortex/utils/critical186LUT.raw.gz
```

SHA256:

```text
cbf1e339d78a0a2c673c939ee2c8a30ee3a35bfd9603d07ed2d48864bb0e88be
```

This lookup table is used by the topology-correction implementation in
`src/simcortex/utils/tca.py`.

The same byte-identical lookup table is distributed by:

- Nighres:
  `nighres/atlases/topology_lut/critical186LUT.raw.gz`
  https://github.com/nighres/nighres
- CBS Tools:
  `de/mpg/cbs/structures/critical186LUT.raw.gz`
  https://github.com/piloubazin/cbstools-public

The SimCortex copy is unmodified.

CBS Tools identifies its software and associated files as:

> Copyright (c) 2016 Max Planck Institute for Human Cognitive and Brain Sciences

and distributes them under the Creative Commons Attribution-ShareAlike 4.0
International License (CC BY-SA 4.0), unless explicitly disclaimed in
individual files.

For conservative redistribution, SimCortex treats this lookup table as a
separately licensed third-party asset under CC BY-SA 4.0.

License:

https://creativecommons.org/licenses/by-sa/4.0/

The surrounding SimCortex topology-correction Python implementation is a
separate reimplementation based on the topology-correction work of
Pierre-Louis Bazin and colleagues; see `src/simcortex/utils/tca.py` for the
original-paper citations and upstream implementation references.
