# kspacefilter 0.1.0

This experimental MRD service returns each acquisition immediately. It can zero
both readout edges across all coils while preserving the acquisition header,
trajectory, sample count, discard regions, ordering and every other sample.
It performs no FFT, reconstruction, volume buffering or phase encoding filtering.

Run `kspacefilter` on port 9002, or use `start-service.sh` for the Docker image.
The default is passthrough. Opt in with this additional MRD JSON configuration:

```json
{"parameters":{"config":"kspacefilter"},"kspacefilter":{"mode":"readout-edge-zero","input_domain":"raw-uniform-cartesian-kspace","measurement_role":"imaging"}}
```

For the usable interval `[discard_pre, number_of_samples-discard_post)`, width N,
the service zeros `floor(N/20)` samples at each end. For N=100 this leaves exactly
90 central samples. For other widths it retains at least 90%; N<20 is unchanged.
It uses the actual readout width including any retained vendor oversampling.
The ordinary usable center is N//2. With IS_REVERSE it also accepts N-1-N//2.
No samples are reversed and ky/kz values do not affect the mask.

The selected metadata encoding must be Cartesian and the acquisition must have
no trajectory, valid shape/discards and a supported center. Auxiliary data,
combined calibration/imaging, unknown flags and unsupported acquisitions pass through.
Absent options, adjustment or unknown measurement roles, and other input domains
also pass through. Integrated calibration-and-imaging lines remain unmasked, so an
integrated-reference dataset does not receive a uniform readout mask. Malformed
option types, unknown keys and modes reject the session.

## FIRE/ICE integration boundary

The adapter must assert that the tap precedes the readout FFT and contains full,
uniform, centered raw Cartesian k-space readouts. Metadata alone cannot establish
this domain. The usual ACSRSS x-ky input is unsuitable.

A custom acquisition-response adapter must retain each original native ICE
header, match returned MRD message 1008 records in order, replace only the native
sample payload, and resume downstream reconstruction without recursive emission.
It must bypass adjustments and concurrently send/receive to avoid backpressure.
Existing image-response injectors do not implement this contract. No native SDK
or scanner acquisition injector is supplied; target hardware integration remains
necessary. Treat an early close or missing acquisition response as a failed
exchange and stop native reinsertion. Text, image and waveform responses are
separate MRD messages and must not replace native acquisition samples. See
`fire-integration-contract.json` for the adapter obligations.

`smoke_kspacefilter.py` starts the pinned main.py TCP server and verifies returned
acquisitions before the next input or EOF, numerical masks and unsupported-input
passthrough. The release fulltest runs this same test in the built container.
