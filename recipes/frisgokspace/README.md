# frisgokspace 0.1.0

This experimental MRD service returns complex acquisitions to a custom FIRE/ICE acquisition-response adapter. It preserves acquisition count, order, header bytes and trajectory. It does not reconstruct images or estimate B0 frequency offsets. Original FRISGO operates on magnitude temporal interpolation; this service is a separate complex temporal smoothing extension.

## Run the service

With the built Docker image available, run `bash start-service.sh`. The image tag is `frisgokspace:0.1.0` and port 9002 is exposed. Set `FRISGOKSPACE_PORT` to change the host port. Inside the container, `frisgokspace` starts the pinned Python MRD server, `frisgokspace --version` reports the container version, and `frisgokspace --help` lists server options. Runtime files reside in `/opt/code/python-ismrmrd-server`.

Connections default to unchanged passthrough, including parameterless adjustments. Send the ordinary MRD config/metadata handshake, followed by the additional JSON configuration shown in `fire-integration-contract.json`, to opt in. `measurement_role: imaging` is an adapter assertion identifying a main imaging measurement; unknown roles and adjustments bypass. Unsupported configurations terminate the session before data return and fail visibly in the server log. The adapter must send and receive concurrently to prevent TCP backpressure from deadlocking a large stream.

## Input contract and behavior

The adapter must verify that samples have already been mapped to identical coordinates and a common phase reference in either `aligned-cartesian-kspace` or `aligned-x-ky`. It must supply fixed physical coil channels and verify that `ACQ_IS_REVERSE` represents readout polarity after alignment. The processor performs no FFT, reversal, regridding or coil conversion. Repetition parity alone never establishes polarity.

Imaging repetitions must be contiguous and explicitly delimited by `FIRST_IN_REPETITION` and `LAST_IN_REPETITION` on eligible imaging acquisitions. Noise and other auxiliary preambles bypass without starting a frame. Each triplet must have consecutive repetition counters and identical acquired acquisition-key sets, including user counters, echo/contrast, average, slice, phase, set, segment, ky and kz. Sparse accelerated and partial Fourier acquired masks are supported when they match. This is a stream-completeness assertion from the adapter's boundary flags, not an assumption that the encoded matrix was fully sampled.

For every matching readout, the service requires equal positive timestamp gaps, alternating verified polarity, exact matching geometry, dwell, sample count, discard counts, center sample, channel counts/masks and trajectory, and finite samples. A failure in any line leaves the entire center frame unchanged. Integrated calibration-and-imaging lines or unknown non-auxiliary flags bypass the entire triplet to avoid mixing corrected and uncorrected imaging lines. Timestamp gaps must match exactly in MRD ticks; quantized alternating gaps and midnight rollover bypass correction. For Siemens timestamps, choose a TR divisible by the 2.5 ms tick or leave correction disabled. Ambiguous stream boundaries latch passthrough for the remainder of the connection. Strict exact geometry comparison intentionally refuses even small geometry changes.

For compatible interior repetitions, the complex filter is `Y[t] = (X[t-1] + 2 X[t] + X[t+1]) / 4`. It cancels an exactly alternating additive error and preserves a linear temporal trend. It smooths temporal content with response `cos²(ω/2)` and needs one complete future repetition. A stationary alternating phase error produces `S cos(φ)`, so phase cancellation can attenuate magnitude. Motion, changing phase and coil sensitivity can blur or attenuate data. This method has not been clinically validated. First and last repetitions are unchanged. Only original samples enter later windows.

Noise, parallel calibration (including calibration-and-imaging), phase correction, navigators, dummy scans, feedback and other auxiliary acquisitions remain unchanged. Images, waveforms and text preserve their position in the ordered stream. Each connection owns its state. Byte and record caps cover retained originals, sample copies, trajectories and auxiliary records, with conservative per-record overhead and reserved space for atomic correction results. Overflow flushes pending records unchanged and latches passthrough; a previously emitted corrected prefix remains corrected. EOF flushes incomplete and boundary frames in order. Transport or programming failures are logged and terminate the connection; buffered records may not be returned. The first boundary repetition and its preamble wait for the third repetition to complete. Later interior repetitions wait for one future repetition.

## Scanner integration remains required

Choose buffer caps for the actual sequence before enabling correction. Accounting reserves about four times the complex imaging payload plus 4 KiB per record across three retained repetitions. For 32 channels and 400 samples, each readout needs about 404 KiB of accounting, so 512 MiB holds about 1,300 readouts across the whole window. Large volumes can need several GiB; exceeding the default cap disables correction. The buffer caps limit retained stream data, not total process memory. The server decodes each incoming record before the processor checks its size.

`fire-integration-contract.json` documents a native MRD integration contract. It is not an OpenReconLabel and is not a Siemens scanner configuration. No scanner-ready raw injector is supplied. Existing OpenRecon image injectors do not establish an acquisition-return path.

A custom FIRE/ICE component must consume returned MRD acquisitions, associate them with saved original scanner headers, preserve count and ordering, bypass adjustments, re-enter reconstruction after the chosen aligned complex emitter point, and prevent recursive re-emission. Verify polarity semantics, coil basis, timing and repetition boundaries on the target sequence. Scanner reinjection requires separate validation on the intended scanner/FIRE version.

## Verify

Start with passthrough on a short phantom acquisition and compare the native reconstruction before enabling correction. Confirm the emitter and return hook operate on the same domain and that the native downstream reconstruction receives each acquisition once.

The scientific starting point is [Huber et al., 2025](https://doi.org/10.1002/mrm.30489), which describes complex dual-polarity averaging for Fuzzy Ripple mitigation. The centered three-repetition filter here is an experimental extension of that approach.

`python /opt/code/python-ismrmrd-server/smoke_frisgokspace.py` launches the actual packaged `main.py` and tests its MRD TCP path, default passthrough, explicit correction, auxiliary preservation, session isolation, incompatible sampling, bounded buffering and EOF. Numerical tests verify cancellation, trend preservation and phase attenuation. On a host with the pinned server installed elsewhere, pass `--server-dir PATH`.
