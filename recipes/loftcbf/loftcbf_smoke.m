% Run LOFT_CBFquantify on synthetic pCASL control/label pairs and compare the
% mean CBF with the toolbox's pCASL model. perf_reconstruct_2D.m scales by its
% fixed MoCSF (2.1412e5) and uses the M0 image only for masking.
root = tempname(); mkdir(root);
[x, y, z] = ndgrid(1:24, 1:24, 1:10);
anatomy = 600 + 400 * exp(-((x - 12).^2 + (y - 12).^2) / 60 - (z - 5).^2 / 12);
rand('seed', 7);

function cbf = run_case(root, name, anatomy, delta)
  d = fullfile(root, name); mkdir(d);
  V = struct('dim', size(anatomy), 'mat', diag([3 3 4 1]) - [zeros(4,3) [37.5; 37.5; 22; 0]], ...
             'dt', [16 0], 'pinfo', [1; 0; 0]);
  images = cell(1, 16);
  for k = 1:16
    % Control first: odd volumes are control, even volumes are label.
    V.fname = fullfile(d, sprintf('asl_%02d.nii', k));
    spm_write_vol(V, anatomy + rand(size(anatomy)) - delta * (mod(k, 2) == 0));
    images{k} = V.fname;
  end
  V.fname = fullfile(d, 'm0.nii');
  spm_write_vol(V, anatomy * 2);
  old = cd(d);
  % 3T pCASL, control first, control-label = Img1-Img2, simple subtraction,
  % PLD 1.8 s, label 1.8 s, single-shot readout.
  [~, ~, ~, ~, ~, cbf] = LOFT_CBFquantify(images, {V.fname}, 1, 2, 0, 1, 0, 1.8, 1.8, 0, 0.1, 0, 1, 1);
  cd(old);
  assert(numel(dir(fullfile(d, 'QC_*.txt'))) == 1, 'QC report missing');
end

R = 1 / 1.65; alp = 0.85; pld = 1.8; tau = 1.8;
model = @(delta) 2700 * delta * R / alp / ((exp(-pld * R) - exp(-(pld + tau) * R)) * 2.1412e5);
for delta = [6 12]
  cbf = run_case(root, sprintf('delta%d', delta), anatomy, delta);
  printf('LOFT-CBF mean CBF %.4f, model %.4f\n', cbf, model(delta));
  assert(abs(cbf / model(delta) - 1) < 0.03, 'CBF differs from the pCASL model by more than 3%');
end
printf('LOFT-CBF smoke test passed\n');
