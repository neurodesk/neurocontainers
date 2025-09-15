#!/usr/bin/env julia

using QSM
using NIfTI
using Statistics
using JSON

println("[INFO] Starting laplacian_vsharp_rts pipeline...")

println("[INFO] Loading input JSON file...")
input_file = "inputs.json"
json_data = JSON.parsefile(input_file)
mask_file = json_data["mask"]
mag_files = json_data["mag_nii"]
phas_files = json_data["phase_nii"]
TEs = json_data["EchoTime"]
B0 = json_data["MagneticFieldStrength"]

println("[INFO] Loading input JSON file...")
input_file = "inputs.json"
json_data = JSON.parsefile(input_file)
mask_file = json_data["mask"]
mag_files = json_data["mag_nii"]
phas_files = json_data["phase_nii"]
TEs = json_data["EchoTime"]
B0 = json_data["MagneticFieldStrength"]

# constants
γ = 267.52 # gyromagnetic ratio (MHz/T)
bdir = (0.,0.,1.)   # direction of B-field
vsz  = (1.0, 1.0, 1.0)   # voxel size (assuming isotropic)

println("[INFO] Loading magnitude image for echo-1 to get shape...")
nii_mag = niread(mag_files[1])
phs_shape = size(Float32.(nii_mag))
num_images = length(mag_files)
mag_shape = tuple(phs_shape..., num_images)
phas_shape = tuple(phs_shape..., num_images)
mag = Array{Float32}(undef, mag_shape...)
phas = Array{Float32}(undef, phas_shape...)

println("[INFO] Concatenating magnitude and phase images...")
for i in 1:num_images
    println("[INFO] Loading images for echo $i...")
    mag_tmp = niread(mag_files[i])
    phas_tmp = niread(phas_files[i])

    mag_tmp = Float32.(mag_tmp)
    phas_tmp = Float32.(phas_tmp)

    mag[:,:,:,i] = mag_tmp
    phas[:,:,:,i] = phas_tmp
end

# Load the mask file
println("[INFO] Loading mask: $mask_file")
mask = niread(mask_file)
mask = Bool.(mask)
println("[INFO] Mask loaded.")

# Unwrap phase and correct for harmonic background field
println("[INFO] Unwrapping phase and correcting for harmonic background fields...")
uphas = unwrap_laplacian(phas, mask, vsz)

# Convert units
println("[INFO] Converting phase units...")
@views for t in axes(uphas, 4)
    uphas[:,:,:,t] .*= inv(B0 * γ * TEs[t])
end

# Remove non-harmonic background fields
println("[INFO] Removing non-harmonic background fields...")
fl, mask2 = vsharp(uphas, mask, vsz)

# Perform dipole inversion
println("[INFO] Performing dipole inversion...")
x = rts(fl, mask2, vsz, bdir=bdir)
x = mean(x, dims = 4)
println("[INFO] Dipole inversion completed.")

# Save the output
output_file = "out.nii.gz"
println("[INFO] Saving output to $output_file")
ni = NIVolume(x[:,:,:]; voxel_size=vsz, orientation=nothing, dim_info=Integer.(vsz), time_step=0f0)
niwrite(output_file, ni)

println("[INFO] Pipeline completed successfully.")

