#!/usr/bin/env julia

using ArgParse
using QSM, NIfTI, Statistics

function parse_commandline()
    s = ArgParseSettings()
    @add_arg_table s begin
        "output_file"
            help = "Output QSM map file path"
            required = true
        "--mag_files"
            help = "Multi-echo magnitude image files"
            nargs = '+'
            required = true
        "--phase_files"
            help = "Multi-echo phase image files"
            nargs = '+'
            required = true
        "--echo_times"
            help = "Echo times in seconds"
            arg_type = Float64
            nargs = '+'
            required = true
        "--field_strength"
            help = "Magnetic field strength in Tesla"
            arg_type = Float64
            required = true
        "--mask_file"
            help = "Brain mask file"
            required = true
    end
    return parse_args(s)
end

function main()
    println("[INFO] Starting Laplacian + V-SHARP + RTS pipeline...")

    # Parse command line arguments
    args = parse_commandline()
    output_file = args["output_file"]
    mag_files = args["mag_files"]
    phas_files = args["phase_files"]
    TEs = args["echo_times"]
    B0 = args["field_strength"]
    mask_file = args["mask_file"]

    println("[INFO] Algorithm parameters:")
    println("  - Number of echoes: $(length(mag_files))")
    println("  - Magnetic field strength: $(B0)T")
    println("  - Echo times: $TEs")

    # Validate input
    if length(mag_files) != length(phas_files)
        error("[ERROR] Number of magnitude files ($(length(mag_files))) must match number of phase files ($(length(phas_files)))")
    end
    if length(mag_files) != length(TEs)
        error("[ERROR] Number of image files ($(length(mag_files))) must match number of echo times ($(length(TEs)))")
    end

    # Constants
    γ = 267.52  # gyromagnetic ratio (MHz/T)
    bdir = (0.,0.,1.)   # direction of B-field
    vsz = (1.0, 1.0, 1.0)   # voxel size (assuming isotropic)

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
    println("[INFO] Unwrapping phase with Laplacian method...")
    uphas = unwrap_laplacian(phas, mask, vsz)

    # Convert units
    println("[INFO] Converting phase units...")
    @views for t in axes(uphas, 4)
        uphas[:,:,:,t] .*= inv(B0 * γ * TEs[t])
    end

    # Remove non-harmonic background fields
    println("[INFO] Removing non-harmonic background fields with V-SHARP...")
    fl, mask2 = vsharp(uphas, mask, vsz)

    # Perform dipole inversion
    println("[INFO] Performing dipole inversion with RTS...")
    x = rts(fl, mask2, vsz, bdir=bdir)
    x = mean(x, dims = 4)
    println("[INFO] Dipole inversion completed.")

    # Save the output
    println("[INFO] Saving output to $output_file")
    ni = NIVolume(x[:,:,:]; voxel_size=vsz, orientation=nothing, dim_info=Integer.(vsz), time_step=0f0)
    niwrite(output_file, ni)

    println("[INFO] Laplacian + V-SHARP + RTS pipeline completed successfully.")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end