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
        "--tgv_iterations"
            help = "Number of TGV iterations"
            arg_type = Int
            default = 1000
        "--tgv_alpha1"
            help = "TGV regularization parameter alpha1"
            arg_type = Float64
            default = 0.0015
        "--tgv_alpha2"
            help = "TGV regularization parameter alpha2"
            arg_type = Float64
            default = 0.0005
    end
    return parse_args(s)
end

function main()
    println("[INFO] Starting QSM-TGV algorithm...")

    # Parse command line arguments
    args = parse_commandline()
    output_file = args["output_file"]
    mag_files = args["mag_files"]
    phase_files = args["phase_files"]
    TEs = args["echo_times"]
    B0 = args["field_strength"]
    mask_file = args["mask_file"]
    tgv_iterations = args["tgv_iterations"]
    tgv_alpha1 = args["tgv_alpha1"]
    tgv_alpha2 = args["tgv_alpha2"]

    println("[INFO] Parameters:")
    println("  - TGV iterations: $tgv_iterations")
    println("  - TGV alpha1: $tgv_alpha1")
    println("  - TGV alpha2: $tgv_alpha2")
    println("  - Number of echoes: $(length(mag_files))")
    println("  - Magnetic field strength: $(B0)T")
    println("  - Echo times: $TEs")

    # Validate input
    if length(mag_files) != length(phase_files)
        error("[ERROR] Number of magnitude files ($(length(mag_files))) must match number of phase files ($(length(phase_files)))")
    end
    if length(mag_files) != length(TEs)
        error("[ERROR] Number of image files ($(length(mag_files))) must match number of echo times ($(length(TEs)))")
    end

    # Load images
    println("[INFO] Loading magnitude and phase images...")
    num_echoes = length(mag_files)
    mag_tmp = niread(mag_files[1])
    img_shape = size(Float32.(mag_tmp))
    mag = Array{Float32}(undef, tuple(img_shape..., num_echoes))
    phas = Array{Float32}(undef, tuple(img_shape..., num_echoes))

    for i in 1:num_echoes
        println("[INFO] Loading echo $i...")
        mag[:,:,:,i] = Float32.(niread(mag_files[i]))
        phas[:,:,:,i] = Float32.(niread(phase_files[i]))
    end

    # Load mask
    println("[INFO] Loading mask: $mask_file")
    mask = Bool.(niread(mask_file))

    # Algorithm parameters
    vsz = (1.0, 1.0, 1.0)
    bdir = (0.,0.,1.)
    γ = 267.52  # gyromagnetic ratio (MHz/T)

    # QSM Pipeline with TGV
    println("[INFO] Unwrapping phase...")
    uphas = unwrap_laplacian(phas, mask, vsz)

    println("[INFO] Converting phase units...")
    for t in axes(uphas, 4)
        uphas[:,:,:,t] .*= inv(B0 * γ * TEs[t])
    end

    println("[INFO] Removing background fields with V-SHARP...")
    fl, mask2 = vsharp(uphas, mask, vsz)

    println("[INFO] Performing TGV dipole inversion...")
    x = tgv_qsm(fl, mask2, vsz, bdir=bdir, iterations=tgv_iterations, α=tgv_alpha1, β=tgv_alpha2)
    x = mean(x, dims=4)

    println("[INFO] Saving output to: $output_file")
    ni = NIVolume(x[:,:,:]; voxel_size=vsz)
    niwrite(output_file, ni)

    println("[INFO] QSM-TGV algorithm completed successfully!")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end