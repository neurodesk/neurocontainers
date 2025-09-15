#!/usr/bin/env bash

# Laplacian V-SHARP RTS QSM Pipeline
# Adapted for neurocontainers BIDS format

set -e

# Parse command line arguments
BIDS_DIR=""
OUTPUT_DIR=""
SUBJECT=""
SESSION=""
RUN=""
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --subject)
            if [[ -n "$2" ]] && [[ "$2" != --* ]]; then
                SUBJECT="$2"
                shift 2
            else
                shift 1
            fi
            ;;
        --session)
            if [[ -n "$2" ]] && [[ "$2" != --* ]]; then
                SESSION="$2"
                shift 2
            else
                shift 1
            fi
            ;;
        --run)
            if [[ -n "$2" ]] && [[ "$2" != --* ]]; then
                RUN="$2"
                shift 2
            else
                shift 1
            fi
            ;;
        --*)
            if [[ -n "$2" ]] && [[ "$2" != --* ]]; then
                EXTRA_ARGS="$EXTRA_ARGS $1 $2"
                shift 2
            else
                EXTRA_ARGS="$EXTRA_ARGS $1"
                shift 1
            fi
            ;;
        *)
            if [ -z "$BIDS_DIR" ]; then
                BIDS_DIR="$1"
            elif [ -z "$OUTPUT_DIR" ]; then
                OUTPUT_DIR="$1"
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BIDS_DIR" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$SUBJECT" ]; then
    echo "Error: Missing required arguments"
    echo "Usage: $0 <bids_dir> <output_dir> --subject <subject_id> [--session <session>] [--run <run>]"
    exit 1
fi

echo "[INFO] Starting Laplacian V-SHARP RTS pipeline..."
echo "[INFO] BIDS directory: $BIDS_DIR"
echo "[INFO] Output directory: $OUTPUT_DIR"
echo "[INFO] Subject: $SUBJECT"
echo "[INFO] Session: $SESSION"
echo "[INFO] Run: $RUN"

# Create output directories
DERIVATIVES_DIR="$OUTPUT_DIR/derivatives/laplacian-vsharp-rts"
SUBJECT_DIR="$DERIVATIVES_DIR/sub-$SUBJECT"
if [ -n "$SESSION" ]; then
    ANAT_DIR="$SUBJECT_DIR/ses-$SESSION/anat"
else
    ANAT_DIR="$SUBJECT_DIR/anat"
fi
mkdir -p "$ANAT_DIR"

# Find input files in BIDS structure
SUBJECT_BIDS_DIR="$BIDS_DIR/sub-$SUBJECT"
if [ -n "$SESSION" ]; then
    SUBJECT_BIDS_DIR="$SUBJECT_BIDS_DIR/ses-$SESSION"
fi
ANAT_INPUT_DIR="$SUBJECT_BIDS_DIR/anat"

echo "[INFO] Looking for input files in: $ANAT_INPUT_DIR"

# Find magnitude and phase files
if [ -n "$RUN" ]; then
    MAG_FILES=($(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*run-${RUN}*part-mag*T2starw.nii*" | sort))
    PHASE_FILES=($(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*run-${RUN}*part-phase*T2starw.nii*" | sort))
    JSON_FILE=$(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*run-${RUN}*T2starw.json" | head -1)
else
    MAG_FILES=($(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*part-mag*T2starw.nii*" | sort))
    PHASE_FILES=($(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*part-phase*T2starw.nii*" | sort))
    JSON_FILE=$(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*T2starw.json" | head -1)
fi

# Check if files were found
if [ ${#MAG_FILES[@]} -eq 0 ] || [ ${#PHASE_FILES[@]} -eq 0 ]; then
    echo "[ERROR] No magnitude or phase files found in $ANAT_INPUT_DIR"
    echo "Looking for pattern: sub-${SUBJECT}*part-mag*T2starw.nii*"
    echo "Available files:"
    ls -la "$ANAT_INPUT_DIR" || echo "Directory not found"
    exit 1
fi

if [ ! -f "$JSON_FILE" ]; then
    echo "[ERROR] No JSON sidecar found for echo times"
    exit 1
fi

echo "[INFO] Found ${#MAG_FILES[@]} magnitude files and ${#PHASE_FILES[@]} phase files"
echo "[INFO] JSON file: $JSON_FILE"

# Extract metadata from JSON
ECHO_TIMES=($(jq -r '.EchoTime // empty' "$JSON_FILE"))
if [ ${#ECHO_TIMES[@]} -eq 0 ]; then
    # Try to get from multiple echo files
    ECHO_TIMES=($(find "$ANAT_INPUT_DIR" -name "sub-${SUBJECT}*echo-*T2starw.json" | sort | xargs -I {} jq -r '.EchoTime' {}))
fi

FIELD_STRENGTH=$(jq -r '.MagneticFieldStrength // 3.0' "$JSON_FILE")

if [ ${#ECHO_TIMES[@]} -eq 0 ]; then
    echo "[ERROR] Could not extract echo times from JSON files"
    exit 1
fi

echo "[INFO] Echo times: ${ECHO_TIMES[@]}"
echo "[INFO] Field strength: $FIELD_STRENGTH T"

# Create a simple mask (you might want to use a more sophisticated approach)
echo "[INFO] Creating brain mask..."
MASK_FILE="$ANAT_DIR/mask.nii.gz"

# Use first magnitude image to create mask
if command -v bet > /dev/null; then
    bet "${MAG_FILES[0]}" "$ANAT_DIR/temp_brain" -m -f 0.3
    mv "$ANAT_DIR/temp_brain_mask.nii.gz" "$MASK_FILE"
    rm -f "$ANAT_DIR/temp_brain.nii.gz"
else
    # Simple threshold-based mask if bet not available
    echo "[WARNING] FSL bet not available, creating simple threshold mask"
    python3 -c "
import nibabel as nib
import numpy as np
mag = nib.load('${MAG_FILES[0]}')
data = mag.get_fdata()
threshold = np.percentile(data[data > 0], 20)
mask = data > threshold
mask_img = nib.Nifti1Image(mask.astype(np.uint8), mag.affine, mag.header)
nib.save(mask_img, '$MASK_FILE')
"
fi

# Create inputs.json for Julia pipeline
INPUTS_JSON="inputs.json"
echo "[INFO] Creating inputs.json..."

# Build JSON with file arrays
MAG_JSON=$(printf '%s\n' "${MAG_FILES[@]}" | jq -R . | jq -s .)
PHASE_JSON=$(printf '%s\n' "${PHASE_FILES[@]}" | jq -R . | jq -s .)
ECHO_JSON=$(printf '%s\n' "${ECHO_TIMES[@]}" | jq -R . | jq -s . | jq 'map(tonumber)')

cat > "$INPUTS_JSON" << EOF
{
    "mask": "$MASK_FILE",
    "mag_nii": $MAG_JSON,
    "phase_nii": $PHASE_JSON,
    "EchoTime": $ECHO_JSON,
    "MagneticFieldStrength": $FIELD_STRENGTH
}
EOF

echo "[INFO] Created inputs.json:"
cat "$INPUTS_JSON"

# Check if Julia is already installed (in container)
if command -v julia > /dev/null; then
    echo "[INFO] Julia already installed"
    JULIA_CMD="julia"

    # Install packages at runtime to avoid precompilation issues
    echo "[INFO] Installing Julia packages at runtime"
    $JULIA_CMD /opt/qsm/install_packages.jl
else
    # Download and setup Julia
    echo "[INFO] Downloading Julia"
    apt-get update
    apt-get install wget build-essential libfftw3-dev python3 python3-pip -y

    # Install nibabel for mask creation
    pip3 install nibabel

    wget https://julialang-s3.julialang.org/bin/linux/x64/1.9/julia-1.9.4-linux-x86_64.tar.gz
    tar xf julia-1.9.4-linux-x86_64.tar.gz
    JULIA_CMD="julia-1.9.4/bin/julia"

    echo "[INFO] Installing Julia packages"
    $JULIA_CMD /opt/qsm/install_packages.jl
fi

echo "[DEBUG] Available disk space:"
df -h
echo "[DEBUG] Available memory:"
free -h

echo "[INFO] Starting reconstruction with QSM.jl"
$JULIA_CMD /opt/qsm/pipeline.jl

# Move output to BIDS derivatives location
if [ -f "out.nii.gz" ]; then
    OUTPUT_FILE="$ANAT_DIR/sub-${SUBJECT}"
    if [ -n "$SESSION" ]; then
        OUTPUT_FILE="${OUTPUT_FILE}_ses-${SESSION}"
    fi
    if [ -n "$RUN" ]; then
        OUTPUT_FILE="${OUTPUT_FILE}_run-${RUN}"
    fi
    OUTPUT_FILE="${OUTPUT_FILE}_Chimap.nii.gz"

    mv "out.nii.gz" "$OUTPUT_FILE"
    echo "[INFO] QSM reconstruction completed - output file: $OUTPUT_FILE"
    ls -la "$OUTPUT_FILE"
else
    echo "[ERROR] Output file not generated"
    exit 1
fi

echo "[INFO] Pipeline completed successfully"