#!/usr/bin/env bash
# QSM-CI Runner Script using External Boutiques Execution
# This script extracts descriptors and executes QSM algorithms via bosh exec launch

set -e

# Parse arguments
ALGORITHM=${1:-}
BIDS_DIR=${2:-}
OUTPUT_DIR=${3:-}
CONTAINER_PATH=${4:-}
CONFIG_JSON=${5:-}

# Function to display usage
usage() {
    echo "Usage: $0 <algorithm> <bids_dir> <output_dir> <container_path> [config_json]"
    echo ""
    echo "External Boutiques execution for QSM-CI algorithms"
    echo ""
    echo "Available algorithms:"
    echo "  qsm-tgv      - Total Generalized Variation"
    echo "  qsm-nextqsm  - NeXtQSM deep learning"
    echo "  qsm-rts      - Rapid Two-Step"
    echo "  qsm-tv       - Total Variation"
    echo ""
    echo "Arguments:"
    echo "  algorithm      - QSM algorithm to run"
    echo "  bids_dir       - Input BIDS directory"
    echo "  output_dir     - Output directory"
    echo "  container_path - Path to QSMxT-CI container (.sif or docker://)"
    echo "  config_json    - Optional: JSON file with algorithm parameters"
    echo ""
    echo "Examples:"
    echo "  $0 qsm-tgv /data/bids /results qsmxt-ci.sif"
    echo "  $0 qsm-nextqsm /data/bids /results docker://vnmd/qsmxt-ci:latest inputs.json"
    exit 1
}

# Check arguments
if [ -z "$ALGORITHM" ] || [ -z "$BIDS_DIR" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$CONTAINER_PATH" ]; then
    usage
fi

# Validate algorithm choice
case "$ALGORITHM" in
    qsm-tgv|qsm-nextqsm|qsm-rts|qsm-tv)
        echo "Running QSM-CI with algorithm: $ALGORITHM"
        ;;
    *)
        echo "Error: Unknown algorithm '$ALGORITHM'"
        usage
        ;;
esac

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check dependencies
if ! command -v bosh &> /dev/null; then
    echo "Installing Boutiques..."
    pip install boutiques --user --quiet
    export PATH=$PATH:$HOME/.local/bin
fi

# Set up working directory
WORK_DIR="/tmp/qsm-ci-work-$$"
mkdir -p "$WORK_DIR"
trap "rm -rf $WORK_DIR" EXIT

echo "Working directory: $WORK_DIR"

# Extract descriptors from container
DESCRIPTOR_DIR="$WORK_DIR/descriptors"
echo "Extracting Boutiques descriptors from container..."

# Use the extraction script we created
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
EXTRACT_SCRIPT="$SCRIPT_DIR/../../scripts/extract_descriptors.sh"

if [ -f "$EXTRACT_SCRIPT" ]; then
    "$EXTRACT_SCRIPT" "$CONTAINER_PATH" "$DESCRIPTOR_DIR"
else
    # Fallback extraction method
    echo "Using fallback extraction method..."
    mkdir -p "$DESCRIPTOR_DIR"
    
    if [[ "$CONTAINER_PATH" == docker://* ]]; then
        # Docker extraction
        CONTAINER_ID=$(docker create "$CONTAINER_PATH")
        docker cp "$CONTAINER_ID:/boutique/" "$DESCRIPTOR_DIR/" 2>/dev/null || {
            echo "Error: Could not extract descriptors from Docker container"
            docker rm "$CONTAINER_ID"
            exit 1
        }
        docker rm "$CONTAINER_ID"
        
        # Move files up from boutique subdirectory
        if [ -d "$DESCRIPTOR_DIR/boutique" ]; then
            mv "$DESCRIPTOR_DIR/boutique"/*.json "$DESCRIPTOR_DIR/" 2>/dev/null || true
            rmdir "$DESCRIPTOR_DIR/boutique" 2>/dev/null || true
        fi
    else
        # Singularity extraction
        singularity exec --bind "$DESCRIPTOR_DIR:/tmp/extract_out" "$CONTAINER_PATH" \
            bash -c "cp /boutique/*.json /tmp/extract_out/ 2>/dev/null || echo 'No descriptors found'"
    fi
fi

# Verify descriptor exists
DESCRIPTOR_FILE="$DESCRIPTOR_DIR/${ALGORITHM}.json"
if [ ! -f "$DESCRIPTOR_FILE" ]; then
    echo "Error: Descriptor not found: $DESCRIPTOR_FILE"
    echo "Available descriptors:"
    ls -la "$DESCRIPTOR_DIR"/*.json 2>/dev/null || echo "No descriptors found"
    exit 1
fi

echo "Using descriptor: $DESCRIPTOR_FILE"

# Validate descriptor
echo "Validating Boutiques descriptor..."
bosh validate "$DESCRIPTOR_FILE" || {
    echo "Warning: Descriptor validation failed, continuing anyway..."
}

# Create or use provided inputs JSON
if [ -z "$CONFIG_JSON" ]; then
    CONFIG_JSON="$WORK_DIR/inputs.json"
    echo "Creating default inputs JSON..."
    
    # Extract BIDS parameters
    SUBJECT=$(ls "$BIDS_DIR" | grep "^sub-" | head -1 | sed 's/sub-//')
    SESSION=$(ls "$BIDS_DIR/sub-${SUBJECT}" | grep "^ses-" | head -1 | sed 's/ses-//' || echo "")
    
    # Create base inputs
    cat > "$CONFIG_JSON" << EOF
{
  "bids_dir": "$BIDS_DIR",
  "output_dir": "$OUTPUT_DIR",
  "subject": "${SUBJECT:-01}"
EOF
    
    # Add session if found
    if [ -n "$SESSION" ]; then
        echo "Adding session: $SESSION"
        python3 -c "
import json
with open('$CONFIG_JSON', 'r') as f:
    data = json.load(f)
data['session'] = '$SESSION'
with open('$CONFIG_JSON', 'w') as f:
    json.dump(data, f, indent=2)
"
    fi
    
    # Add algorithm-specific parameters
    case "$ALGORITHM" in
        qsm-tgv)
            python3 -c "
import json
with open('$CONFIG_JSON', 'r') as f:
    data = json.load(f)
data.update({
    'tgv_iterations': 1000,
    'tgv_alpha1': 0.0015,
    'tgv_alpha2': 0.0005
})
with open('$CONFIG_JSON', 'w') as f:
    json.dump(data, f, indent=2)
"
            ;;
        qsm-rts|qsm-tv)
            python3 -c "
import json
with open('$CONFIG_JSON', 'r') as f:
    data = json.load(f)
data.update({
    'unwrapping': 'romeo',
    'bf_algorithm': 'vsharp'
})
with open('$CONFIG_JSON', 'w') as f:
    json.dump(data, f, indent=2)
"
            ;;
    esac
    
    # Close JSON
    echo "}" >> "$CONFIG_JSON"
    
    # Fix JSON formatting
    python3 -c "
import json
with open('$CONFIG_JSON', 'r') as f:
    content = f.read()
    # Remove extra closing brace
    content = content.rstrip().rstrip('}')
    data = json.loads(content + '}')
with open('$CONFIG_JSON', 'w') as f:
    json.dump(data, f, indent=2)
"
fi

echo "Input configuration:"
cat "$CONFIG_JSON"

# Execute algorithm using external Boutiques
echo ""
echo "Executing $ALGORITHM via external Boutiques..."
echo "Command: bosh exec launch $DESCRIPTOR_FILE $CONFIG_JSON --imagepath $CONTAINER_PATH"

bosh exec launch "$DESCRIPTOR_FILE" "$CONFIG_JSON" --imagepath "$CONTAINER_PATH" -v

# Check execution success
EXEC_STATUS=$?
if [ $EXEC_STATUS -eq 0 ]; then
    echo "✓ Algorithm execution successful"
else
    echo "✗ Algorithm execution failed with status: $EXEC_STATUS"
fi

# Run evaluation if ground truth is available
GROUND_TRUTH="$BIDS_DIR/derivatives/ground_truth/chi.nii.gz"
if [ -f "$GROUND_TRUTH" ]; then
    echo ""
    echo "Ground truth found, running QSM-CI evaluation..."
    
    # Find the output QSM file
    QSM_OUTPUT=$(find "$OUTPUT_DIR" -name "*chi*.nii.gz" -o -name "*Chimap*.nii.gz" 2>/dev/null | head -1)
    ROI="$BIDS_DIR/derivatives/ground_truth/roi.nii.gz"
    
    if [ -f "$QSM_OUTPUT" ] && [ -f "$ROI" ]; then
        echo "Found QSM output: $QSM_OUTPUT"
        echo "Using ROI: $ROI"
        
        # Check if we have the evaluation script
        EVAL_SCRIPT="$SCRIPT_DIR/qsm_ci_evaluate.py"
        if [ ! -f "$EVAL_SCRIPT" ]; then
            EVAL_SCRIPT="/opt/qsm-ci/evaluate.py"
        fi
        
        if command -v python3 &> /dev/null && [ -f "$EVAL_SCRIPT" ]; then
            python3 "$EVAL_SCRIPT" \
                --estimate "$QSM_OUTPUT" \
                --ground_truth "$GROUND_TRUTH" \
                --roi "$ROI" \
                --output_dir "$OUTPUT_DIR/metrics" \
                --algorithm "$ALGORITHM"
            
            echo "Evaluation complete. Metrics saved to $OUTPUT_DIR/metrics/"
            
            # Display metrics summary
            if [ -f "$OUTPUT_DIR/metrics/metrics.json" ]; then
                echo ""
                echo "=== QSM-CI Results for $ALGORITHM ==="
                python3 -c "
import json
with open('$OUTPUT_DIR/metrics/metrics.json', 'r') as f:
    metrics = json.load(f)
print(f'RMSE:  {metrics.get(\"RMSE\", \"N/A\"):.6f}')
print(f'NRMSE: {metrics.get(\"NRMSE\", \"N/A\"):.6f}')
print(f'CC:    {metrics.get(\"CC\", \"N/A\"):.6f}')
print(f'XSIM:  {metrics.get(\"XSIM\", \"N/A\"):.6f}')
print(f'MAD:   {metrics.get(\"MAD\", \"N/A\"):.6f}')
"
            fi
        else
            echo "Warning: Could not run evaluation - missing Python or evaluation script"
        fi
    else
        echo "Warning: Could not find QSM output or ROI for evaluation"
        echo "Looking for files in $OUTPUT_DIR:"
        find "$OUTPUT_DIR" -name "*.nii.gz" 2>/dev/null | head -10
    fi
else
    echo "No ground truth available - skipping evaluation"
fi

echo ""
echo "=== QSM-CI Pipeline Summary ==="
echo "Algorithm: $ALGORITHM"
echo "Status: $([ $EXEC_STATUS -eq 0 ] && echo 'SUCCESS' || echo 'FAILED')"
echo "Output directory: $OUTPUT_DIR"
echo "Container: $CONTAINER_PATH"

# Clean up temporary files is handled by trap