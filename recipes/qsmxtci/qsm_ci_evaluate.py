#!/usr/bin/env python
"""
QSM Evaluation Script for Neurocontainers
Adapted from QSM-CI eval.py for container integration
"""

import json
import argparse
import os
import numpy as np
import nibabel as nib
from sklearn.metrics import mean_squared_error
from skimage.metrics import structural_similarity
from sklearn.metrics import normalized_mutual_info_score
from scipy.ndimage import gaussian_laplace
from scipy.stats import pearsonr
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

def calculate_rmse(pred_data, ref_data):
    """Calculate Root Mean Square Error"""
    return np.sqrt(mean_squared_error(ref_data, pred_data))

def calculate_nrmse(pred_data, ref_data):
    """Calculate Normalized Root Mean Square Error"""
    rmse = calculate_rmse(pred_data, ref_data)
    data_range = np.max(ref_data) - np.min(ref_data)
    return rmse / data_range if data_range != 0 else 0

def calculate_hfen(pred_data, ref_data):
    """Calculate High Frequency Error Norm"""
    sigma = 1.5
    pred_log = gaussian_laplace(pred_data, sigma=sigma)
    ref_log = gaussian_laplace(ref_data, sigma=sigma)
    hfen = np.linalg.norm(pred_log - ref_log) / np.linalg.norm(ref_log)
    return hfen

def calculate_xsim(pred_data, ref_data):
    """Calculate Structural Similarity Index"""
    data_range = np.max(ref_data) - np.min(ref_data)
    xsim = structural_similarity(
        pred_data, ref_data, 
        data_range=data_range,
        full=False
    )
    return xsim

def calculate_mad(pred_data, ref_data):
    """Calculate Median Absolute Deviation"""
    return np.median(np.abs(pred_data - ref_data))

def calculate_cc(pred_data, ref_data):
    """Calculate Correlation Coefficient"""
    correlation, _ = pearsonr(pred_data.flatten(), ref_data.flatten())
    return correlation

def calculate_nmi(pred_data, ref_data):
    """Calculate Normalized Mutual Information"""
    # Discretize the data for NMI calculation
    n_bins = 100
    pred_discrete = np.digitize(pred_data.flatten(), 
                                np.linspace(pred_data.min(), pred_data.max(), n_bins))
    ref_discrete = np.digitize(ref_data.flatten(), 
                               np.linspace(ref_data.min(), ref_data.max(), n_bins))
    return normalized_mutual_info_score(ref_discrete, pred_discrete)

def calculate_gxe(pred_data, ref_data):
    """Calculate Gradient Difference Error"""
    # Calculate gradients
    pred_grad = np.gradient(pred_data)
    ref_grad = np.gradient(ref_data)
    
    # Calculate gradient magnitude
    pred_grad_mag = np.sqrt(sum([g**2 for g in pred_grad]))
    ref_grad_mag = np.sqrt(sum([g**2 for g in ref_grad]))
    
    # Calculate error
    gxe = np.mean(np.abs(pred_grad_mag - ref_grad_mag))
    return gxe

def all_metrics(pred_data, ref_data, roi):
    """Calculate all metrics"""
    # Apply ROI mask
    pred_roi = pred_data[roi]
    ref_roi = ref_data[roi]
    
    metrics = {
        'RMSE': float(calculate_rmse(pred_roi, ref_roi)),
        'NRMSE': float(calculate_nrmse(pred_roi, ref_roi)),
        'HFEN': float(calculate_hfen(pred_data, ref_data)),  # Full volume for HFEN
        'XSIM': float(calculate_xsim(pred_data, ref_data)),  # Full volume for XSIM
        'MAD': float(calculate_mad(pred_roi, ref_roi)),
        'CC': float(calculate_cc(pred_roi, ref_roi)),
        'NMI': float(calculate_nmi(pred_roi, ref_roi)),
        'GXE': float(calculate_gxe(pred_data, ref_data))  # Full volume for gradients
    }
    
    return metrics

def create_sagittal_visualization(estimate_data, ground_truth_data, roi_data, output_path, algorithm_name):
    """Create sagittal view comparison visualization"""
    # Get middle sagittal slice
    sagittal_slice = estimate_data.shape[0] // 2

    # Extract sagittal slices
    est_sagittal = estimate_data[sagittal_slice, :, :]
    gt_sagittal = ground_truth_data[sagittal_slice, :, :]
    roi_sagittal = roi_data[sagittal_slice, :, :].astype(bool)

    # Create figure with subplots
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # Common colormap and range
    vmin = min(np.min(est_sagittal), np.min(gt_sagittal))
    vmax = max(np.max(est_sagittal), np.max(gt_sagittal))

    # Plot ground truth
    im1 = axes[0].imshow(gt_sagittal.T, cmap='gray', vmin=vmin, vmax=vmax, origin='lower')
    axes[0].set_title('Ground Truth', fontsize=12)
    axes[0].axis('off')

    # Plot reconstruction
    im2 = axes[1].imshow(est_sagittal.T, cmap='gray', vmin=vmin, vmax=vmax, origin='lower')
    axes[1].set_title(f'{algorithm_name} Reconstruction', fontsize=12)
    axes[1].axis('off')

    # Plot difference with ROI overlay
    diff = est_sagittal - gt_sagittal
    diff_masked = np.where(roi_sagittal, diff, np.nan)
    im3 = axes[2].imshow(diff_masked.T, cmap='RdBu_r', origin='lower')
    axes[2].set_title('Difference (ROI)', fontsize=12)
    axes[2].axis('off')

    # Add colorbars
    plt.colorbar(im1, ax=axes[0], shrink=0.8)
    plt.colorbar(im2, ax=axes[1], shrink=0.8)
    plt.colorbar(im3, ax=axes[2], shrink=0.8)

    plt.suptitle(f'QSM Reconstruction Comparison: {algorithm_name}', fontsize=14)
    plt.tight_layout()

    # Save the figure
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Sagittal visualization saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Evaluate QSM reconstruction metrics')
    parser.add_argument('--estimate', required=True, help='Path to estimated QSM')
    parser.add_argument('--ground_truth', required=True, help='Path to ground truth QSM')
    parser.add_argument('--roi', required=True, help='Path to ROI mask')
    parser.add_argument('--output_dir', required=True, help='Output directory for metrics')
    parser.add_argument('--algorithm', help='Algorithm name for metadata')
    
    args = parser.parse_args()
    
    # Load data
    print(f"Loading estimate: {args.estimate}")
    estimate_nii = nib.load(args.estimate)
    estimate_data = estimate_nii.get_fdata()
    
    print(f"Loading ground truth: {args.ground_truth}")
    truth_nii = nib.load(args.ground_truth)
    truth_data = truth_nii.get_fdata()
    
    print(f"Loading ROI: {args.roi}")
    roi_nii = nib.load(args.roi)
    roi_data = roi_nii.get_fdata().astype(bool)
    
    # Calculate metrics
    print("Calculating metrics...")
    metrics = all_metrics(estimate_data, truth_data, roi_data)
    
    # Add metadata
    if args.algorithm:
        metrics['algorithm'] = args.algorithm
    
    # Save metrics
    os.makedirs(args.output_dir, exist_ok=True)
    metrics_file = os.path.join(args.output_dir, 'metrics.json')
    
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"Metrics saved to: {metrics_file}")

    # Create visualization
    print("Creating sagittal visualization...")
    algorithm_name = args.algorithm if args.algorithm else "Unknown"
    viz_file = os.path.join(args.output_dir, f'{algorithm_name}_sagittal_comparison.png')
    create_sagittal_visualization(estimate_data, truth_data, roi_data, viz_file, algorithm_name)

    # Print metrics
    print("\nEvaluation Results:")
    print("-" * 40)
    for metric, value in metrics.items():
        if metric != 'algorithm':
            print(f"{metric:8s}: {value:.6f}")
    
    return 0

if __name__ == '__main__':
    exit(main())