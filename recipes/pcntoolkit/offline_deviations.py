"""Analyse and classify synthetic deviation scores using PCNtoolkit 1.3.

Run: python offline_deviations.py ./deviation-example-output
The independent normative reference contains only synthetic controls. Target
labels are never used to fit the normative model. The classifier's test set is
held out before scaling or classifier fitting, including cross-validation.
"""

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pcntoolkit import BLR, NormData, NormativeModel
from scipy.stats import false_discovery_control, ttest_ind
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import RocCurveDisplay, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    features = ["region_a", "region_b", "region_c"]
    slopes = np.array([0.03, -0.02, 0.04])
    reference_age = rng.uniform(20, 80, (240, 1))
    reference_y = reference_age * slopes + rng.normal(0, 0.3, (240, 3))
    reference = NormData.from_ndarrays(
        "reference", reference_age, reference_y,
        batch_effects=np.zeros((240, 1), dtype=int),
    )

    labels = np.repeat([0, 1], 120)
    target_age = rng.uniform(20, 80, (labels.size, 1))
    # Only region_a and region_b receive an artificial case effect.
    target_y = (target_age * slopes + rng.normal(0, 0.3, (labels.size, 3))
                + labels[:, None] * np.array([0.9, -0.9, 0.0]))
    target = NormData.from_ndarrays(
        "target", target_age, target_y,
        batch_effects=np.zeros((labels.size, 1), dtype=int),
    )
    model = NormativeModel(
        BLR(), inscaler="standardize", outscaler="standardize",
        savemodel=True, save_dir=str(output / "normative-model"),
    )
    model.fit_predict(reference, target)
    deviations = target.Z.values
    assert deviations.shape == target_y.shape and np.isfinite(deviations).all()
    np.savez(output / "deviations.npz", deviations=deviations, labels=labels,
             age=target_age, response=target_y)

    # Post-hoc group comparisons are exploratory and separate from prediction.
    controls, cases = deviations[labels == 0], deviations[labels == 1]
    result = ttest_ind(cases, controls, axis=0, equal_var=False)
    adjusted = false_discovery_control(result.pvalue, method="bh")
    differences = cases.mean(axis=0) - controls.mean(axis=0)
    with (output / "posthoc.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["region", "case_minus_control_mean_z", "welch_t",
                         "p_value", "bh_adjusted_p", "control_fraction_abs_z_gt_2",
                         "case_fraction_abs_z_gt_2"])
        for i, feature in enumerate(features):
            writer.writerow([feature, differences[i], result.statistic[i],
                             result.pvalue[i], adjusted[i],
                             np.mean(np.abs(controls[:, i]) > 2),
                             np.mean(np.abs(cases[:, i]) > 2)])
    assert differences[0] > 0 and differences[1] < 0
    assert np.all(adjusted[:2] < 0.05), "Planted group effects were not recovered"

    # Keep all regions: selecting them using the complete target labels would
    # leak information from the held-out test set into classifier training.
    train, test = train_test_split(
        np.arange(labels.size), test_size=0.25, stratify=labels, random_state=42,
    )
    classifier = make_pipeline(StandardScaler(), LogisticRegression(random_state=42))
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_auc = cross_val_score(classifier, deviations[train], labels[train],
                             cv=cv, scoring="roc_auc")
    classifier.fit(deviations[train], labels[train])
    probability = classifier.predict_proba(deviations[test])[:, 1]
    test_auc = roc_auc_score(labels[test], probability)
    assert np.isfinite(cv_auc).all() and np.isfinite(probability).all()
    assert test_auc > 0.8, "Classifier failed to recover the strong synthetic signal"
    np.savez(output / "prediction.npz", train_indices=train, test_indices=test,
             test_labels=labels[test], test_probability=probability)
    metrics = {"reference_subjects": 240, "target_subjects": int(labels.size),
               "classifier_train_subjects": int(train.size),
               "classifier_test_subjects": int(test.size),
               "cross_validation_auc": cv_auc.tolist(), "test_auc": float(test_auc)}
    (output / "prediction.json").write_text(json.dumps(metrics, indent=2) + "\n")

    figure, axes = plt.subplots(1, 2, figsize=(9, 4))
    for group, name in [(0, "controls"), (1, "synthetic cases")]:
        axes[0].hist(deviations[labels == group, 0], bins=20, alpha=0.6, label=name)
    axes[0].set(xlabel="Region A deviation score", ylabel="Subjects")
    axes[0].legend()
    RocCurveDisplay.from_predictions(labels[test], probability, ax=axes[1])
    figure.tight_layout()
    figure.savefig(output / "deviations.png", dpi=120)
    plt.close(figure)
    print(f"Offline post-hoc and deviation prediction passed; held-out AUC={test_auc:.3f}")


if __name__ == "__main__":
    main()
