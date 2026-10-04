"""Fit a small BLR model on generated data without downloading a dataset."""

import argparse
from pathlib import Path

import numpy as np
from pcntoolkit import BLR, NormData, NormativeModel


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    age = rng.uniform(20, 80, (100, 1))
    response = 0.05 * age + rng.normal(0, 0.2, (100, 1))
    data = NormData.from_ndarrays(
        "synthetic", age, response, batch_effects=np.zeros((100, 1), dtype=int)
    )
    train, test = data.train_test_split(random_state=42)
    model = NormativeModel(
        BLR(),
        inscaler="standardize",
        outscaler="standardize",
        savemodel=True,
        save_dir=str(args.output_dir / "model"),
    )
    model.fit_predict(train, test)
    predictions = test.Yhat.values
    deviations = test.Z.values
    assert predictions.shape == test.Y.shape, "prediction shape differs from responses"
    assert predictions.size > 0 and np.isfinite(predictions).all(), "non-finite predictions"
    assert np.isfinite(deviations).all(), "non-finite deviation scores"
    np.savez(args.output_dir / "predictions.npz", predictions=predictions, deviations=deviations)
    print(f"Offline BLR fit/predict passed with {predictions.size} finite predictions")


if __name__ == "__main__":
    main()
