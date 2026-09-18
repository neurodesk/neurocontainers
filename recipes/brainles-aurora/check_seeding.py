"""Assert that seeding actually reaches AURORA's test-time augmentation.

Upstream's `_apply_test_time_augmentations` draws fresh `RandGaussianNoised`
noise in each of its four rounds and nothing seeds it, so the default
configuration is not reproducible: the same image, the same flags and the same
container return slightly different lesion volumes run to run.

The obvious fix does not work. `monai.utils.set_determinism` seeds `torch`,
`random` and `np.random`, but its own docstring says it "will not affect the
randomizable objects in monai.transforms.Randomizable, which have independent
random states". Those objects draw from `Randomizable.R`, a class-level
`RandomState` built at import time, and `RandGaussianNoised` never calls
`set_random_state`, so `R` has to be replaced directly -- which is what the
`aurora` wrapper does.

This check takes seconds and fails the build if that stops being true, e.g.
because a MONAI release changes where the transforms get their randomness.
"""

import numpy as np
import torch
from monai.transforms import RandGaussianNoised, Randomizable
from monai.utils import set_determinism


def draw(seed):
    """One augmented tensor, produced the way the wrapper seeds a run."""
    set_determinism(seed=seed)
    Randomizable.R = np.random.RandomState(seed)
    data = {"images": torch.zeros(1, 4, 4)}
    return RandGaussianNoised(keys="images", prob=1.0, std=0.001)(data)["images"].numpy()


same = draw(0), draw(0)
if not np.array_equal(*same):
    raise SystemExit(
        "seeding does not reach the test-time augmentation; the --seed flag "
        "would be inert and default runs would stay irreproducible"
    )

# A different seed must still give different noise, otherwise the check above
# would pass on a transform that had silently stopped being random at all.
if np.array_equal(draw(0), draw(1)):
    raise SystemExit("two different seeds produced identical noise; check is not testing anything")

print("seeded augmentation is reproducible")
