# Container identity labels

Candidate and manual builds pass recipe metadata to Docker before creating or
testing the SIF. The resulting image config overrides base-image labels with the
concrete container name, software version plus build date, source commit,
documentation, description, declared licenses, and base image. SIF referrer
annotations copy these values from the built image instead of recalculating them
at publication time.

Inspect the arguments for a recipe:

```bash
python3 -m builder.oci_labels recipe recipes/niimath \
  --architecture x86_64 --build-date 20261003 --revision "$(git rev-parse HEAD)"
```

The creation timestamp represents the recipe's dated build at midnight UTC;
it is not a measured build completion time. Recipes without license metadata
report `NOASSERTION`, rather than inheriting the base distribution's license.
Declared component licenses are combined with `AND`; this is metadata from the
recipe, not a license audit of every installed dependency. Descriptions prefer
the structured README, with the first content line of the README as a fallback.

Content comparison ignores creation time and source commit, normalizes the
commit in the repository documentation link, and removes only the trailing
eight-digit build date from the version label. A software version or other
metadata change still changes the fingerprint. Existing images retain their
current labels until rebuilt; no published digest is mutated.
