# Neurodesktop runtime configuration

## Nested Apptainer/Singularity app containers

Neurodesktop is expected to run in environments where nested container execution is allowed, such as HPC systems, Docker deployments with host-level setup, or a VM/container host configured for this workflow.

On Ubuntu hosts, AppArmor can block rootless nested Apptainer/Singularity even when unprivileged user namespaces are otherwise enabled. A typical failure is:

```text
Could not write info to setgroups: Permission denied
Error while waiting event for user namespace mappings: no event received
```

This is a host security-policy limitation rather than a broken Neurodesk app container. Supported workarounds are to run on a host profile that permits the nested runtime, disable Ubuntu's AppArmor user-namespace restriction for the deployment, or bind a host setuid Singularity runtime and set `NEURODESKTOP_NESTED_CONTAINER_RUNTIME=host`.

The default `neurodesk_singularity_opts` uses `/tmp/apptainer_overlay` for app containers. Some setuid Singularity installations reject directory overlays for non-root users; in those environments use a rootless/user-namespace launch mode, a writable overlay image, or the host-runtime path above.
