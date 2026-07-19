# Runbook-002: CD Fails at Deploy to Prod — "Kubernetes Cluster Unreachable"

## Symptoms
The `deploy-prod` job in `cd.yml` fails with:

Error: Kubernetes cluster unreachable: Get "http://localhost:8080/version": dial tcp [::1]:8080: connect: connection refused

## Impact
The CD pipeline stops at `deploy-prod`; the `notify-slack` job never runs (it depends on `deploy-prod`).

## Immediate Actions
```bash
# Root cause confirmed: `helm upgrade` (even with --dry-run) still needs to connect
# to a real K8s cluster to validate — the GitHub Actions runner has none available.
# Edit .github/workflows/cd.yml, change:
#   run: helm upgrade --install pulseops-prod ./helm/pulseops --values ... --dry-run
# to:
#   run: helm template pulseops-prod ./helm/pulseops --values ./helm/pulseops/values-prod.yaml
git add .
git commit -m "fix(cd): change helm upgrade to template for offline validation"
git push
```

## Root Cause
`helm upgrade` (even in dry-run mode) always attempts to connect to a real K8s cluster for validation — the GitHub Actions runner has no cluster available. `helm template` only renders manifests offline, requiring no live cluster, which fits a CI environment without Kubernetes.

## Escalation
If real deployment validation (not just offline rendering) is needed later, consider spinning up a `kind` cluster directly inside `cd.yml` (using the `helm/kind-action` action) before running a real `helm upgrade` — out of scope for this 9-day build, but a reasonable future extension.

## Note
This issue was inherited and fixed before the PulseOps pivot (originally from the FintechFlow predecessor project) — `cd.yml` has used `helm template` since Day 1 of PulseOps and has passed consistently across every run in the first 6 days, with no recurrence.
