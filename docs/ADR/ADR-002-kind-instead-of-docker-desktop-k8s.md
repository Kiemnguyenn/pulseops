# ADR-002: Use kind (Kubernetes in Docker) Instead of Docker Desktop K8s

## Context
A K8s environment was needed to test the Helm chart, but Docker Desktop's built-in Kubernetes feature failed to start via the UI.

## Decision
Switch to kind — a tool that runs a full K8s cluster inside a single Docker container.

## Reasons
kind reuses the already-running Docker Engine, doesn't depend on Docker Desktop's broken built-in K8s feature, installs quickly, and fits a local dev/test environment well.

## Trade-offs
kind isn't suitable for real production use, only local testing — but that matches this project's actual use case exactly (testing Helm lint + dry-run via `helm template`, see Runbook-002).
