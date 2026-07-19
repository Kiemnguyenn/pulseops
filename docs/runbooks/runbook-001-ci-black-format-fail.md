# Runbook-001: CI Fails at Code Formatting Check (Black)

## Symptoms
The `code-quality-and-test` job in `ci.yml` fails at the `black --check .` step. Log output looks like:

>would reformat /home/runner/work/pulseops/pulseops/<file>.py
1 file would be reformatted, N files left unchanged.
Error: Process completed with exit code 1.

## Impact
The PR cannot be merged; the entire CD pipeline downstream is blocked until CI passes.

## Immediate Actions
```bash
python -m black .
git diff              # confirm Black only changed formatting, not logic
git add .
git commit -m "style: auto format code with black to pass CI"
git push origin <feature-branch>
```

## Root Cause
Code was written locally without running Black before commit (no pre-commit hook triggered, or it was skipped). This incident recurred 3 times during the actual PulseOps build (Day 3 — `bronze_writer.py`, `test_bronze_writer.py`; Day 5 — `core_pipeline.py`; Day 6 — `custom_exporter.py`) — each time on a newly written Python file that had never been run through `black .` before its first commit.

## Escalation
If `black --check` still fails after local formatting, check whether the `black` version in CI (installed via `requirements.txt`) differs from the local version — pin the version in `requirements.txt` to keep them in sync.
