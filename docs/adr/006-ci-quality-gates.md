# 006 — CI quality gates

**Status:** Accepted · **Phase:** 7, extended in Phase 11

**Decision.** GitHub Actions runs Ruff linting and the fast pytest suite through `make ci` on each push and pull request. The fast suite includes unit, contract, enrichment, sink, and data quality tests without external services. Tests marked `integration` are excluded from this job. A separate job runs `make test-integration` against isolated Kafka, Schema Registry, and PostgreSQL services.

**Reason.** Separating fast checks from real-service integration checks keeps the dependency boundary visible while verifying process boundaries in CI.

**Verification.** The [workflow](../../.github/workflows/ci.yml) has separate `checks` and `integration` jobs. The [integration runner](../../scripts/run_integration.py) owns setup, readiness, test execution, and teardown.
