# 006 — CI quality gates

**Status:** Proposed · **Phase:** 7

**Decision.** GitHub Actions will run Ruff linting and the fast pytest suite on each push and pull request. The fast suite includes unit, contract, enrichment, sink, and data quality tests that do not require Kafka, Schema Registry, or Postgres. Tests that require real services are marked `integration` and excluded from this initial CI workflow. `make ci` runs the same checks locally.

**Reason.** Fast, service-independent checks give contributors prompt and reproducible feedback without the setup and failure modes of Docker services. Integration tests still provide valuable coverage of process boundaries and can run locally with the required services available.

**Adoption gate.** The workflow fails when Ruff or any fast test fails and passes from a clean checkout without starting Kafka, Schema Registry, or Postgres. Local integration tests remain available through `make test-integration`; adding service-backed CI coverage is a later decision.
