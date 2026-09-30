# 006 — CI quality gates

**Status:** Proposed · **Phase:** 6

**Decision.** Run formatting, lint, unit tests, contract tests, and focused Kafka/Postgres integration tests in GitHub Actions. Integration services use disposable local containers and synthetic records; no Kpow license or live earthquake API is required.

**Reason.** The pipeline's main failure modes cross process boundaries and cannot be covered by unit tests alone. CI must remain reproducible for contributors without private credentials.

**Adoption gate.** The workflow fails on a broken contract, delivery path, or sink replay; it passes from a clean checkout with repository defaults.
