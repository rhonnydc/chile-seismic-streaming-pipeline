# 007 — Optional cloud infrastructure

**Status:** Proposed · **Phase:** 7

**Decision.** Add Terraform only after the local pipeline and CI are stable. Keep Compose as the reproducible development path. Isolate cloud credentials, state, and environment-specific settings from application code and committed files.

**Reason.** Provisioning should deploy a proven data flow, not determine its event semantics or block local validation.

**Adoption gate.** A cloud deployment consumes the same versioned event contracts and passes the same contract tests. Local tests and demos continue to run without cloud access.
