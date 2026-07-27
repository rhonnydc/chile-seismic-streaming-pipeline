# Phase 0 Design

## Objective

Phase 0 establishes the project foundation: repository structure, documentation, naming, configuration examples, initial schemas, and Python packaging.

The phase is intentionally non-runtime. Kafka, Postgres, Schema Registry, Kpow, producers, and consumers are introduced in later phases.

## Deliverables

| Area | Deliverable |
| --- | --- |
| Repository | Base folders for source code, schemas, tests, docs, scripts, and infrastructure. |
| Python | Package skeleton under `src/seismic_pipeline`. |
| Contracts | Initial Avro schema files for raw, enriched, and metric events. |
| Configuration | `.env.example` with local runtime variables. |
| Tooling | `pyproject.toml` and `Makefile` with standard development commands. |
| Documentation | Architecture, roadmap, naming conventions, and technical decisions. |

## Boundaries

Phase 0 does not start services or implement application logic. It prepares the repo so runtime behavior can be added without reorganizing the project.

Out of scope:

- Kafka and topic creation.
- Schema Registry configuration.
- Kpow setup.
- Postgres schema creation.
- Producer and consumer code.
- Dead-letter handling logic.
- Data quality checks.
- GitHub Actions workflows.
- Terraform resources.
- External API integration.

## Completion Checklist

- [x] Repository structure is in place.
- [x] README describes the project, current phase, planned runtime, and exclusions.
- [x] Documentation exists under `docs/`.
- [x] Python package skeleton exists under `src/`.
- [x] Initial Avro schemas exist under `schemas/`.
- [x] Tests are separated into unit, contract, and integration folders.
- [x] `.env.example` contains non-secret local defaults.
- [x] Makefile exposes install, lint, test, and reserved Docker commands.
- [x] Terraform is documented as optional and future-facing.
- [x] Fake data is documented as the default first source.
