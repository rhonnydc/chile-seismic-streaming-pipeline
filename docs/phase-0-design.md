# Phase 0 Design

## Objective

Phase 0 prepares the repository foundation for a streaming Data Engineering portfolio project. It focuses on structure, conventions, documentation, and placeholders.

It does not implement the streaming pipeline yet.

## Included

- Base repository structure.
- Initial README.
- Python package layout under `src/`.
- Topic and schema naming conventions.
- Documentation structure under `docs/`.
- `.env.example` with planned local configuration.
- `Makefile` with stable workflow commands.
- Placeholder infrastructure folders for Docker and Terraform.
- Placeholder test folders for unit, integration, and contract tests.
- Initial Avro schemas for planned event contracts.

## Not Included

- Running Kafka.
- Running Schema Registry.
- Running Kpow.
- Running Postgres.
- Producer implementation.
- Consumer implementation.
- Data quality implementation.
- GitHub Actions implementation.
- Terraform resources.
- External API integration.

## Completion Checklist

- [x] Repository has a clear base structure.
- [x] README explains the project, target architecture, current phase, planned tools, and MVP exclusions.
- [x] `.env.example` exists without real secrets.
- [x] `Makefile` defines the expected local workflow commands.
- [x] `pyproject.toml` defines the Python package and dev tooling.
- [x] `docs/` contains architecture, naming, decisions, roadmap, and phase scope.
- [x] `schemas/` contains initial contract files.
- [x] `tests/` is split into unit, integration, and contract areas.
- [x] `infra/` separates Docker and Terraform placeholders.
- [x] Terraform is documented as future cloud-ready infrastructure, not a local requirement.
- [x] Fake data is documented as the first source.
- [x] Heavy tools excluded from the MVP are documented.
