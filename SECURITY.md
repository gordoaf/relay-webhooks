# Security

For a vulnerability, use the repository's private vulnerability reporting feature. Please do not put credentials or live payloads in public issues.

Version 0.1 is an initial release. It has automated boundary and failure tests, but has not had an independent security audit.

The administrator token controls all projects on an instance. Project keys can only ingest events for their own project. The database holds signing secrets and event payloads; restrict filesystem access, encrypt storage as appropriate, and keep backups private.

See [operations](docs/operations.md) for deployment boundaries and [architecture](docs/architecture.md) for delivery semantics.
