# Repository instructions

- Inspect the existing implementation before editing.
- Preserve the v1 event contract in `schemas/user-behaviour-v1.schema.json`.
- Keep feature changes small and coherent; avoid unnecessary frameworks and infrastructure.
- Run relevant tests and report only results that were actually observed.
- Distinguish verified behavior from assumptions.
- Never delete Kafka volumes, Parquet datasets, or Spark checkpoints without explicit approval.
- Report correctness and security issues separately from optional improvements.
- Do not claim exactly-once delivery end to end.
- Document important AI-assisted engineering decisions in the relevant project documentation.
