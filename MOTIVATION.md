# Project Motivation

Solana data ingestion is operationally difficult. A useful stream processor needs to handle more than connecting to an endpoint:

- streams can disconnect;
- providers differ in replay and start-slot support;
- events may be duplicated around reconnect boundaries;
- downstream storage can slow down;
- process crashes can happen between receive and persist;
- without metrics, lag and failures are hard to diagnose;
- without replay mode, failure cases are hard to test.

The project is intended to be a reliability-first ingestion layer rather than a trading bot, universal Solana indexer, or full decoder for every Solana program.

## Core Value

The core value is a small, observable, idempotent pipeline:

```text
Yellowstone gRPC / replay source
  -> normalization
  -> batching
  -> deduplication
  -> durable storage
  -> metrics and status
```

The first MVP starts with replay mode so the storage, cursor, deduplication, and observability model can be built and tested before depending on a live Yellowstone provider.

## Target Users

- Solana teams that need a simple program/account activity ingestor.
- Analytics teams that want events in PostgreSQL or future ClickHouse storage.
- Infrastructure engineers looking for a reference Rust streaming pipeline.
- Teams that need a reliable upstream ingestion layer before custom business logic.

## Reliability Position

The project should be explicit about its guarantees:

- at-least-once processing inside the pipeline;
- idempotent persistence through stable event IDs;
- cursor updates only after successful persistence;
- bounded channels to avoid unbounded memory growth;
- provider-dependent recovery semantics for live Yellowstone mode.

It should not claim exactly-once upstream delivery or gap-free live recovery unless the selected provider actually supports the needed replay semantics.
