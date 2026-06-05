# Documentation

This directory keeps detailed project notes out of the root README. The root README should stay short: what the project is, what works, how to run it, and where deeper documents live.

## Operations

- [configuration.md](configuration.md) — environment variables, CLI examples, local run, and verification.
- [live-recovery.md](live-recovery.md) — current live reconnect behavior, recovery state, and gap-risk semantics.
- [provider-compatibility.md](provider-compatibility.md) — checklist for validating a Yellowstone provider.
- [provider-matrix.md](provider-matrix.md) — status matrix for candidate providers.
- [benchmarks.md](benchmarks.md) — measured throughput, latency, and reproduction steps.

## Data Model And Reliability

- [event-identity.md](event-identity.md) — current event identity contract, guarantees, and limitations.
- [finalized-reconciliation.md](finalized-reconciliation.md) — design for finalized-slot reconciliation and gap-aware cursor semantics.
- [reliability.md](reliability.md) — at-least-once model, idempotency, cursor semantics, and non-guarantees.

## Architecture

- [architecture.md](architecture.md) — crate structure, data flow, and design decisions.

## Documentation Rules

- Keep README concise and link to docs for detail.
- Separate implemented behavior from roadmap and design.
- Do not claim provider support without a dated verification record.
- Do not claim gap-free recovery until finalized-slot reconciliation is implemented and tested.
