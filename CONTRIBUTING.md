# Contributing

Thank you for your interest in this project. Constructive feedback and small improvements are welcome.

## How to contribute

1. Open an issue to discuss the change before large pull requests.
2. Keep changes focused and minimal.
3. Follow the existing Rust style (`cargo fmt`, `cargo clippy`).
4. Ensure `make verify` passes locally.
5. Write clear commit messages using [Conventional Commits](https://www.conventionalcommits.org/).

## Development setup

```bash
# Start PostgreSQL
docker compose up -d postgres

# Run quality gate
make verify

# Run benchmarks
make bench
```

## Scope

The project prioritizes a narrow, reliable MVP over broad feature expansion. Please check [BACKLOG.md](internal_documents/BACKLOG.md) (if available) or open an issue to align on roadmap before proposing large changes.

## License

By contributing, you agree that your contributions will be licensed under the same terms as the project: MIT OR Apache-2.0.
