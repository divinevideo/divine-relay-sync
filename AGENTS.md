# Repository Guidelines

## Project Structure & Module Organization
- CLI and library source lives in `src/`.
- Tests live in `tests/`.
- User-facing documentation lives in `README.md` and `CHANGELOG.md`.
- Keep new Rust modules focused. Prefer small, clear modules over expanding `main.rs` or `lib.rs` into broad catch-all files.

## Build, Test, and Development Commands
- `cargo build --release`: build the CLI in release mode.
- `cargo check`: run a fast compile-only validation pass.
- `cargo test`: run the full test suite.
- `cargo run -- ...`: run the CLI locally against test or staging relays.
- If you change flags, config parsing, or sync semantics, update `README.md` examples and test coverage together.

## Coding Style & Naming Conventions
- Use idiomatic Rust with explicit error handling and clear boundaries between config parsing, relay IO, and output formatting.
- Prefer focused helper functions and modules over broad utility collections.
- Keep PRs tightly scoped. Do not mix unrelated cleanup, formatting churn, or speculative refactors into the same change.
- Temporary or transitional code must include `TODO(#issue):` with the tracking issue for removal.

## Pull Request Guardrails
- PR titles must use Conventional Commit format: `type(scope): summary` or `type: summary`.
- Set the correct PR title when opening the PR. Do not rely on fixing it afterward.
- If a PR title changes after opening, verify that the semantic PR title check reruns successfully.
- PR descriptions must include a short summary, motivation, linked issue, and manual test plan.
- Changes to CLI flags, relay filtering, auth behavior, or sync semantics should include representative commands or config examples when helpful.

## Security & Sensitive Information
- Do not commit secrets, relay credentials, private keys, or sensitive relay data.
- Public issues, PRs, branch names, screenshots, and descriptions must not mention corporate partners, customers, brands, campaign names, or other sensitive external identities unless a maintainer explicitly approves it. Use generic descriptors instead.
