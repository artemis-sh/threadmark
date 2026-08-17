# Contributing

Before requesting review, run the same checks as CI:

```bash
cargo fmt --check
cargo clippy --all-features --all-targets -- -D warnings
cargo test --all-features
```

Run `cargo fmt` before committing when the formatting check reports differences.
