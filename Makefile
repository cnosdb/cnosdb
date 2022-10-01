docs_check:
	cargo doc --no-deps --document-private-items --all-features

docs:
	cargo doc --no-deps --document-private-items --all-features --open

fmt_check:
	cargo +nightly fmt --all -- --check

fmt:
	cargo +nightly fmt --all

clippy_check:
	cargo clippy --workspace --all-features --all-targets -- -D warnings

clippy:
	cargo clippy --workspace --all-features --all-targets --fix

build:
	cargo build --all-features --all-targets

test:
	cargo test --workspace --all-features --exclude e2e_test

check: fmt_check clippy_check build test docs_check

clean:
	cargo clean

.PHONY: docs check fmt fmt_check clippy clippy_check build test docs_check clean 