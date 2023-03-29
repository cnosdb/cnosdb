docs_check:
	cargo doc --no-deps --document-private-items --all-features

docs:
	cargo doc --no-deps --document-private-items --all-features --open

fmt_check:
	cargo +nightly fmt --all -- --check

fmt:
	cargo +nightly fmt --all

clippy_check:
	cargo clippy --workspace  --all-targets -- -D warnings

clippy:
	cargo clippy --workspace  --all-targets --fix --allow-staged

build:
	cargo build --workspace --bins

build_release:
	cargo build --release --workspace --bins

build_trace:
	cargo clean;
	git stash; export BACKTRACE=on; cargo build --workspace --bins --features backtrace; git reset --hard; git stash pop || true
test:
	cargo test --workspace --exclude e2e_test

check: fmt_check clippy_check build test docs_check

clean:
	cargo clean

run:
	cargo run -- run

.PHONY: docs check fmt fmt_check clippy clippy_check build build_release build_trace test docs_check clean run