docs_check:
	cargo doc --no-deps --document-private-items --all-features

docs:
	cargo doc --no-deps --document-private-items --all-features --open

fmt_check:
	cargo +nightly fmt --all -- --check

fmt:
	cargo +nightly fmt --all

clippy_check:
	BUILD_PROTOS=1 cargo clippy --workspace  --all-targets --features coordinator_e2e_test --features meta_e2e_test -- -D warnings

clippy:
	BUILD_PROTOS=1 cargo clippy --workspace  --all-targets --features coordinator_e2e_test --features meta_e2e_test --fix --allow-staged

build:
	cargo build --workspace --bins

build_release:
	BUILD_PROTOS=1 cargo build --release --workspace --bins

build_trace:
	cargo clean;
	git stash;
	export BACKTRACE=on; BUILD_PROTOS=1 cargo build --workspace --bins --features backtrace;
	git reset --hard; git stash pop || true

test:
	BUILD_PROTOS=1 cargo test --workspace --exclude e2e_test

check: fmt_check clippy_check build test docs_check

clean:
	cargo clean

run:
	cargo run -- run

.PHONY: docs_check docs fmt_check fmt clippy_check clippy build build_release build_trace test check clean run