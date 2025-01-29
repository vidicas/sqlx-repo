.PHONY: fmt
fmt:
	cargo fmt --all && cargo clippy --fix --allow-dirty
