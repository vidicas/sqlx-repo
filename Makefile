.PHONY: fmt
fmt:
	cargo fmt && cargo clippy --fix --allow-dirty \
	&& cargo fmt && cargo clippy --fix --allow-dirty -p repo_macro
