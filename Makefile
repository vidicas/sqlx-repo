.PHONY: fmt
fmt:
	cargo fmt --all && cargo clippy --fix --allow-dirty

.PHONY: setup_db
setup_db:
	docker run --name sqlx-db-repo-test-postgres -it --rm -e POSTGRES_PASSWORD=root -p 5432:5432 -d postgres
	docker run --name sqlx-db-repo-test-mysql -it --rm -e MYSQL_ROOT_PASSWORD=root -p 3306:3306 -d mysql

.PHONY: teardown_db
teardown_db:
	docker rm -f sqlx-db-repo-test-postgres 
	docker rm -f sqlx-db-repo-test-mysql

.PHONY: test
test:
	cargo test -- --nocapture
