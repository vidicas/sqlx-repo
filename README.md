# `sqlx-db-repo`

**A database-agnostic async repository framework built on top of [SQLx](https://docs.rs/sqlx) and [SeaQuery](https://docs.rs/sea-query), with support for dynamic backends, compile-time type safety, and trait-object-safe `dyn Repo` patterns.**

---

## Features

- Generic over all SQLx backends: `Postgres`, `MySQL`, `SQLite`
- Type-safe queries with [`sea-query`](https://docs.rs/sea-query)
- Connection pooling via `sqlx::Pool`
- Trait-object-safe `dyn Repo` interface via procedural macro
- Query builder and schema builder abstraction
- Designed for async-first, ergonomic application-layer usage