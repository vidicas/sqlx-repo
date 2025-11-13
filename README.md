# `sqlx-repo`

 The `sqlx_repo` is built around Repository Pattern and provides a consistent interface to interact
 with relational databases, abstracting away the differences between query syntaxes.

 It supports:
 - SQLite
 - PostgreSQL
 - MySQL

 The objective is to define a minimal, shared core of database operations that present and behave
 consistently and predictably across supported backends. Features which are only specific to a particular
 database are deliberately excluded from this core.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional
terms or conditions.
