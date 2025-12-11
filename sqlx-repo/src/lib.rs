//! The `sqlx_repo` is built around Repository Pattern and provides a consistent interface to interact
//! with relational databases, abstracting away the differences between query syntaxes.
//!
//! It supports:
//! - SQLite
//! - PostgreSQL
//! - MySQL
//!
//! The objective is to define a minimal, shared core of database operations that present and behave
//! consistently and predictably across supported backends. Features which are only specific to a particular
//! database are deliberately excluded from this core.
//!
//! # Overview
//!
//! This is how it all fits together.
//!
//! ```rust
//! use sqlx_repo::prelude::*;
//! use anyhow::Result;
//!
//! // Create your first migration.
//! fn migration1() -> Migration {
//!     migration!(
//!         "first migration",
//!         "create table test(id int primary key autoincrement)"
//!     )
//! }
//!
//! // Create your database repo layer, note `repo`, `migrator`, `query` macros.
//! #[repo(Send + Sync + std::fmt::Debug)]
//! impl Repo for DatabaseRepository {
//!     async fn migrate(&self) -> Result<()> {
//!         let migrator = migrator!(&[migration1()]).await?;
//!         migrator.run(&self.pool).await?;
//!         Ok(())
//!     }
//!
//!     async fn insert(&self) -> Result<()> {
//!         let query = query!("insert into test values (?)");
//!         let mut transaction = self.pool.start_transaction().await?;
//!         sqlx::query(query)
//!             .bind(1)
//!             .execute(&mut *transaction)
//!             .await?;
//!         sqlx::query(query)
//!             .bind(2)
//!             .execute(&mut *transaction)
//!             .await?;
//!         transaction.commit().await?;
//!         Ok(())
//!     }
//!
//!     async fn select_all(&self) -> Result<Vec<i32>> {
//!         let query = query!("select * from test");
//!         let res = sqlx::query(query)
//!             .fetch_all(&self.pool)
//!             .await?
//!             .into_iter()
//!             .map(|row| row.get::<i32, _>(0))
//!             .collect();
//!         Ok(res)
//!     }
//!
//!     async fn delete_all(&self) -> Result<()> {
//!         let query = query!("delete from test");
//!         sqlx::query(query).execute(&self.pool).await?;
//!         Ok(())
//!     }
//! }
//!
//!# use tokio::runtime::Builder;
//!# let rt = Builder::new_current_thread().enable_all().build().unwrap();
//!# rt.block_on(async {
//! // Pick any of supported database backends: SQLite, PostgreSQL, MySQL
//! // let url = "postgres://postgres:root@127.0.0.1:5432/postgres"
//! // let url = "mysql://root:root@127.0.0.1:3306/mysql"
//! let url = "sqlite::memory:";
//! let repo = <dyn Repo>::new(url).await.unwrap();
//! repo.migrate().await.unwrap();
//! repo.delete_all().await.unwrap();
//! repo.insert().await.unwrap();
//! assert_eq!(vec![1, 2], repo.select_all().await.unwrap());
//!# });
//!  
//! ```
//!
//! # Supported queries
//!
//! ## Create table
//!
//! ### Supported types
//!
//! ```sql
//! CREATE TABLE test (
//!     id32 SERIAL PRIMARY KEY,
//!     i16 SMALLINT,
//!     i32 INTEGER,
//!     i64 BIGINT,
//!     numeric NUMERIC(10, 2),
//!     real REAL,
//!     double DOUBLE PRECISION,
//!     bool BOOLEAN,
//!     char CHAR(5),
//!     varchar VARCHAR(100),
//!     text TEXT,
//!     date DATE,
//!     time TIME,
//!     timestamp TIMESTAMP,
//!     uuid UUID,
//!     bytes BYTEA,
//!     json JSON
//! );
//! ```
//!
//! ### Primary Key
//!
//! ```sql
//! CREATE TABLE test (id SMALLSERIAL PRIMARY KEY);
//! ```
//!
//! ```sql
//! CREATE TABLE test (id SERIAL PRIMARY KEY);
//! ```
//!
//! ```sql
//! CREATE TABLE test (id BIGSERIAL PRIMARY KEY);
//! ```
//!
//! ```sql
//! CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT);
//! ```
//!
//! Composite primary key supported as well:
//! ```sql
//! CREATE TABLE test(left INT, right INT, value TEXT, PRIMARY KEY (left, right));
//! ```
//!
//! ### Foreign key
//!
//! ```sql
//! CREATE TABLE employees (
//!     emp_id INT PRIMARY KEY,
//!     dept_id INT,
//!     location_id INT,
//!     name TEXT,
//!     FOREIGN KEY (dept_id, location_id) REFERENCES departments(dept_id, location_id)
//! );
//! ```
//!
//! ### On delete
//!
//! Supported actions `ON DELETE` are `CASCADE`, `SET NULL`, `RESTRICT`:
//!
//! ```sql
//! CREATE TABLE employees (
//!     emp_id INT PRIMARY KEY,
//!     dept_id INT,
//!     FOREIGN KEY (dept_id) REFERENCES departments(dept_id) ON DELETE CASCADE
//! );
//! ```
//!
//! ```sql
//! CREATE TABLE employees (
//!     emp_id INT PRIMARY KEY,
//!     dept_id INT,
//!     FOREIGN KEY (dept_id) REFERENCES departments(dept_id) ON DELETE SET NULL
//! );
//! ```
//!
//! ```sql
//! CREATE TABLE employees (
//!     emp_id INT PRIMARY KEY,
//!     dept_id INT,
//!     FOREIGN KEY (dept_id) REFERENCES departments(dept_id) ON DELETE RESTRICT
//! );
//! ```
//!
//! ## Alter table
//!
//! ```sql
//! ALTER TABLE test RENAME TO foo;
//! ```
//!
//! ```sql
//! ALTER TABLE test ADD COLUMN foo INT;
//! ```
//!
//! ```sql
//! ALTER TABLE test DROP COLUMN foo;
//! ```
//!
//! ```sql
//! ALTER TABLE test RENAME COLUMN old_col TO new_col;
//! ```
//!
//! ## Create index
//!
//! ```sql
//! CREATE INDEX idx ON table_name (id, org);
//! ```
//!
//! ```sql
//! CREATE UNIQUE INDEX idx ON table_name (id, org);
//! ```
//!
//! ```sql
//! CREATE INDEX IF NOT EXISTS idx ON table_name (id, org);
//! ```
//!
//! ## Drop
//!
//! ```sql
//! DROP TABLE test;
//! ```
//!
//! ```sql
//! DROP TABLE IF EXISTS test;
//! ```
//!
//! ```sql
//! DROP INDEX idx ON test;
//! ```
//!
//! ```sql
//! DROP INDEX IF EXISTS idx ON test;
//! ```
//!
//! ## Insert
//!
//! ```sql
//! INSERT INTO TEST(id, key, value) VALUES(NULL, 1, "one"), (NULL, 2, "two");
//! ```
//!
//! With placeholders:
//! ```sql
//! INSERT INTO TEST(id, key, value) VALUES(?, ?, ?), (?, ?, ?);
//! ```
//!
//! With placeholders casts:
//! ```sql
//! INSERT INTO TEST(id, key, value) VALUES(?::json, ?::uuid, ?);
//! ```
//!
//! ## Update
//!
//! ```sql
//! UPDATE TEST SET value="foo" WHERE key = 1;
//! ```
//!
//! ## Select
//!
//! ```sql
//! SELECT * FROM test;
//! ```
//!
//! ```sql
//! SELECT id, key, * FROM test;
//! ```
//!
//! Two-parts compound identifiers are supported:
//! ```sql
//! SELECT test.id, key FROM test;
//! ```
//!
//! ```sql
//! SELECT test.id, test.key, * FROM test;
//! ```
//!
//! ### Count
//!
//! ```sql
//! SELECT count(*) FROM test;
//! ```
//!
//! ```sql
//! SELECT count(id) FROM test;
//! ```
//!
//! ### Group by
//!
//! ```sql
//! SELECT key, COUNT(*) FROM test GROUP BY key;
//! ```
//!
//! ### Order by
//!
//! ```sql
//! SELECT * FROM test ORDER BY id ASC, key DESC;
//! ```
//!
//! ### Where
//!
//! ```sql
//! SELECT * FROM test WHERE id = 1 AND key = "foo";
//! ```
//!
//! ```sql
//! SELECT * FROM test WHERE id = ?;
//! ```
//!
//! ```sql
//! SELECT * FROM test WHERE id = ? AND value = ? OR id = ?;
//! ```
//!
//! ```sql
//! SELECT * FROM test WHERE id IN (1, "2", ?);
//! ```
//!
//! ```sql
//! SELECT * FROM test WHERE id NOT IN (1, "2", ?);
//! ```
//!
//! ### Join
//!
//! ```sql
//! SELECT * FROM foo
//!     JOIN bar ON foo.id = bar.id
//!     JOIN baz ON foo.id = baz.id
//! ;
//! ```
//!
//! ```sql
//! SELECT * FROM foo
//!     INNER JOIN bar ON foo.id = bar.id
//!     INNER JOIN baz ON foo.id = baz.id
//! ;
//! ```
//!
//! ## Delete
//! ```sql
//! DELETE FROM test;
//! ```
//!
//! ```sql
//! DELETE FROM test WHERE key = 1;
//! ```
mod ext;
mod migrator;

#[doc(hidden)]
pub use ext::AcquireExt;

pub struct DatabaseRepository<D>
where
    D: sqlx::Database,
{
    pub database_url: String,
    pub pool: sqlx::Pool<D>,
}

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = StdError> = std::result::Result<T, E>;

impl<D: sqlx::Database + std::fmt::Debug> std::fmt::Debug for DatabaseRepository<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseRepository")
            .field("database_url", &self.database_url)
            .field("pool", &self.pool)
            .finish()
    }
}

fn hide_credentials(url: &str) -> Result<String> {
    let mut url = url::Url::parse(url)?;
    match url.scheme() {
        "sqlite" => Ok(url.to_string()),
        _ => {
            url.set_username("").or(Err("failed to hide username"))?;
            url.set_password(None).or(Err("failed to hide password"))?;
            Ok(url.to_string())
        }
    }
}

impl<D: sqlx::Database> DatabaseRepository<D> {
    pub async fn new(url: &str, pool: sqlx::Pool<D>) -> Result<Self> {
        Ok(Self {
            database_url: hide_credentials(url)?,
            pool,
        })
    }
}

#[doc(hidden)]
pub trait SqlxDBNum: std::fmt::Debug {
    fn pos() -> usize {
        usize::MAX
    }
}

impl SqlxDBNum for sqlx::Postgres {
    fn pos() -> usize {
        0
    }
}

impl SqlxDBNum for sqlx::Sqlite {
    fn pos() -> usize {
        1
    }
}

impl SqlxDBNum for sqlx::MySql {
    fn pos() -> usize {
        2
    }
}

#[macro_export]
macro_rules! migration {
    ($name:expr, $migration:expr) => {
        ::sqlx_repo::prelude::Migration {
            name: $name,
            queries: ::sqlx_repo::__hidden::gen_query!($migration),
        }
    };
}

#[macro_export]
macro_rules! migrator {
    ($($migrations:tt)*) => {
        ::sqlx_repo::prelude::init_migrator::<D>($($migrations)*)
    }
}

pub mod prelude {
    pub use super::{
        DatabaseRepository, SqlxDBNum,
        ext::AcquireExt,
        migration, migrator,
        migrator::{Migration, init_migrator},
    };
    pub use chrono;
    pub use futures;
    pub use serde_json;
    pub use sqlx::{self, Acquire, Arguments, Row as _};
    pub use sqlx_repo_macros::repo;
    pub use url;
    pub use uuid;
}

#[doc(hidden)]
pub mod __hidden {
    pub use sqlx_repo_macros::{gen_query, query};
}
