mod migrator;

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
    pub async fn new(url: &str) -> Result<Self> {
        Ok(Self {
            database_url: hide_credentials(url)?,
            pool: sqlx::Pool::<D>::connect(url).await?,
        })
    }
}

pub trait SqlxDBNum {
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
        ::sqlx_db_repo::prelude::Migration {
            name: $name,
            queries: ::macros::gen_query!($migration),
        }
    };
}

pub mod prelude {
    pub use super::{
        migration,
        migrator::{Migration, Migrator},
        DatabaseRepository, SqlxDBNum,
    };
    pub use chrono;
    pub use futures;
    pub use macros::repo;
    pub use serde_json;
    pub use sqlx::{self, Row as _};
    pub use url;
    pub use uuid;
}
