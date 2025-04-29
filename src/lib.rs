pub struct DatabaseRepository<D = sqlx::Sqlite>
where
    D: sqlx::Database,
{
    pub database_url: String,
    pub pool: sqlx::Pool<D>,
    pub query_builder: Box<dyn sea_query::QueryBuilder + Send + Sync>,
    pub schema_builder: Box<dyn sea_query::SchemaBuilder + Send + Sync>,
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
    pub async fn new(
        url: &str,
        query_builder: Box<dyn sea_query::QueryBuilder + Send + Sync>,
        schema_builder: Box<dyn sea_query::SchemaBuilder + Send + Sync>,
    ) -> Result<Self> {
        Ok(Self {
            database_url: hide_credentials(url)?,
            pool: sqlx::Pool::<D>::connect(url).await?,
            query_builder,
            schema_builder,
        })
    }
}

pub mod prelude {
    pub use super::DatabaseRepository;
    pub use chrono;
    pub use futures;
    pub use repo_macro::repo;
    pub use serde_json;
    pub use sqlx::{self, Row as _};
    pub use url;
    pub use uuid;
}
