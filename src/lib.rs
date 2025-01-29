pub struct DatabaseRepository<D = sqlx::Sqlite>
where
    D: sqlx::Database,
{
    pub pool: sqlx::Pool<D>,
    pub query_builder: Box<dyn sea_query::QueryBuilder + Send + Sync>,
    pub schema_builder: Box<dyn sea_query::SchemaBuilder + Send + Sync>,
}

impl<D: sqlx::Database> std::fmt::Debug for DatabaseRepository<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseRepository")
            .field("pool", &self.pool)
            .finish()
    }
}

impl<D: sqlx::Database> DatabaseRepository<D> {
    pub async fn new(
        url: &str,
        query_builder: Box<dyn sea_query::QueryBuilder + Send + Sync>,
        schema_builder: Box<dyn sea_query::SchemaBuilder + Send + Sync>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
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
    pub use sea_query;
    pub use sea_query_binder;
    pub use serde_json;
    pub use sqlx;
    pub use url;
    pub use uuid;
}
