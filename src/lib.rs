use sea_query::{QueryBuilder, SchemaBuilder};
use sqlx::{Database, Pool};

pub struct DatabaseRepository<D: Database> {
    pub pool: Pool<D>,
    pub query_builder: Box<dyn QueryBuilder + Send + Sync>,
    pub schema_builder: Box<dyn SchemaBuilder + Send + Sync>,
}

impl<D: Database> std::fmt::Debug for DatabaseRepository<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseRepository")
            .field("pool", &self.pool)
            .finish()
    }
}

impl<D: Database> DatabaseRepository<D> {
    pub async fn new(
        url: &str,
        query_builder: Box<dyn QueryBuilder + Send + Sync>,
        schema_builder: Box<dyn SchemaBuilder + Send + Sync>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(Self {
            pool: Pool::<D>::connect(url).await?,
            query_builder,
            schema_builder,
        })
    }
}

pub mod prelude {
    pub use repo_derive_macro::repo;
    pub use sqlx;
    pub use sea_query;
    pub use sea_query_binder;
    pub use super::DatabaseRepository;
    pub use url;
}
