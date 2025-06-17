use sqlx::{Database, Error};
use std::{borrow::Cow, future::Future};

pub trait AcquireExt<D: Database> {
    fn start_transaction(
        &self,
    ) -> impl Future<Output = Result<sqlx::Transaction<'_, D>, Error>> + Send + 'static;
}

impl AcquireExt<sqlx::Sqlite> for sqlx::Pool<sqlx::Sqlite> {
    fn start_transaction(
        &self,
    ) -> impl Future<Output = Result<sqlx::Transaction<'_, sqlx::Sqlite>, Error>> + Send + 'static
    {
        let conn = self.acquire();

        async move {
            sqlx::Transaction::begin(conn.await?, Some(Cow::Borrowed("BEGIN IMMEDIATE"))).await
        }
    }
}

impl AcquireExt<sqlx::Postgres> for sqlx::Pool<sqlx::Postgres> {
    fn start_transaction(
        &self,
    ) -> impl Future<Output = Result<sqlx::Transaction<'_, sqlx::Postgres>, Error>> + Send + 'static
    {
        let conn = self.acquire();

        async move { sqlx::Transaction::begin(conn.await?, None).await }
    }
}

impl AcquireExt<sqlx::MySql> for sqlx::Pool<sqlx::MySql> {
    fn start_transaction(
        &self,
    ) -> impl Future<Output = Result<sqlx::Transaction<'_, sqlx::MySql>, Error>> + Send + 'static
    {
        let conn = self.acquire();

        async move { sqlx::Transaction::begin(conn.await?, None).await }
    }
}
