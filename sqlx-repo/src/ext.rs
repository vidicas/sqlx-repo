use std::borrow::Cow;

use futures::future::BoxFuture;
use sqlx::{Database, Error};

pub trait AcquireExt<D: Database> {
    fn start_transaction<'a>(self) -> BoxFuture<'static, Result<sqlx::Transaction<'a, D>, Error>>;
}

impl AcquireExt<sqlx::Sqlite> for &'_ sqlx::Pool<sqlx::Sqlite> {
    fn start_transaction<'a>(
        self,
    ) -> BoxFuture<'static, Result<sqlx::Transaction<'a, sqlx::Sqlite>, Error>> {
        let conn = self.acquire();

        Box::pin(async move {
            sqlx::Transaction::begin(conn.await?, Some(Cow::Borrowed("BEGIN IMMEDIATE"))).await
        })
    }
}

impl AcquireExt<sqlx::Postgres> for &'_ sqlx::Pool<sqlx::Postgres> {
    fn start_transaction<'a>(
        self,
    ) -> BoxFuture<'static, Result<sqlx::Transaction<'a, sqlx::Postgres>, Error>> {
        let conn = self.acquire();

        Box::pin(async move { sqlx::Transaction::begin(conn.await?, None).await })
    }
}

impl AcquireExt<sqlx::MySql> for &'_ sqlx::Pool<sqlx::MySql> {
    fn start_transaction<'a>(
        self,
    ) -> BoxFuture<'static, Result<sqlx::Transaction<'a, sqlx::MySql>, Error>> {
        let conn = self.acquire();

        Box::pin(async move { sqlx::Transaction::begin(conn.await?, None).await })
    }
}
