use anyhow::Result;
use sea_query::{ColumnDef, Table};
use sqlx_db_repo::prelude::*;

#[derive(sea_query::Iden)]
enum Test {
    Table,
    I64,
    I8,
    I16,
    I32,
    F32,
    F64,
    Str,
    Boolean,
    Bytes,
    Date,
    DateTime,
    TimeStamp,
}

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn crate_table(&self) -> Result<()> {
        let query = Table::create()
            .table(Test::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(Test::I64)
                    .big_integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(Test::I8).tiny_integer().not_null())
            .col(ColumnDef::new(Test::I16).small_integer().not_null())
            .col(ColumnDef::new(Test::I32).integer().not_null())
            .col(ColumnDef::new(Test::F32).float().not_null())
            .col(ColumnDef::new(Test::F64).double().not_null())
            .col(ColumnDef::new(Test::Str).string().not_null())
            .col(ColumnDef::new(Test::Boolean).boolean().not_null())
            .col(ColumnDef::new(Test::Bytes).blob().not_null())
            .col(ColumnDef::new(Test::Date).date().not_null())
            .col(ColumnDef::new(Test::DateTime).date_time().not_null())
            .col(
                ColumnDef::new(Test::TimeStamp)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .build_any(&*self.schema_builder);
        let connection = &mut *self.pool.acquire().await?;
        sqlx::query(&query).execute(connection).await?;
        Ok(())
    }
}

// basic smoke test
#[tokio::test]
async fn test_database_creation() {
    let urls = [
        "sqlite::memory:",
        "postgres://postgres:root@127.0.0.1:5432/postgres",
        "mysql://root:root@127.0.0.1:3306/mysql",
    ];
    let mut repos = vec![];
    for url in urls {
        let res = <dyn Repo>::new(url).await;
        assert!(res.is_ok(), "at {url}, {res:?}");
        println!("{:?}", res);
        repos.push(res.unwrap());
    }
    for repo in repos {
        repo.crate_table().await.unwrap();
    }
}
