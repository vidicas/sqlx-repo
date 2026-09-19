use anyhow::Result;
use sqlx::migrate::MigrateDatabase;
use sqlx_repo::prelude::*;

#[repo(Send + Sync + std::fmt::Debug)]
impl TextMigrationsRepo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!("tests/fixtures/text_migrations").await?;
        migrator.run(&self.pool).await?;
        Ok(())
    }

    async fn select_all(&self) -> Result<Vec<i32>> {
        let query = query!("select * from text_migration_demo order by id");
        let res = sqlx::query(query)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| row.get::<i32, _>(0))
            .collect();
        Ok(res)
    }
}

// runs in its own database rather than the one shared by the other integration tests, since
// `_sqlx_migrations` is global per database and would otherwise conflict with their migrations
#[tokio::test]
async fn test_text_migrations() {
    let postgres_url = "postgres://postgres:root@127.0.0.1:5432/sqlx_repo_text_migrations";
    let mysql_url = "mysql://root:root@127.0.0.1:3306/sqlx_repo_text_migrations";

    if !sqlx::Postgres::database_exists(postgres_url).await.unwrap() {
        sqlx::Postgres::create_database(postgres_url).await.unwrap();
    }
    if !sqlx::MySql::database_exists(mysql_url).await.unwrap() {
        sqlx::MySql::create_database(mysql_url).await.unwrap();
    }

    let urls = ["sqlite::memory:", postgres_url, mysql_url];
    for url in urls {
        let repo = <dyn TextMigrationsRepo>::new(url).await.unwrap();
        repo.migrate().await.unwrap();
        assert_eq!(vec![1, 2, 3], repo.select_all().await.unwrap(), "at {url}");
    }
}
