use anyhow::Result;
use sqlx_repo::prelude::*;

#[repo(Send + Sync + std::fmt::Debug)]
impl TextMigrationsRepo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(migrations!("tests/fixtures/text_migrations")).await?;
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

#[tokio::test]
async fn test_text_migrations() {
    let urls = [
        "sqlite::memory:",
        "postgres://postgres:root@127.0.0.1:5432/postgres",
        "mysql://root:root@127.0.0.1:3306/mysql",
    ];
    for url in urls {
        let repo = <dyn TextMigrationsRepo>::new(url).await.unwrap();
        repo.migrate().await.unwrap();
        assert_eq!(vec![1, 2, 3], repo.select_all().await.unwrap(), "at {url}");
    }
}
