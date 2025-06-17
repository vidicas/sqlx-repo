use anyhow::Result;
use sqlx_repo::prelude::*;

fn migration1() -> Migration {
    migration!(
        "first migration",
        "create table test(id int primary key autoincrement)"
    )
}

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(&[migration1()]).await?;
        migrator.run(&self.pool).await?;
        Ok(())
    }

    async fn insert(&self) -> Result<()> {
        let query = query!("insert into test values (?)");
        let mut transaction = self.pool.start_transaction().await?;
        sqlx::query(query)
            .bind(1)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(query)
            .bind(2)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn select_all(&self) -> Result<Vec<i32>> {
        let query = query!("select * from test");
        let res = sqlx::query(query)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| row.get::<i32, _>(0))
            .collect();
        Ok(res)
    }

    async fn delete_all(&self) -> Result<()> {
        let query = query!("delete from test");
        sqlx::query(query).execute(&self.pool).await?;
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
        repos.push(res.unwrap());
    }
    for repo in repos {
        repo.migrate().await.unwrap();
        repo.delete_all().await.unwrap();
        repo.insert().await.unwrap();
        assert_eq!(vec![1, 2], repo.select_all().await.unwrap());
        println!()
    }
}

#[repo(Send + Sync + std::fmt::Debug)]
impl ShouldCompile for DatabaseRepository {
    async fn optional_type(&self, i: Option<i32>) -> Result<()> {
        let query = query!("insert into test values (?)");
        sqlx::query(query).bind(i).execute(&self.pool).await?;
        Ok(())
    }

    async fn optional_type_with_lifetime(&self, i: Option<&str>) -> Result<()> {
        let query = query!("insert into test values (?)");
        sqlx::query(query).bind(i).execute(&self.pool).await?;
        Ok(())
    }
}
