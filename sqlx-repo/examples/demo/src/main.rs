use anyhow::Result;
use sqlx_repo::prelude::*;

// Create your first migration.
fn migration1() -> Migration {
    migration!(
        "first migration",
        "create table test(id int primary key autoincrement)"
    )
}

// Create your database repo layer, note `repo`, `migrator`, `query` macros.
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

#[tokio::main]
async fn main() {
    // Pick any of supported database backends: SQLite, PostgreSQL, MySQL
    // let url = "postgres://postgres:root@127.0.0.1:5432/postgres"
    // let url = "mysql://root:root@127.0.0.1:3306/mysql"
    let url = "sqlite::memory:";
    let repo = <dyn Repo>::new(url).await.unwrap();
    repo.migrate().await.unwrap();
    repo.delete_all().await.unwrap();
    repo.insert().await.unwrap();
    assert_eq!(vec![1, 2], repo.select_all().await.unwrap());
}
