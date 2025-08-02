use anyhow::Result;
use sqlx_repo::prelude::*;

#[repo(Send + Sync)]
impl Repo for DatabaseRepository {
    async fn create_tables_and_insert_data(&self) -> Result<()> {
        let query = query!(
            "
            create table foo(id int primary key);
            create table bar(id int primary key, foreign key (id) REFERENCES foo(id) on delete cascade);
            insert into foo values(1), (2), (3);
            insert into bar values(1), (2), (3);
        "
        );
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn delete_all_foo(&self) -> Result<()> {
        let query = query!("delete from foo");
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn select_all_bar(&self) -> Result<Vec<i64>> {
        let query = query!("select * from bar");
        Ok(sqlx::query(query)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect())
    }
}

#[tokio::test]
async fn test_database_creation_defaults() {
    let url = "sqlite::memory:";
    let repo = <dyn Repo>::new(url).await.unwrap();
    repo.create_tables_and_insert_data().await.unwrap();
    assert_eq!(vec![1, 2, 3], repo.select_all_bar().await.unwrap(),);
    repo.delete_all_foo().await.unwrap();
    assert_eq!(Vec::<i64>::new(), repo.select_all_bar().await.unwrap(),)
}

#[tokio::test]
async fn test_database_creation_foreign_key_not_enforced() {
    let url = "sqlite::memory:?foreign_keys=false";
    let repo = <dyn Repo>::new(url).await.unwrap();
    repo.create_tables_and_insert_data().await.unwrap();
    assert_eq!(vec![1, 2, 3], repo.select_all_bar().await.unwrap(),);
    repo.delete_all_foo().await.unwrap();
    assert_eq!(vec![1, 2, 3], repo.select_all_bar().await.unwrap(),)
}
