use anyhow::Result;
use sqlx_db_repo::prelude::*;

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn create_table(&self) -> Result<()> {
        let query = query!("select * from test");
        println!("query: {}", query);
        Ok(())
    }

    async fn test(&self) -> Result<()> {
        Ok(())
    }

    async fn f2(&self) -> Result<()> {
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
        println!("repo: {repo:?}");
        repo.create_table().await.unwrap();
        println!()
    }
}
