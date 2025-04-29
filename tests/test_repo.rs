use anyhow::Result;
use sqlx_db_repo::prelude::*;

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn crate_table(&self) -> Result<()> {
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
