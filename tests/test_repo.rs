use anyhow::Result;
use sqlx_db_repo::prelude::*;

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn crate_table(&self) -> Result<()> {
        println!("picked query: {}", query!("select 1"));
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
        repo.crate_table().await.unwrap();
        println!()
    }
}

#[test]
fn test_query() {
    query!("select 1");
    query!("create table employees (
            id int auto_increment primary key, 
            first_name varchar(50) not null,
            last_name varchar(50) not null,
            hire_date date not null,
            salary decimal(10, 2) not null
        ) engine=innodb
    ");

    query!("CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        hire_date DATE NOT NULL,
        salary NUMERIC(10, 2) NOT NULL
    )");

    query!("CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            hire_date DATE NOT NULL,
            salary NUMERIC(10, 2) NOT NULL
    )");
}