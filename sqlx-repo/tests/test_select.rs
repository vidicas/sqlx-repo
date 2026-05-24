use anyhow::Result;
use futures::{StreamExt, stream::FuturesUnordered};
use sqlx_repo::prelude::*;

mod migrations;

#[repo(Send + Sync + std::fmt::Debug)]
impl SelectRepo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(&migrations::all_migrations()).await?;
        migrator.run(&self.pool).await?;
        Ok(())
    }

    async fn select_order_asc(&self) -> Result<Vec<i32>> {
        let q = query!("select * from select_items order by id asc");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_order_desc(&self) -> Result<Vec<i32>> {
        let q = query!("select * from select_items order by id desc");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_limit(&self) -> Result<Vec<i32>> {
        let q = query!("select * from select_items order by id limit 3");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_limit_offset(&self) -> Result<Vec<i32>> {
        let q = query!("select * from select_items order by id limit 2 offset 2");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_order_desc_limit(&self) -> Result<Vec<i32>> {
        let q = query!("select * from select_items order by id desc limit 3");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_where_eq(&self, id: i32) -> Result<Vec<i32>> {
        let q = query!("select * from select_items where id = ? order by id");
        Ok(sqlx::query(q)
            .bind(id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_where_or(&self, a: i32, b: i32) -> Result<Vec<i32>> {
        let q = query!("select * from select_items where id = ? or id = ? order by id");
        Ok(sqlx::query(q)
            .bind(a)
            .bind(b)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn count_all(&self) -> Result<i64> {
        let q = query!("select count(*) from select_items");
        Ok(sqlx::query(q).fetch_one(&self.pool).await?.get::<i64, _>(0))
    }

    async fn select_distinct_category(&self) -> Result<Vec<i32>> {
        let q = query!("select distinct category from select_pairs order by category");
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }

    async fn select_group_by_count(&self) -> Result<Vec<i64>> {
        let q = query!("select category, count(*) from select_pairs group by category");
        let mut counts: Vec<i64> = sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i64, _>(1))
            .collect();
        counts.sort_unstable();
        Ok(counts)
    }

    async fn select_join(&self) -> Result<Vec<i32>> {
        let q = query!(
            "select select_items.id from select_items join select_pairs on select_items.id = select_pairs.category order by select_items.id"
        );
        Ok(sqlx::query(q)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|r| r.get::<i32, _>(0))
            .collect())
    }
}

#[tokio::test]
async fn test_select() {
    let urls = &[
        "sqlite::memory:",
        "postgres://postgres:root@127.0.0.1:5432/postgres",
        "mysql://root:root@127.0.0.1:3306/mysql",
    ];
    let mut repos = urls
        .iter()
        .map(|url| async move {
            let repo = <dyn SelectRepo>::new(url).await.unwrap();
            repo.migrate().await.unwrap();

            assert_eq!(
                vec![1, 2, 3, 4, 5],
                repo.select_order_asc().await.unwrap(),
                "order asc at {url}"
            );
            assert_eq!(
                vec![5, 4, 3, 2, 1],
                repo.select_order_desc().await.unwrap(),
                "order desc at {url}"
            );
            assert_eq!(
                vec![1, 2, 3],
                repo.select_limit().await.unwrap(),
                "limit at {url}"
            );
            assert_eq!(
                vec![3, 4],
                repo.select_limit_offset().await.unwrap(),
                "limit+offset at {url}"
            );
            assert_eq!(
                vec![5, 4, 3],
                repo.select_order_desc_limit().await.unwrap(),
                "order desc+limit at {url}"
            );
            assert_eq!(
                vec![3],
                repo.select_where_eq(3).await.unwrap(),
                "where at {url}"
            );
            assert_eq!(
                vec![2, 4],
                repo.select_where_or(2, 4).await.unwrap(),
                "where or at {url}"
            );
            assert_eq!(5, repo.count_all().await.unwrap(), "count at {url}");

            assert_eq!(
                vec![1, 2, 3],
                repo.select_distinct_category().await.unwrap(),
                "distinct at {url}"
            );
            assert_eq!(
                vec![1, 2, 2],
                repo.select_group_by_count().await.unwrap(),
                "group by at {url}"
            );
            assert_eq!(
                vec![1, 1, 2, 2, 3],
                repo.select_join().await.unwrap(),
                "join at {url}"
            );
        })
        .collect::<FuturesUnordered<_>>();
    while let Some(()) = repos.next().await {}
}
