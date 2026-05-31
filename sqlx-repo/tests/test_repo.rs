use anyhow::Result;
use sqlx_repo::prelude::*;

mod migrations;

#[repo(Send + Sync + std::fmt::Debug)]
impl Repo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(&migrations::all_migrations()).await?;
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
        println!();
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

#[derive(Debug)]
pub struct AllTypesRow {
    small: i16,
    medium: i32,
    big: i64,
    r32: f32,
    r64: f64,
    b: bool,
    s: String,
    ch: String,
    vc: String,
    blob: Vec<u8>,
    json: serde_json::Value,
    uuid: uuid::Uuid,
    ts: chrono::NaiveDateTime,
    dt: chrono::NaiveDate,
    tm: chrono::NaiveTime,
}

#[repo(Send + Sync + std::fmt::Debug)]
impl AllTypesRepo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(&migrations::all_migrations()).await?;
        migrator.run(&self.pool).await?;
        Ok(())
    }

    async fn round_trip(&self) -> Result<AllTypesRow> {
        let mut tx = self.pool.start_transaction().await?;

        let insert_q =
            query!("INSERT INTO all_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        sqlx::query(insert_q)
            .bind(1i32)
            .bind(1000i16)
            .bind(100_000i32)
            .bind(10_000_000_000i64)
            .bind(1.5f32)
            .bind(2.5f64)
            .bind(true)
            .bind("hello")
            .bind("world!!!")
            .bind("sqlx-repo")
            .bind(b"\x01\x02\x03".as_slice())
            .bind(serde_json::json!({"k": 1}))
            .bind(uuid::Uuid::nil())
            .bind(chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ))
            .bind(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
            .bind(chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap())
            .execute(&mut *tx)
            .await?;

        let select_q = query!(
            "SELECT small, medium, big, r32, r64, b, s, ch, vc, blob, json, uuid, ts, dt, tm
             FROM all_types WHERE id = ?"
        );
        let row = sqlx::query(select_q).bind(1i32).fetch_one(&mut *tx).await?;
        let result = AllTypesRow {
            small: row.get(0),
            medium: row.get(1),
            big: row.get(2),
            r32: row.get(3),
            r64: row.get(4),
            b: row.get(5),
            s: row.get(6),
            ch: row.get(7),
            vc: row.get(8),
            blob: row.get(9),
            json: row.get(10),
            uuid: row.get(11),
            ts: row.get(12),
            dt: row.get(13),
            tm: row.get(14),
        };

        tx.rollback().await?;
        Ok(result)
    }
}

#[tokio::test]
async fn test_all_types_round_trip() {
    let urls = [
        "sqlite::memory:",
        "postgres://postgres:root@127.0.0.1:5432/postgres",
        "mysql://root:root@127.0.0.1:3306/mysql",
    ];
    for url in urls {
        let repo = <dyn AllTypesRepo>::new(url).await.unwrap();
        repo.migrate().await.unwrap();
        let row = repo.round_trip().await.unwrap();

        assert_eq!(1000i16, row.small, "small at {url}");
        assert_eq!(100_000i32, row.medium, "medium at {url}");
        assert_eq!(10_000_000_000i64, row.big, "big at {url}");
        assert!((row.r32 - 1.5f32).abs() < f32::EPSILON, "r32 at {url}");
        assert!((row.r64 - 2.5f64).abs() < f64::EPSILON, "r64 at {url}");
        assert!(row.b, "b at {url}");
        assert_eq!("hello", row.s, "s at {url}");
        assert_eq!("world!!!", row.ch, "ch at {url}"); // fill all 8 chars to avoid CHAR padding/stripping differences across DBs
        assert_eq!("sqlx-repo", row.vc, "vc at {url}");
        assert_eq!(vec![1u8, 2, 3], row.blob, "blob at {url}");
        assert_eq!(serde_json::json!({"k": 1}), row.json, "json at {url}");
        assert_eq!(uuid::Uuid::nil(), row.uuid, "uuid at {url}");
        assert_eq!(
            chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            ),
            row.ts,
            "ts at {url}"
        );
        assert_eq!(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            row.dt,
            "dt at {url}"
        );
        assert_eq!(
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            row.tm,
            "tm at {url}"
        );
    }
}

#[repo(Send + Sync + std::fmt::Debug)]
impl DecimalRepo for DatabaseRepository {
    async fn migrate(&self) -> Result<()> {
        let migrator = migrator!(&migrations::all_migrations()).await?;
        migrator.run(&self.pool).await?;
        Ok(())
    }

    async fn round_trip(&self, val: Decimal) -> Result<Decimal> {
        let mut tx = self.pool.start_transaction().await?;

        let insert_q = query!("INSERT INTO decimal_types VALUES (?, ?)");
        sqlx::query(insert_q)
            .bind(1i32)
            .bind(val)
            .execute(&mut *tx)
            .await?;

        let select_q = query!("SELECT amount FROM decimal_types WHERE id = ?");
        let row = sqlx::query(select_q).bind(1i32).fetch_one(&mut *tx).await?;
        let result = row.get::<Decimal, _>(0);

        tx.rollback().await?;
        Ok(result)
    }
}

#[tokio::test]
async fn test_decimal_round_trip() {
    let cases: &[&str] = &[
        "0",
        "1",
        "123.45678",
        "-123.45678",
        "0.00001",
        "-0.00001",
        "999999999999999.99999",
        "-999999999999999.99999",
    ];
    let urls = [
        "sqlite::memory:",
        "postgres://postgres:root@127.0.0.1:5432/postgres",
        "mysql://root:root@127.0.0.1:3306/mysql",
    ];
    for url in urls {
        let repo = <dyn DecimalRepo>::new(url).await.unwrap();
        repo.migrate().await.unwrap();
        for s in cases {
            let val: Decimal = s.parse().unwrap();
            let result = repo.round_trip(val).await.unwrap();
            assert_eq!(val, result, "{s} at {url}");
        }
    }
}
