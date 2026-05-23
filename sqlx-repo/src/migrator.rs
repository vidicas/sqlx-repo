use std::marker::PhantomData;

use crate::{Result, SqlxDBNum};
use futures::future::BoxFuture;
use sqlx::{
    SqlStr,
    migrate::{Migration as SqlxMigration, MigrationSource, MigrationType},
};

#[derive(Debug)]
pub struct RepoMigrationSource<D> {
    migrations: Vec<Migration>,
    marker: PhantomData<D>,
}

#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub name: &'static str,
    pub queries: &'static [&'static str],
}

impl<'a, D: SqlxDBNum> MigrationSource<'a> for RepoMigrationSource<D> {
    fn resolve(self) -> BoxFuture<'a, Result<Vec<SqlxMigration>, sqlx::error::BoxDynError>> {
        Box::pin(async move {
            let migrations = self.migrations
                .iter()
                .enumerate()
                .map(|(pos, migration)| {
                    let query_pos = D::pos();
                    let query = match migration.queries.get(query_pos) {
                        Some(&query) => query,
                        None => Err("failed to generate migration, tried to get query at index {query_pos}, which doesn't exist")?
                    };
                    #[allow(clippy::cast_possible_wrap)]
                    Ok(SqlxMigration::new(pos as _, migration.name.into(), MigrationType::Simple, SqlStr::from_static(query), false))
                })
                .collect::<Result<_>>()?;
            Ok(migrations)
        })
    }
}

impl<D: SqlxDBNum> Default for RepoMigrationSource<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: SqlxDBNum> RepoMigrationSource<D> {
    pub fn new() -> Self {
        Self {
            migrations: vec![],
            marker: PhantomData,
        }
    }

    pub fn add_migration(&mut self, migration: Migration) {
        self.migrations.push(migration);
    }
}

pub async fn init_migrator<D: SqlxDBNum>(
    migrations: &[Migration],
) -> Result<sqlx::migrate::Migrator, sqlx::migrate::MigrateError> {
    let mut source = RepoMigrationSource::<D>::new();
    for migration in migrations {
        source.add_migration(*migration);
    }
    sqlx::migrate::Migrator::new(source).await
}
