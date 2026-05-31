use sqlx_repo::prelude::*;

const fn migration_test() -> Migration {
    migration!(
        "first migration",
        "create table test(id int primary key autoincrement)"
    )
}

const fn migration_select_items() -> Migration {
    migration!("create select_items", "create table select_items(id int)")
}

const fn migration_select_pairs() -> Migration {
    migration!(
        "create select_pairs",
        "create table select_pairs(category int, value int)"
    )
}

const fn migration_seed_select_items() -> Migration {
    migration!(
        "seed select_items",
        "insert into select_items values (1), (2), (3), (4), (5)"
    )
}

const fn migration_seed_select_pairs() -> Migration {
    migration!(
        "seed select_pairs",
        "insert into select_pairs values (1, 10), (1, 20), (2, 30), (2, 40), (3, 50)"
    )
}

pub const fn migration_all_types() -> Migration {
    migration!(
        "create all_types table",
        "CREATE TABLE all_types (
            id INT PRIMARY KEY,
            small SMALLINT,
            medium INT,
            big BIGINT,
            r32 REAL,
            r64 DOUBLE PRECISION,
            b BOOLEAN,
            s TEXT,
            ch CHAR(8),
            vc VARCHAR(16),
            blob BYTEA,
            json JSON,
            uuid UUID,
            ts TIMESTAMP,
            dt DATE,
            tm TIME
        )"
    )
}

pub fn all_migrations() -> &'static [Migration] {
    static MIGRATIONS: &[Migration] = &[
        migration_test(),
        migration_select_items(),
        migration_select_pairs(),
        migration_seed_select_items(),
        migration_seed_select_pairs(),
        migration_all_types(),
    ];
    MIGRATIONS
}
