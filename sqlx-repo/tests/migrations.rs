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

pub fn all_migrations() -> &'static [Migration] {
    static MIGRATIONS: &[Migration] = &[
        migration_test(),
        migration_select_items(),
        migration_select_pairs(),
    ];
    MIGRATIONS
}
