use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn rename_table() {
    let query = "alter table test rename to foo";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "ALTER TABLE `test` RENAME TO `foo`",
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "ALTER TABLE \"test\" RENAME TO \"foo\"",
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "ALTER TABLE `test` RENAME TO `foo`",
    );
}

#[test]
fn add_column() {
    let query = "alter table test add column foo int";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "ALTER TABLE `test` ADD COLUMN `foo` INT"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "ALTER TABLE \"test\" ADD COLUMN \"foo\" INT"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "ALTER TABLE `test` ADD COLUMN `foo` INTEGER"
    );
}

#[test]
fn drop_column() {
    let query = "alter table test drop column foo";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "ALTER TABLE `test` DROP COLUMN `foo`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "ALTER TABLE \"test\" DROP COLUMN \"foo\""
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "ALTER TABLE `test` DROP COLUMN `foo`"
    );
}

#[test]
fn rename_column() {
    let query = "alter table test rename column old_col to new_col";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "ALTER TABLE `test` RENAME COLUMN `old_col` TO `new_col`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "ALTER TABLE \"test\" RENAME COLUMN \"old_col\" TO \"new_col\""
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "ALTER TABLE `test` RENAME COLUMN `old_col` TO `new_col`"
    );
}

#[test]
fn add_column_if_not_exists() {
    let query = "ALTER TABLE table_name ADD COLUMN IF NOT EXISTS col INTEGER";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::AlterTable {
            reason: "if not exists"
        }
    ));
    assert_eq!(err.to_string(), "unsupported alter table: if not exists");
}

#[test]
fn drop_column_if_not_exists() {
    let query = "ALTER TABLE table_name DROP COLUMN IF EXISTS column_name";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::AlterTable {
            reason: "if exists"
        }
    ));
    assert_eq!(err.to_string(), "unsupported alter table: if exists");
}

#[test]
fn drop_behaviour() {
    let query = "ALTER TABLE table_name DROP COLUMN column_name CASCADE";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::AlterTable {
            reason: "drop behaviour"
        }
    ));
    assert_eq!(err.to_string(), "unsupported alter table: drop behaviour");
}

#[test]
fn column_position() {
    let query = "ALTER TABLE users ADD COLUMN age INT AFTER name";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::AlterTable {
            reason: "column position"
        }
    ));
    assert_eq!(err.to_string(), "unsupported alter table: column position");
}
