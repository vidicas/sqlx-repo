use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

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
