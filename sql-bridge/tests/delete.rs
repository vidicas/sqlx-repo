use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn delete_from() {
    let input = "DELETE FROM test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(ast.to_sql(&MySqlDialect {}).unwrap(), "DELETE FROM `test`");

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DELETE FROM \"test\""
    );

    assert_eq!(ast.to_sql(&SQLiteDialect {}).unwrap(), "DELETE FROM `test`");
}

#[test]
fn delete_from_where() {
    let input = "DELETE FROM test WHERE `key` = 1";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "DELETE FROM `test` WHERE `key` = 1"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DELETE FROM \"test\" WHERE \"key\" = 1"
    );

    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "DELETE FROM `test` WHERE `key` = 1"
    );
}
