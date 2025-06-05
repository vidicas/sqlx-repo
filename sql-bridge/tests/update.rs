use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_basic_insert() {
    let input = "update test set value='foo' where key = 1";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "UPDATE `test` SET `value`='foo' WHERE `key` = 1"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "UPDATE `test` SET `value`='foo' WHERE `key` = 1"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "UPDATE \"test\" SET \"value\"='foo' WHERE \"key\" = 1"
    );
}
