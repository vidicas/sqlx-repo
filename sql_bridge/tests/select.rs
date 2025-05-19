use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_simple_query() {
    let input = "select * from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(ast.to_sql(MySqlDialect {}).unwrap(), "SELECT * FROM `test`");
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\""
    );
}

#[test]
fn test_query_with_projection() {
    let input = "select id, key, * from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "SELECT `id`, `key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT `id`, `key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT \"id\", \"key\", * FROM \"test\""
    );
}

#[test]
fn test_query_with_predicates() {
    let input = "select * from test where id = 1 AND key = 'foo'";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = 1 AND `key` = 'foo'"
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = 1 AND `key` = 'foo'"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" = 1 AND \"key\" = 'foo'"
    );
}
