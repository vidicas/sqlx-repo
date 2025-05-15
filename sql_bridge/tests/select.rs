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
