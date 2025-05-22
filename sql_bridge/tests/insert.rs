use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_basic_insert() {
    let input = "insert into test(id, key, value) values(null, 1, 'one'), (null, 2, 'two')";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" ORDER BY \"id\" ASC, \"key\" DESC"
    );
}

fn test_basic_insert_with_placeholders() {
    let input = "insert into test(id, key, value) values(?, ?, ?), (?, ?, ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" ORDER BY \"id\" ASC, \"key\" DESC"
    );
}

fn test_basic_insert_with_placeholders_with_cast() {
    let input = "insert into test(id, key, value) values(?::json, ?::uuid, ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" ORDER BY \"id\" ASC, \"key\" DESC"
    );
}