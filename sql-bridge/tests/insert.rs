use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_basic_insert() {
    let input = "insert into test(id, key, value) values(null, 1, 'one'), (null, 2, 'two')";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (NULL, 1, 'one'), (NULL, 2, 'two')",
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (NULL, 1, 'one'), (NULL, 2, 'two')",
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "INSERT INTO \"test\"(\"id\", \"key\", \"value\") VALUES (NULL, 1, 'one'), (NULL, 2, 'two')",
    );
}

#[test]
fn test_basic_insert_with_placeholders() {
    let input = "insert into test(id, key, value) values(?, ?, ?), (?, ?, ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (?, ?, ?), (?, ?, ?)"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (?, ?, ?), (?, ?, ?)"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "INSERT INTO \"test\"(\"id\", \"key\", \"value\") VALUES ($1, $2, $3), ($4, $5, $6)"
    );
}

#[test]
fn test_basic_insert_with_placeholders_with_cast() {
    let input = "insert into test(id, key, value) values(?::json, ?::uuid, ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (?, ?, ?)"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "INSERT INTO `test`(`id`, `key`, `value`) VALUES (?, ?, ?)"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "INSERT INTO \"test\"(\"id\", \"key\", \"value\") VALUES ($1::JSON, $2::UUID, $3)"
    );
}
