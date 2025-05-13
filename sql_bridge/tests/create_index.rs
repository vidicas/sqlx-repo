use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn create_unique_index() {
    let input = "CREATE UNIQUE INDEX idx ON table_name (id, org)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "CREATE UNIQUE INDEX `idx` ON `table_name` (`id`, `org`)"
    );

    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "CREATE UNIQUE INDEX \"idx\" ON \"table_name\" (\"id\", \"org\")",
    );

    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "CREATE UNIQUE INDEX `idx` ON `table_name` (`id`, `org`)"
    );
}


#[test]
fn create_index() {
    let input = "CREATE INDEX idx ON table_name (id, org)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "CREATE INDEX `idx` ON `table_name` (`id`, `org`)"
    );

    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "CREATE INDEX \"idx\" ON \"table_name\" (\"id\", \"org\")",
    );

    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "CREATE INDEX `idx` ON `table_name` (`id`, `org`)"
    );
}
