use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn create_index_unique() {
    let input = "CREATE UNIQUE INDEX idx ON table_name (id, org)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "CREATE UNIQUE INDEX `idx` ON `table_name` (`id`, `org`)"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "CREATE UNIQUE INDEX \"idx\" ON \"table_name\" (\"id\", \"org\")",
    );

    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
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
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "CREATE INDEX `idx` ON `table_name` (`id`, `org`)"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "CREATE INDEX \"idx\" ON \"table_name\" (\"id\", \"org\")",
    );

    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "CREATE INDEX `idx` ON `table_name` (`id`, `org`)"
    );
}

#[test]
fn create_index_if_not_exists() {
    let input = "CREATE INDEX IF NOT EXISTS idx ON table_name (id, org)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with existance check is not supported"
    );
}

#[test]
fn create_index_auto_named() {
    let input = "CREATE INDEX ON table (column)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` without name is not supported"
    );
}

#[test]
fn create_index_concurrent() {
    let input = "CREATE INDEX CONCURRENTLY idx_name ON table_name (column_name)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "concurrent `CREATE INDEX` is not supported",
    );
}

#[test]
fn create_index_using_index_method() {
    let input = "CREATE INDEX index_name ON table_name USING index_method (column_list)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with `USING` keyword is not supported"
    );
}

#[test]
fn create_index_with_include() {
    let input = "CREATE INDEX idx_name ON table(column) INCLUDE (other_column)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with `INCLUDE` is not supported"
    );
}

#[test]
fn create_index_distinct_nulls() {
    let input = "CREATE INDEX idx_name ON table_name (col) NULLS DISTINCT";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with `DISTINCT NULLS` is not supported",
    );
}

#[test]
fn create_index_with() {
    let input = "CREATE INDEX idx_name ON table_name (col) WITH (fillfactor = 70)";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with `WITH` keyword is not supported"
    );
}

#[test]
fn create_index_with_predicate() {
    let input = "CREATE INDEX idx_name ON table_name(col) WHERE col > 500";
    assert_eq!(
        parse(input).unwrap_err().to_string(),
        "`CREATE INDEX` with predicates is not supported"
    );
}
