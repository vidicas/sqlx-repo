use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

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
        "unsupported create index: existance check"
    );
}

#[test]
fn create_index_auto_named() {
    let input = "CREATE INDEX ON table (column)";
    let error = parse(input).unwrap_err();
    assert!(matches!(error, Error::CreateIndex { reason: "nameless" }));
    assert_eq!(error.to_string(), "unsupported create index: nameless");
}

#[test]
fn create_index_concurrent() {
    let input = "CREATE INDEX CONCURRENTLY idx_name ON table_name (column_name)";
    let error = parse(input).unwrap_err();
    assert!(matches!(
        error,
        Error::CreateIndex {
            reason: "concurrent"
        }
    ));
    assert_eq!(error.to_string(), "unsupported create index: concurrent");
}

#[test]
fn create_index_using_index_method() {
    let input = "CREATE INDEX index_name ON table_name USING index_method (column_list)";
    let error = parse(input).unwrap_err();
    assert!(matches!(error, Error::CreateIndex { reason: "using" }));
    assert_eq!(error.to_string(), "unsupported create index: using");
}

#[test]
fn create_index_with_include() {
    let input = "CREATE INDEX idx_name ON table(column) INCLUDE (other_column)";
    let error = parse(input).unwrap_err();
    assert!(matches!(error, Error::CreateIndex { reason: "include" }));
    assert_eq!(error.to_string(), "unsupported create index: include");
}

#[test]
fn create_index_distinct_nulls() {
    let input = "CREATE INDEX idx_name ON table_name (col) NULLS DISTINCT";
    let error = parse(input).unwrap_err();
    assert!(matches!(
        error,
        Error::CreateIndex {
            reason: "distinct nulls"
        }
    ));
    assert_eq!(
        error.to_string(),
        "unsupported create index: distinct nulls"
    );
}

#[test]
fn create_index_with() {
    let input = "CREATE INDEX idx_name ON table_name (col) WITH (fillfactor = 70)";
    let error = parse(input).unwrap_err();
    assert!(matches!(error, Error::CreateIndex { reason: "with" }));
    assert_eq!(error.to_string(), "unsupported create index: with");
}

#[test]
fn create_index_with_predicate() {
    let input = "CREATE INDEX idx_name ON table_name(col) WHERE col > 500";
    let error = parse(input).unwrap_err();
    assert!(matches!(
        error,
        Error::CreateIndex {
            reason: "predicate"
        }
    ));
    assert_eq!(error.to_string(), "unsupported create index: predicate");
}
