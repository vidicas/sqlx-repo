use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn basic_insert() {
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
fn basic_insert_with_placeholders() {
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
fn basic_insert_with_placeholders_with_cast() {
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

#[test]
fn insert_with_or() {
    let input = "INSERT OR REPLACE INTO table_name (id, name) VALUES (1, 'Alice')";
    let err = parse(input).unwrap_err();
    assert!(matches!(err, Error::Insert { reason: "or" }));
    assert_eq!(err.to_string(), "unsupported insert: or");
}

#[test]
fn insert_ignore() {
    let input = "INSERT IGNORE INTO table_name (id, name) VALUES (1, 'Alice')";
    let err = parse(input).unwrap_err();
    assert!(matches!(err, Error::Insert { reason: "ignore" }));
    assert_eq!(err.to_string(), "unsupported insert: ignore");
}

#[test]
fn insert_without_into() {
    let input = "INSERT table_name (id, name) VALUES (1, 'Alice')";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "missing into"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: missing into");
}

#[test]
fn insert_overwrite() {
    let input = "INSERT OVERWRITE INTO table_name (id, name) VALUES (1, 'Alice')";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "overwrite"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: overwrite");
}

#[test]
fn insert_partitioned() {
    let input = "INSERT INTO table_name PARTITION (partition_name) VALUES (col1_value, col2_value)";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "partitioned"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: partitioned");
}

#[test]
fn insert_has_table_keyword() {
    let input = "INSERT INTO TABLE table_name VALUES (col1_value, col2_value)";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "table keyword"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: table keyword");
}

#[test]
fn insert_on_conflict() {
    let input =
        "INSERT INTO table_name VALUES (col1_value, col2_value) ON CONFLICT (id) DO NOTHING;";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "on keyword"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: on keyword");
}

#[test]
fn insert_returning() {
    let input = "INSERT INTO table_name VALUES (col1_value, col2_value) RETURNING id";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Insert {
            reason: "returning"
        }
    ));
    assert_eq!(err.to_string(), "unsupported insert: returning");
}

#[test]
fn insert_replace() {
    let input = "REPLACE INTO table_name VALUES (col1_value, col2_value)";
    let err = parse(input).unwrap_err();
    assert!(matches!(err, Error::Insert { reason: "replace" }));
    assert_eq!(err.to_string(), "unsupported insert: replace");
}

#[test]
fn insert_priority() {
    let input = "INSERT HIGH_PRIORITY INTO table_name VALUES (col1_value, col2_value)";
    let err = parse(input).unwrap_err();
    assert!(matches!(err, Error::Insert { reason: "priority" }));
    assert_eq!(err.to_string(), "unsupported insert: priority");
}
