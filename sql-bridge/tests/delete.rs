use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

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
    let input = "DELETE FROM test WHERE key = 1";
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

#[test]
fn delete_multiple_tables() {
    let query = "DELETE FROM t1, t2 WHERE id = 1";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::Delete {
            reason: "multiple tables"
        }
    ));
    assert_eq!(err.to_string(), "unsupported delete: multiple tables");
}

#[test]
fn delete_using() {
    let query = "DELETE FROM t1 USING t2";
    let err = parse(query).unwrap_err();
    assert!(matches!(err, Error::Delete { reason: "using" }));
    assert_eq!(err.to_string(), "unsupported delete: using");
}

#[test]
fn delete_returning() {
    let query = "DELETE FROM t1 RETURNING t1.id";
    let err = parse(query).unwrap_err();
    assert!(matches!(
        err,
        Error::Delete {
            reason: "returning"
        }
    ));
    assert_eq!(err.to_string(), "unsupported delete: returning");
}

#[test]
fn delete_order_by() {
    let query = "DELETE FROM t1 ORDER BY created_at";
    let err = parse(query).unwrap_err();
    assert!(matches!(err, Error::Delete { reason: "order by" }));
    assert_eq!(err.to_string(), "unsupported delete: order by");
}

#[test]
fn delete_limit() {
    let query = "DELETE FROM t1 LIMIT 10";
    let err = parse(query).unwrap_err();
    assert!(matches!(err, Error::Delete { reason: "limit" }));
    assert_eq!(err.to_string(), "unsupported delete: limit");
}
