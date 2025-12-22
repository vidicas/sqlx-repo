use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};
use sqlparser::ast::ObjectType;

#[test]
fn drop_index() {
    let input = "DROP INDEX idx ON tbl";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "DROP INDEX `idx` ON `tbl`"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DROP INDEX \"idx\"",
    );

    assert_eq!(ast.to_sql(&SQLiteDialect {}).unwrap(), "DROP INDEX `idx`",);
}

#[test]
fn drop_multiple_index() {
    let input = "DROP INDEX idx1, idx2";
    let err = parse(input).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Drop {
                reason: "multiple names",
                object_type: None
            }
        ),
        "{:?}",
        err
    );
}

#[test]
fn drop_table() {
    let input = "DROP TABLE test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(ast.to_sql(&MySqlDialect {}).unwrap(), "DROP TABLE `test`");

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DROP TABLE \"test\""
    );

    assert_eq!(ast.to_sql(&SQLiteDialect {}).unwrap(), "DROP TABLE `test`");
}

#[test]
fn drop_multiple_table() {
    let input = "DROP TABLE test1, test2";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Drop {
            reason: "multiple names",
            object_type: None
        }
    ));
    assert_eq!(err.to_string(), "unsupported drop: multiple names");
}

#[test]
fn drop_index_if_exists() {
    let input = "DROP INDEX IF EXISTS idx ON tbl";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "DROP INDEX IF EXISTS `idx` ON `tbl`"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DROP INDEX IF EXISTS \"idx\"",
    );

    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "DROP INDEX IF EXISTS `idx`",
    );
}

#[test]
fn drop_table_if_exists() {
    let input = "DROP TABLE IF EXISTS test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "DROP TABLE IF EXISTS `test`"
    );

    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "DROP TABLE IF EXISTS \"test\""
    );

    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "DROP TABLE IF EXISTS `test`"
    );
}

#[test]
fn drop_index_no_table_name() {
    let input = "DROP INDEX foo_idx";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::DropIndex {
            reason: "table name required",
        }
    ));
    assert_eq!(
        err.to_string(),
        "unsupported drop index: table name required"
    )
}

#[test]
fn drop_view() {
    let input = "DROP VIEW foo_view";
    let err = parse(input).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Drop {
                reason: "object type",
                object_type: Some(ObjectType::View)
            }
        ),
        "{err:?}",
    );
    assert_eq!(err.to_string(), "unsupported drop: object type View")
}
