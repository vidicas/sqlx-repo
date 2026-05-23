use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn basic_insert() {
    let input = "update test set value='foo' where key = 1";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "UPDATE `test` SET `value`='foo' WHERE `key` = 1"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "UPDATE `test` SET `value`='foo' WHERE `key` = 1"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "UPDATE \"test\" SET \"value\"='foo' WHERE \"key\" = 1"
    );
}

#[test]
fn update_from() {
    let input = "UPDATE target_table SET key=value FROM source_table";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Update {
            reason: "from table"
        }
    ));
    assert_eq!(err.to_string(), "unsupported update: from table");
}

#[test]
fn update_returning() {
    let input = "UPDATE target_table SET key=value RETURNING table.id";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Update {
            reason: "returning"
        }
    ));
    assert_eq!(err.to_string(), "unsupported update: returning");
}

#[test]
fn update_or() {
    let input = "UPDATE OR REPLACE target_table SET key='value'";
    let err = parse(input).unwrap_err();
    assert!(matches!(
        err,
        Error::Update {
            reason: "update with OR is not supported"
        }
    ));
    assert_eq!(
        err.to_string(),
        "unsupported update: update with OR is not supported"
    );
}

#[test]
fn update_order_by() {
    let input = "UPDATE foo SET a=1 ORDER BY id";
    let err = parse(input).unwrap_err();
    assert!(
        matches!(err, Error::Update { reason: "order by" }),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "unsupported update: order by");
}

#[test]
fn update_output_clause() {
    let input = "UPDATE t1 SET a=1 OUTPUT INSERTED.a";
    let err = parse(input).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Update {
                reason: "output clause"
            }
        ),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "unsupported update: output clause");
}

#[test]
fn update_optimizer_hints() {
    let input = "UPDATE /*+ INDEX(t pk) */ t SET a=1 WHERE id=1";
    let err = parse(input).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Update {
                reason: "optimizer hints"
            }
        ),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "unsupported update: optimizer hints");
}
