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
