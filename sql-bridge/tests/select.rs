use sql_bridge::{Error, MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_simple_query() {
    let input = "select * from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\""
    );
}

#[test]
fn test_query_with_projection() {
    let input = "select id, key, * from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT `id`, `key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT `id`, `key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT \"id\", \"key\", * FROM \"test\""
    );
}

#[test]
fn test_query_with_compound_ident_and_all() {
    let input = "select test.id, test.key, * from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT `test`.`id`, `test`.`key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT `test`.`id`, `test`.`key`, * FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT \"test\".\"id\", \"test\".\"key\", * FROM \"test\""
    );
}

#[test]
fn test_query_with_compound_ident_and_ident() {
    let input = "select test.id, key from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT `test`.`id`, `key` FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT `test`.`id`, `key` FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT \"test\".\"id\", \"key\" FROM \"test\""
    );
}

#[test]
fn query_with_too_many_ident_compounds() {
    let input = "select schema.foo.id from foo";
    let res = parse(input);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(err, Error::CompoundIdentifier { length: 3 });
    assert_eq!(
        err.to_string(),
        "unsupported compound identifier with length 3"
    );
}

#[test]
fn test_query_with_predicates() {
    let input = "select * from test where id = 1 AND key = 'foo'";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = 1 AND `key` = 'foo'"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = 1 AND `key` = 'foo'"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" = 1 AND \"key\" = 'foo'"
    );
}

#[test]
fn test_count_function() {
    let input = "select count(*) from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT COUNT(*) FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT COUNT(*) FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT COUNT(*) FROM \"test\""
    );

    let input = "select count(id) from test";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT COUNT(`id`) FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT COUNT(`id`) FROM `test`"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT COUNT(\"id\") FROM \"test\""
    );
}

#[test]
fn test_query_with_group_by() {
    let input = "select key, count(*) from test group by key";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT `key`, COUNT(*) FROM `test` GROUP BY (`key`)"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT `key`, COUNT(*) FROM `test` GROUP BY (`key`)"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT \"key\", COUNT(*) FROM \"test\" GROUP BY (\"key\")"
    );
}

#[test]
fn test_query_with_order_by() {
    let input = "select * from test order by id asc, key desc";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` ORDER BY `id` ASC, `key` DESC"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" ORDER BY \"id\" ASC, \"key\" DESC"
    );
}

#[test]
fn select_literal_constant_number() {
    let input = "select 1";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(ast.to_sql(&MySqlDialect {}).unwrap(), "SELECT 1");
    assert_eq!(ast.to_sql(&SQLiteDialect {}).unwrap(), "SELECT 1");
    assert_eq!(ast.to_sql(&PostgreSqlDialect {}).unwrap(), "SELECT 1");
}

#[test]
fn select_literal_constant_string() {
    let input = "select '1'";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(ast.to_sql(&MySqlDialect {}).unwrap(), "SELECT '1'");
    assert_eq!(ast.to_sql(&SQLiteDialect {}).unwrap(), "SELECT '1'");
    assert_eq!(ast.to_sql(&PostgreSqlDialect {}).unwrap(), "SELECT '1'");
}

#[test]
fn select_with_placeholder() {
    let input = "select * from test where id = ?";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = ?"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = ?"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" = $1"
    );
}

#[test]
fn select_with_multiple_placeholder() {
    let input = "select * from test where id = ? and value = ? or id = ?";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = ? AND `value` = ? OR `id` = ?"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` = ? AND `value` = ? OR `id` = ?"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" = $1 AND \"value\" = $2 OR \"id\" = $3"
    );
}

#[test]
fn select_in() {
    let input = "select * from test where id IN (1, '2', ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` IN (1, '2', ?)"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` IN (1, '2', ?)"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" IN (1, '2', $1)"
    );
}

#[test]
fn select_not_in() {
    let input = "select * from test where id NOT IN (1, '2', ?)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` NOT IN (1, '2', ?)"
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `test` WHERE `id` NOT IN (1, '2', ?)"
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"test\" WHERE \"id\" NOT IN (1, '2', $1)"
    );
}

#[test]
fn select_with_join() {
    let input = "
        select * from foo
        join bar on foo.id = bar.id
        join baz on foo.id = baz.id
    ";

    let res = parse(input);
    if let Err(e) = res {
        println!("{e}")
    };
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `foo` JOIN `bar` ON `foo`.`id` = `bar`.`id` JOIN `baz` ON `foo`.`id` = `baz`.`id`",
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `foo` JOIN `bar` ON `foo`.`id` = `bar`.`id` JOIN `baz` ON `foo`.`id` = `baz`.`id`",
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"foo\" JOIN \"bar\" ON \"foo\".\"id\" = \"bar\".\"id\" JOIN \"baz\" ON \"foo\".\"id\" = \"baz\".\"id\"",
    );
}

#[test]
fn select_with_inner_join() {
    let input = "
        select * from foo
        inner join bar on foo.id = bar.id
        inner join baz on foo.id = baz.id
    ";

    let res = parse(input);
    if let Err(e) = res {
        println!("{e}")
    };
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(&MySqlDialect {}).unwrap(),
        "SELECT * FROM `foo` INNER JOIN `bar` ON `foo`.`id` = `bar`.`id` INNER JOIN `baz` ON `foo`.`id` = `baz`.`id`",
    );
    assert_eq!(
        ast.to_sql(&SQLiteDialect {}).unwrap(),
        "SELECT * FROM `foo` INNER JOIN `bar` ON `foo`.`id` = `bar`.`id` INNER JOIN `baz` ON `foo`.`id` = `baz`.`id`",
    );
    assert_eq!(
        ast.to_sql(&PostgreSqlDialect {}).unwrap(),
        "SELECT * FROM \"foo\" INNER JOIN \"bar\" ON \"foo\".\"id\" = \"bar\".\"id\" INNER JOIN \"baz\" ON \"foo\".\"id\" = \"baz\".\"id\"",
    );
}

#[test]
fn select_with_join_too_many_indent_compounds() {
    let input = "select * from foo inner join bar on schema.foo.id = schema.bar.id ";
    let res = parse(input);
    assert!(res.is_err());

    let err = res.unwrap_err();
    assert_eq!(err, Error::CompoundIdentifier { length: 3 });
    assert_eq!(
        err.to_string(),
        "unsupported compound identifier with length 3"
    );
}

#[test]
fn select_empty_projection() {
    let input = "select from foo";
    let res = parse(input);
    assert!(res.is_err());
}
