use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[test]
fn test_serial_no_primary_key() {
    let input = "CREATE TABLE test (id smallserial)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );

    let input = "CREATE TABLE test (id serial)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );

    let input = "CREATE TABLE test (id bigserial)";
    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap_err().to_string(),
        "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint",
    );
}

#[test]
fn test_primary_key_creation() {
    let query = "CREATE TABLE test (id smallserial primary key)";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        "CREATE TABLE `test` (\n`id` SMALLINT PRIMARY KEY AUTO_INCREMENT\n)",
        ast.to_sql(MySqlDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY\n)",
        ast.to_sql(SQLiteDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE \"test\" (\n\"id\" SMALLSERIAL PRIMARY KEY\n)",
        ast.to_sql(PostgreSqlDialect {}).unwrap()
    );

    let query = "CREATE TABLE test (id serial primary key)";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INT PRIMARY KEY AUTO_INCREMENT\n)",
        ast.to_sql(MySqlDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY\n)",
        ast.to_sql(SQLiteDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE \"test\" (\n\"id\" SERIAL PRIMARY KEY\n)",
        ast.to_sql(PostgreSqlDialect {}).unwrap()
    );

    let query = "CREATE TABLE test (id bigserial primary key)";
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();
    assert_eq!(
        "CREATE TABLE `test` (\n`id` BIGINT PRIMARY KEY AUTO_INCREMENT\n)",
        ast.to_sql(MySqlDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY\n)",
        ast.to_sql(SQLiteDialect {}).unwrap()
    );
    assert_eq!(
        "CREATE TABLE \"test\" (\n\"id\" BIGSERIAL PRIMARY KEY\n)",
        ast.to_sql(PostgreSqlDialect {}).unwrap()
    );
}

#[test]
fn test_all_supported_types() {
    let query = r#"
    CREATE TABLE sample_types (
        id32 SERIAL PRIMARY KEY,
        i16 SMALLINT,
        i32 INTEGER,
        i64 BIGINT,
        numeric NUMERIC(10, 2),
        real REAL,
        double DOUBLE PRECISION,
        bool BOOLEAN,
        char CHAR(5),
        varchar VARCHAR(100),
        text TEXT,
        date DATE,
        time TIME,
        timestamp TIMESTAMP,
        uuid UUID,
        bytes BYTEA,
        json JSON
    )"#;
    let mut ast = parse(query).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        r#"CREATE TABLE `sample_types` (
`id32` INT PRIMARY KEY AUTO_INCREMENT,
`i16` SMALLLINT,
`i32` INT,
`i64` BIGINT,
`numeric` DECIMAL(10, 2),
`real` REAL,
`double` DOUBLE,
`bool` BOOLEAN,
`char` CHAR(5),
`varchar` VARCHAR(100),
`text` TEXT,
`date` DATE,
`time` TIME,
`timestamp` DATETIME,
`uuid` CHAR(36),
`bytes` BLOB,
`json` JSON
)"#,
        ast.to_sql(MySqlDialect {}).unwrap()
    );

    assert_eq!(
        r#"CREATE TABLE "sample_types" (
"id32" SERIAL PRIMARY KEY,
"i16" SMALLLINT,
"i32" INT,
"i64" BIGINT,
"numeric" DECIMAL(10, 2),
"real" REAL,
"double" DOUBLE,
"bool" BOOLEAN,
"char" CHAR(5),
"varchar" VARCHAR(100),
"text" TEXT,
"date" DATE,
"time" TIME,
"timestamp" TIMESTAMP,
"uuid" UUID,
"bytes" BYTEA,
"json" JSON
)"#,
        ast.to_sql(PostgreSqlDialect {}).unwrap()
    );
    assert_eq!(
        r#"CREATE TABLE `sample_types` (
`id32` INTEGER PRIMARY KEY,
`i16` INTEGER,
`i32` INTEGER,
`i64` INTEGER,
`numeric` DECIMAL(10, 2),
`real` FLOAT,
`double` DOUBLE,
`bool` BOOLEAN,
`char` CHAR(5),
`varchar` VARCHAR(100),
`text` TEXT,
`date` TEXT,
`time` TEXT,
`timestamp` TEXT,
`uuid` UUID,
`bytes` BLOB,
`json` JSON
)"#,
        ast.to_sql(SQLiteDialect {}).unwrap()
    );
}

#[test]
fn test_mysql_style_primary_key() {
    let input = "CREATE TABLE test (id integer primary key autoincrement)";

    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "CREATE TABLE `test` (\n`id` INT PRIMARY KEY AUTO_INCREMENT\n)"
    );

    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY\n)"
    );

    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "CREATE TABLE \"test\" (\n\"id\" SERIAL PRIMARY KEY\n)"
    );
}

#[test]
fn test_composite_primary_key() {
    let input = "CREATE TABLE foo_baz(l int, r int, value text, primary key (l, p))";

    let mut ast = parse(input).unwrap();
    assert!(ast.len() == 1);
    let ast = ast.pop().unwrap();

    assert_eq!(
        ast.to_sql(MySqlDialect {}).unwrap(),
        "CREATE TABLE `foo_baz` (\n`l` INT,\n`r` INT,\n`value` TEXT,\nPRIMARY KEY (`l`, `p`)\n)"
    );

    assert_eq!(
        ast.to_sql(SQLiteDialect {}).unwrap(),
        "CREATE TABLE `foo_baz` (\n`l` INTEGER,\n`r` INTEGER,\n`value` TEXT,\nPRIMARY KEY (`l`, `p`)\n)"
    );

    assert_eq!(
        ast.to_sql(PostgreSqlDialect {}).unwrap(),
        "CREATE TABLE \"foo_baz\" (\n\"l\" INT,\n\"r\" INT,\n\"value\" TEXT,\nPRIMARY KEY (\"l\", \"p\")\n)"
    );
}
