use sql_bridge::{parse, MySqlDialect, SQLiteDialect, PostgreSqlDialect};

#[test]
fn test_serial_no_primary_key() {
    let query = "CREATE TABLE test (id serial)";
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(MySqlDialect {})
            .unwrap_err()
            .to_string()
    );
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(SQLiteDialect {})
            .unwrap_err()
            .to_string()
    );
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(PostgreSqlDialect {})
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn test_bigserial_no_primary_key() {
    let query = "CREATE TABLE test (id bigserial)";
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(MySqlDialect {})
            .unwrap_err()
            .to_string()
    );
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(SQLiteDialect {})
            .unwrap_err()
            .to_string()
    );
    assert_eq!(
        "expected serial/bigserial with `PRIMARY KEY` constraint",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(PostgreSqlDialect {})
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn test_primary_key_creation() {
    let query = "CREATE TABLE test (id serial primary key)";
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INT PRIMARY KEY AUTO_INCREMENT\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(MySqlDialect {})
            .unwrap()
    );
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY AUTOINCREMENT\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(SQLiteDialect {})
            .unwrap()
    );
    assert_eq!(
        "CREATE TABLE \"test\" (\n\"id\" SERIAL PRIMARY KEY\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(PostgreSqlDialect {})
            .unwrap()
    );

    let query = "CREATE TABLE test (id bigserial primary key)";
    assert_eq!(
        "CREATE TABLE `test` (\n`id` BIGINT PRIMARY KEY AUTO_INCREMENT\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(MySqlDialect {})
            .unwrap()
    );
    assert_eq!(
        "CREATE TABLE `test` (\n`id` INTEGER PRIMARY KEY AUTOINCREMENT\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(SQLiteDialect {})
            .unwrap()
    );
    assert_eq!(
        "CREATE TABLE \"test\" (\n\"id\" BIGSERIAL PRIMARY KEY\n)",
        parse(query)
            .unwrap()
            .first()
            .unwrap()
            .to_sql(PostgreSqlDialect {})
            .unwrap()
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
`id32` INTEGER PRIMARY KEY AUTOINCREMENT,
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
