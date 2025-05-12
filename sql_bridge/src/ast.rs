#![allow(unused)]

use std::{
    borrow::Cow,
    io::{Cursor, Write},
    ops::Deref,
    path::Display,
};

use sqlparser::{
    ast::{
        CharacterLength, ColumnDef, ColumnOptionDef, CreateTable, ExactNumberInfo, ObjectName,
        ObjectNamePart, Statement, TableConstraint,
    }, dialect::{self, MySqlDialect, PostgreSqlDialect, SQLiteDialect}, keywords::Keyword, parser::Parser, tokenizer::{Token, Word}
};

use crate::{Result, Error};

/// Common datatypes which are supported across all databases
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    SmallSerial,
    Serial,
    BigSerial,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    String,
    Char(u64),
    VarChar(u64),
    Bytes,
    JSON,
    UUID,
    Decimal{
        precision: u64,
        scale: u64
    },
    Date,
    Time,
    Timestamp,
}

impl TryFrom<&sqlparser::ast::DataType> for DataType {
    type Error = Error;

    fn try_from(value: &sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        let dt = match value {
            sqlparser::ast::DataType::SmallInt(_) => DataType::I16,
            sqlparser::ast::DataType::Int(_) => DataType::I32,
            sqlparser::ast::DataType::Integer(_) => DataType::I32,
            sqlparser::ast::DataType::BigInt(_) => DataType::I64,
            sqlparser::ast::DataType::Real => DataType::F32,
            sqlparser::ast::DataType::Double(_) => DataType::F64,
            sqlparser::ast::DataType::DoublePrecision => DataType::F64,
            sqlparser::ast::DataType::Bool => DataType::Bool,
            sqlparser::ast::DataType::Boolean => DataType::Bool,
            sqlparser::ast::DataType::Text => DataType::String,
            sqlparser::ast::DataType::Char(Some(CharacterLength::IntegerLength {
                length, ..
            })) => DataType::Char(*length),
            sqlparser::ast::DataType::Varchar(Some(CharacterLength::IntegerLength {
                length,
                ..
            })) => DataType::VarChar(*length),
            sqlparser::ast::DataType::Bytea => DataType::Bytes,
            sqlparser::ast::DataType::JSON => DataType::JSON,
            sqlparser::ast::DataType::Uuid => DataType::UUID,
            sqlparser::ast::DataType::Decimal(ExactNumberInfo::PrecisionAndScale(
                precision,
                scale,
            )) => DataType::Decimal{precision: *precision, scale: *scale},
            sqlparser::ast::DataType::Numeric(ExactNumberInfo::PrecisionAndScale(
                precision,
                scale,
            )) => DataType::Decimal{precision: *precision, scale: *scale},
            sqlparser::ast::DataType::Custom(ObjectName(name_parts), _) => {
                match extract_serial(name_parts) {
                    Some(dt) => dt,
                    None => Err("unsupported data type: {value:?}")?,
                }
            }
            sqlparser::ast::DataType::Date => DataType::Date,
            sqlparser::ast::DataType::Time(_, _) => DataType::Time,
            sqlparser::ast::DataType::Timestamp(_, _) => DataType::Timestamp,
            sqlparser::ast::DataType::Datetime(_) => DataType::Timestamp,
            _ => Err(format!("unsupported data type: {value:?}"))?,
        };
        Ok(dt)
    }
}

fn extract_serial(name_parts: &[ObjectNamePart]) -> Option<DataType> {
    if name_parts.len() != 1 {
        return None;
    }
    let ObjectNamePart::Identifier(name) = name_parts.first().unwrap();
    let name = name.value.to_ascii_lowercase();
    match name.as_str() {
        "bigserial" => Some(DataType::BigSerial),
        "serial" => Some(DataType::Serial),
        "smallserial" => Some(DataType::SmallSerial),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub options: ColumnOptions,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnOptions(u32);

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u32)]
pub enum ColumnOption {
    PrimaryKey = 1,
    AutoInrement = 1 << 1,
    Nullable = 1 << 2,
    NotNull = 1 << 3,
    Unique = 1 << 4,
}

impl ColumnOptions {
    fn new() -> Self {
        Self(0)
    }

    fn set_primary_key(mut self) -> Self {
        self.0 |= ColumnOption::PrimaryKey as u32;
        self
    }

    fn is_primary_key(self) -> bool {
        self.0 & ColumnOption::PrimaryKey as u32 != 0
    }

    fn set_auto_increment(mut self) -> Self {
        self.0 |= ColumnOption::AutoInrement as u32;
        self
    }

    fn unset_auto_increment(mut self) -> Self {
        self.0 &= !(ColumnOption::AutoInrement as u32);
        self
    }

    fn is_auto_increment(self) -> bool {
        self.0 & ColumnOption::AutoInrement as u32 != 0
    }

    fn set_nullable(mut self) -> Self {
        self.0 |= ColumnOption::Nullable as u32;
        self
    }

    fn is_nullable(self) -> bool {
        self.0 & ColumnOption::Nullable as u32 != 0
    }

    fn set_not_null(mut self) -> Self {
        self.0 |= ColumnOption::NotNull as u32;
        self
    }

    fn is_not_null(self) -> bool {
        self.0 & ColumnOption::NotNull as u32 != 0
    }

    fn set_unique(mut self) -> Self {
        self.0 |= ColumnOption::Unique as u32;
        self
    }

    fn is_unique(self) -> bool {
        self.0 & ColumnOption::Unique as u32 != 0
    }

    fn mapping() -> &'static [(ColumnOption, fn(Self) -> bool)] {
        &[
            (ColumnOption::PrimaryKey, ColumnOptions::is_primary_key),
            (ColumnOption::AutoInrement, ColumnOptions::is_auto_increment),
            (ColumnOption::Nullable, ColumnOptions::is_nullable),
            (ColumnOption::NotNull, ColumnOptions::is_not_null),
            (ColumnOption::Unique, ColumnOptions::is_unique),
        ]
    }
}

pub struct ColumnIterator {
    column_options: ColumnOptions,
    pos: usize,
}

impl Iterator for ColumnIterator {
    type Item = ColumnOption;

    fn next(&mut self) -> Option<Self::Item> {
        let mapping = ColumnOptions::mapping();
        loop {
            if self.pos >= mapping.len() {
                return None;
            }
            let (option, check) = mapping[self.pos];
            self.pos += 1;
            if check(self.column_options) {
                return Some(option);
            }
        }
    }
}

impl IntoIterator for ColumnOptions {
    type Item = ColumnOption;
    type IntoIter = ColumnIterator;

    fn into_iter(self) -> Self::IntoIter {
        ColumnIterator {
            column_options: self,
            pos: 0,
        }
    }
}

impl std::fmt::Display for ColumnOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for option in self.into_iter() {
            write!(f, "{:?} ", option)?;
        }
        Ok(())
    }
}

impl TryFrom<&[ColumnOptionDef]> for ColumnOptions {
    type Error = Error;

    fn try_from(values: &[ColumnOptionDef]) -> Result<Self, Self::Error> {
        values.iter().try_fold(
            ColumnOptions::new(),
            |mut options, value| -> Result<_, Error> {
                let options = match &value.option {
                    sqlparser::ast::ColumnOption::Unique { is_primary, .. } if *is_primary => {
                        options.set_primary_key()
                    }
                    sqlparser::ast::ColumnOption::NotNull => options.set_not_null(),
                    sqlparser::ast::ColumnOption::Null => options.set_nullable(),
                    option if is_auto_increment_option(option) => options.set_auto_increment(),
                    option => Err(format!("unsupported column option: {option:?}"))?,
                };
                Ok(options)
            },
        )
    }
}

fn is_auto_increment_option(option: &sqlparser::ast::ColumnOption) -> bool {
    match option {
        sqlparser::ast::ColumnOption::DialectSpecific(tokens) if tokens.len() == 1 => {
            tokens.first().map(|token| {
                match token {
                    Token::Word(Word{ keyword, ..}) => {
                        *keyword == Keyword::AUTOINCREMENT || 
                        *keyword == Keyword::AUTO_INCREMENT
                    },
                    _ => false
                } 
            }).unwrap()
        },
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Constraint {}

#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    CreateTable {
        if_not_exists: bool,
        name: String,
        columns: Vec<Column>,
        constraints: Vec<Constraint>,
    },
}

impl Ast {
    fn parse_create_table(
        if_not_exists: bool,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Ast> {
        let name = {
            let name_parts = &name.0;
            if name_parts.len() > 1 {
                Err("schema-qualified names are not supported")?
            }
            let ObjectNamePart::Identifier(ident) = name_parts.first().unwrap();
            ident.value.clone()
        };
        let columns = {
            columns
                .iter()
                .map(
                    |ColumnDef {
                         name,
                         data_type,
                         options,
                     }| {
                        Ok(Column {
                            name: name.value.clone(),
                            data_type: data_type.try_into()?,
                            options: ColumnOptions::try_from(options.as_slice())?,
                        })
                    },
                )
                .collect::<Result<Vec<Column>>>()?
        };
        Ok(Ast::CreateTable {
            if_not_exists,
            name,
            columns,
            constraints: vec![],
        })
    }

    pub fn parse(query: &str) -> Result<Vec<Ast>> {
        Parser::parse_sql(&dialect::GenericDialect {}, query)?
            .iter()
            .map(|statement| {
                let result = match statement {
                    Statement::CreateTable(CreateTable {
                        if_not_exists,
                        name,
                        columns,
                        constraints,
                        ..
                    }) => Self::parse_create_table(*if_not_exists, name, columns, constraints)?,
                    Statement::AlterTable {
                        name,
                        if_exists,
                        only,
                        operations,
                        location,
                        on_cluster,
                    } => {
                        unimplemented!()
                    }
                    Statement::CreateIndex(index) => {
                        unimplemented!()
                    }
                    Statement::AlterIndex { name, operation } => {
                        unimplemented!()
                    }
                    Statement::Drop {
                        object_type,
                        if_exists,
                        names,
                        cascade,
                        restrict,
                        purge,
                        temporary,
                    } => {
                        unimplemented!()
                    }
                    Statement::Insert(insert) => {
                        unimplemented!()
                    }
                    Statement::Update {
                        table,
                        assignments,
                        from,
                        selection,
                        returning,
                        or,
                    } => {
                        unimplemented!()
                    }
                    Statement::Delete(delete) => {
                        unimplemented!()
                    }
                    _ => Err(format!("unsupported statement: {statement:?}"))?,
                };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn to_sql(&self, dialect: impl ToQuery) -> Result<String> {
        let mut buf = Cursor::new(Vec::with_capacity(1024));
        match self {
            Ast::CreateTable {
                if_not_exists,
                name,
                columns,
                constraints,
            } => {
                buf.write_all(b"CREATE TABLE ")?;
                if *if_not_exists {
                    buf.write_all(b"IF NOT EXISTS ")?;
                }

                buf.write_all(dialect.quote())?;
                buf.write_all(name.as_bytes())?;
                buf.write_all(dialect.quote())?;
                buf.write_all(b" (\n")?;
                for (pos, column) in columns.iter().enumerate() {
                    buf.write_all(dialect.quote())?;
                    buf.write_all(column.name.as_bytes())?;
                    buf.write_all(dialect.quote())?;
                    buf.write_all(b" ")?;

                    dialect.emit_column_spec(column, &mut buf)?;

                    // push comma if column is not last
                    if pos != columns.len() - 1 {
                        buf.write_all(b",\n")?;
                    }
                }
                buf.write_all(b"\n)")?;
            }
        };
        Ok(String::from_utf8(buf.into_inner())?)
    }
}

pub trait ToQuery {
    fn quote(&self) -> &'static [u8];

    fn placeholder(&self, pos: usize) -> Cow<'static, str>;

    fn emit_column_spec(&self, column: &Column, buf: &mut dyn Write) -> Result<()>;
}

impl ToQuery for MySqlDialect {
    fn quote(&self) -> &'static [u8] {
        b"`"
    }

    fn placeholder(&self, _: usize) -> Cow<'static, str> {
        Cow::Borrowed("?")
    }

    fn emit_column_spec(
        &self,
        Column {
            name,
            data_type,
            options,
        }: &Column,
        buf: &mut dyn Write,
    ) -> Result<()> {
        let mut options = *options;
        let spec = match data_type {
            DataType::SmallSerial if options.is_primary_key() => {
                options = options.set_auto_increment();
                Cow::Borrowed("SMALLINT")
            }
            DataType::Serial if options.is_primary_key() => {
                options = options.set_auto_increment();
                Cow::Borrowed("INT")
            }
            DataType::BigSerial if options.is_primary_key() => {
                options = options.set_auto_increment();
                Cow::Borrowed("BIGINT")
            }
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => {
                Err("expected smallserial/serial/bigserial with `PRIMARY KEY` constraint")?
            }
            DataType::I16 => Cow::Borrowed("SMALLLINT"),
            DataType::I32 => Cow::Borrowed("INT"),
            DataType::I64 => Cow::Borrowed("BIGINT"),
            DataType::F32 => Cow::Borrowed("REAL"),
            DataType::F64 => Cow::Borrowed("DOUBLE"),
            DataType::Bool => Cow::Borrowed("BOOLEAN"),
            DataType::String => Cow::Borrowed("TEXT"),
            DataType::Char(len) => Cow::Owned(format!("CHAR({len})")),
            DataType::VarChar(len) => Cow::Owned(format!("VARCHAR({len})")),
            DataType::Bytes => Cow::Borrowed("BLOB"),
            DataType::JSON => Cow::Borrowed("JSON"),
            DataType::UUID => Cow::Borrowed("CHAR(36)"),
            DataType::Decimal{precision, scale} => {
                Cow::Owned(format!("DECIMAL({precision}, {scale})"))
            }
            DataType::Date => Cow::Borrowed("DATE"),
            DataType::Time => Cow::Borrowed("TIME"),
            DataType::Timestamp => Cow::Borrowed("DATETIME"),
        };

        buf.write_all(spec.as_bytes())?;
        let options = options
            .into_iter()
            .map(|option| match option {
                ColumnOption::PrimaryKey => "PRIMARY KEY",
                ColumnOption::AutoInrement => "AUTO_INCREMENT",
                ColumnOption::NotNull => "NOT NULL",
                ColumnOption::Nullable => "NULL",
                ColumnOption::Unique => "UNIQUE",
            })
            .collect::<Vec<_>>()
            .join(" ");
        if !options.is_empty() {
            buf.write_all(b" ")?;
            buf.write_all(options.as_bytes())?;
        }
        Ok(())
    }
}

impl ToQuery for PostgreSqlDialect {
    fn quote(&self) -> &'static [u8] {
        b"\""
    }

    fn placeholder(&self, pos: usize) -> Cow<'static, str> {
        Cow::Owned(format!("${pos}"))
    }

    fn emit_column_spec(
        &self,
        Column {
            name,
            data_type,
            options,
        }: &Column,
        buf: &mut dyn Write,
    ) -> Result<()> {
        let mut options = *options;
        let spec = match data_type {
            DataType::SmallSerial if options.is_primary_key() => Cow::Borrowed("SMALLSERIAL"),
            DataType::Serial if options.is_primary_key() => Cow::Borrowed("SERIAL"),
            DataType::BigSerial if options.is_primary_key() => Cow::Borrowed("BIGSERIAL"),
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => {
                Err("expected smallserial/serial/bigserial with `PRIMARY KEY` constraint")?
            },
            DataType::I16 if options.is_primary_key() && options.is_auto_increment() => {
                Cow::Borrowed("SMALLSERIAL")
            }
            DataType::I32 if options.is_primary_key() && options.is_auto_increment() => {
                Cow::Borrowed("SERIAL")
            }
            DataType::I64 if options.is_primary_key() && options.is_auto_increment() => {
                Cow::Borrowed("BIGSERIAL")
            }
            DataType::I16 => Cow::Borrowed("SMALLLINT"),
            DataType::I32 => Cow::Borrowed("INT"),
            DataType::I64 => Cow::Borrowed("BIGINT"),
            DataType::F32 => Cow::Borrowed("REAL"),
            DataType::F64 => Cow::Borrowed("DOUBLE"),
            DataType::Bool => Cow::Borrowed("BOOLEAN"),
            DataType::String => Cow::Borrowed("TEXT"),
            DataType::Char(len) => Cow::Owned(format!("CHAR({len})")),
            DataType::VarChar(len) => Cow::Owned(format!("VARCHAR({len})")),
            DataType::Bytes => Cow::Borrowed("BYTEA"),
            DataType::JSON => Cow::Borrowed("JSON"),
            DataType::UUID => Cow::Borrowed("UUID"),
            DataType::Decimal{precision, scale} => {
                Cow::Owned(format!("DECIMAL({precision}, {scale})"))
            }
            DataType::Date => Cow::Borrowed("DATE"),
            DataType::Time => Cow::Borrowed("TIME"),
            DataType::Timestamp => Cow::Borrowed("TIMESTAMP"),
        };
        buf.write_all(spec.as_bytes())?;
        let options = options
            .into_iter()
            .filter_map(|option| match option {
                ColumnOption::PrimaryKey => Some("PRIMARY KEY"),
                ColumnOption::AutoInrement => None,
                ColumnOption::NotNull => Some("NOT NULL"),
                ColumnOption::Nullable => Some("NULL"),
                ColumnOption::Unique => Some("UNIQUE"),
            })
            .collect::<Vec<_>>()
            .join(" ");
        if !options.is_empty() {
            buf.write_all(b" ")?;
            buf.write_all(options.as_bytes())?;
        }
        Ok(())
    }
}

impl ToQuery for SQLiteDialect {
    fn quote(&self) -> &'static [u8] {
        b"`"
    }

    fn placeholder(&self, _: usize) -> Cow<'static, str> {
        Cow::Borrowed("?")
    }

    fn emit_column_spec(
        &self,
        Column {
            name,
            data_type,
            options,
        }: &Column,
        buf: &mut dyn Write,
    ) -> Result<()> {
        let mut options = *options;
        let spec = match data_type {
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial if options.is_primary_key() => {
                Cow::Borrowed("INTEGER")
            }
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => {
                Err("expected smallserial/serial/bigserial with `PRIMARY KEY` constraint")?
            }
            DataType::I16 | DataType::I32 | DataType::I64 if options.is_primary_key() => {
                // Sqlite doesn't need auto increment for integer primary key, since it's already auto incremented
                // in nature
                options = options.unset_auto_increment();
                Cow::Borrowed("INTEGER")
            }
            DataType::I32 => Cow::Borrowed("INTEGER"),
            DataType::I64 => Cow::Borrowed("INTEGER"),
            DataType::I16 => Cow::Borrowed("INTEGER"),
            DataType::I32 => Cow::Borrowed("INTEGER"),
            DataType::I64 => Cow::Borrowed("INTEGER"),
            DataType::F32 => Cow::Borrowed("FLOAT"),
            DataType::F64 => Cow::Borrowed("DOUBLE"),
            DataType::Bool => Cow::Borrowed("BOOLEAN"),
            DataType::String => Cow::Borrowed("TEXT"),
            DataType::Char(len) => Cow::Owned(format!("CHAR({len})")),
            DataType::VarChar(len) => Cow::Owned(format!("VARCHAR({len})")),
            DataType::Bytes => Cow::Borrowed("BLOB"),
            DataType::JSON => Cow::Borrowed("JSON"),
            DataType::UUID => Cow::Borrowed("UUID"),
            DataType::Decimal{precision, scale} => {
                Cow::Owned(format!("DECIMAL({precision}, {scale})"))
            }
            DataType::Date => Cow::Borrowed("TEXT"),
            DataType::Time => Cow::Borrowed("TEXT"),
            DataType::Timestamp => Cow::Borrowed("TEXT"),
        };
        buf.write_all(spec.as_bytes())?;
        let options = options
            .into_iter()
            .map(|option| match option {
                ColumnOption::PrimaryKey => "PRIMARY KEY",
                ColumnOption::AutoInrement => "AUTOINCREMENT",
                ColumnOption::NotNull => "NOT NULL",
                ColumnOption::Nullable => "NULL",
                ColumnOption::Unique => "UNIQUE",
            })
            .collect::<Vec<_>>()
            .join(" ");
        if !options.is_empty() {
            buf.write_all(b" ")?;
            buf.write_all(options.as_bytes())?;
        }
        Ok(())
    }
}