#![allow(unused)]

use std::{
    any::Any,
    borrow::Cow,
    io::{Cursor, Write},
    ops::Deref,
    path::Display,
    slice,
};

use sqlparser::{
    ast::{
        Assignment, AssignmentTarget, BinaryOperator, CastKind, CharacterLength, ColumnDef,
        ColumnOptionDef, CreateIndex, CreateTable as SqlParserCreateTable, CreateTableOptions,
        Delete, ExactNumberInfo, Expr, FromTable, FunctionArguments, HiveDistributionStyle,
        HiveFormat, Ident, IndexColumn, JoinConstraint, ObjectName, ObjectNamePart, ObjectType,
        OrderByExpr, Query, ReferentialAction, SelectItem, SetExpr, SqliteOnConflict, Statement,
        Table, TableConstraint, TableFactor, TableWithJoins, UpdateTableFromKind, Value,
        ValueWithSpan,
    },
    dialect::{self, Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect},
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, Word},
};

use crate::{Error, Result};

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
    Json,
    Uuid,
    Decimal { precision: u64, scale: i64 },
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
            sqlparser::ast::DataType::JSON => DataType::Json,
            sqlparser::ast::DataType::Uuid => DataType::Uuid,
            sqlparser::ast::DataType::Decimal(ExactNumberInfo::PrecisionAndScale(
                precision,
                scale,
            )) => DataType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            sqlparser::ast::DataType::Numeric(ExactNumberInfo::PrecisionAndScale(
                precision,
                scale,
            )) => DataType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            sqlparser::ast::DataType::Custom(ObjectName(name_parts), _) => {
                match extract_serial(name_parts) {
                    Some(dt) => dt,
                    None => Err(Error::DataType {
                        data_type: Box::new(value.clone()),
                    })?,
                }
            }
            sqlparser::ast::DataType::Date => DataType::Date,
            sqlparser::ast::DataType::Time(_, _) => DataType::Time,
            sqlparser::ast::DataType::Timestamp(_, _) => DataType::Timestamp,
            sqlparser::ast::DataType::Datetime(_) => DataType::Timestamp,
            _ => Err(Error::DataType {
                data_type: Box::new(value.clone()),
            })?,
        };
        Ok(dt)
    }
}

fn extract_serial(name_parts: &[ObjectNamePart]) -> Option<DataType> {
    let name = match name_parts.first() {
        None => return None,
        Some(ObjectNamePart::Function(_)) => return None,
        Some(ObjectNamePart::Identifier(name)) => name,
    };
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

impl TryFrom<&ColumnDef> for Column {
    type Error = Error;

    fn try_from(value: &ColumnDef) -> std::result::Result<Self, Self::Error> {
        let ColumnDef {
            name,
            data_type,
            options,
        } = value;
        Ok(Self {
            name: name.value.clone(),
            data_type: data_type.try_into()?,
            options: ColumnOptions::try_from(options.as_slice())?,
        })
    }
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

    #[allow(clippy::type_complexity)]
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
            write!(f, "{option:?} ")?;
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
                    option => Err(Error::ColumnOption {
                        option: Box::new(option.clone()),
                    })?,
                };
                Ok(options)
            },
        )
    }
}

fn is_auto_increment_option(option: &sqlparser::ast::ColumnOption) -> bool {
    match option {
        sqlparser::ast::ColumnOption::DialectSpecific(tokens) if tokens.len() == 1 => tokens
            .first()
            .map(|token| match token {
                Token::Word(Word { keyword, .. }) => {
                    *keyword == Keyword::AUTOINCREMENT || *keyword == Keyword::AUTO_INCREMENT
                }
                _ => false,
            })
            .unwrap(),
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    PrimaryKey(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        referred_columns: Vec<String>,
        foreign_table: String,
        on_delete: Option<OnDeleteAction>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnDeleteAction {
    Cascade,
    SetNull,
    Restrict,
}

impl TryFrom<&ReferentialAction> for OnDeleteAction {
    type Error = Error;

    fn try_from(value: &ReferentialAction) -> Result<Self, Self::Error> {
        match value {
            ReferentialAction::Cascade => Ok(OnDeleteAction::Cascade),
            ReferentialAction::Restrict => Ok(OnDeleteAction::Restrict),
            ReferentialAction::SetNull => Ok(OnDeleteAction::SetNull),
            other => Err(Error::OnDeleteConstrait {
                referential_action: *other,
            })?,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Constraints(Vec<Constraint>);

impl Constraints {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
pub struct ConstraintsIter<'a> {
    constraints: &'a Constraints,
    pos: usize,
}

impl<'a> Iterator for ConstraintsIter<'a> {
    type Item = &'a Constraint;

    fn next(&mut self) -> Option<Self::Item> {
        if self.constraints.0.len() > self.pos {
            let item = &self.constraints.0[self.pos];
            self.pos += 1;
            return Some(item);
        }
        None
    }
}

impl<'a> IntoIterator for &'a Constraints {
    type IntoIter = ConstraintsIter<'a>;
    type Item = &'a Constraint;

    fn into_iter(self) -> Self::IntoIter {
        ConstraintsIter {
            pos: 0,
            constraints: self,
        }
    }
}

impl TryFrom<&[TableConstraint]> for Constraints {
    type Error = Error;

    fn try_from(value: &[TableConstraint]) -> Result<Self, Self::Error> {
        let constraints = value
            .iter()
            .map(|constraint| -> Result<_> {
                let res = match constraint {
                    TableConstraint::PrimaryKey {
                        columns,
                        name,
                        index_name,
                        index_type,
                        index_options,
                        characteristics,
                    } => {
                        if name.is_some() {
                            Err(Error::PrimaryKey { reason: "name" })?
                        }
                        if index_name.is_some() {
                            Err(Error::PrimaryKey {
                                reason: "index name",
                            })?
                        }
                        if index_type.is_some() {
                            Err(Error::PrimaryKey {
                                reason: "index type",
                            })?
                        }
                        if !index_options.is_empty() {
                            Err(Error::PrimaryKey {
                                reason: "index options",
                            })?
                        }
                        if characteristics.is_some() {
                            Err(Error::PrimaryKey {
                                reason: "characteristics",
                            })?
                        }
                        let columns = columns
                            .iter()
                            .map(
                                |IndexColumn {
                                     column,
                                     operator_class,
                                 }| {
                                    if operator_class.is_some() {
                                        Err(Error::PrimaryKey {
                                            reason: "operator class",
                                        })?
                                    };
                                    let OrderByExpr {
                                        expr,
                                        options,
                                        with_fill,
                                    } = column;
                                    if with_fill.is_some() {
                                        Err(Error::PrimaryKey {
                                            reason: "`WITH FILL`",
                                        })?
                                    }
                                    if options.nulls_first.is_some() || options.asc.is_some() {
                                        Err(Error::PrimaryKey { reason: "options" })?
                                    }
                                    match expr {
                                        Expr::Identifier(ident) => Ok(ident.value.clone()),
                                        _ => Err(Error::PrimaryKeyWithExpression {
                                            expr: Box::new(expr.clone()),
                                        })?,
                                    }
                                },
                            )
                            .collect::<Result<Vec<_>>>()?;
                        Constraint::PrimaryKey(columns)
                    }
                    TableConstraint::ForeignKey {
                        name,
                        columns,
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        characteristics,
                        index_name,
                    } => {
                        if name.is_some() {
                            Err(Error::ForeignKey {
                                reason: "constraint",
                            })?
                        }
                        if on_update.is_some() {
                            Err(Error::ForeignKey {
                                reason: "on update",
                            })?
                        }
                        if characteristics.is_some() {
                            Err(Error::ForeignKey {
                                reason: "charecteristics",
                            })?
                        }
                        let on_delete = match on_delete {
                            None => None,
                            Some(action) => Some(action.try_into()?),
                        };
                        let columns = columns
                            .iter()
                            .map(|Ident { value, .. }| value.clone())
                            .collect();
                        let referred_columns = referred_columns
                            .iter()
                            .map(|Ident { value, .. }| value.clone())
                            .collect();
                        let foreign_table = Ast::parse_object_name(foreign_table)?;
                        Constraint::ForeignKey {
                            columns,
                            referred_columns,
                            foreign_table,
                            on_delete,
                        }
                    }
                    _ => Err(Error::TableConstraint {
                        constraint: Box::new(constraint.clone()),
                    })?,
                };
                Ok(res)
            })
            .collect::<Result<Vec<Constraint>, _>>()?;
        Ok(Constraints(constraints))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    CreateTable {
        if_not_exists: bool,
        name: String,
        columns: Vec<Column>,
        constraints: Constraints,
    },
    AlterTable {
        name: String,
        operation: AlterTableOperation,
    },
    CreateIndex {
        unique: bool,
        name: String,
        table: String,
        columns: Vec<String>,
    },
    Select {
        distinct: bool,
        projections: Vec<Projection>,
        from_clause: FromClause,
        selection: Option<Selection>,
        group_by: Vec<GroupByParameter>,
        order_by: Vec<OrderByParameter>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        source: Vec<Vec<InsertSource>>,
    },
    Update {
        table: String,
        assignments: Vec<UpdateAssignment>,
        selection: Option<Selection>,
    },
    Delete {
        from_clause: FromClause,
        selection: Option<Selection>,
    },
    Drop {
        object_type: DropObjectType,
        if_exists: bool,
        name: String,
        table: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    WildCard,
    Identifier(String),
    CompoundIdentifier(CompoundIdentifier),
    Function(Function),
    NumericLiteral(String),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompoundIdentifier {
    table: String,
    column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Function {
    Count(FunctionArg),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    Wildcard,
    Ident(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    BinaryOp {
        op: Op,
        left: Box<Selection>,
        right: Box<Selection>,
    },
    Ident(String),
    CompoundIdentifier(CompoundIdentifier),
    Number(String),
    String(String),
    Placeholder,
    InList {
        negated: bool,
        ident: String,
        list: Vec<Selection>,
    },
}

impl TryFrom<&Expr> for Selection {
    type Error = Error;

    fn try_from(expr: &Expr) -> std::result::Result<Self, Self::Error> {
        let selection = match expr {
            Expr::BinaryOp { left, op, right } => Selection::BinaryOp {
                op: op.try_into()?,
                left: {
                    let left: Selection = left.as_ref().try_into()?;
                    Box::new(left)
                },
                right: {
                    let right: Selection = right.as_ref().try_into()?;
                    Box::new(right)
                },
            },
            Expr::Identifier(id) => Selection::Ident(id.value.clone()),
            Expr::CompoundIdentifier(ids) => {
                // SQLite only supports table.column, not schema.table.column or database.table.column
                if ids.len() != 2 {
                    Err(Error::CompoundIdentifier { length: ids.len() })?
                }
                Selection::CompoundIdentifier(CompoundIdentifier {
                    table: ids[0].value.clone(),
                    column: ids[1].value.clone(),
                })
            }
            Expr::Value(value) => match &value.value {
                Value::Number(number, _) => Selection::Number(number.clone()),
                Value::SingleQuotedString(string) => Selection::String(string.clone()),
                Value::Placeholder(_) => Selection::Placeholder,
                _ => Err(Error::SelectionValue {
                    value: Box::new(value.value.clone()),
                })?,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let ident = match expr.as_ref().try_into()? {
                    Selection::Ident(ident) => ident,
                    selection => Err(Error::Selection {
                        selection: Box::new(selection.clone()),
                        r#where: None,
                    })?,
                };
                let list = list
                    .iter()
                    .map(|expr| {
                        let ok = match expr.try_into()? {
                            ok @ Selection::String(_) => ok,
                            ok @ Selection::Number(_) => ok,
                            ok @ Selection::Placeholder => ok,
                            selection => Err(Error::Selection {
                                selection: Box::new(selection.clone()),
                                r#where: Some("InList"),
                            })?,
                        };
                        Ok(ok)
                    })
                    .collect::<Result<Vec<Selection>>>()?;
                Selection::InList {
                    negated: *negated,
                    ident,
                    list,
                }
            }
            expr => Err(Error::SelectionFromExpr { expr: Box::new(expr.clone()) })?,
        };
        Ok(selection)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupByParameter {
    Ident(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByParameter {
    ident: String,
    option: OrderOption,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderOption {
    Asc,
    Desc,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    String(String),
    Number(String),
    Null,
    Placeholder,
    Cast {
        cast: String,
        source: Box<InsertSource>,
    },
}

impl TryFrom<&Expr> for InsertSource {
    type Error = Error;

    fn try_from(value: &Expr) -> Result<Self, Self::Error> {
        let value = match value {
            Expr::Value(value) => &value.value,
            Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } if *kind == CastKind::DoubleColon && format.is_none() => match expr.as_ref() {
                Expr::Value(_) => {
                    return Ok(InsertSource::Cast {
                        cast: data_type.to_string(),
                        source: Box::new(expr.as_ref().try_into()?),
                    });
                }
                _ => Err(Error::InsertSourceExpression {
                    expr: expr.clone(),
                })?,
            },
            value => Err(Error::InsertSourceExpression {
                expr: Box::new(value.clone()),
            })?,
        };
        let insert_source = match value {
            Value::Null => InsertSource::Null,
            Value::Number(number, _) => InsertSource::Number(number.clone()),
            Value::SingleQuotedString(string) => InsertSource::String(string.clone()),
            Value::Placeholder(_) => InsertSource::Placeholder,
            value => Err(Error::InsertSourceValue {
                value: Box::new(value.clone()),
            })?,
        };
        Ok(insert_source)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAssignment {
    target: String,
    value: UpdateValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateValue {
    String(String),
    Number(String),
    Null,
    Placeholder,
}

impl TryFrom<&Expr> for UpdateValue {
    type Error = Error;

    fn try_from(expr: &Expr) -> Result<Self, Self::Error> {
        let value = match expr {
            Expr::Value(value) => &value.value,
            expr => Err(Error::UpdateExpression { expr: Box::new(expr.clone()) })?,
        };
        let update_value = match value {
            Value::Null => UpdateValue::Null,
            Value::Number(number, _) => UpdateValue::Number(number.clone()),
            Value::SingleQuotedString(string) => UpdateValue::String(string.clone()),
            Value::Placeholder(_) => UpdateValue::Placeholder,
            value => Err(Error::UpdateValue {
                value: Box::new(value.clone()),
            })?,
        };
        Ok(update_value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Eq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DropObjectType {
    Table,
    Index,
}

impl TryFrom<&BinaryOperator> for Op {
    type Error = Error;

    fn try_from(op: &BinaryOperator) -> std::result::Result<Self, Self::Error> {
        let op = match op {
            BinaryOperator::And => Op::And,
            BinaryOperator::Eq => Op::Eq,
            BinaryOperator::Or => Op::Or,
            _ => Err(Error::BinaryOperator { op: op.clone() })?,
        };
        Ok(op)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table(String),
    TableWithJoin(TableJoin),
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableJoin {
    name: String,
    join: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    name: String,
    operator: JoinOperator,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinOperator {
    Join(Selection),
    Inner(Selection),
}

impl TryFrom<&sqlparser::ast::Join> for Join {
    type Error = Error;

    fn try_from(table: &sqlparser::ast::Join) -> Result<Self, Self::Error> {
        /// ClickHouse supports the optional `GLOBAL` keyword before the join operator.
        /// See [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/join)
        if table.global {
            Err(Error::Keyword { keyword: "GLOBAL" })?
        };

        let name = table_relation_to_object_name(&table.relation)?;
        let operator = match &table.join_operator {
            sqlparser::ast::JoinOperator::Join(constraint) => match constraint {
                JoinConstraint::On(expr) => JoinOperator::Join(expr.try_into()?),
                other => Err(Error::JoinConstraint {
                    constraint: Box::new(other.clone()),
                })?,
            },
            sqlparser::ast::JoinOperator::Inner(constraint) => match constraint {
                JoinConstraint::On(expr) => JoinOperator::Inner(expr.try_into()?),
                other => Err(Error::JoinConstraint {
                    constraint: Box::new(other.clone()),
                })?,
            },
            other => Err(Error::JoinOperator { op: Box::new(other.clone()) })?,
        };

        Ok(Self { name, operator })
    }
}

fn table_relation_to_object_name(relation: &TableFactor) -> Result<String> {
    match relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            if alias.is_some() {
                Err(Error::TableAlias)?
            }
            if args.is_some() {
                Err(Error::TableValuedFunctions)?
            }
            if !with_hints.is_empty() {
                Err(Error::TableWithHints)?
            }
            if version.is_some() {
                Err(Error::TableVersioning)?
            }
            if *with_ordinality {
                Err(Error::TableWithOrdinality)?
            }
            if !partitions.is_empty() {
                Err(Error::TableWithPartitions)?
            }
            if json_path.is_some() {
                Err(Error::TableWithJsonPath)?
            }
            if sample.is_some() {
                Err(Error::TableWithSampleModifier)?
            }
            if !index_hints.is_empty() {
                Err(Error::TableWithIndexHints)?
            }
            Ok(Ast::parse_object_name(name)?)
        }
        other => Err(Error::TableFactor {
            factor: Box::new(other.clone()),
        })?,
    }
}

impl TryFrom<&[TableWithJoins]> for FromClause {
    type Error = Error;

    fn try_from(tables: &[TableWithJoins]) -> Result<Self, Self::Error> {
        let from = match tables {
            &[
                TableWithJoins {
                    ref relation,
                    ref joins,
                },
            ] => {
                let name = table_relation_to_object_name(relation)?;
                if joins.is_empty() {
                    Self::Table(name)
                } else {
                    Self::TableWithJoin(TableJoin {
                        name,
                        join: joins.iter().map(|t| t.try_into()).collect::<Result<_>>()?,
                    })
                }
            }
            &[] => Self::None,
            other => Err(Error::TableJoins {
                table_joins: other.iter().map(Clone::clone).collect(),
            })?,
        };
        Ok(from)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn { column: Column },
    RenameColumn { from: String, to: String },
    DropColumn { name: String },
    RenameTable { to: String },
}

impl TryFrom<&sqlparser::ast::AlterTableOperation> for AlterTableOperation {
    type Error = Error;

    fn try_from(
        op: &sqlparser::ast::AlterTableOperation,
    ) -> std::result::Result<Self, Self::Error> {
        let op = match op {
            sqlparser::ast::AlterTableOperation::AddColumn {
                column_keyword,
                if_not_exists,
                column_def,
                column_position,
            } => {
                let _ = column_keyword;
                if *if_not_exists {
                    Err(Error::AlterTable {
                        reason: "if not exists",
                    })?
                }
                if column_position.is_some() {
                    Err(Error::AlterTable {
                        reason: "column position",
                    })?
                }
                let column = column_def.try_into()?;
                AlterTableOperation::AddColumn { column }
            }
            sqlparser::ast::AlterTableOperation::RenameTable { table_name } => {
                let table_name = match table_name {
                    // only mysql
                    sqlparser::ast::RenameTableNameKind::As(_) => Err(Error::AlterTable {
                        reason: "as keyword",
                    })?,
                    sqlparser::ast::RenameTableNameKind::To(table_name) => table_name,
                };
                AlterTableOperation::RenameTable {
                    to: Ast::parse_object_name(table_name)?,
                }
            }
            sqlparser::ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => AlterTableOperation::RenameColumn {
                from: old_column_name.value.clone(),
                to: new_column_name.value.clone(),
            },
            sqlparser::ast::AlterTableOperation::DropColumn {
                if_exists,
                drop_behavior,
                has_column_keyword,
                column_names,
            } => {
                if *if_exists {
                    Err(Error::AlterTable {
                        reason: "if exists",
                    })?
                }
                if drop_behavior.is_some() {
                    Err(Error::AlterTable {
                        reason: "drop behaviour",
                    })?;
                }
                if column_names.len() > 1 {
                    Err(Error::AlterTable {
                        reason: "multiple columns names",
                    })?
                }
                AlterTableOperation::DropColumn {
                    name: column_names.first().unwrap().value.clone(),
                }
            }
            op => Err(Error::AlterTableOp { op: Box::new(op.clone()) })?,
        };
        Ok(op)
    }
}

impl Ast {
    fn parse_object_name(name: &ObjectName) -> Result<String> {
        let name_parts = &name.0;
        if name_parts.len() > 1 {
            Err(Error::ObjectName {
                reason: "schema-qualified names are not supported",
            })?
        }
        match name_parts.first() {
            None => Err(Error::ObjectName {
                reason: "name parts are empty",
            })?,
            Some(ObjectNamePart::Identifier(ident)) => Ok(ident.value.clone()),
            Some(ObjectNamePart::Function(_)) => Err(Error::ObjectName {
                reason: "function names are not supported",
            })?,
        }
    }

    fn parse_create_table(
        SqlParserCreateTable {
            or_replace,
            temporary,
            external,
            global,
            if_not_exists,
            transient,
            volatile,
            iceberg,
            name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            file_format,
            location,
            query,
            without_rowid,
            like,
            clone,
            comment,
            on_commit,
            on_cluster,
            primary_key,
            order_by,
            partition_by,
            cluster_by,
            clustered_by,
            inherits,
            strict,
            copy_grants,
            enable_schema_evolution,
            change_tracking,
            data_retention_time_in_days,
            max_data_extension_time_in_days,
            default_ddl_collation,
            with_aggregation_policy,
            with_row_access_policy,
            with_tags,
            external_volume,
            base_location,
            catalog,
            catalog_sync,
            storage_serialization_policy,
            dynamic,
            table_options,
            version,
            target_lag,
            warehouse,
            refresh_mode,
            initialize,
            require_user,
        }: &SqlParserCreateTable,
    ) -> Result<Ast> {
        if *or_replace {
            Err(Error::CreateTable {
                reason: "or replace",
            })?
        }
        if *temporary {
            Err(Error::CreateTable {
                reason: "temporary",
            })?
        }
        if *external {
            Err(Error::CreateTable { reason: "external" })?
        }
        if global.is_some() {
            Err(Error::CreateTable { reason: "global" })?
        }
        if *transient {
            Err(Error::CreateTable {
                reason: "transient",
            })?
        }
        if *volatile {
            Err(Error::CreateTable { reason: "volatile" })?
        }
        if *iceberg {
            Err(Error::CreateTable { reason: "iceberg" })?
        }
        match hive_distribution {
            HiveDistributionStyle::NONE => {}
            _ => Err(Error::CreateTable {
                reason: "hive distribution style",
            })?,
        }

        // Hive formats for some reason are always Some()
        if let Some(HiveFormat {
            row_format,
            serde_properties,
            storage,
            location,
        }) = hive_formats
            && (row_format.is_some()
                || serde_properties.is_some()
                || storage.is_some()
                || location.is_some())
        {
            Err(Error::CreateTable {
                reason: "hive formats",
            })?
        }

        if file_format.is_some() {
            Err(Error::CreateTable {
                reason: "file format",
            })?
        }
        if location.is_some() {
            Err(Error::CreateTable { reason: "location" })?
        }
        if query.is_some() {
            Err(Error::CreateTable { reason: "query" })?
        }
        if *without_rowid {
            Err(Error::CreateTable {
                reason: "without rowid",
            })?
        }
        if like.is_some() {
            Err(Error::CreateTable { reason: "like" })?
        }
        if clone.is_some() {
            Err(Error::CreateTable { reason: "clone" })?
        }
        if comment.is_some() {
            Err(Error::CreateTable { reason: "comment" })?
        }
        if on_commit.is_some() {
            Err(Error::CreateTable {
                reason: "on commit",
            })?
        }
        // ClickHouse "ON CLUSTER" clause:
        if on_cluster.is_some() {
            Err(Error::CreateTable {
                reason: "on cluster",
            })?
        }
        // ClickHouse "PRIMARY KEY " clause.
        if primary_key.is_some() {
            Err(Error::CreateTable {
                reason: "primary key",
            })?
        }
        // ClickHouse "ORDER BY " clause.
        if order_by.is_some() {
            Err(Error::CreateTable { reason: "order by" })?
        }
        // BigQuery: A partition expression for the table.
        if partition_by.is_some() {
            Err(Error::CreateTable {
                reason: "partition by",
            })?
        }
        // BigQuery: Table clustering column list.
        if cluster_by.is_some() {
            Err(Error::CreateTable {
                reason: "cluster by",
            })?
        }
        // Hive: Table clustering column list.
        if clustered_by.is_some() {
            Err(Error::CreateTable {
                reason: "clustered by",
            })?
        }
        // Postgres `INHERITs` clause, which contains the list of tables from which the new table inherits.
        if inherits.is_some() {
            Err(Error::CreateTable { reason: "inherits" })?
        }
        // SQLite "STRICT" clause.
        if *strict {
            Err(Error::CreateTable { reason: "strict" })?
        }
        // Snowflake "COPY GRANTS" clause.
        if *copy_grants {
            Err(Error::CreateTable {
                reason: "copy grant",
            })?
        }
        // Snowflake "ENABLE_SCHEMA_EVOLUTION" clause.
        if enable_schema_evolution.is_some() {
            Err(Error::CreateTable {
                reason: "enable schema evolution",
            })?
        }
        // Snowflake "CHANGE_TRACKING" clause.
        if change_tracking.is_some() {
            Err(Error::CreateTable {
                reason: "change tracking",
            })?
        }
        // Snowflake "DATA_RETENTION_TIME_IN_DAYS" clause.
        if data_retention_time_in_days.is_some() {
            Err(Error::CreateTable {
                reason: "data retention time in days",
            })?
        }
        // Snowflake "MAX_DATA_EXTENSION_TIME_IN_DAYS" clause.
        if max_data_extension_time_in_days.is_some() {
            Err(Error::CreateTable {
                reason: "max data extension time in days",
            })?
        }
        // Snowflake "DEFAULT_DDL_COLLATION" clause.
        if default_ddl_collation.is_some() {
            Err(Error::CreateTable {
                reason: "default ddl collation",
            })?
        }
        // Snowflake "WITH AGGREGATION POLICY" clause.
        if with_aggregation_policy.is_some() {
            Err(Error::CreateTable {
                reason: "with aggragation policy",
            })?
        }
        // Snowflake "WITH ROW ACCESS POLICY" clause.
        if with_row_access_policy.is_some() {
            Err(Error::CreateTable {
                reason: "with row access policy",
            })?
        }
        // Snowflake "WITH TAG" clause.
        if with_tags.is_some() {
            Err(Error::CreateTable {
                reason: "with tags",
            })?
        }
        // Snowflake "EXTERNAL_VOLUME" clause for Iceberg tables
        if external_volume.is_some() {
            Err(Error::CreateTable {
                reason: "external volume",
            })?
        }
        // Snowflake "BASE_LOCATION" clause for Iceberg tables
        if base_location.is_some() {
            Err(Error::CreateTable {
                reason: "base location",
            })?
        }
        // Snowflake "CATALOG" clause for Iceberg tables
        if catalog.is_some() {
            Err(Error::CreateTable { reason: "catalog" })?
        }
        // Snowflake "CATALOG_SYNC" clause for Iceberg tables
        if catalog_sync.is_some() {
            Err(Error::CreateTable {
                reason: "catalog sync",
            })?
        }
        // Snowflake "STORAGE_SERIALIZATION_POLICY" clause for Iceberg tables
        if storage_serialization_policy.is_some() {
            Err(Error::CreateTable {
                reason: "storage serialization policy",
            })?
        }
        if *dynamic {
            Err(Error::CreateTable { reason: "dynamic" })?
        }
        match table_options {
            CreateTableOptions::None => (),
            _ => Err(Error::CreateTable {
                reason: "table options",
            })?,
        };

        if version.is_some() {
            Err(Error::CreateTable { reason: "versions" })?
        }

        // Snowflake "TARGET_LAG" clause for dybamic tables
        if target_lag.is_some() {
            Err(Error::CreateTable {
                reason: "target lag",
            })?
        }
        // Snowflake "WAREHOUSE" clause for dybamic tables
        if warehouse.is_some() {
            Err(Error::CreateTable {
                reason: "warehouse",
            })?
        }
        // Snowflake "REFRESH_MODE" clause for dybamic tables
        if refresh_mode.is_some() {
            Err(Error::CreateTable {
                reason: "refresh mode",
            })?
        }
        // Snowflake "INITIALIZE" clause for dybamic tables
        if initialize.is_some() {
            Err(Error::CreateTable {
                reason: "initialize",
            })?
        }
        // Snowflake "REQUIRE USER" clause for dybamic tables
        if *require_user {
            Err(Error::CreateTable {
                reason: "require user",
            })?
        }

        let name = Self::parse_object_name(name)?;
        let columns = {
            columns
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        Ok(Ast::CreateTable {
            if_not_exists: *if_not_exists,
            name,
            columns,
            constraints: Constraints::try_from(constraints.as_slice())?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_alter_table(
        name: &ObjectName,
        if_exists: bool,
        only: bool,
        operations: &[sqlparser::ast::AlterTableOperation],
        location: Option<&sqlparser::ast::HiveSetLocation>,
        on_cluster: Option<&Ident>,
        iceberg: bool,
        _end_token: &sqlparser::ast::helpers::attached_token::AttachedToken,
    ) -> Result<Ast> {
        // sqlite doesn't support if exists in alter
        if if_exists {
            Err(Error::AlterTable {
                reason: "if exists",
            })?
        }
        // sqlite doesn't support `ON` clause in alter table
        if only {
            Err(Error::AlterTable { reason: "on" })?
        }
        // clickhouse syntax
        if on_cluster.is_some() {
            Err(Error::AlterTable {
                reason: "on cluster",
            })?
        }
        // hive syntax
        if location.is_some() {
            Err(Error::AlterTable { reason: "location" })?
        }
        // iceberg syntax
        if iceberg {
            Err(Error::AlterTable { reason: "iceberg" })?
        }
        let name = Self::parse_object_name(name)?;
        if operations.len() != 1 {
            Err(Error::AlterTable {
                reason: "only supports single operation",
            })?
        }
        let operation = operations.first().unwrap().try_into()?;
        Ok(Ast::AlterTable { name, operation })
    }

    fn parse_function_args(args: &FunctionArguments) -> Result<Vec<FunctionArg>> {
        let args = match args {
            FunctionArguments::None => vec![],
            FunctionArguments::List(list) => {
                if !list.clauses.is_empty() {
                    Err(Error::FunctionArguments {
                        reason: "function clauses are not yet supported",
                        arguments: Box::new(args.clone()),
                    })?
                };
                if list.duplicate_treatment.is_some() {
                    Err(Error::FunctionArguments {
                        reason: "function duplicate treatment not supported",
                        arguments: Box::new(args.clone()),
                    })?
                }
                list.args
                    .iter()
                    .map(|arg| -> Result<_> {
                        // FIXME: move to TryFrom
                        let arg = match arg {
                            sqlparser::ast::FunctionArg::ExprNamed { .. } => {
                                Err(Error::FunctionArgument {
                                    reason: "named expressions",
                                    argument: Box::new(arg.clone()),
                                })?
                            }
                            sqlparser::ast::FunctionArg::Named { .. } => {
                                Err(Error::FunctionArgument {
                                    reason: "named columns",
                                    argument: Box::new(arg.clone()),
                                })?
                            }
                            sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                                sqlparser::ast::FunctionArgExpr::Wildcard => FunctionArg::Wildcard,
                                sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(ident)) => {
                                    FunctionArg::Ident(ident.value.clone())
                                }
                                _ => Err(Error::FunctionArgument {
                                    reason: "unnamed",
                                    argument: Box::new(arg.clone()),
                                })?,
                            },
                        };
                        Ok(arg)
                    })
                    .collect::<Result<_>>()?
            }
            FunctionArguments::Subquery(query) => Err(Error::FunctionArguments {
                reason: "subquery",
                arguments: Box::new(args.clone()),
            })?,
        };
        Ok(args)
    }

    fn parse_create_index(
        CreateIndex {
            name,
            table_name,
            columns,
            if_not_exists,
            unique,
            concurrently,
            using,
            include,
            nulls_distinct,
            with,
            predicate,
            index_options,
            alter_options,
        }: &CreateIndex,
    ) -> Result<Self> {
        if *if_not_exists {
            Err(Error::CreateIndex {
                reason: "existance check",
            })?
        };
        if name.is_none() {
            Err(Error::CreateIndex { reason: "nameless" })?
        }
        if *concurrently {
            Err(Error::CreateIndex {
                reason: "concurrent",
            })?
        }
        if using.is_some() {
            Err(Error::CreateIndex { reason: "using" })?
        }
        if !include.is_empty() {
            Err(Error::CreateIndex { reason: "include" })?
        }
        if nulls_distinct.is_some() {
            Err(Error::CreateIndex {
                reason: "distinct nulls",
            })?
        }
        if !with.is_empty() {
            Err(Error::CreateIndex { reason: "with" })?
        }
        if predicate.is_some() {
            Err(Error::CreateIndex {
                reason: "predicate",
            })?
        }
        // PG only
        if !index_options.is_empty() {
            Err(Error::CreateIndex {
                reason: "index options",
            })?
        }
        // Mysql only
        if !alter_options.is_empty() {
            Err(Error::CreateIndex {
                reason: "alter options",
            })?
        }
        let columns = columns
            .iter()
            .map(
                |index_column @ IndexColumn { column, .. }| -> Result<String> {
                    match &column.expr {
                        Expr::Identifier(Ident { value, .. }) => Ok(value.clone()),
                        expr => Err(Error::CreateIndexColumn {
                            column: Box::new(index_column.clone()),
                        })?,
                    }
                },
            )
            .collect::<Result<Vec<String>>>()?;
        Ok(Ast::CreateIndex {
            unique: *unique,
            name: Self::parse_object_name(name.as_ref().unwrap())?,
            table: Self::parse_object_name(table_name)?,
            columns,
        })
    }

    fn parse_query(query: &Query) -> Result<Ast> {
        // FIXME:
        if query.with.is_some() {
            Err(Error::CTE)?
        }
        if query.fetch.is_some() {
            Err(Error::Fetch)?
        }
        // FIXME:
        if query.limit_clause.is_some() {
            Err(Error::Limit)?
        }
        if !query.locks.is_empty() {
            Err(Error::Locks)?
        }
        if query.for_clause.is_some() {
            Err(Error::For)?
        }
        let select = match &*query.body {
            SetExpr::Select(select) => &**select,
            other => Err(Error::Select {
                set_expr: Box::new(other.clone()),
            })?,
        };
        if select.top.is_some() || select.top_before_distinct {
            return Err(Error::Top)?;
        }
        let projections = select
            .projection
            .iter()
            .map(|projection| -> Result<_> {
                match projection {
                    SelectItem::Wildcard(_) => Ok(Projection::WildCard),
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        Ok(Projection::Identifier(ident.value.clone()))
                    }
                    SelectItem::UnnamedExpr(Expr::Function(function)) => {
                        let function_name = Self::parse_object_name(&function.name)?.to_lowercase();
                        match function_name.as_str() {
                            "count" => {
                                let mut args = Self::parse_function_args(&function.args)?;
                                if args.len() != 1 {
                                    Err(Error::Count {
                                        reason: "function can only have single argument",
                                        args: args.clone(),
                                    })?
                                }
                                let arg = args.pop().unwrap();
                                Ok(Projection::Function(Function::Count(arg)))
                            }
                            name => Err(Error::Function {
                                name: function_name,
                            })?,
                        }
                    }
                    SelectItem::UnnamedExpr(Expr::Value(value)) => match &value.value {
                        Value::Number(value, _) => Ok(Projection::NumericLiteral(value.clone())),
                        Value::SingleQuotedString(value) => Ok(Projection::String(value.clone())),
                        value => Err(Error::SelectionValue {
                            value: Box::new(value.clone()),
                        })?,
                    },
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(values)) => {
                        // SQLite only supports table.column, not schema.table.column or database.table.column
                        if values.len() != 2 {
                            Err(Error::CompoundIdentifier {
                                length: values.len(),
                            })?
                        }
                        Ok(Projection::CompoundIdentifier(CompoundIdentifier {
                            table: values[0].value.clone(),
                            column: values[1].value.clone(),
                        }))
                    }
                    _ => Err(Error::Projection {
                        select_item: Box::new(projection.clone()),
                    })?,
                }
            })
            .collect::<Result<Vec<Projection>>>()?;

        let from_clause = select.from.as_slice().try_into()?;

        let selection = select
            .selection
            .as_ref()
            .map(|selection| selection.try_into())
            .transpose()?;
        let group_by = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(expr, modifier) if modifier.is_empty() => expr
                .iter()
                .map(|expr| match expr {
                    Expr::Identifier(ident) => Ok(GroupByParameter::Ident(ident.value.clone())),
                    _ => Err(Error::GroupBy {
                        reason: "unsupported expression in group by",
                    })?,
                })
                .collect::<Result<Vec<GroupByParameter>>>()?,
            expr => Err(Error::GroupBy {
                reason: "unsupported group by expression",
            })?,
        };
        let order_by = match query.order_by.as_ref() {
            Some(order_by) => {
                if order_by.interpolate.is_some() {
                    Err(Error::OrderBy {
                        reason: "order by interpolate is not supported",
                    })?;
                };
                match &order_by.kind {
                    sqlparser::ast::OrderByKind::All(_) => Err(Error::OrderBy {
                        reason: "order by all is not supported",
                    })?,
                    sqlparser::ast::OrderByKind::Expressions(expressions) => expressions
                        .iter()
                        .map(|expression| {
                            if expression.with_fill.is_some() {
                                Err(Error::OrderBy {
                                    reason: "with fill is not supported",
                                })?
                            }
                            let ident = match &expression.expr {
                                Expr::Identifier(ident) => ident.value.clone(),
                                expr => Err(Error::OrderBy {
                                    reason: "unsupported order by expression",
                                })?,
                            };
                            let option = match &expression.options {
                                sqlparser::ast::OrderByOptions { nulls_first, .. }
                                    if nulls_first.is_some() =>
                                {
                                    Err(Error::OrderBy {
                                        reason: "order by with nulls first not supported",
                                    })?
                                }
                                sqlparser::ast::OrderByOptions { asc, .. } => match asc {
                                    None => OrderOption::None,
                                    Some(true) => OrderOption::Asc,
                                    Some(false) => OrderOption::Desc,
                                },
                            };
                            Ok(OrderByParameter { ident, option })
                        })
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            None => vec![],
        };
        let ast = Ast::Select {
            distinct: select.distinct.is_some(),
            projections,
            from_clause,
            selection,
            group_by,
            order_by,
        };
        Ok(ast)
    }

    fn parse_insert(insert: &sqlparser::ast::Insert) -> Result<Ast> {
        let sqlparser::ast::Insert {
            or,
            ignore,
            into,
            table,
            table_alias,
            columns,
            overwrite,
            source,
            assignments,
            partitioned,
            after_columns,
            has_table_keyword,
            on,
            returning,
            replace_into,
            priority,
            insert_alias,
            settings,
            format_clause,
        } = insert;
        // FIXME:
        if or.is_some() {
            Err(Error::Insert {
                reason: "insert or not yet supported",
            })?
        };
        // FIXME:
        if *ignore {
            Err(Error::Insert {
                reason: "insert ignore is not yet supported",
            })?
        }
        if !*into {
            Err(Error::Insert {
                reason: "insert without into is not supported",
            })?
        }
        if table_alias.is_some() {
            Err(Error::Insert {
                reason: "table alias in insert it not supported",
            })?
        }
        if *overwrite {
            Err(Error::Insert {
                reason: "overwrite in insert it not supported",
            })?
        }
        if !assignments.is_empty() {
            Err(Error::Insert {
                reason: "insert assignments are not supported",
            })?
        }
        if partitioned.is_some() || !after_columns.is_empty() {
            Err(Error::Insert {
                reason: "partitioned inserts are not supported",
            })?
        }
        if *has_table_keyword {
            Err(Error::Insert {
                reason: "insert doesn't support TABLE keyword",
            })?
        }
        // FIXME:
        if on.is_some() {
            Err(Error::Insert {
                reason: "insert with ON is not supported",
            })?
        }
        if returning.is_some() {
            Err(Error::Insert {
                reason: "insert RETURNING is not supported",
            })?
        }
        if *replace_into {
            Err(Error::Insert {
                reason: "insert with replace into is not supported",
            })?
        }
        if priority.is_some() {
            Err(Error::Insert {
                reason: "insert with priority is not supported",
            })?
        }
        if insert_alias.is_some() {
            Err(Error::Insert {
                reason: "insert with insert alias is not supported",
            })?
        }
        if settings.is_some() {
            Err(Error::Insert {
                reason: "insert with settings is not supported",
            })?
        }
        if format_clause.is_some() {
            Err(Error::Insert {
                reason: "insert with format clause is not supported",
            })?
        }
        let name = match &table {
            sqlparser::ast::TableObject::TableName(name) => Self::parse_object_name(name)?,
            _ => Err(Error::InsertTableObject)?,
        };
        let source = match source {
            Some(source) => source,
            None => Err(Error::InsertSourceEmpty)?,
        };
        Ok(Ast::Insert {
            table: name,
            columns: columns.iter().map(|ident| ident.value.clone()).collect(),
            source: Self::parse_insert_source(source)?,
        })
    }

    fn parse_insert_source(values: &Query) -> Result<Vec<Vec<InsertSource>>> {
        let values = match values.body.as_ref() {
            SetExpr::Values(values) if !values.explicit_row => values
                .rows
                .iter()
                .map(|row| -> Result<Vec<InsertSource>> {
                    row.iter()
                        .map(|value| value.try_into())
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>()?,
            _ => Err(Error::Insert {
                reason: "unsupported insert source values",
            })?,
        };
        Ok(values)
    }

    fn parse_update(
        table: &TableWithJoins,
        assignments: &[Assignment],
        from: Option<&UpdateTableFromKind>,
        selection: Option<&Expr>,
        returning: Option<&[SelectItem]>,
        or: Option<&SqliteOnConflict>,
        limit: Option<&Expr>,
    ) -> Result<Ast> {
        if from.is_some() {
            Err(Error::Update {
                reason: "update from table from kind is not supported",
            })?
        }
        if returning.is_some() {
            Err(Error::Update {
                reason: "update with returning is not supported",
            })?
        }
        if or.is_some() {
            Err(Error::Update {
                reason: "update with OR is not supported",
            })?
        }
        if limit.is_some() {
            Err(Error::Update {
                reason: "update with LIMIT is not supported",
            })?
        }
        let table = match &table.relation {
            TableFactor::Table { name, .. } => Self::parse_object_name(name)?,
            _ => Err(Error::UpdateTableType)?,
        };
        let assignments = assignments
            .iter()
            .map(|assigment| {
                let target = match &assigment.target {
                    AssignmentTarget::ColumnName(name) => Self::parse_object_name(name)?,
                    target => Err(Error::UpdateAssignmentTarget)?,
                };
                let value = (&assigment.value).try_into()?;
                Ok(UpdateAssignment { target, value })
            })
            .collect::<Result<Vec<UpdateAssignment>>>()?;
        let selection: Option<Selection> = selection
            .map(|selection| selection.try_into())
            .transpose()?;
        Ok(Ast::Update {
            table,
            assignments,
            selection,
        })
    }

    fn parse_delete(delete: &Delete) -> Result<Ast> {
        if !delete.tables.is_empty() {
            Err(Error::Delete {
                reason: "multiple tables",
            })?
        }
        if delete.using.is_some() {
            Err(Error::Delete { reason: "using" })?
        }
        if delete.returning.is_some() {
            Err(Error::Delete {
                reason: "returning",
            })?
        }
        if !delete.order_by.is_empty() {
            Err(Error::Delete { reason: "order by" })?
        }
        if delete.limit.is_some() {
            Err(Error::Delete { reason: "limit" })?
        }

        let tables = match &delete.from {
            FromTable::WithFromKeyword(tables) => tables,
            FromTable::WithoutKeyword(_) => Err(Error::Delete {
                reason: "without from",
            })?,
        };
        if tables.len() != 1 {
            Err(Error::Delete {
                reason: "multiple tables",
            })?
        }

        let from_clause = tables.as_slice().try_into()?;

        let selection = delete
            .selection
            .as_ref()
            .map(|selection| selection.try_into())
            .transpose()?;
        Ok(Ast::Delete {
            from_clause,
            selection,
        })
    }

    fn parse_drop(
        object_type: &ObjectType,
        if_exists: bool,
        names: &[ObjectName],
        table: Option<&ObjectName>,
    ) -> Result<Self> {
        let (object_type, table) = match object_type {
            ObjectType::Table => (DropObjectType::Table, None),
            ObjectType::Index => (
                DropObjectType::Index,
                table.map(Self::parse_object_name).transpose()?,
            ),
            _ => Err(Error::DropObjectType {
                object_type: *object_type,
            })?,
        };
        if names.len() > 1 {
            Err(Error::Drop {
                reason: "multiple names are not supported",
            })?
        }
        let name = Self::parse_object_name(names.first().ok_or(Error::Drop {
            reason: "no drop names found",
        })?)?;
        Ok(Ast::Drop {
            object_type,
            if_exists,
            name,
            table,
        })
    }

    pub fn parse(query: &str) -> Result<Vec<Ast>> {
        Parser::parse_sql(&dialect::GenericDialect {}, query)?
            .iter()
            .map(|statement| {
                let result = match statement {
                    Statement::CreateTable(create_table) => Self::parse_create_table(create_table)?,
                    Statement::AlterTable {
                        name,
                        if_exists,
                        only,
                        operations,
                        location,
                        on_cluster,
                        iceberg,
                        end_token,
                    } => Self::parse_alter_table(
                        name,
                        *if_exists,
                        *only,
                        operations.as_slice(),
                        location.as_ref(),
                        on_cluster.as_ref(),
                        *iceberg,
                        end_token,
                    )?,
                    Statement::CreateIndex(index) => Self::parse_create_index(index)?,
                    Statement::Query(query) => Self::parse_query(query)?,
                    Statement::Drop {
                        object_type,
                        if_exists,
                        names,
                        cascade,
                        restrict,
                        purge,
                        temporary,
                        table,
                    } => Self::parse_drop(object_type, *if_exists, names, table.as_ref())?,
                    Statement::Insert(insert) => Self::parse_insert(insert)?,
                    Statement::Update {
                        table,
                        assignments,
                        from,
                        selection,
                        returning,
                        or,
                        limit,
                    } => Self::parse_update(
                        table,
                        assignments.as_slice(),
                        from.as_ref(),
                        selection.as_ref(),
                        returning.as_deref(),
                        or.as_ref(),
                        limit.as_ref(),
                    )?,
                    Statement::Delete(delete) => Self::parse_delete(delete)?,
                    _ => Err(Error::Statement)?,
                };
                Ok(result)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn create_table_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        if_not_exists: bool,
        name: &str,
        columns: &[Column],
        constraints: &Constraints,
    ) -> Result<()> {
        buf.write_all(b"CREATE TABLE ")?;
        if if_not_exists {
            buf.write_all(b"IF NOT EXISTS ")?;
        }
        Self::write_quoted(dialect, buf, name)?;
        buf.write_all(b" (\n")?;
        Self::table_columns_to_sql(dialect, buf, columns, "\n")?;
        if constraints.len() > 0 {
            buf.write_all(b",\n")?;
        }
        for (pos, constraint) in constraints.into_iter().enumerate() {
            match constraint {
                Constraint::PrimaryKey(fields) => {
                    buf.write_all(b"PRIMARY KEY (")?;
                    for (pos, field) in fields.iter().enumerate() {
                        Self::write_quoted(dialect, buf, field)?;
                        if pos != fields.len() - 1 {
                            buf.write_all(b", ")?;
                        }
                    }
                    buf.write_all(b")")?;
                }
                Constraint::ForeignKey {
                    columns,
                    referred_columns,
                    foreign_table,
                    on_delete,
                } => {
                    buf.write_all(b"FOREIGN KEY (")?;
                    for (pos, column) in columns.iter().enumerate() {
                        Self::write_quoted(dialect, buf, column)?;
                        if pos != columns.len() - 1 {
                            buf.write_all(b", ")?;
                        }
                    }
                    buf.write_all(b") REFERENCES ")?;
                    buf.write_all(foreign_table.as_bytes());
                    buf.write_all(b"(")?;
                    for (pos, column) in referred_columns.iter().enumerate() {
                        Self::write_quoted(dialect, buf, column)?;
                        if pos != columns.len() - 1 {
                            buf.write_all(b", ")?;
                        }
                    }
                    buf.write_all(b")")?;

                    if let Some(delete_action) = on_delete {
                        buf.write_all(b" ON DELETE ")?;
                        match delete_action {
                            OnDeleteAction::Cascade => buf.write_all(b"CASCADE")?,
                            OnDeleteAction::SetNull => buf.write_all(b"SET NULL")?,
                            OnDeleteAction::Restrict => buf.write_all(b"RESTRICT")?,
                        }
                    }
                }
            }
            if pos != constraints.len() - 1 {
                buf.write_all(b",\n")?;
            }
        }
        buf.write_all(b"\n)")?;
        Ok(())
    }

    fn table_columns_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        columns: &[Column],
        separator: &str,
    ) -> Result<()> {
        for (pos, column) in columns.iter().enumerate() {
            Self::write_quoted(dialect, buf, &column.name)?;
            buf.write_all(b" ")?;

            dialect.emit_column_spec(column, buf)?;
            if pos != columns.len() - 1 {
                buf.write_all(b",")?;
                buf.write_all(separator.as_bytes())?;
            }
        }
        Ok(())
    }

    fn create_index_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        unique: bool,
        name: &str,
        table: &str,
        columns: &[String],
    ) -> Result<()> {
        if unique {
            buf.write_all(b"CREATE UNIQUE INDEX ")?;
        } else {
            buf.write_all(b"CREATE INDEX ")?;
        }
        Self::write_quoted(dialect, buf, name)?;
        buf.write_all(b" ON ")?;
        Self::write_quoted(dialect, buf, table)?;
        buf.write_all(b" (")?;
        for (pos, column) in columns.iter().enumerate() {
            Self::write_quoted(dialect, buf, column)?;
            if pos != columns.len() - 1 {
                buf.write_all(b", ")?;
            }
        }
        buf.write_all(b")")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn select_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        distinct: bool,
        projections: &[Projection],
        from_clause: &FromClause,
        selection: Option<&Selection>,
        group_by: &[GroupByParameter],
        order_by: &[OrderByParameter],
    ) -> Result<()> {
        buf.write_all(b"SELECT ")?;
        if distinct {
            buf.write_all(b"DISTINCT ")?;
        }
        for (pos, projection) in projections.iter().enumerate() {
            match projection {
                Projection::WildCard => buf.write_all(b"*")?,
                Projection::Identifier(ident) => {
                    Self::write_quoted(dialect, buf, ident)?;
                }
                Projection::CompoundIdentifier(compound) => {
                    Self::write_quoted(dialect, buf, &compound.table)?;
                    buf.write_all(b".");
                    Self::write_quoted(dialect, buf, &compound.column)?;
                }
                Projection::Function(function) => match function {
                    Function::Count(FunctionArg::Wildcard) => buf.write_all(b"COUNT(*)")?,
                    Function::Count(FunctionArg::Ident(ident)) => {
                        buf.write_all(b"COUNT(")?;
                        Self::write_quoted(dialect, buf, ident)?;
                        buf.write_all(b")")?
                    }
                },
                Projection::NumericLiteral(value) => buf.write_all(value.as_bytes())?,
                Projection::String(value) => Self::write_single_quoted(dialect, buf, value)?,
            };
            if pos != projections.len() - 1 {
                buf.write_all(b", ")?;
            }
        }

        match from_clause {
            FromClause::Table(name) => {
                buf.write_all(b" FROM ")?;
                Self::write_quoted(dialect, buf, name)?
            }
            FromClause::None => (),
            FromClause::TableWithJoin(table) => {
                buf.write_all(b" FROM ")?;
                Self::write_quoted(dialect, buf, &table.name)?;
                for table in table.join.iter() {
                    match &table.operator {
                        JoinOperator::Join(selection) => {
                            buf.write_all(b" JOIN ")?;
                            Self::write_quoted(dialect, buf, &table.name);
                            buf.write_all(b" ON ")?;
                            Self::selection_to_sql(dialect, buf, selection)?;
                        }
                        JoinOperator::Inner(selection) => {
                            buf.write_all(b" INNER JOIN ")?;
                            Self::write_quoted(dialect, buf, &table.name);
                            buf.write_all(b" ON ")?;
                            Self::selection_to_sql(dialect, buf, selection)?;
                        }
                    }
                }
            }
        }
        if let Some(selection) = selection.as_ref() {
            buf.write_all(b" WHERE ")?;
            Self::selection_to_sql(dialect, buf, selection)?;
        };

        if !group_by.is_empty() {
            buf.write_all(b" GROUP BY (")?;
            for (pos, parameter) in group_by.iter().enumerate() {
                match parameter {
                    GroupByParameter::Ident(ident) => Self::write_quoted(dialect, buf, ident)?,
                }
                if pos != group_by.len() - 1 {
                    buf.write_all(b", ")?;
                }
            }
            buf.write_all(b")")?;
        }
        if !order_by.is_empty() {
            buf.write_all(b" ORDER BY ")?;
            for (pos, order_option) in order_by.iter().enumerate() {
                Self::write_quoted(dialect, buf, order_option.ident.as_str())?;
                match &order_option.option {
                    OrderOption::Asc => buf.write_all(b" ASC")?,
                    OrderOption::Desc => buf.write_all(b" DESC")?,
                    OrderOption::None => (),
                };
                if pos != order_by.len() - 1 {
                    buf.write_all(b", ")?;
                }
            }
        }
        Ok(())
    }

    fn insert_source_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        insert_source: &InsertSource,
        place_holder_num: usize,
    ) -> Result<()> {
        match insert_source {
            InsertSource::Null => buf.write_all(b"NULL")?,
            InsertSource::Number(num) => buf.write_all(num.as_bytes())?,
            InsertSource::String(string) => Self::write_single_quoted(dialect, buf, string)?,
            InsertSource::Placeholder => {
                buf.write_all(dialect.placeholder(place_holder_num).as_bytes())?;
            }
            InsertSource::Cast { cast, source } => {
                Self::insert_source_to_sql(dialect, buf, source, place_holder_num)?;
                if dialect.placeholder_supports_cast() {
                    buf.write_all(b"::")?;
                    buf.write_all(cast.as_bytes())?;
                }
            }
        };
        Ok(())
    }

    fn insert_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        table: &str,
        columns: &[String],
        values: &[Vec<InsertSource>],
    ) -> Result<()> {
        buf.write_all(b"INSERT INTO ")?;
        Self::write_quoted(dialect, buf, table)?;
        if !columns.is_empty() {
            buf.write_all(b"(")?;
            for (pos, column) in columns.iter().enumerate() {
                if pos != 0 {
                    buf.write_all(b", ")?;
                }
                Self::write_quoted(dialect, buf, column)?;
            }
            buf.write_all(b")")?;
        }
        buf.write_all(b" VALUES ")?;
        for (row_pos, row) in values.iter().enumerate() {
            if row_pos != 0 {
                buf.write_all(b", ")?;
            }
            buf.write_all(b"(")?;
            for (col_pos, insert_source) in row.iter().enumerate() {
                if col_pos != 0 {
                    buf.write_all(b", ")?;
                }
                Self::insert_source_to_sql(
                    dialect,
                    buf,
                    insert_source,
                    row_pos * row.len() + col_pos + 1,
                )?;
            }
            buf.write_all(b")")?;
        }
        Ok(())
    }

    fn selection_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        selection: &Selection,
    ) -> Result<()> {
        let mut placeholder_count = 0;
        Self::selection_to_sql_with_placeholder_count(
            dialect,
            buf,
            selection,
            &mut placeholder_count,
        )
    }

    fn selection_to_sql_with_placeholder_count(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        selection: &Selection,
        placeholder_count: &mut usize,
    ) -> Result<()> {
        match selection {
            Selection::BinaryOp { op, left, right } => {
                Self::selection_to_sql_with_placeholder_count(
                    dialect,
                    buf,
                    left,
                    placeholder_count,
                )?;
                match op {
                    Op::And => buf.write_all(b" AND ")?,
                    Op::Eq => buf.write_all(b" = ")?,
                    Op::Or => buf.write_all(b" OR ")?,
                }
                Self::selection_to_sql_with_placeholder_count(
                    dialect,
                    buf,
                    right,
                    placeholder_count,
                )?;
            }
            Selection::Ident(ident) => Self::write_quoted(dialect, buf, ident)?,
            Selection::CompoundIdentifier(compound) => {
                Self::write_quoted(dialect, buf, &compound.table)?;
                buf.write_all(b".");
                Self::write_quoted(dialect, buf, &compound.column)?;
            }
            Selection::Number(number) => buf.write_all(number.as_bytes())?,
            Selection::String(string) => {
                for chunk in [b"'", string.as_bytes(), b"'"] {
                    buf.write_all(chunk)?;
                }
            }
            Selection::Placeholder => {
                *placeholder_count += 1;
                buf.write_all(dialect.placeholder(*placeholder_count).as_bytes())?;
            }
            Selection::InList {
                negated,
                ident,
                list,
            } => {
                Self::write_quoted(dialect, buf, ident)?;
                if *negated {
                    buf.write_all(b" NOT")?;
                }
                buf.write_all(b" IN ")?;
                buf.write_all(b"(")?;
                for (pos, selection) in list.iter().enumerate() {
                    if pos != 0 {
                        buf.write_all(b", ")?;
                    }
                    Self::selection_to_sql_with_placeholder_count(
                        dialect,
                        buf,
                        selection,
                        placeholder_count,
                    )?;
                }
                buf.write_all(b")")?;
            }
        };
        Ok(())
    }

    fn update_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        table: &str,
        assignments: &[UpdateAssignment],
        selection: Option<&Selection>,
    ) -> Result<()> {
        buf.write_all(b"UPDATE ")?;
        Self::write_quoted(dialect, buf, table)?;
        buf.write_all(b" SET ")?;
        for (pos, assignment) in assignments.iter().enumerate() {
            if pos != 0 {
                buf.write_all(b", ")?;
            }
            Self::write_quoted(dialect, buf, assignment.target.as_bytes())?;
            buf.write_all(b"=")?;
            match &assignment.value {
                UpdateValue::Null => buf.write_all(b"NULL")?,
                UpdateValue::String(string) => Self::write_single_quoted(dialect, buf, string)?,
                UpdateValue::Number(number) => buf.write_all(number.as_bytes())?,
                UpdateValue::Placeholder => {
                    buf.write_all(dialect.placeholder(pos + 1).as_bytes())?
                }
            }
        }
        if let Some(selection) = selection.as_ref() {
            buf.write_all(b" WHERE ")?;
            Self::selection_to_sql(dialect, buf, selection)?;
        };
        Ok(())
    }

    fn delete_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        from_clause: &FromClause,
        selection: Option<&Selection>,
    ) -> Result<()> {
        buf.write_all(b"DELETE FROM ")?;
        match from_clause {
            FromClause::Table(name) => Self::write_quoted(dialect, buf, name)?,
            FromClause::None => Err(Error::DeleteToSql {
                reason: "DELETE without FROM is not supported",
            })?,
            FromClause::TableWithJoin(_) => Err(Error::DeleteToSql {
                reason: "DELETE with joins is not supported",
            })?,
        }
        if let Some(selection) = selection.as_ref() {
            buf.write_all(b" WHERE ")?;
            Self::selection_to_sql(dialect, buf, selection)?;
        };
        Ok(())
    }

    fn drop_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        object_type: DropObjectType,
        if_exists: bool,
        name: &str,
        table: Option<&str>,
    ) -> Result<()> {
        match object_type {
            DropObjectType::Table => buf.write_all(b"DROP TABLE ")?,
            DropObjectType::Index => buf.write_all(b"DROP INDEX ")?,
        };
        if if_exists {
            buf.write_all(b"IF EXISTS ")?;
        }
        Self::write_quoted(dialect, buf, name);
        match (
            object_type == DropObjectType::Index,
            dialect.drop_index_requires_table(),
            table,
        ) {
            (true, true, Some(table)) => {
                buf.write_all(b" ON ")?;
                Self::write_quoted(dialect, buf, table)?;
            }
            (true, _, None) => Err(Error::DropIndex)?,
            _ => (),
        };
        Ok(())
    }

    fn write_quoted(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        input: impl AsRef<[u8]>,
    ) -> Result<()> {
        buf.write_all(dialect.quote())?;
        buf.write_all(input.as_ref())?;
        buf.write_all(dialect.quote())?;
        Ok(())
    }

    fn write_single_quoted(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        input: impl AsRef<[u8]>,
    ) -> Result<()> {
        buf.write_all(b"'")?;
        buf.write_all(input.as_ref())?;
        buf.write_all(b"'")?;
        Ok(())
    }

    fn alter_table_to_sql(
        dialect: &dyn ToQuery,
        buf: &mut dyn Write,
        table_name: &str,
        operation: &AlterTableOperation,
    ) -> Result<()> {
        buf.write_all(b"ALTER TABLE ")?;
        Self::write_quoted(dialect, buf, table_name)?;
        match operation {
            AlterTableOperation::AddColumn { column } => {
                buf.write_all(b" ADD COLUMN ")?;
                let columns: &[Column] = slice::from_ref(column);
                Self::table_columns_to_sql(dialect, buf, columns, "")?;
            }
            AlterTableOperation::RenameColumn { from, to } => {
                buf.write_all(b" RENAME COLUMN ")?;
                Self::write_quoted(dialect, buf, from)?;
                buf.write_all(b" TO ")?;
                Self::write_quoted(dialect, buf, to)?;
            }
            AlterTableOperation::DropColumn { name } => {
                buf.write_all(b" DROP COLUMN ")?;
                Self::write_quoted(dialect, buf, name)?;
            }
            AlterTableOperation::RenameTable { to } => {
                buf.write_all(b" RENAME TO ")?;
                Self::write_quoted(dialect, buf, to)?;
            }
        }
        Ok(())
    }

    pub fn to_sql(&self, dialect: &dyn ToQuery) -> Result<String> {
        let buf = &mut Cursor::new(Vec::with_capacity(1));
        match self {
            Ast::CreateTable {
                if_not_exists,
                name,
                columns,
                constraints,
            } => {
                Self::create_table_to_sql(dialect, buf, *if_not_exists, name, columns, constraints)?
            }
            Ast::AlterTable { name, operation } => {
                Self::alter_table_to_sql(dialect, buf, name.as_str(), operation)?
            }
            Ast::CreateIndex {
                unique,
                name,
                table,
                columns,
            } => Self::create_index_to_sql(dialect, buf, *unique, name, table, columns)?,
            Ast::Select {
                distinct,
                projections,
                from_clause,
                selection,
                group_by,
                order_by,
            } => Self::select_to_sql(
                dialect,
                buf,
                *distinct,
                projections,
                from_clause,
                selection.as_ref(),
                group_by,
                order_by,
            )?,
            Ast::Insert {
                table,
                columns,
                source,
            } => Self::insert_to_sql(
                dialect,
                buf,
                table.as_str(),
                columns.as_slice(),
                source.as_slice(),
            )?,
            Ast::Update {
                table,
                assignments,
                selection,
            } => Self::update_to_sql(
                dialect,
                buf,
                table.as_str(),
                assignments.as_slice(),
                selection.as_ref(),
            )?,
            Ast::Delete {
                from_clause,
                selection,
            } => Self::delete_to_sql(dialect, buf, from_clause, selection.as_ref())?,
            Ast::Drop {
                object_type,
                if_exists,
                name,
                table,
            } => Self::drop_to_sql(
                dialect,
                buf,
                *object_type,
                *if_exists,
                name,
                table.as_ref().map(AsRef::as_ref),
            )?,
        };
        let buf = std::mem::replace(buf, Cursor::new(Vec::new()));
        Ok(String::from_utf8(buf.into_inner())?)
    }
}

pub trait ToQuery {
    fn quote(&self) -> &'static [u8];

    fn placeholder(&self, pos: usize) -> Cow<'static, str>;

    fn placeholder_supports_cast(&self) -> bool {
        false
    }

    fn emit_column_spec(&self, column: &Column, buf: &mut dyn Write) -> Result<()>;

    fn drop_index_requires_table(&self) -> bool {
        false
    }
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
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => Err(Error::Serial)?,
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
            DataType::Json => Cow::Borrowed("JSON"),
            DataType::Uuid => Cow::Borrowed("CHAR(36)"),
            DataType::Decimal { precision, scale } => {
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

    fn drop_index_requires_table(&self) -> bool {
        true
    }
}

impl ToQuery for PostgreSqlDialect {
    fn quote(&self) -> &'static [u8] {
        b"\""
    }

    fn placeholder(&self, pos: usize) -> Cow<'static, str> {
        Cow::Owned(format!("${pos}"))
    }

    fn placeholder_supports_cast(&self) -> bool {
        true
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
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => Err(Error::Serial)?,
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
            DataType::Json => Cow::Borrowed("JSON"),
            DataType::Uuid => Cow::Borrowed("UUID"),
            DataType::Decimal { precision, scale } => {
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
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial
                if options.is_primary_key() =>
            {
                Cow::Borrowed("INTEGER")
            }
            DataType::SmallSerial | DataType::Serial | DataType::BigSerial => Err(Error::Serial)?,
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
            DataType::Json => Cow::Borrowed("JSON"),
            DataType::Uuid => Cow::Borrowed("UUID"),
            DataType::Decimal { precision, scale } => {
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
