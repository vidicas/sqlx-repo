use sqlparser::ast::{
    AlterTableOperation, BinaryOperator, ColumnOption, DataType, Expr, FunctionArg,
    FunctionArguments, IndexColumn, JoinConstraint, JoinOperator, ObjectType, ReferentialAction,
    SelectItem, SetExpr, TableConstraint, TableFactor, TableWithJoins, Value,
};

use crate::ast::Selection;

use sqlparser::parser::ParserError;
use std::io;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub struct IoErrorWrap(io::Error);

impl PartialEq for IoErrorWrap {
    fn eq(&self, other: &Self) -> bool {
        // FIXME: not sure if it's reasonable
        self.0.kind() == other.0.kind() && self.0.to_string() == other.0.to_string()
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    DataType {
        data_type: Box<DataType>,
    },
    ColumnOption {
        option: Box<ColumnOption>,
    },
    OnDeleteConstrait {
        referential_action: ReferentialAction,
    },
    PrimaryKey {
        reason: &'static str,
    },
    PrimaryKeyWithExpression {
        expr: Box<Expr>,
    },
    ForeignKey {
        reason: &'static str,
    },
    TableConstraint {
        constraint: Box<TableConstraint>,
    },
    CompoundIdentifier {
        length: usize,
    },
    SelectionValue {
        value: Box<Value>,
    },
    Selection {
        selection: Box<Selection>,
        r#where: Option<&'static str>,
    },
    SelectionFromExpr {
        expr: Box<Expr>,
    },
    InsertSourceExpression {
        expr: Box<Expr>,
    },
    InsertSourceValue {
        value: Box<Value>,
    },
    UpdateExpression {
        expr: Box<Expr>,
    },
    UpdateValue {
        value: Box<Value>,
    },
    BinaryOperator {
        op: BinaryOperator,
    },
    Keyword {
        keyword: &'static str,
    },
    JoinConstraint {
        constraint: Box<JoinConstraint>,
    },
    JoinOperator {
        op: Box<JoinOperator>,
    },
    TableAlias,
    /// Postgre + MSSQL
    TableValuedFunctions,
    ///  MSSQL-specific `WITH (...)` hints such as NOLOCK.
    TableWithHints,
    /// Table time-travel, as supported by BigQuery and MSSQL.
    TableVersioning,
    /// Postgres.
    TableWithOrdinality,
    /// Mysql
    TableWithPartitions,
    /// Optional PartiQL JsonPath: <https://partiql.org/dql/from.html>
    TableWithJsonPath,
    /// Optional table sample modifier
    /// See: <https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#sample-clause>
    TableWithSampleModifier,
    /// Optional index hints(mysql)
    /// See: <https://dev.mysql.com/doc/refman/8.4/en/index-hints.html>
    TableWithIndexHints,
    TableFactor {
        factor: Box<TableFactor>,
    },
    TableJoins {
        table_joins: Vec<TableWithJoins>,
    },
    DropObjectType {
        object_type: ObjectType,
    },
    Drop {
        reason: &'static str,
    },
    AlterTable {
        reason: &'static str,
    },
    AlterTableOp {
        op: Box<AlterTableOperation>,
    },
    ObjectName {
        reason: &'static str,
    },
    CreateTable {
        reason: &'static str,
    },
    FunctionArguments {
        reason: &'static str,
        arguments: Box<FunctionArguments>,
    },
    FunctionArgument {
        reason: &'static str,
        argument: Box<FunctionArg>,
    },
    CreateIndex {
        reason: &'static str,
    },
    CreateIndexColumn {
        column: Box<IndexColumn>,
    },
    CTE,
    Fetch,
    Limit,
    Locks,
    For,
    Select {
        set_expr: Box<SetExpr>,
    },
    Top,
    Count {
        reason: &'static str,
        args: Vec<crate::ast::FunctionArg>,
    },
    Function {
        name: String,
    },
    Projection {
        select_item: Box<SelectItem>,
    },
    OrderBy {
        reason: &'static str,
    },
    GroupBy {
        reason: &'static str,
    },
    Insert {
        reason: &'static str,
    },
    InsertSourceEmpty,
    InsertTableObject,
    InsertSource {
        set_expr: Box<SetExpr>,
    },
    Update {
        reason: &'static str,
    },
    UpdateTableType,
    UpdateAssignmentTarget,
    Delete {
        reason: &'static str,
    },
    DeleteToSql {
        reason: &'static str,
    },
    DropIndex,
    Statement,
    Serial,
    Io(IoErrorWrap),
    Parser(ParserError),
    Utf8(FromUtf8Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DataType { data_type } => {
                write!(f, "unsupported data type: {data_type:?}")
            }
            Error::ColumnOption { option } => {
                write!(f, "unsupported column option: {option:?}")
            }
            Error::OnDeleteConstrait { referential_action } => {
                write!(
                    f,
                    "unsupported on delete constraint in foreign key: {}",
                    referential_action
                )
            }
            Error::PrimaryKey { reason } => {
                write!(f, "unsupported primary key with {}", reason)
            }
            Error::PrimaryKeyWithExpression { expr } => {
                write!(
                    f,
                    "unsupported primary key with unsupported expression: {expr:?}"
                )
            }
            Error::ForeignKey { reason } => {
                write!(f, "unsupported foreign key with {}", reason)
            }
            Error::TableConstraint { constraint } => {
                write!(f, "unsupported table constraint: {constraint:?}")
            }
            Error::CompoundIdentifier { length } => {
                write!(f, "unsupported compound identifier with length {}", length)
            }
            Error::SelectionValue { value } => {
                write!(f, "unsupporetd selection value: {value:?} ")
            }
            Error::Selection { selection, r#where } => match r#where {
                None => write!(f, "unsupporetd selection: {selection:?} "),
                Some(w) => write!(f, "unsupported selection in {w}: {selection:?}"),
            },
            Error::SelectionFromExpr { expr } => {
                write!(f, "unsupported selection expr: {expr:?}")
            }
            Error::InsertSourceExpression { expr } => {
                write!(f, "unsupported insert source expr: {expr:?}")
            }
            Error::InsertSourceValue { value } => {
                write!(f, "unsupported insert source value: {value:?}")
            }
            Error::UpdateExpression { expr } => {
                write!(f, "unsupported update from expr: {expr:?}")
            }
            Error::UpdateValue { value } => {
                write!(f, "unsupported update value: {value:?}")
            }
            Error::BinaryOperator { op } => {
                write!(f, "unsupported binary operator: {op:?}")
            }
            Error::Keyword { keyword } => {
                write!(f, "unsupported keyword: '{keyword}'")
            }
            Error::JoinConstraint { constraint } => {
                write!(f, "unsupported join constraint: {constraint:?}")
            }
            Error::JoinOperator { op } => {
                write!(f, "unsupported join operator: {op:?}")
            }
            Error::TableAlias => {
                write!(f, "table aliasing is unsupported")
            }
            Error::TableValuedFunctions => {
                write!(
                    f,
                    "arguments of a table-valued function are not supported in table factor"
                )
            }
            Error::TableWithHints => {
                write!(f, "with hints are not supported in table factor")
            }
            Error::TableVersioning => {
                write!(f, "table versioning is not supported in table factor")
            }
            Error::TableWithOrdinality => {
                write!(f, "table with ordinality is not supported in table factor")
            }
            Error::TableWithPartitions => {
                write!(f, "partitions are not supported in table factor")
            }
            Error::TableWithJsonPath => {
                write!(f, "json path is not supported in table factor")
            }
            Error::TableWithSampleModifier => {
                write!(f, "sample is not supported in table factor")
            }
            Error::TableWithIndexHints => {
                write!(f, "index hints are not supported in table factor")
            }
            Error::TableFactor { factor } => {
                write!(f, "unsupported table factor: {factor:?}")
            }
            Error::TableJoins { table_joins } => {
                write!(
                    f,
                    "select with multiple tables is not supported yet: {table_joins:?}"
                )
            }
            Error::DropObjectType { object_type } => {
                write!(f, "unsupported drop of object type: {object_type:?}")
            }
            Error::Drop { reason } => {
                write!(f, "unsupported drop: {reason}")
            }
            Error::AlterTable { reason } => {
                write!(f, "unsupported alter table: {reason}")
            }
            Error::AlterTableOp { op } => {
                write!(f, "unsupported operation: {op:?}")
            }
            Error::ObjectName { reason } => {
                write!(f, "failed to parse object name: {reason}")
            }
            Error::CreateTable { reason } => {
                write!(f, "unsupported create table: {reason}")
            }
            Error::FunctionArguments { reason, arguments } => {
                write!(
                    f,
                    "unsupported function arguments: {reason}, function arguments: {arguments:?}"
                )
            }
            Error::FunctionArgument { reason, argument } => {
                write!(
                    f,
                    "unsupported function argument: {reason}, function argument: {argument:?}"
                )
            }
            Error::CreateIndex { reason } => {
                write!(f, "unsupported create index: {reason}")
            }
            Error::CreateIndexColumn { column } => {
                write!(f, "unsupported create index column: {column:?}")
            }
            Error::CTE => {
                write!(f, "CTE are not supported")
            }
            Error::Fetch => {
                write!(f, "Fetch is not supported")
            }
            Error::Limit => {
                write!(f, "limit is not supported")
            }
            Error::Locks => {
                write!(f, "locks are not supported")
            }
            Error::For => {
                write!(f, "for clause is not supported")
            }
            Error::Select { set_expr } => {
                write!(f, "unsupported select set expr: {set_expr:?}")
            }
            Error::Top => {
                write!(f, "top is not supported")
            }
            Error::Count { reason, args } => {
                write!(f, "unsupported count: {reason}, args: {args:?}")
            }
            Error::Function { name } => {
                write!(f, "unsupported function '{name}'")
            }
            Error::Projection { select_item } => {
                write!(f, "unsupported projection select item: {select_item:?}")
            }
            Error::OrderBy { reason } => {
                write!(f, "{reason}")
            }
            Error::GroupBy { reason } => {
                write!(f, "{reason}")
            }
            Error::Insert { reason } => {
                write!(f, "unsupported insert: {reason}")
            }
            Error::InsertSourceEmpty => {
                write!(f, "unsupported insert, source is empty")
            }
            Error::InsertTableObject => {
                write!(f, "unsupported table name type")
            }
            Error::InsertSource { set_expr } => {
                write!(f, "unsupported insert source: {set_expr:?}")
            }
            Error::Update { reason } => {
                write!(f, "unsupported update: {reason}")
            }
            Error::UpdateTableType => {
                write!(f, "unsupported table type")
            }
            Error::UpdateAssignmentTarget => {
                write!(f, "unsupported assignment target")
            }
            Error::Delete { reason } => {
                write!(f, "unsupported delete: {reason}")
            }
            Error::DeleteToSql { reason } => {
                write!(f, "unsupported delete: {reason}")
            }
            Error::DropIndex => {
                write!(f, "`DROP INDEX` requires table name")
            }
            Error::Statement => {
                write!(f, "unsupported statement")
            }
            Error::Serial => {
                write!(
                    f,
                    "expected smallserial/serial/bigserial with `PRIMARY KEY` constraint"
                )
            }
            Error::Io(err) => {
                write!(f, "IO error: {}", err.0)
            }
            Error::Parser(err) => {
                write!(f, "Parser error: {err}")
            }
            Error::Utf8(err) => {
                write!(f, "UTF-8 error: {err}")
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(IoErrorWrap(err))
    }
}

impl From<ParserError> for Error {
    fn from(err: ParserError) -> Self {
        Error::Parser(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Utf8(err)
    }
}

impl std::error::Error for Error {}
