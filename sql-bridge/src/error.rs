use sqlparser::ast::{
    BinaryOperator, ColumnOption, DataType, Expr, JoinConstraint, JoinOperator, ReferentialAction,
    TableConstraint, TableFactor, TableWithJoins, Value,
};

use crate::ast::Selection;

#[derive(Debug)]
pub enum Error {
    DataType {
        data_type: DataType,
    },
    ColumnOption {
        option: ColumnOption,
    },
    OnDeleteConstrait {
        referential_action: ReferentialAction,
    },
    PrimaryKey {
        reason: &'static str,
    },
    PrimaryKeyWithExpression {
        expr: Expr,
    },
    ForeignKey {
        reason: &'static str,
    },
    TableConstraint {
        constraint: TableConstraint,
    },
    CompoundIdentifier {
        length: usize,
    },
    SelectionValue {
        value: Value,
    },
    Selection {
        selection: Selection,
        r#where: Option<&'static str>,
    },
    SelectionFromExpr {
        expr: Expr,
    },
    InsertSourceExpression {
        expr: Expr,
    },
    InsertSourceValue {
        value: Value,
    },
    UpdateExpression {
        expr: Expr,
    },
    UpdateValue {
        value: Value,
    },
    BinaryOperator {
        op: BinaryOperator,
    },
    Keyword {
        keyword: &'static str,
    },
    JoinConstraint {
        constraint: JoinConstraint,
    },
    JoinOperator {
        op: JoinOperator,
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
        factor: TableFactor,
    },
    TableJoins {
        table_joins: Vec<TableWithJoins>,
    },
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
        }
    }
}
