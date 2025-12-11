use sqlparser::ast::{BinaryOperator, ColumnOption, DataType, Expr, ReferentialAction, TableConstraint, Value};

use crate::ast::Selection;

#[derive(Debug)]
pub enum Error {
    UnsupportedDataType {
        data_type: DataType,
    },
    UnsupportedColumnOption {
        option: ColumnOption,
    },
    UnsupportedOnDeleteConstrait {
        referential_action: ReferentialAction,
    },
    UnsupportedPrimaryKey {
        reason: &'static str,
    },
    UnsupportedPrimaryKeyWithExpression {
        expr: Expr,
    },
    UnsupportedForeignKey {
        reason: &'static str,
    },
    UnsupportedTableConstraint {
        constraint: TableConstraint,
    },
    UnsupportedCompoundIdentifier {
        length: usize,
    },
    UnsupportedSelectionValue {
        value: Value
    },
    UnsupportedSelection {
        selection: Selection,
        r#where: Option<&'static str>
    },
    UnsupportedSelectionFromExpr {
        expr: Expr,
    },
    UnsupportedInsertSourceExpression{ expr: Expr },
    UnsupportedInsertSourceValue{ value: Value },
    UnsupportedUpdateExpression { expr: Expr },
    UnsupportedUpdateValue { value: Value},
    UnsupportedBinaryOperator {
        op: BinaryOperator
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedDataType { data_type } => {
                write!(f, "unsupported data type: {data_type:?}")
            }
            Self::UnsupportedColumnOption { option } => {
                write!(f, "unsupported column option: {option:?}")
            }
            Self::UnsupportedOnDeleteConstrait { referential_action } => {
                write!(
                    f,
                    "unsupported on delete constraint in foreign key: {}",
                    referential_action
                )
            }
            Self::UnsupportedPrimaryKey { reason } => {
                write!(f, "unsupported primary key with {}", reason)
            }
            Self::UnsupportedPrimaryKeyWithExpression { expr } => {
                write!(f, "unsupported primary key with unsupported expression: {expr:?}")
            }
            Self::UnsupportedForeignKey { reason } => {
                write!(f, "unsupported foreign key with {}", reason)
            }
            Self::UnsupportedTableConstraint { constraint } => {
                write!(f, "unsupported table constraint: {constraint:?}")
            }
            Self::UnsupportedCompoundIdentifier { length } => {
                write!(
                    f,
                    "unsupported compound identifier with length {}",
                    length
                )
            },
            Self::UnsupportedSelectionValue { value } => {
                write!(f, "unsupporetd selection value: {value:?} ")
            }
            Self::UnsupportedSelection { selection, r#where } => {
                match r#where {
                    None =>  write!(f, "unsupporetd selection: {selection:?} "),
                    Some(w) => write!(f, "unsupported selection in {w}: {selection:?}"),
                }
            },
            Self::UnsupportedSelectionFromExpr { expr } => {
                write!(f, "unsupported selection expr: {expr:?}")
            },
            Self::UnsupportedInsertSourceExpression { expr } => {
                write!(f, "unsupported insert source expr: {expr:?}")
            }
            Self::UnsupportedInsertSourceValue { value } => {
                write!(f, "unsupported insert source value: {value:?}")
            },
            Self::UnsupportedUpdateExpression { expr } => {
                write!(f, "unsupported update from expr: {expr:?}")
            }
            Self::UnsupportedUpdateValue { value } => {
                write!(f, "unsupported update value: {value:?}")
            },
            Self::UnsupportedBinaryOperator { op } => {
                write!(f, "unsupported binary operator: {op:?}")
            }
        }
    }
}
