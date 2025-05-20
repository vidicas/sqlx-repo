#![allow(unused)]

use std::{
    borrow::Cow,
    io::{Cursor, Write},
    ops::Deref,
    path::Display,
};

use sqlparser::{
    ast::{
        BinaryOperator, CharacterLength, ColumnDef, ColumnOptionDef, CreateIndex, CreateTable,
        ExactNumberInfo, Expr, FunctionArguments, Ident, IndexColumn, ObjectName, ObjectNamePart,
        OrderByExpr, Query, SelectItem, SetExpr, Statement, Table, TableConstraint, TableFactor,
        TableWithJoins, Value,
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
    Decimal { precision: u64, scale: u64 },
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
                    None => Err(format!("unsupported data type: {value:?}"))?,
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
            .map(|constraint| match constraint {
                TableConstraint::PrimaryKey { columns, .. } => Ok(Constraint::PrimaryKey(
                    columns
                        .iter()
                        .map(|Ident { value, .. }| value.clone())
                        .collect(),
                )),
                _ => Err(format!("unsupported constraint: {constraint:?}")),
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
    CreateIndex {
        unique: bool,
        name: String,
        table: String,
        columns: Vec<String>,
    },
    Select {
        distinct: bool,
        projections: Vec<Projection>,
        from: String,
        selection: Option<Selection>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    WildCard,
    Identifier(String),
    Function(Function),
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
    Number(String),
    String(String),
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
            Expr::Value(value) => match &value.value {
                Value::Number(number, _) => Selection::Number(number.clone()),
                Value::SingleQuotedString(string) => Selection::String(string.clone()),
                _ => Err(format!("unsupported value: {:#?}", value.value))?,
            },
            _ => unimplemented!("{expr:?}"),
        };
        Ok(selection)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Eq,
    And,
}

impl TryFrom<&BinaryOperator> for Op {
    type Error = Error;
    fn try_from(op: &BinaryOperator) -> std::result::Result<Self, Self::Error> {
        let op = match op {
            BinaryOperator::And => Op::And,
            BinaryOperator::Eq => Op::Eq,
            _ => Err(format!("binary operator not supported {op:?}"))?,
        };
        Ok(op)
    }
}

impl Ast {
    fn parse_object_name(name: &ObjectName) -> Result<String> {
        let name_parts = &name.0;
        if name_parts.len() > 1 {
            Err("schema-qualified names are not supported")?
        }
        let ObjectNamePart::Identifier(ident) = name_parts.first().unwrap();
        Ok(ident.value.clone())
    }

    fn parse_create_table(
        if_not_exists: bool,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Ast> {
        let name = Self::parse_object_name(name)?;
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
            constraints: Constraints::try_from(constraints)?,
        })
    }

    //pub enum FunctionArg {
    //    /// `name` is identifier
    //    ///
    //    /// Enabled when `Dialect::supports_named_fn_args_with_expr_name` returns 'false'
    //    Named {
    //        name: Ident,
    //        arg: FunctionArgExpr,
    //        operator: FunctionArgOperator,
    //    },
    //    /// `name` is arbitrary expression
    //    ///
    //    /// Enabled when `Dialect::supports_named_fn_args_with_expr_name` returns 'true'
    //    ExprNamed {
    //        name: Expr,
    //        arg: FunctionArgExpr,
    //        operator: FunctionArgOperator,
    //    },
    //    Unnamed(FunctionArgExpr),
    //}
    fn parse_function_args(args: &FunctionArguments) -> Result<Vec<FunctionArg>> {
        let args = match args {
            FunctionArguments::None => vec![],
            FunctionArguments::List(list) => {
                if !list.clauses.is_empty() {
                    Err("function clauses are not yet supported: {list:?}")?
                };
                if list.duplicate_treatment.is_some() {
                    Err("function duplicate treatment not supported: {list:?}")?
                }
                list.args
                    .iter()
                    .map(|arg| -> Result<_> {
                        let arg = match arg {
                            sqlparser::ast::FunctionArg::ExprNamed { .. } => {
                                Err("named expressions are not supported in function arguments")?
                            }
                            sqlparser::ast::FunctionArg::Named { .. } => {
                                Err("named columns are not supported in function arguments(yet)")?
                            }
                            sqlparser::ast::FunctionArg::Unnamed(expr) => match expr {
                                sqlparser::ast::FunctionArgExpr::Wildcard => FunctionArg::Wildcard,
                                sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(ident)) => {
                                    FunctionArg::Ident(ident.value.clone())
                                }
                                _ => Err(format!("unsupported function argument: {expr:?}"))?,
                            },
                        };
                        Ok(arg)
                    })
                    .collect::<Result<_>>()?
            }
            FunctionArguments::Subquery(query) => Err(format!(
                "function arguments are not yet supported: {query:?}"
            ))?,
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
            ..
        }: &CreateIndex,
    ) -> Result<Self> {
        if *if_not_exists {
            Err("index with existance check is not supported")?
        };
        if name.is_none() {
            Err("index without name are not supported")?
        }
        if *concurrently {
            Err("index with concurrent creation are not supported")?
        }
        let columns = columns
            .iter()
            .map(|IndexColumn { column, .. }| -> Result<String> {
                match &column.expr {
                    Expr::Identifier(Ident { value, .. }) => Ok(value.clone()),
                    expr => Err("unsupported index column: {expr:?}")?,
                }
            })
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
            Err("CTE is not yet supported")?
        }
        if query.fetch.is_some() {
            Err("FETCH is not supported")?;
        }
        // FIXME:
        if query.limit_clause.is_some() {
            Err("LIMIT is not yet supported")?
        }
        if !query.locks.is_empty() {
            Err("LOCKS are not supported")?
        }
        if query.for_clause.is_some() {
            Err("FOR clause is not yet supported")?
        }
        let select = match &*query.body {
            SetExpr::Select(select) => &**select,
            other => Err(format!("only SELECT supported, got:\n{other:#?}"))?,
        };
        if select.top.is_some() || select.top_before_distinct {
            return Err("TOP statement is not supported")?;
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
                                    Err("COUNT function can only have single argument: {args:?}")?
                                }
                                let arg = args.pop().unwrap();
                                Ok(Projection::Function(Function::Count(arg)))
                            }
                            name => Err(format!("unsupported function '{name}'"))?,
                        }
                    }
                    _ => Err(format!("unsupported projection: {projection:?}"))?,
                }
            })
            .collect::<Result<Vec<Projection>>>()?;

        let from = match select.from.as_slice() {
            &[
                TableWithJoins {
                    ref relation,
                    ref joins,
                    ..
                },
            ] if joins.is_empty() => match relation {
                TableFactor::Table { name, .. } => Self::parse_object_name(name)?,
                other => Err(format!("unsupported table factor: {other:?}"))?,
            },
            other => Err("joins are not supported yet: {tables:?}")?,
        };
        let selection = match select
            .selection
            .as_ref()
            .map(|selection| selection.try_into())
        {
            None => None,
            Some(Err(e)) => Err(e)?,
            Some(Ok(selection)) => Some(selection),
        };
        let ast = Ast::Select {
            distinct: select.distinct.is_some(),
            projections,
            from,
            selection,
        };
        Ok(ast)
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
                    Statement::CreateIndex(index) => Self::parse_create_index(index)?,
                    Statement::Query(query) => Self::parse_query(query)?,
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

    fn create_table_to_sql(
        dialect: impl ToQuery,
        mut buf: impl Write,
        if_not_exists: bool,
        name: &str,
        columns: &[Column],
        constraints: &Constraints,
    ) -> Result<()> {
        buf.write_all(b"CREATE TABLE ")?;
        if if_not_exists {
            buf.write_all(b"IF NOT EXISTS ")?;
        }
        Self::write_quoted(&dialect, &mut buf, name)?;
        buf.write_all(b" (\n")?;
        for (pos, column) in columns.iter().enumerate() {
            Self::write_quoted(&dialect, &mut buf, &column.name)?;
            buf.write_all(b" ")?;

            dialect.emit_column_spec(column, &mut buf)?;

            // push comma if column is not last
            if pos != columns.len() - 1 {
                buf.write_all(b",\n")?;
            }
        }
        if constraints.len() > 0 {
            buf.write_all(b",\n")?;
        }
        for (pos, constraint) in constraints.into_iter().enumerate() {
            match constraint {
                Constraint::PrimaryKey(fields) => {
                    buf.write_all(b"PRIMARY KEY (")?;
                    for (pos, field) in fields.iter().enumerate() {
                        Self::write_quoted(&dialect, &mut buf, field)?;
                        if pos != fields.len() - 1 {
                            buf.write_all(b", ")?;
                        }
                    }
                    buf.write_all(b")")?;
                }
            }
            if pos != constraints.len() - 1 {
                buf.write_all(b",\n")?;
            }
        }
        buf.write_all(b"\n)")?;
        Ok(())
    }

    fn create_index_to_sql(
        dialect: impl ToQuery,
        mut buf: impl Write,
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
        Self::write_quoted(&dialect, &mut buf, name)?;
        buf.write_all(b" ON ")?;
        Self::write_quoted(&dialect, &mut buf, table)?;
        buf.write_all(b" (")?;
        for (pos, column) in columns.iter().enumerate() {
            Self::write_quoted(&dialect, &mut buf, column)?;
            if pos != columns.len() - 1 {
                buf.write_all(b", ")?;
            }
        }
        buf.write_all(b")")?;
        Ok(())
    }

    fn select_to_sql(
        dialect: impl ToQuery,
        mut buf: impl Write,
        distinct: bool,
        projections: &[Projection],
        from: &str,
        selection: Option<&Selection>,
    ) -> Result<()> {
        buf.write_all(b"SELECT ")?;
        if distinct {
            buf.write_all(b"DISTINCT ")?;
        }
        for (pos, projection) in projections.iter().enumerate() {
            match projection {
                Projection::WildCard => buf.write_all(b"*")?,
                Projection::Identifier(ident) => {
                    Self::write_quoted(&dialect, &mut buf, ident)?;
                }
                Projection::Function(function) => match function {
                    Function::Count(FunctionArg::Wildcard) => buf.write_all(b"COUNT(*)")?,
                    Function::Count(FunctionArg::Ident(ident)) => {
                        buf.write_all(b"COUNT(")?;
                        Self::write_quoted(&dialect, &mut buf, ident)?;
                        buf.write_all(b")")?
                    }
                },
            };
            if pos != projections.len() - 1 {
                buf.write_all(b", ")?;
            }
        }
        buf.write_all(b" FROM ")?;
        Self::write_quoted(&dialect, &mut buf, from)?;
        if selection.is_none() {
            return Ok(());
        };
        buf.write_all(b" WHERE ")?;
        Self::selection_to_sql(&dialect, &mut buf, selection.unwrap())?;
        Ok(())
    }

    fn selection_to_sql(
        dialect: &impl ToQuery,
        buf: &mut impl Write,
        selection: &Selection,
    ) -> Result<()> {
        match selection {
            Selection::BinaryOp { op, left, right } => {
                Self::selection_to_sql(dialect, buf, left)?;
                match op {
                    Op::And => buf.write_all(b" AND ")?,
                    Op::Eq => buf.write_all(b" = ")?,
                }
                Self::selection_to_sql(dialect, buf, right)?;
            }
            Selection::Ident(ident) => Self::write_quoted(dialect, buf, ident)?,
            Selection::Number(number) => buf.write_all(number.as_bytes())?,
            Selection::String(string) => {
                for chunk in [b"'", string.as_bytes(), b"'"] {
                    buf.write_all(chunk)?;
                }
            }
        };
        Ok(())
    }

    fn write_quoted(
        dialect: &impl ToQuery,
        mut buf: impl Write,
        input: impl AsRef<[u8]>,
    ) -> Result<()> {
        buf.write_all(dialect.quote())?;
        buf.write_all(input.as_ref())?;
        buf.write_all(dialect.quote())?;
        Ok(())
    }

    pub fn to_sql(&self, dialect: impl ToQuery) -> Result<String> {
        let mut buf = Cursor::new(Vec::with_capacity(1024));
        match self {
            Ast::CreateTable {
                if_not_exists,
                name,
                columns,
                constraints,
            } => Self::create_table_to_sql(
                dialect,
                &mut buf,
                *if_not_exists,
                name,
                columns,
                constraints,
            )?,
            Ast::CreateIndex {
                unique,
                name,
                table,
                columns,
            } => Self::create_index_to_sql(dialect, &mut buf, *unique, name, table, columns)?,
            Ast::Select {
                distinct,
                projections,
                from,
                selection,
            } => Self::select_to_sql(
                dialect,
                &mut buf,
                *distinct,
                projections,
                from,
                selection.as_ref(),
            )?,
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
            }
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
