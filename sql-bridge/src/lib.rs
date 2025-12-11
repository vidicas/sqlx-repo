mod ast;
mod error;

pub use ast::{Ast, ToQuery};
pub use error::Error;
pub use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn parse<T: AsRef<str>>(statement: T) -> Result<Vec<Ast>> {
    Ast::parse(statement.as_ref())
}
