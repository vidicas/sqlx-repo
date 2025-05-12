mod ast;

pub use ast::Ast;
pub use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn parse<T: AsRef<str>>(statement: T) -> Result<Vec<Ast>> {
    Ast::parse(statement.as_ref())
}
