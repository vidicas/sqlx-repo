use quote::quote_spanned;
use sql_bridge::{Ast, MySqlDialect, PostgreSqlDialect, SQLiteDialect, ToQuery};
use syn::spanned::Spanned as _;

fn asts_to_query(
    ast: &[Ast],
    dialect: &dyn ToQuery,
) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let queries = ast
        .iter()
        .map(|ast| ast.to_sql(dialect))
        .collect::<Result<Vec<String>, _>>()?
        .join(";");
    Ok(queries)
}

fn get_literal(input: &syn::Expr) -> Result<&syn::LitStr, proc_macro2::TokenStream> {
    match input {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit),
            ..
        }) => Ok(lit),
        syn::Expr::Group(group) => get_literal(&group.expr),
        u => {
            let compile_error = format!("expected string, got: {u:#?}");
            let span = u.span();
            Err(quote_spanned! { span => compile_error!(#compile_error); })
        }
    }
}

fn build_queries(
    span: proc_macro2::Span,
    query: &str,
) -> Result<(String, String, String), proc_macro2::TokenStream> {
    let ast_list = match sql_bridge::Ast::parse(query) {
        Ok(ast) => ast,
        Err(e) => {
            let err = format!("failed to parse query: {e}");
            return Err(quote_spanned! { span => compile_error!(#err) });
        }
    };
    let ast_list = ast_list.as_slice();
    let dialects: &[&dyn ToQuery] = &[&PostgreSqlDialect {}, &SQLiteDialect {}, &MySqlDialect {}];
    let mut query_list = match dialects
        .iter()
        .map(|dialect| asts_to_query(ast_list, *dialect))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(q) => q,
        Err(e) => {
            let err = format!("failed to build queries list: {e}");
            return Err(quote_spanned! { span => compile_error!(#err) });
        }
    };
    if query_list.len() != 3 {
        return Err(
            quote_spanned!(span => compile_error!("query list length is expected to be 3")),
        );
    }
    let mysql_query = query_list.pop().unwrap();
    let sqlite_query = query_list.pop().unwrap();
    let postgres_query = query_list.pop().unwrap();
    Ok((postgres_query, sqlite_query, mysql_query))
}

pub fn gen_query(input: proc_macro::TokenStream) -> proc_macro2::TokenStream {
    let input: syn::Expr = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error(),
    };
    let lit = match get_literal(&input) {
        Ok(lit) => lit,
        Err(token_stream) => return token_stream,
    };
    let query = lit.value();
    let (postgres_query, sqlite_query, mysql_query) = match build_queries(lit.span(), &query) {
        Ok(v) => v,
        Err(e) => return e,
    };
    quote_spanned! {
        lit.span() => {
            &[
                #postgres_query,
                #sqlite_query,
                #mysql_query
            ]
        }
    }
}

pub fn query(input: proc_macro::TokenStream) -> proc_macro2::TokenStream {
    let input: syn::Expr = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error(),
    };
    let lit = match get_literal(&input) {
        Ok(lit) => lit,
        Err(token_stream) => return token_stream,
    };
    let query = lit.value();
    let (postgres_query, sqlite_query, mysql_query) = match build_queries(lit.span(), &query) {
        Ok(v) => v,
        Err(e) => return e,
    };
    quote_spanned! {
        lit.span() => {
            static QUERIES: [&str; 3] = [
                #postgres_query,
                #sqlite_query,
                #mysql_query
            ];
            QUERIES[<D as SqlxDBNum>::pos()]
        }
    }
}
