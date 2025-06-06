use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, quote, quote_spanned};
use sql_bridge::{Ast, MySqlDialect, PostgreSqlDialect, SQLiteDialect, ToQuery};
use syn::{
    Expr, ExprLit, Ident, ImplItem, ItemImpl, Lit, LitStr, Path, PathArguments, ReturnType,
    Signature, Token, Type, TypeParam, parse_quote, parse_quote_spanned, punctuated::Punctuated,
    spanned::Spanned,
};

fn impl_dyn_repo(trait_name: &Path) -> proc_macro2::TokenStream {
    quote! {
        impl dyn #trait_name {
            pub async fn new(database_url: &str) ->
                Result<Box<dyn #trait_name>, Box<dyn std::error::Error + Send + Sync + 'static>>
            {
                let mut database_url = url::Url::parse(database_url)?;
                let mut params: std::collections::HashMap<std::borrow::Cow<str>, std::borrow::Cow<str>> = database_url.query_pairs().collect();
                let db: Box<dyn #trait_name> = match database_url.scheme() {
                    "sqlite" => {
                        if !params.contains_key("mode") {
                            params.insert("mode".into(), "rwc".into());
                        };
                        let query = params
                            .into_iter()
                            .map(|(key, value)| format!("{key}={value}"))
                            .collect::<Vec<_>>()
                            .join("&");
                        database_url.set_query(Some(&query));
                        Box::new(
                            DatabaseRepository::<sqlx::Sqlite>::new(
                                database_url.as_ref(),
                            )
                            .await?,
                        )
                    }
                    "postgres" => Box::new(
                        DatabaseRepository::<sqlx::Postgres>::new(
                            database_url.as_ref(),
                        )
                        .await?,
                    ),
                    "mysql" => Box::new(
                        DatabaseRepository::<sqlx::MySql>::new(
                            database_url.as_ref(),
                        )
                        .await?,
                    ),
                    unsupported => Err(format!("unsupported database: {unsupported}"))?,
                };
                Ok(db)
            }
        }
    }
}

/// Rewrite impl block for repository:
/// - Add generic D and where clause
/// - Rewrite async funcs into type-erased futures
/// - Preserver original spans
fn setup_generics_and_where_clause(input: &mut ItemImpl) {
    let where_clause = parse_quote! {
        where D: sqlx::Database + ::sqlx_repo::SqlxDBNum,
        // Types, that Database should support
        for<'e> i8: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> i16: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> i32: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> i64: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> f32: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> f64: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> String: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> &'e str: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> Vec<u8>: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> uuid::Uuid: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,
        for<'e> serde_json::Value: sqlx::Type<D> + sqlx::Encode<'e, D> + sqlx::Decode<'e, D>,

        // col access through usize index
        usize: sqlx::ColumnIndex<D::Row>,

        // sqlx bounds
        for<'e> &'e mut <D as sqlx::Database>::Connection: sqlx::Executor<'e, Database = D>,
        for<'e> &'e sqlx::Pool<D>: sqlx::Executor<'e, Database = D>,
        //for<'q> <D as sqlx::database::HasArguments<'q>>::Arguments: IntoArguments<'q, D>,
        D::QueryResult: std::fmt::Debug,

        // Database transactions should be deref-able into database connection
        for<'e> sqlx::Transaction<'e, D>: std::ops::Deref<Target = <D as sqlx::Database>::Connection>,
        for<'e> sqlx::Transaction<'e, D>: std::ops::DerefMut<Target = <D as sqlx::Database>::Connection>,
        for<'e> <D as sqlx::Database>::Arguments<'e>: sqlx::IntoArguments<'e, D>,

        // db connection should be able to run migrations
        D::Connection: sqlx::migrate::Migrate,
    };

    if let Type::Path(type_path) = &mut *input.self_ty {
        if let Some(segment) = type_path.path.segments.first_mut() {
            segment.arguments = PathArguments::AngleBracketed(parse_quote! { <D> })
        }
    }
    input
        .generics
        .params
        .push(syn::GenericParam::Type(TypeParam {
            ident: Ident::new("D", input.span()),
            attrs: vec![],
            colon_token: None,
            bounds: Punctuated::new(),
            eq_token: None,
            default: None,
        }));
    input.generics.where_clause = Some(where_clause);
    input.items.iter_mut().for_each(|item| {
        let f = match item {
            ImplItem::Fn(f) => {
                f
            },
            _ => return
        };
        // remove async keyword and replace fn span with async span
        f.sig.fn_token.span = match f.sig.asyncness.take() {
            Some(token) => token.span(),
            None => return
        };

        // update return type to Pin<Box<Future<Output=...>> + Send + '_>>;
        let output_span = f.sig.output.span();
        let output_type: Type = match &f.sig.output {
            ReturnType::Default => parse_quote_spanned!{
                output_span => std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>
            },
            ReturnType::Type(_, ty) => {
                let tokens = ty.to_token_stream();
                parse_quote_spanned!{
                    output_span => std::pin::Pin<Box<dyn std::future::Future<Output = #tokens> + Send + '_>>
                }
            }
        };
        f.sig.output = ReturnType::Type(Token![->](output_span), Box::new(output_type));
        // wrap function body into Box::pin(async move { #body })
        let block_span = f.block.span();
        let block = &f.block;
        f.block = parse_quote_spanned!{
            block_span => { Box::pin(async move #block ) }
        };
    });
}

#[proc_macro_attribute]
pub fn repo(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let attrs: proc_macro2::TokenStream = attrs.into();
    let mut input: syn::Item = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error().into(),
    };
    let input_span = input.span();

    match input {
        syn::Item::Impl(ref mut i) => {
            // macro will inject own generic bounds, where clause and type for DatabaseRepository
            if i.generics.lt_token.is_some() {
                return quote_spanned! {
                    i.generics.params.span() => compile_error!("Repo implementation should not have any generics");
                }.into()
            }
            if i.generics.where_clause.is_some() {
                return quote_spanned! {
                    i.generics.where_clause.span() => compile_error!("Repo implementation should not have where clause");
                }.into()
            }
            let trait_name = match i.trait_.as_ref() {
                Some(trait_name) => trait_name.1.clone(),
                None => {
                    return quote_spanned! {
                        input.span() => compile_error!("works only for 'impl Trait for DatabaseRepository'");
                    }.into()
                }
            };
            // rewrite impl block
            setup_generics_and_where_clause(i);
            let database_constructor = impl_dyn_repo(&trait_name);
            let trait_func_sigs = i.items.iter().filter_map(|func| {
                if let ImplItem::Fn(f) = func {
                    Some(&f.sig)
                } else {
                    None
                }
            }).collect::<Vec<&Signature>>();
            let private_impl = quote!{
                use sqlx_repo::__hidden::query;
                #database_constructor
            };
            let repo_trait = quote! {
                pub trait #trait_name: #attrs {
                    #(#trait_func_sigs;)*
                }
            };
            let db_repo_decl = input.to_token_stream();
            quote_spanned! {
                input_span =>
                const _:() = {
                    #db_repo_decl
                    #private_impl
                };
                #repo_trait
            }
        }
        other => quote_spanned! {
            other.span() =>
            compile_error!("works only for 'impl Trait for DatabaseRepository'");
        },
    }
    .into()
}

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

fn get_literal(input: &syn::Expr) -> Result<&LitStr, TokenStream> {
    match input {
        Expr::Lit(ExprLit {
            lit: Lit::Str(lit), ..
        }) => Ok(lit),
        Expr::Group(group) => get_literal(&group.expr),
        u => {
            let compile_error = format!("expected string, got: {u:#?}");
            Err(quote! { compile_error!(#compile_error); }.into())
        }
    }
}

fn build_queries(span: Span, query: &str) -> Result<(String, String, String), TokenStream> {
    let ast_list = match sql_bridge::Ast::parse(query) {
        Ok(ast) => ast,
        Err(e) => {
            let err = format!("failed to parse query: {e}");
            return Err(quote_spanned! {
                span => compile_error!(#err)
            }
            .into());
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
            return Err(quote_spanned! { span => compile_error!(#err) }.into());
        }
    };
    if query_list.len() != 3 {
        return Err(quote!(compile_error!("query list length is expected to be 3")).into());
    }
    let mysql_query = query_list.pop().unwrap();
    let sqlite_query = query_list.pop().unwrap();
    let postgres_query = query_list.pop().unwrap();
    Ok((postgres_query, sqlite_query, mysql_query))
}

#[proc_macro]
pub fn gen_query(input: TokenStream) -> TokenStream {
    let input: syn::Expr = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error().into(),
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
    .into()
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input: syn::Expr = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error().into(),
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
    .into()
}
