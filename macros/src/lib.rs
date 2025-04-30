use proc_macro::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::{
    parse_quote, parse_quote_spanned, punctuated::Punctuated, spanned::Spanned, 
    Expr, Ident, ImplItem, ItemImpl, Lit, ExprLit, Path, 
    PathArguments, ReturnType, Signature, Token, Type, TypeParam
};

fn get_database_constructor(trait_name: &Path) -> proc_macro2::TokenStream {
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
                                Box::new(sea_query::SqliteQueryBuilder),
                                Box::new(sea_query::SqliteQueryBuilder),
                            )
                            .await?,
                        )
                    }
                    "postgres" => Box::new(
                        DatabaseRepository::<sqlx::Postgres>::new(
                            database_url.as_ref(),
                            Box::new(sea_query::PostgresQueryBuilder),
                            Box::new(sea_query::PostgresQueryBuilder),
                        )
                        .await?,
                    ),
                    "mysql" => Box::new(
                        DatabaseRepository::<sqlx::MySql>::new(
                            database_url.as_ref(),
                            Box::new(sea_query::MysqlQueryBuilder),
                            Box::new(sea_query::MysqlQueryBuilder),
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
        where D: sqlx::Database + __private::SqlxDBNum,
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

        // sea-query-binder
        for<'e> sea_query_binder::SqlxValues: sqlx::IntoArguments<'e, D>,
        

        // sqlx bounds
        for<'c> &'c mut <D as sqlx::Database>::Connection: sqlx::Executor<'c, Database = D>,
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
    let mut input: syn::Item = syn::parse(input).unwrap();
    
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
            let database_constructor = get_database_constructor(&trait_name);
            let trait_func_sigs = i.items.iter().filter_map(|func| {
                if let ImplItem::Fn(f) = func {
                    Some(&f.sig)
                } else {
                    None
                }
            }).collect::<Vec<&Signature>>();
            
            let private_module = quote!{
                use macros::query;
                use super::*;
                
                pub trait SqlxDBNum {
                    fn pos() -> usize {
                        usize::MAX
                    }
                }

                impl SqlxDBNum for sqlx::Postgres {
                    fn pos() -> usize {
                        0
                    }
                }

                impl SqlxDBNum for sqlx::Sqlite {
                    fn pos() -> usize {
                        1
                    }
                }

                impl SqlxDBNum for sqlx::MySql {
                    fn pos() -> usize {
                        2
                    }
                }

                pub(crate) struct Query<D: SqlxDBNum> {
                    _marker: std::marker::PhantomData<D>,
                }

                impl<D: SqlxDBNum> Query<D> {
                    pub fn query(options: &'static [&'static str]) -> &'static str {
                        options[D::pos()]                            
                    }
                }

                pub trait #trait_name: #attrs {
                    #(#trait_func_sigs;)*
                }

                #database_constructor
            };

            let db_repo_decl = input.to_token_stream();
            quote_spanned! { 
                input.span() => 
                mod __private {
                    #private_module

                    #db_repo_decl
                }
                use __private::#trait_name; 
            }
        }
        _ => quote! {
            compile_error!("works only for 'impl Trait for DatabaseRepository'");
        },
    }
    .into()
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input: syn::Expr = syn::parse(input).expect("expected expression");
    match input {
        Expr::Lit(ExprLit{lit: Lit::Str(lit), ..})=> {
            let query = lit.value();
            let mut ast = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect{}, &query)
                .expect("failed to parse query");
            if ast.len() != 1 {
                panic!("expected exactly one statement in the query");
            }
            let ast = ast.pop().unwrap();
            println!("ast: {}", ast);
        },
        _ => {
            return quote!{
                compile_error!("expected string");
            }.into()
        }
    }
    quote!{ 
        {
            static QUERIES: [&'static str; 3] = [
                "select postgres",
                "select sqlite",
                "select mysql",
            ];
            __private::Query::<D>::query(QUERIES.as_slice())
        }
    }.into()
}