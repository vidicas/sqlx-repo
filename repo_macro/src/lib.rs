use proc_macro::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::{spanned::Spanned, ImplItem, ImplItemFn, ItemImpl, Path, ReturnType};

fn get_database_constructor(trait_name: &Path) -> proc_macro2::TokenStream {
    quote! {
        pub async fn new_repository(database_url: &str) ->
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

fn setup_generics_and_where_clause(
    input: &ItemImpl,
    trait_name: &Path,
) -> proc_macro2::TokenStream {
    let type_name = &*input.self_ty;
    let items = input
        .items
        .iter()
        .map(|item| convert_async_func(item, true))
        .collect::<Vec<proc_macro2::TokenStream>>();
    let where_clause = quote! {
        D: sqlx::Database,
        // Types, that Database should support
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

        // db connection should be able to run migrations
        D::Connection: sqlx::migrate::Migrate,
    };
    quote! {
        impl<D> #trait_name for #type_name<D> where #where_clause {
            #(#items)*
        }
    }
}

fn convert_async_func(item: &ImplItem, with_body: bool) -> proc_macro2::TokenStream {
    if let ImplItem::Fn(ImplItemFn {
        ref sig, ref block, ..
    }) = item
    {
        println!("{item:#?}");
        if sig.asyncness.is_some() {
            let name = &sig.ident;
            let generics = sig
                .generics
                .params
                .iter()
                .map(|i| i.to_token_stream())
                .collect::<Vec<proc_macro2::TokenStream>>();
            let inputs = sig
                .inputs
                .iter()
                .map(|input| input.to_token_stream())
                .collect::<Vec<proc_macro2::TokenStream>>();
            let output = match &sig.output {
                ReturnType::Default => quote! { () },
                ReturnType::Type(_, ty) => ty.to_token_stream(),
            };
            let output = quote! {
                std::pin::Pin<Box<dyn std::future::Future<Output = #output> + '_>>
            };
            let body = match with_body {
                false => quote! {},
                true => quote! {
                    {
                        unimplemented!()
                    }
                },
            };
            return quote! {
                fn #name<#(#generics,)*>(#(#inputs,)*) -> #output #body
            };
        }
    }
    quote! { #item }
}

#[proc_macro_attribute]
pub fn repo(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let attrs: proc_macro2::TokenStream = attrs.into();
    let input: syn::Item = syn::parse(input).unwrap();

    match input {
        syn::Item::Impl(ref i) => {
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
            let mut func_sigs: Vec<proc_macro2::TokenStream> = vec![];
            let trait_name = &i.trait_.as_ref().unwrap().1;
            let database_constructor = get_database_constructor(trait_name);
            let input = setup_generics_and_where_clause(i, trait_name);
            for item in i.items.as_slice() {
                if let ImplItem::Fn(f) = item {
                    func_sigs.push(convert_async_func(item, false))
                }
            }
            quote! {
                pub trait #trait_name: #attrs {
                    #(#func_sigs;)*
                }

                #input

                #database_constructor
            }
        }
        _ => quote! {
            compile_error!("works only for 'impl Trait for Something'");
        },
    }
    .into()
}
