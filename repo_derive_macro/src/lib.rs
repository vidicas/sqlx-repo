use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItem, Path, Signature};

fn get_database_constructor(trait_name: &Path) -> proc_macro2::TokenStream {
    quote!{ 
        async fn new_repository(database_url: &str) -> 
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
    }.into()
}

#[proc_macro_attribute]
pub fn repo(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let attrs: proc_macro2::TokenStream = attrs.into();
    let input: syn::Item = syn::parse(input).unwrap();

    match input {
        syn::Item::Impl(ref i) => {
            let mut func_sigs: Vec<&Signature> = vec![];
            let trait_name = &i.trait_.as_ref().unwrap().1;
            let database_constructor = get_database_constructor(trait_name);
            for item in i.items.as_slice() {
                if let ImplItem::Fn(f) = item {
                    func_sigs.push(&f.sig);
                }
            }
            quote! {
                pub trait #trait_name: #attrs{
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