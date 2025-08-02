use proc_macro2::Span;
use quote::{ToTokens as _, quote_spanned};
use syn::{
    Token, WhereClause, parse_quote, parse_quote_spanned,
    spanned::Spanned as _,
    visit_mut::{self, VisitMut},
};

struct Expander {
    error: Option<ValidationError>,
    trait_name: Option<syn::Ident>,
    signatures: Vec<syn::Signature>,
}

enum ValidationError {
    NoGenericsAllowed(Span),
    NoWhereClauseAllowed(Span),
    NotTraitImplBlock(Span),
    NoAssocTypesAllowed(Span),
    IncorrectImplTypeName(Span),
    UnsupportedFuncGenericType(Span),
}

impl ValidationError {
    fn to_compile_error(&self) -> proc_macro2::TokenStream {
        let span = self.span();
        let error = self.error();
        quote_spanned! (
            span => compile_error!(#error)
        )
    }

    fn span(&self) -> Span {
        *match self {
            Self::NoGenericsAllowed(span) => span,
            Self::NoWhereClauseAllowed(span) => span,
            Self::NotTraitImplBlock(span) => span,
            Self::NoAssocTypesAllowed(span) => span,
            Self::IncorrectImplTypeName(span) => span,
            Self::UnsupportedFuncGenericType(span) => span,
        }
    }

    fn error(&self) -> &'static str {
        match self {
            Self::NoGenericsAllowed(_) => "No generics allowed in trait implementation",
            Self::NoWhereClauseAllowed(_) => "No where clause allowed in trait implementation",
            Self::NotTraitImplBlock(_) => "Only trait implementation blocks are supported",
            Self::NoAssocTypesAllowed(_) => "Trait associated types are not supported",
            Self::IncorrectImplTypeName(_) => "Incorrect type name in trait implementation",
            Self::UnsupportedFuncGenericType(_) => "only type and lifetimes supported as generics",
        }
    }
}

impl Expander {
    fn new() -> Self {
        Self {
            error: None,
            trait_name: None,
            signatures: vec![],
        }
    }

    fn where_clause(&self) -> WhereClause {
        // FIXME: this if a bruteforce solution, need to find a better way to represent types which are supported by
        // database
        parse_quote! (
            where D: sqlx::Database + sqlx_repo::SqlxDBNum,
            // Acquire Extension which adds `start_transaction` method, which forces `BEGIN IMMEDIATE` for sqlite
            for<'e> sqlx::Pool<D>: sqlx_repo::AcquireExt<D>,
            // Decodable Types
            for<'e> i8: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> i16: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> i32: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> i64: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> f32: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> f64: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> String: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> &'e str: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> Vec<u8>: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> &'e [u8]: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> uuid::Uuid: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Decode<'e, D>,
            for<'e> serde_json::Value: sqlx::Type<D> + sqlx::Decode<'e, D>,

            // Encodable Types
            for<'e> i8: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> i16: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> i32: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> i64: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> f32: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> f64: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> String: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e str: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Vec<u8>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e [u8]: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> uuid::Uuid: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> serde_json::Value: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e uuid::Uuid: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> &'e serde_json::Value: sqlx::Type<D> + sqlx::Encode<'e, D>,

            // Encodable optional
            for<'e> Option<i8>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<i16>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<i32>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<i64>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<f32>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<f64>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<String>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e str>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<Vec<u8>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<uuid::Uuid>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<sqlx::types::Json<serde_json::Value>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<chrono::DateTime<chrono::Utc>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<chrono::NaiveDateTime>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e uuid::Uuid>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e sqlx::types::Json<serde_json::Value>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e chrono::DateTime<chrono::Utc>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e chrono::NaiveDateTime>: sqlx::Type<D> + sqlx::Encode<'e, D>,
            for<'e> Option<&'e serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,

            // col access through usize index
            usize: sqlx::ColumnIndex<D::Row>,

            // sqlx bounds
            for<'e> &'e mut <D as sqlx::Database>::Connection: sqlx::Executor<'e, Database = D>,
            for<'e> &'e sqlx::Pool<D>: sqlx::Executor<'e, Database = D>,
            D::QueryResult: std::fmt::Debug,

            // Database transactions should be deref-able into database connection
            for<'e> sqlx::Transaction<'e, D>: std::ops::Deref<Target = <D as sqlx::Database>::Connection>,
            for<'e> sqlx::Transaction<'e, D>: std::ops::DerefMut<Target = <D as sqlx::Database>::Connection>,

            // query execution
            for<'e> <D as sqlx::Database>::Arguments<'e>: sqlx::IntoArguments<'e, D>,

            // db connection should be able to run migrations
            D::Connection: sqlx::migrate::Migrate,
        )
    }

    fn visit_func(&mut self, func: &mut syn::ImplItemFn) {
        // if function is not async, leave it as it is
        if func.sig.asyncness.take().is_none() {
            self.signatures.push(func.sig.clone());
            return;
        }

        // rewrite function arguments with explicit lifetimes, collect all existing lifetimes
        // for signature transformation
        let mut lifetimes = func
            .sig
            .inputs
            .iter_mut()
            .fold((&mut 0, vec![]), |(pos, mut acc), arg| {
                self.visit_function_arg(pos, arg, &mut acc);
                (pos, acc)
            })
            .1;

        // add additional lifetime to lifetimes which will be used for return type in future bound
        lifetimes
            .push(syn::parse_str("'future_lifetime").expect("failed to parse future lifetime"));

        // rewrite function signature
        self.visit_func_signature(&mut func.sig, lifetimes.as_slice());

        // rewrite function block into pinned box
        self.visit_func_block(&mut func.block);
    }

    fn get_lifetime(pos: &mut usize) -> syn::Lifetime {
        let lifetime: syn::Lifetime =
            syn::parse_str(&format!("'life{pos}")).expect("failed to parse lifetime");
        *pos += 1;
        lifetime
    }

    // rewrite function argument lifetimes, collect all lifetimes
    fn visit_function_arg(
        &mut self,
        pos: &mut usize,
        arg: &mut syn::FnArg,
        acc: &mut Vec<syn::Lifetime>,
    ) {
        match arg {
            syn::FnArg::Receiver(receiver) => {
                if let Some((and_token, None)) = receiver.reference {
                    let lifetime = Self::get_lifetime(pos);
                    receiver.reference = Some((and_token, Some(lifetime.clone())));
                    acc.push(lifetime);
                }
            }
            syn::FnArg::Typed(typed) => self.visit_type(pos, &mut typed.ty, acc),
        }
    }

    fn visit_type(&mut self, pos: &mut usize, ty: &mut syn::Type, acc: &mut Vec<syn::Lifetime>) {
        match ty {
            syn::Type::Reference(rf) => {
                if rf.lifetime.is_none() {
                    let lifetime = Self::get_lifetime(pos);
                    rf.lifetime = Some(lifetime.clone());
                    acc.push(lifetime);
                }
            }
            syn::Type::Path(path) => self.visit_path(pos, &mut path.path, acc),
            syn::Type::Tuple(tuple) => {
                for ty in tuple.elems.iter_mut() {
                    self.visit_type(pos, ty, acc);
                }
            }
            _ => (),
        }
    }

    fn visit_path(&mut self, pos: &mut usize, path: &mut syn::Path, acc: &mut Vec<syn::Lifetime>) {
        for segment in path.segments.iter_mut() {
            if let syn::PathArguments::AngleBracketed(params) = &mut segment.arguments {
                for arg in params.args.iter_mut() {
                    if let syn::GenericArgument::Type(ty) = arg {
                        self.visit_type(pos, ty, acc)
                    }
                }
            }
        }
    }

    // rewrite function signature
    // return type will be boxed future
    // where clause will be extended with bounds
    fn visit_func_signature(
        &mut self,
        func_signature: &mut syn::Signature,
        lifetimes: &[syn::Lifetime],
    ) {
        // update return type
        let output_span = func_signature.output.span();
        let output: syn::Type = match &func_signature.output {
            syn::ReturnType::Default => {
                parse_quote_spanned! (
                    output_span => std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'future_lifetime>>
                )
            }
            syn::ReturnType::Type(_, ty) => {
                parse_quote_spanned! (
                    output_span => std::pin::Pin<Box<dyn std::future::Future<Output = #ty> + Send + 'future_lifetime>>
                )
            }
        };
        func_signature.output = syn::ReturnType::Type(Token![->](output_span), Box::new(output));

        // update function generics with
        let mut type_generics = vec![];
        while matches!(
            func_signature.generics.params.last(),
            Some(syn::GenericParam::Type(_))
        ) {
            type_generics.push(func_signature.generics.params.pop().unwrap().into_tuple().0);
        }

        for lifetime in lifetimes {
            func_signature
                .generics
                .params
                .push(syn::GenericParam::Lifetime(parse_quote!(#lifetime)));
        }

        while let Some(type_generic) = type_generics.pop() {
            func_signature.generics.params.push(type_generic);
        }

        if func_signature.generics.where_clause.is_none() {
            func_signature.generics.where_clause = Some(syn::WhereClause {
                where_token: Default::default(),
                predicates: syn::punctuated::Punctuated::new(),
            });
        }

        // for each generic parameter specify 'future_lifetime as a superclass
        // add bounds to all generics
        if let Some(where_clause) = &mut func_signature.generics.where_clause {
            for param in func_signature.generics.params.iter() {
                let (ident, param) = match param {
                    syn::GenericParam::Type(param) => {
                        let ident = &param.ident;
                        (ident, quote::quote!(#ident))
                    }
                    syn::GenericParam::Lifetime(lt) => {
                        let ident = &lt.lifetime.ident;
                        let lifetime = &lt.lifetime;
                        (ident, quote::quote!(#lifetime))
                    }
                    _ => {
                        self.error =
                            Some(ValidationError::UnsupportedFuncGenericType(param.span()));
                        return;
                    }
                };
                if *ident == "future_lifetime" {
                    continue;
                }
                where_clause
                    .predicates
                    .push(parse_quote!(#param: 'future_lifetime));
            }
        }

        // store function signature for trait generation
        self.signatures.push(func_signature.clone());
    }

    fn visit_func_block(&mut self, func_block: &mut syn::Block) {
        let tokens = func_block.to_token_stream();
        *func_block = parse_quote_spanned!(func_block.span() => {
            Box::pin(async move #tokens)
        });
    }

    fn generate_trait_implementation(
        &self,
        attributes: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let trait_name = self
            .trait_name
            .as_ref()
            .expect("expander was not able to extract trait name");
        let func_signatures = &self.signatures;
        quote::quote! (
            pub trait #trait_name: #attributes {
                #(#func_signatures;)*
            }
        )
    }

    fn generate_trait_constuctor(&self) -> proc_macro2::TokenStream {
        let trait_name = &self
            .trait_name
            .as_ref()
            .expect("expander was not able to extract trait name");
        quote::quote! (
            impl dyn #trait_name {
                pub async fn new(database_url: &str) ->
                    Result<Box<dyn #trait_name>, Box<dyn std::error::Error + Send + Sync + 'static>>
                {
                    let mut database_url = url::Url::parse(database_url)?;
                    let mut params: std::collections::HashMap<String, String> = database_url
                        .query_pairs()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect();
                    let db: Box<dyn #trait_name> = match database_url.scheme() {
                        "sqlite" => {
                            database_url.set_query(None);
                            let mut sqlite_options: sqlx::sqlite::SqliteConnectOptions = database_url
                                .as_str()
                                .parse()?;
                            sqlite_options = sqlite_options
                                .foreign_keys(true)
                                .create_if_missing(true);
                            if let Some(param) = params.get("foreign_keys") && (param == "off" || param == "false") {
                                sqlite_options = sqlite_options.foreign_keys(false);
                            }
                            let pool = sqlx::Pool::<sqlx::Sqlite>::connect_with(sqlite_options).await?;
                            Box::new(
                                DatabaseRepository::new( database_url.as_ref(), pool).await?
                            )
                        }
                        "postgres" => {
                            let pool = sqlx::Pool::<sqlx::Postgres>::connect(database_url.as_str()).await?;
                            Box::new(
                                DatabaseRepository::new( database_url.as_str(), pool ).await?
                            )
                        },
                        "mysql" => {
                            let pool = sqlx::Pool::<sqlx::MySql>::connect(database_url.as_str()).await?;
                            Box::new(
                                DatabaseRepository::new(database_url.as_str(), pool).await?
                            )
                        },
                        unsupported => Err(format!("unsupported database: {unsupported}"))?,
                    };
                    Ok(db)
                }
            }
        )
    }
}

impl VisitMut for Expander {
    fn visit_item_mut(&mut self, item: &mut syn::Item) {
        match item {
            syn::Item::Impl(i) if !i.generics.params.is_empty() => {
                self.error = Some(ValidationError::NoGenericsAllowed(i.span()));
            }
            syn::Item::Impl(i) if i.generics.where_clause.is_some() => {
                self.error = Some(ValidationError::NoWhereClauseAllowed(i.span()));
            }
            syn::Item::Impl(i)
                if i.self_ty.to_token_stream().to_string() != "DatabaseRepository" =>
            {
                self.error = Some(ValidationError::IncorrectImplTypeName(i.span()))
            }
            syn::Item::Impl(i) if i.trait_.is_some() => {
                let trait_path = &i.trait_.as_ref().unwrap().1;
                if trait_path.segments.first().unwrap().arguments != syn::PathArguments::None {
                    self.error = Some(ValidationError::NoGenericsAllowed(i.span()));
                    return;
                }
                let trait_name = trait_path.get_ident();
                if trait_name.is_none() {
                    self.error = Some(ValidationError::NotTraitImplBlock(i.span()));
                    return;
                }
                self.trait_name = trait_name.cloned();
                visit_mut::visit_item_mut(self, item)
            }
            _ => {
                self.error = Some(ValidationError::NotTraitImplBlock(item.span()));
            }
        }
    }

    // impl Trait for DatabaseRepository
    fn visit_item_impl_mut(&mut self, i: &mut syn::ItemImpl) {
        fn type_param<T: syn::parse::Parse>(param: &str) -> T {
            match syn::parse_str(param) {
                Ok(value) => value,
                Err(e) => {
                    unreachable!("failed to parse type parameter in item implementation: {e}")
                }
            }
        }

        // append <D> after impl
        i.generics
            .params
            .push(syn::GenericParam::Type(type_param("D")));

        // append <D> after DatabaseRepository
        match &mut *i.self_ty {
            syn::Type::Path(path) => {
                let path_segment = path.path.segments.first_mut().unwrap();
                path_segment.arguments = syn::PathArguments::AngleBracketed(type_param("<D>"))
            }
            _ => {
                self.error = Some(ValidationError::NotTraitImplBlock(i.span()));
                return;
            }
        };

        // set where clause
        i.generics.where_clause = Some(self.where_clause());
        visit_mut::visit_item_impl_mut(self, i)
    }

    fn visit_impl_item_type_mut(&mut self, i: &mut syn::ImplItemType) {
        self.error = Some(ValidationError::NoAssocTypesAllowed(i.span()));
    }

    // visit functions
    fn visit_impl_item_mut(&mut self, i: &mut syn::ImplItem) {
        if let syn::ImplItem::Fn(func) = i {
            return self.visit_func(func);
        }
        // NOTE: do we really need to dive deeper?
        visit_mut::visit_impl_item_mut(self, i);
    }
}

pub fn expand(
    attrs: proc_macro2::TokenStream,
    item: &mut syn::Item,
) -> Result<
    (
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    ),
    proc_macro2::TokenStream,
> {
    let mut expander = Expander::new();
    expander.visit_item_mut(item);

    if let Some(err) = expander.error.take() {
        return Err(err.to_compile_error());
    }
    Ok((
        item.to_token_stream(),
        expander.generate_trait_implementation(attrs),
        expander.generate_trait_constuctor(),
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use quote::quote;

    fn prettify(item: syn::Item) -> String {
        let file = syn::File {
            shebang: None,
            attrs: vec![],
            items: vec![item],
        };
        prettyplease::unparse(&file)
    }

    #[test]
    fn test_expand() {
        let code = quote! (
            impl Repo for DatabaseRepository {
                async fn elided_lifetime_on_receiver<T>(&self, arg: &T) -> Result<()> {
                    Ok(())
                }

                async fn explicit_lifetime_on_receiver<'a, 'b, T>(&'a self, arg: &'b T) -> Result<()> {
                    Ok(())
                }

                async fn with_where_clause<'a, 'b, T>(&'a self, arg: &'b T) -> Result<()>
                    where 'a: 'b
                {
                    Ok(())
                }

                async fn bound_without_where<'a, 'b: 'a, T>(&'a self, arg: &'b T) -> Result<()> {
                    Ok(())
                }

                fn non_async_elided<T>(&self, arg: &T) -> Result<()> {
                    Ok(())
                }

                fn non_async_explicit<'a, 'b, T>(&'a self, arg: &'b T) -> Result<()> {
                    Ok(())
                }

                async fn nested_lifetime(&self, arg: Option<&str>) -> Result<()> {
                    Ok(())
                }

                async fn nested_nested_lifetime(&self, arg: Option<Option<&str>>) -> Result<()> {
                    Ok(())
                }
                async fn nested_multiple<'a, 'b>(&self, arg: Result<(&'a str, &str), (&str, Option<(&'b str, &str)>)>) -> Result<()> {
                    Ok(())
                }
            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();

        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);

        assert!(result.is_ok(), "{}", result.unwrap_err().to_string());

        let (expanded_item, trait_impl, _constructor) = result.unwrap();

        let pretty_expanded_item = prettify(syn::parse2(expanded_item).unwrap());
        let expected = "\
impl<D> Repo for DatabaseRepository<D>
where
    D: sqlx::Database + sqlx_repo::SqlxDBNum,
    for<'e> sqlx::Pool<D>: sqlx_repo::AcquireExt<D>,
    for<'e> i8: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> i16: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> i32: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> i64: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> f32: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> f64: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> String: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> &'e str: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> Vec<u8>: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> &'e [u8]: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> uuid::Uuid: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> serde_json::Value: sqlx::Type<D> + sqlx::Decode<'e, D>,
    for<'e> i8: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> i16: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> i32: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> i64: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> f32: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> f64: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> String: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e str: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Vec<u8>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e [u8]: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> uuid::Uuid: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> sqlx::types::Json<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> serde_json::Value: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e uuid::Uuid: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e sqlx::types::Json<
        serde_json::Value,
    >: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e chrono::DateTime<chrono::Utc>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e chrono::NaiveDateTime: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> &'e serde_json::Value: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<i8>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<i16>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<i32>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<i64>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<f32>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<f64>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<String>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<&'e str>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<Vec<u8>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<uuid::Uuid>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<
        sqlx::types::Json<serde_json::Value>,
    >: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<chrono::DateTime<chrono::Utc>>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<chrono::NaiveDateTime>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<&'e uuid::Uuid>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<
        &'e sqlx::types::Json<serde_json::Value>,
    >: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<
        &'e chrono::DateTime<chrono::Utc>,
    >: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<&'e chrono::NaiveDateTime>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    for<'e> Option<&'e serde_json::Value>: sqlx::Type<D> + sqlx::Encode<'e, D>,
    usize: sqlx::ColumnIndex<D::Row>,
    for<'e> &'e mut <D as sqlx::Database>::Connection: sqlx::Executor<'e, Database = D>,
    for<'e> &'e sqlx::Pool<D>: sqlx::Executor<'e, Database = D>,
    D::QueryResult: std::fmt::Debug,
    for<'e> sqlx::Transaction<
        'e,
        D,
    >: std::ops::Deref<Target = <D as sqlx::Database>::Connection>,
    for<'e> sqlx::Transaction<
        'e,
        D,
    >: std::ops::DerefMut<Target = <D as sqlx::Database>::Connection>,
    for<'e> <D as sqlx::Database>::Arguments<'e>: sqlx::IntoArguments<'e, D>,
    D::Connection: sqlx::migrate::Migrate,
{
    fn elided_lifetime_on_receiver<'life0, 'life1, 'future_lifetime, T>(
        &'life0 self,
        arg: &'life1 T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
        T: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn explicit_lifetime_on_receiver<'a, 'b, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn with_where_clause<'a, 'b, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'b,
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn bound_without_where<'a, 'b: 'a, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn non_async_elided<T>(&self, arg: &T) -> Result<()> {
        Ok(())
    }
    fn non_async_explicit<'a, 'b, T>(&'a self, arg: &'b T) -> Result<()> {
        Ok(())
    }
    fn nested_lifetime<'life0, 'life1, 'future_lifetime>(
        &'life0 self,
        arg: Option<&'life1 str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn nested_nested_lifetime<'life0, 'life1, 'future_lifetime>(
        &'life0 self,
        arg: Option<Option<&'life1 str>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
    fn nested_multiple<'a, 'b, 'life0, 'life1, 'life2, 'life3, 'future_lifetime>(
        &'life0 self,
        arg: Result<
            (&'a str, &'life1 str),
            (&'life2 str, Option<(&'b str, &'life3 str)>),
        >,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
        'life2: 'future_lifetime,
        'life3: 'future_lifetime,
    {
        Box::pin(async move { Ok(()) })
    }
}
";

        assert!(
            pretty_expanded_item == expected,
            "got:\n{pretty_expanded_item}\nexpected:\n{expected}\n"
        );

        let expected = "\
pub trait Repo {
    fn elided_lifetime_on_receiver<'life0, 'life1, 'future_lifetime, T>(
        &'life0 self,
        arg: &'life1 T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
        T: 'future_lifetime;
    fn explicit_lifetime_on_receiver<'a, 'b, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime;
    fn with_where_clause<'a, 'b, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'b,
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime;
    fn bound_without_where<'a, 'b: 'a, 'future_lifetime, T>(
        &'a self,
        arg: &'b T,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        T: 'future_lifetime;
    fn non_async_elided<T>(&self, arg: &T) -> Result<()>;
    fn non_async_explicit<'a, 'b, T>(&'a self, arg: &'b T) -> Result<()>;
    fn nested_lifetime<'life0, 'life1, 'future_lifetime>(
        &'life0 self,
        arg: Option<&'life1 str>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime;
    fn nested_nested_lifetime<'life0, 'life1, 'future_lifetime>(
        &'life0 self,
        arg: Option<Option<&'life1 str>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'life0: 'future_lifetime,
        'life1: 'future_lifetime;
    fn nested_multiple<'a, 'b, 'life0, 'life1, 'life2, 'life3, 'future_lifetime>(
        &'life0 self,
        arg: Result<
            (&'a str, &'life1 str),
            (&'life2 str, Option<(&'b str, &'life3 str)>),
        >,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    >
    where
        'a: 'future_lifetime,
        'b: 'future_lifetime,
        'life0: 'future_lifetime,
        'life1: 'future_lifetime,
        'life2: 'future_lifetime,
        'life3: 'future_lifetime;
}
";

        let generated_trait = prettify(syn::parse2(trait_impl).unwrap());
        assert!(
            generated_trait == expected,
            "got:\n{generated_trait}\nexpected:\n{expected}\n"
        );
    }

    #[test]
    fn validate_trait_with_generic() {
        let code = quote! (
            impl Repo<'a> for DatabaseRepository {

            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let mut expander = Expander::new();

        expander.visit_item_mut(&mut syntax_tree);
        assert!(expander.error.is_some());
        let error = expander.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"No generics allowed in trait implementation\")"
        )
    }

    #[test]
    fn validate_type_with_generic() {
        let code = quote!(
            impl<D> Repo for DatabaseRepository<D> {}
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "compile_error ! (\"No generics allowed in trait implementation\")"
        )
    }

    #[test]
    fn validate_trait_with_where() {
        let code = quote! (
            impl Repo for DatabaseRepository where 'a: 'b {

            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "compile_error ! (\"No where clause allowed in trait implementation\")"
        )
    }

    #[test]
    fn validate_trait_with_assoc_type() {
        let code = quote! (
            impl Repo for DatabaseRepository {
                type Assoc = ();

            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "compile_error ! (\"Trait associated types are not supported\")"
        )
    }

    #[test]
    fn validate_not_trait_impl() {
        let code = quote! (
            impl DatabaseRepository {
            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "compile_error ! (\"Only trait implementation blocks are supported\")"
        )
    }

    #[test]
    fn validate_invalid_type_name() {
        let code = quote! (
            impl Repo for DatabaseRepo {
            }
        );

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let result = expand(proc_macro2::TokenStream::new(), &mut syntax_tree);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "compile_error ! (\"Incorrect type name in trait implementation\")"
        )
    }
}
