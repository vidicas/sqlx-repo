use proc_macro::TokenStream;
use quote::ToTokens as _;
use syn::{
    Token, WhereClause, parse_quote, parse_quote_spanned,
    spanned::Spanned as _,
    visit_mut::{self, VisitMut},
};

struct Expander {
    trait_name: Option<proc_macro2::Ident>,
    signatures: Vec<syn::Signature>,
}

enum LifetimeType {
    Preexisted(syn::Lifetime),
    Generated(syn::Lifetime),
}

impl Expander {
    fn new() -> Self {
        Self {
            trait_name: None,
            signatures: vec![],
        }
    }

    fn where_clause(&self) -> WhereClause {
        parse_quote! {
            where D: ::sqlx::Database + ::sqlx_repo::SqlxDBNum,
            // Types, that Database should support
            for<'e> i8: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> i16: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> i32: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> i64: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> f32: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> f64: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> String: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> &'e str: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> Vec<u8>: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> uuid::Uuid: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> ::sqlx::types::Json<serde_json::Value>: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> chrono::DateTime<chrono::Utc>: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> chrono::NaiveDateTime: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
            for<'e> serde_json::Value: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,

            // col access through usize index
            usize: ::sqlx::ColumnIndex<D::Row>,

            // ::sqlx bounds
            for<'e> &'e mut <D as ::sqlx::Database>::Connection: ::sqlx::Executor<'e, Database = D>,
            for<'e> &'e ::sqlx::Pool<D>: ::sqlx::Executor<'e, Database = D>,
            //for<'q> <D as ::sqlx::database::HasArguments<'q>>::Arguments: IntoArguments<'q, D>,
            D::QueryResult: std::fmt::Debug,

            // Database transactions should be deref-able into database connection
            for<'e> ::sqlx::Transaction<'e, D>: std::ops::Deref<Target = <D as ::sqlx::Database>::Connection>,
            for<'e> ::sqlx::Transaction<'e, D>: std::ops::DerefMut<Target = <D as ::sqlx::Database>::Connection>,
            for<'e> <D as ::sqlx::Database>::Arguments<'e>: ::sqlx::IntoArguments<'e, D>,

            // db connection should be able to run migrations
            D::Connection: ::sqlx::migrate::Migrate,
        }
    }

    fn visit_func(&mut self, func: &mut syn::ImplItemFn) {
        // if function is not async, leave it as it is
        if func.sig.asyncness.take().is_none() {
            self.signatures.push(func.sig.clone());
            return;
        }

        // rewrite function arguments with explicit lifetimes, collect all existing lifetimes
        // for signature transformation
        let lifetimes = func
            .sig
            .inputs
            .iter_mut()
            .enumerate()
            .filter_map(|(pos, arg)| self.visit_function_arg(pos, arg))
            .collect::<Vec<_>>();

        // rewrite function signature
        self.visit_func_signature(&mut func.sig, lifetimes.as_slice());

        // rewrite function block into pinned box
        self.visit_func_block(&mut func.block);
    }

    // rewrite function argument lifetimes, collect all lifetimes
    fn visit_function_arg(&mut self, pos: usize, arg: &mut syn::FnArg) -> Option<LifetimeType> {
        match arg {
            syn::FnArg::Receiver(receiver) => match receiver.reference {
                None => None,
                Some((and_token, None)) => {
                    let lifetime: syn::Lifetime = syn::parse_str(&format!("'life{pos}"))
                        .expect("failed to parse lifetime");
                    receiver.reference = Some((and_token, Some(lifetime.clone())));
                    Some(LifetimeType::Generated(lifetime))
                }
                Some((_, Some(ref lifetime))) => Some(LifetimeType::Preexisted(lifetime.clone())),
            },
            syn::FnArg::Typed(typed) => {
                match &mut *typed.ty  {
                    syn::Type::Reference(rf) => {
                        match rf.lifetime.as_mut() {
                            Some(lf) => Some(LifetimeType::Preexisted(lf.clone())),
                            None => {
                                let lifetime: syn::Lifetime = syn::parse_str(&format!("'life{pos}"))
                                    .expect("failed to parse lifetime");
                                rf.lifetime = Some(lifetime.clone());
                                Some(LifetimeType::Generated(lifetime))
                            }
                        }
                    },
                    _ => None
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
        lifetimes: &[LifetimeType],
    ) {
        // update return type
        let output_span = func_signature.output.span();
        let output: syn::Type = match &func_signature.output {
            syn::ReturnType::Default => parse_quote_spanned! {
                output_span => std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'future_lifetime>>
            },
            syn::ReturnType::Type(_, ty) => {
                let tokens = ty.to_token_stream();
                parse_quote_spanned! {
                    output_span => std::pin::Pin<Box<dyn std::future::Future<Output = #tokens> + Send + 'future_lifetime>>
                }
            }
        };
        func_signature.output = syn::ReturnType::Type(Token![->](output_span), Box::new(output));

        // update function generics with
    }

    fn visit_func_block(&mut self, func_block: &mut syn::Block) {}
}

impl VisitMut for Expander {
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
            _ => unreachable!("DatabaseRepository type path is missing"),
        };

        // set where clause
        i.generics.where_clause = Some(self.where_clause());
        visit_mut::visit_item_impl_mut(self, i)
    }

    fn visit_ident_mut(&mut self, i: &mut proc_macro2::Ident) {
        // first visit happens in trait name, which is stored for future use
        if self.trait_name.is_none() {
            self.trait_name = Some(i.clone())
        }
        visit_mut::visit_ident_mut(self, i)
    }

    // visit functions
    fn visit_impl_item_mut(&mut self, i: &mut syn::ImplItem) {
        if let syn::ImplItem::Fn(func) = i {
            self.visit_func(func);
        }
        visit_mut::visit_impl_item_mut(self, i);
    }
}

pub fn expand(_attrs: proc_macro2::TokenStream, _item: &mut syn::Item) -> TokenStream {
    let _ = Expander::new();
    TokenStream::new()
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
        let code = quote! {
            impl Repo for DatabaseRepo<D> {
                async fn elided_lifetime_on_receiver(&self, arg: &()) -> Result<()> {
                    Ok(())
                }

                async fn explicit_lifetime_on_receiver<'a, 'b>(&'a self, arg: &'b ()) -> Result<()> {
                    Ok(())
                }

                fn non_async_elided(&self, arg: &()) -> Result<()> {
                    Ok(())
                }

                fn non_async_explicit<'a, 'b>(&'a self, arg: &'b ()) -> Result<()> {
                    Ok(())
                }
            }
        };

        let mut syntax_tree: syn::Item = syn::parse2(code).unwrap();
        let mut expander = Expander::new();

        expander.visit_item_mut(&mut syntax_tree);
        println!("{}", prettify(syntax_tree.clone()));
        assert_eq!(
            prettify(syntax_tree),
            "\
impl<D> Repo for DatabaseRepo<D>
where
    D: ::sqlx::Database + ::sqlx_repo::SqlxDBNum,
    for<'e> i8: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> i16: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> i32: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> i64: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> f32: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> f64: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> String: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> &'e str: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> Vec<u8>: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> uuid::Uuid: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> ::sqlx::types::Json<
        serde_json::Value,
    >: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> chrono::DateTime<
        chrono::Utc,
    >: ::sqlx::Type<D> + ::sqlx::Encode<'e, D> + ::sqlx::Decode<'e, D>,
    for<'e> chrono::NaiveDateTime: ::sqlx::Type<D> + ::sqlx::Encode<'e, D>
        + ::sqlx::Decode<'e, D>,
    for<'e> serde_json::Value: ::sqlx::Type<D> + ::sqlx::Encode<'e, D>
        + ::sqlx::Decode<'e, D>,
    usize: ::sqlx::ColumnIndex<D::Row>,
    for<'e> &'e mut <D as ::sqlx::Database>::Connection: ::sqlx::Executor<
        'e,
        Database = D,
    >,
    for<'e> &'e ::sqlx::Pool<D>: ::sqlx::Executor<'e, Database = D>,
    D::QueryResult: std::fmt::Debug,
    for<'e> ::sqlx::Transaction<
        'e,
        D,
    >: std::ops::Deref<Target = <D as ::sqlx::Database>::Connection>,
    for<'e> ::sqlx::Transaction<
        'e,
        D,
    >: std::ops::DerefMut<Target = <D as ::sqlx::Database>::Connection>,
    for<'e> <D as ::sqlx::Database>::Arguments<'e>: ::sqlx::IntoArguments<'e, D>,
    D::Connection: ::sqlx::migrate::Migrate,
{
    fn elided_lifetime_on_receiver(
        &'life0 self,
        arg: &'life1 (),
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    > {
        Ok(())
    }
    fn explicit_lifetime_on_receiver<'a, 'b>(
        &'a self,
        arg: &'b (),
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<()>> + Send + 'future_lifetime>,
    > {
        Ok(())
    }
    fn non_async_elided(&self, arg: &()) -> Result<()> {
        Ok(())
    }
    fn non_async_explicit<'a, 'b>(&'a self, arg: &'b ()) -> Result<()> {
        Ok(())
    }
}
"
        )
    }
}
