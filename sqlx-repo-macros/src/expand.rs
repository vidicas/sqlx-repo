use proc_macro::TokenStream;
use syn::{
    parse_quote, parse_quote_spanned, visit_mut::{self, VisitMut}, Token, WhereClause
};

struct Expander {
    trait_name: Option<proc_macro2::Ident>,
    signatures: Vec<syn::Signature>,
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

        // collect lifetimes from inputs
        let lifetimes = func
            .sig
            .inputs
            .iter_mut()
            .enumerate()
            .filter_map(|(pos, arg)| self.visit_function_arg(pos, arg))
            .collect::<Vec<_>>();

        // rewrite return to BoxedFuture

        // rewrite function block into pinned box

        // rewrite function bounds
    }

    fn visit_function_arg(&mut self, pos: usize, arg: &mut syn::FnArg) -> Option<syn::Lifetime> {
        let lifetime = match arg {
            syn::FnArg::Receiver(receiver) => {
                match receiver.reference {
                    None => return None,
                    Some((and_token, None)) => {
                        let lifetime: syn::Lifetime = syn::parse_str(&format!("'life{pos}"))
                            .expect("failed to parse lifetime");
                        receiver.reference = Some((and_token, Some(lifetime.clone())));
                        lifetime
                    },
                    Some((_, Some(ref lifetime))) => lifetime.clone(),
                }
            },
            syn::FnArg::Typed(typed) => {
                println!("typed: {typed:?}");
                unimplemented!()
            }
        };
        Some(lifetime)
    }
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
                async fn elided_lifetime_on_receiver(&self) -> Result<()> {
                    Ok(())
                }
                
                async fn explicit_lifetime_on_receiver<'a>(&'a self) -> Result<()> {
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
    fn elided_lifetime_on_receiver(&'life0 self) -> Result<()> {
        Ok(())
    }
    fn explicit_lifetime_on_receiver<'a>(&'a self) -> Result<()> {
        Ok(())
    }
}
"
        )
    }
}
