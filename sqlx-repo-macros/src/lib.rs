use proc_macro::TokenStream;
use syn::spanned::Spanned as _;
mod expand;
mod query;
mod validate;

#[proc_macro_attribute]
pub fn repo(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let attrs: proc_macro2::TokenStream = attrs.into();
    let mut input: syn::Item = match syn::parse(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error().into(),
    };
    let span = input.span();
    if let Err(e) = validate::validate(&input) {
        return e.into();
    }
    let (impl_trait, generated_trait, trait_constuctor) = expand::expand(attrs, &mut input);
    quote::quote_spanned! (
        span =>
        const _:() = {
            #[allow(unused)]
            use sqlx_repo::__hidden::query;
            #impl_trait
            #trait_constuctor
        };
        #generated_trait
    )
    .into()
}

#[proc_macro]
pub fn gen_query(input: TokenStream) -> TokenStream {
    query::gen_query(input).into()
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    query::query(input).into()
}
