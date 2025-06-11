use proc_macro::TokenStream;
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
    if let Err(e) = validate::validate(&input) {
        return e.into();
    }
    expand::expand(attrs, &mut input)
}

#[proc_macro]
pub fn gen_query(input: TokenStream) -> TokenStream {
    query::gen_query(input).into()
}

#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    query::query(input).into()
}
