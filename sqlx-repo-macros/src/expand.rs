use proc_macro::TokenStream;

pub fn expand(attrs: proc_macro2::TokenStream, item: &mut syn::Item) -> TokenStream {
    TokenStream::new()
}
