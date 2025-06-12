use proc_macro2::Span;
use quote::quote_spanned;
use syn::{Item, spanned::Spanned, visit::Visit};

struct Validator {
    error: Option<ValidatorError>,
    trait_name: Option<()>,
    type_name: Option<()>,
}

enum ValidatorError {
    NoGenericsAllowed(Span),
    NoWhereClauseAllowed(Span),
    NotTraitImplBlock(Span),
    NoAssocTypesAllowed(Span),
    IncorrectImplTypeName(Span),
}

impl ValidatorError {
    fn to_compile_error(&self) -> proc_macro2::TokenStream {
        let span = self.span();
        let error = self.error();
        quote_spanned! {
            span => compile_error!(#error)
        }
    }

    fn span(&self) -> Span {
        *match self {
            Self::NoGenericsAllowed(span) => span,
            Self::NoWhereClauseAllowed(span) => span,
            Self::NotTraitImplBlock(span) => span,
            Self::NoAssocTypesAllowed(span) => span,
            Self::IncorrectImplTypeName(span) => span,
        }
    }

    fn error(&self) -> &'static str {
        match self {
            Self::NoGenericsAllowed(_) => "No generics allowed in trait implementation",
            Self::NoWhereClauseAllowed(_) => "No where clause allowed in trait implementation",
            Self::NotTraitImplBlock(_) => "Only trait implementation blocks are supported",
            Self::NoAssocTypesAllowed(_) => "Trait associated types are not supported",
            Self::IncorrectImplTypeName(_) => "Incorrect type name in trait implementation",
        }
    }
}

impl Validator {
    fn new() -> Self {
        Self {
            error: None,
            trait_name: None,
            type_name: None,
        }
    }

    fn to_compile_error(&self) -> Option<proc_macro2::TokenStream> {
        self.error.as_ref().map(|err| err.to_compile_error())
    }

    fn valid_ty_name(&self, i: &syn::Item) -> bool {
        let type_path = match i {
            syn::Item::Type(type_path) => type_path,
            _ => return false,
        };
        false
    }
}

impl Visit<'_> for Validator {
    fn visit_impl_item_type(&mut self, i: &'_ syn::ImplItemType) {
        self.error = Some(ValidatorError::NoAssocTypesAllowed(i.span()))
    }

    fn visit_generic_argument(&mut self, i: &syn::GenericArgument) {
        self.error = Some(ValidatorError::NoGenericsAllowed(i.span()));
    }

    fn visit_where_clause(&mut self, i: &syn::WhereClause) {
        self.error = Some(ValidatorError::NoWhereClauseAllowed(i.span()))
    }

    fn visit_ident(&mut self, i: &proc_macro2::Ident) {
        if self.trait_name.is_none() {
            self.trait_name = Some(());
            return;
        }
        if self.type_name.is_none() {
            if *i != "DatabaseRepository" {
                self.error = Some(ValidatorError::IncorrectImplTypeName(i.span()))
            }
            self.type_name = Some(())
        }
    }

    fn visit_item(&mut self, i: &syn::Item) {
        match i {
            Item::Impl(i) if i.trait_.is_some() => syn::visit::visit_item_impl(self, i),
            _ => {
                self.error = Some(ValidatorError::NotTraitImplBlock(i.span()));
            }
        }
    }
}

pub fn validate(item: &Item) -> Result<(), proc_macro2::TokenStream> {
    let mut validator = Validator::new();
    validator.visit_item(item);
    if let Some(err) = validator.to_compile_error() {
        return Err(err);
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use quote::quote;

    #[test]
    fn trait_with_generic() {
        let code = quote! {
            impl Repo<'a> for DatabaseRepository {

            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"No generics allowed in trait implementation\")"
        )
    }

    #[test]
    fn type_with_generic() {
        let code = quote! {
            impl<D> Repo for DatabaseRepository<D> {

            }
        };

        let mut syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&mut syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"No generics allowed in trait implementation\")"
        )
    }

    #[test]
    fn trait_with_where() {
        let code = quote! {
            impl Repo for DatabaseRepository where 'a: 'b {

            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"No where clause allowed in trait implementation\")"
        )
    }

    #[test]
    fn trait_with_assoc_type() {
        let code = quote! {
            impl Repo for DatabaseRepository {
                type Assoc = ();

            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"Trait associated types are not supported\")"
        )
    }

    #[test]
    fn not_trait_impl() {
        let code = quote! {
            impl DatabaseRepository {
            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"Only trait implementation blocks are supported\")"
        )
    }

    #[test]
    fn invalid_type_name() {
        let code = quote! {
            impl Repo for DatabaseRepo {
            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_some());
        let error = validator.error.take().unwrap();
        assert_eq!(
            error.to_compile_error().to_string(),
            "compile_error ! (\"Incorrect type name in trait implementation\")"
        )
    }

    #[test]
    fn valid_impl() {
        let code = quote! {
            impl Repo for DatabaseRepository {
            }
        };

        let syntax_tree: Item = syn::parse2(code).unwrap();
        let mut validator = Validator::new();

        validator.visit_item(&syntax_tree);
        assert!(validator.error.is_none());
    }
}
