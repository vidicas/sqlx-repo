use std::{
    fs,
    path::{Path, PathBuf},
};

use proc_macro2::Span;
use quote::quote_spanned;
use syn::LitStr;

use crate::query::build_queries;

struct MigrationFile {
    version: i64,
    description: String,
    sql: String,
    path: PathBuf,
}

fn manifest_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set");
    PathBuf::from(manifest_dir)
}

// filenames must be `<VERSION>_<DESCRIPTION>.sql`; anything else is silently ignored
#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn resolve_migrations(dir: &Path) -> Result<Vec<MigrationFile>, String> {
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("failed to read migrations directory {}: {e}", dir.display()))?;

    let mut migrations = vec![];
    for entry in entries {
        let entry = entry
            .map_err(|e| format!("failed to read migrations directory {}: {e}", dir.display()))?;

        if !entry
            .file_type()
            .map_err(|e| format!("failed to read migrations directory {}: {e}", dir.display()))?
            .is_file()
        {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        let Some((version, description)) = file_name.split_once('_') else {
            continue;
        };
        let Some(description) = description.strip_suffix(".sql") else {
            continue;
        };
        if description.ends_with(".up") || description.ends_with(".down") {
            return Err(format!(
                "reversible migrations are not supported, found: {file_name}"
            ));
        }
        let Ok(version) = version.parse::<i64>() else {
            continue;
        };

        let path = entry.path();
        let sql = fs::read_to_string(&path)
            .map_err(|e| format!("failed to read migration {}: {e}", path.display()))?;

        migrations.push(MigrationFile {
            version,
            description: description.replace('_', " "),
            sql,
            path,
        });
    }

    migrations.sort_by_key(|m| m.version);
    Ok(migrations)
}

pub fn migrations(input: proc_macro::TokenStream) -> proc_macro2::TokenStream {
    let span = Span::call_site();

    let relative_path = if input.is_empty() {
        "migrations".to_owned()
    } else {
        match syn::parse::<LitStr>(input) {
            Ok(lit) => lit.value(),
            Err(e) => return e.to_compile_error(),
        }
    };

    let dir = manifest_dir().join(relative_path);

    let migrations = match resolve_migrations(&dir) {
        Ok(migrations) => migrations,
        Err(e) => return quote_spanned!(span => compile_error!(#e)),
    };

    if migrations.is_empty() {
        let msg = format!("no migrations found in {}", dir.display());
        return quote_spanned!(span => compile_error!(#msg));
    }

    let mut entries = Vec::with_capacity(migrations.len());
    // tracked via include_str! below so edits to these files trigger a rebuild
    let mut file_paths = Vec::with_capacity(migrations.len());
    for migration in &migrations {
        let (postgres, sqlite, mysql) = match build_queries(span, &migration.sql) {
            Ok(queries) => queries,
            Err(e) => return e,
        };
        let name = format!("{} {}", migration.version, migration.description);
        entries.push(quote_spanned! { span =>
            ::sqlx_repo::prelude::Migration {
                name: #name,
                queries: &[#postgres, #sqlite, #mysql],
            }
        });
        file_paths.push(migration.path.to_string_lossy().into_owned());
    }

    quote_spanned! { span =>
        {
            #(const _: &str = include_str!(#file_paths);)*
            &[#(#entries),*]
        }
    }
}
