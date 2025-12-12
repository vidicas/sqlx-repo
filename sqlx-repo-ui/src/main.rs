#![allow(non_snake_case)]

use dioxus::prelude::*;
use dioxus_logger::tracing::Level;

use sql_bridge::__hidden::sqlparser::{dialect::GenericDialect, parser::Parser as HiddenParser};
use sql_bridge::{MySqlDialect, PostgreSqlDialect, SQLiteDialect, parse};

#[macro_export]
macro_rules! local_asset {
    ($path:expr) => {{
        let p = $path;
        match option_env!("ASSET_PREFIX") {
            Some(prefix) => format!(
                "{}/{}",
                prefix.trim_end_matches('/'),
                p.trim_start_matches('/')
            ),
            None => p.to_string(),
        }
    }};
}

#[derive(Clone, Routable, Debug, PartialEq)]
enum Route {
    #[route("/:..route")]
    Home { route: Vec<String> },
}

fn main() {
    #[cfg(debug_assertions)]
    dioxus_logger::init(Level::INFO).expect("Failed to init logger.");

    #[cfg(not(debug_assertions))]
    dioxus_logger::init(Level::ERROR).expect("Failed to init logger.");

    launch(App);
}

fn App() -> Element {
    rsx! {
        Router::<Route> {}
    }
}

#[component]
pub fn Home(route: Vec<String>) -> Element {
    rsx! {
        div {
            class: "flex w-full px-8 py-4",
            div {
                class: "w-full",
                Header { }
                Body { }
            }
        }
    }
}

pub fn Header() -> Element {
    rsx! {
        div {
            class: "flex pb-4",
            div {
                class: "text-3xl tracking-tighter font-semibold",
                "Sqlx Repo Playground"
            }
            div { class: "flex-grow" }
            a {
                class: "text-xs tracking-tighter",
                href: "https://docs.rs/sqlx-repo/latest/sqlx_repo/#supported-queries",
                rel: "nofollow noopener noreferrer",
                svg {
                    view_box: "0 0 512 512",
                    width: "24",
                    height: "24",
                    fill: "black",
                    path {
                        d: "M488.6 250.2L392 214V105.5c0-15-9.3-28.4-23.4-33.7l-100-37.5c-8.1-3.1-17.1-3.1-25.3 0l-100 37.5c-14.1 5.3-23.4 18.7-23.4 33.7V214l-96.6 36.2C9.3 255.5 0 268.9 0 283.9V394c0 13.6 7.7 26.1 19.9 32.2l100 50c10.1 5.1 22.1 5.1 32.2 0l103.9-52 103.9 52c10.1 5.1 22.1 5.1 32.2 0l100-50c12.2-6.1 19.9-18.6 19.9-32.2V283.9c0-15-9.3-28.4-23.4-33.7zM358 214.8l-85 31.9v-68.2l85-37v73.3zM154 104.1l102-38.2 102 38.2v.6l-102 41.4-102-41.4v-.6zm84 291.1l-85 42.5v-79.1l85-38.8v75.4zm0-112l-102 41.4-102-41.4v-.6l102-38.2 102 38.2v.6zm240 112l-85 42.5v-79.1l85-38.8v75.4zm0-112l-102 41.4-102-41.4v-.6l102-38.2 102 38.2v.6z"
                    }
                }
                "latest"
            }
            div {
                class: "pl-4",
                a {
                    href: "https://github.com/vidicas/sqlx-repo",
                    img {
                        class: "h-6 object-scale-down",
                        src: local_asset!("github-mark.png"),
                    }
                }
            }
        }
    }
}

pub fn Body() -> Element {
    rsx! {
        div {
            class: "flex h-full text-gray-600 space-x-4",
            div {
                class: "flex-1/2 space-y-4 tracking-tighter",
                Playground { },
            }
            div {
                class: "flex-1/2",
                Info { },
            }
        }
    }
}

pub fn Playground() -> Element {
    let mut input_sql = use_signal(|| String::new());
    let message = "Waiting for SQL...";
    let mut sqlite = use_signal(|| String::from(message));
    let mut postgresql = use_signal(|| String::from(message));
    let mut mysql = use_signal(|| String::from(message));
    let mut ast_details = use_signal(|| String::new());

    let mut clear = move || {
        *sqlite.write() = message.to_string();
        *postgresql.write() = message.to_string();
        *mysql.write() = message.to_string();
        *ast_details.write() = "".to_string();
    };

    let mut status = use_signal(|| String::new());
    rsx! {
        fieldset {
            class: "fieldset bg-base-200 border-base-300 border p-4",
            div {
                class: "text-base",
                "Insert your query below to check it against SQLite, PostgreSQL and MySQL"
            }
            textarea {
                class: "textarea w-full",
                placeholder: "Your SQL query",
                oninput: move |evt| {
                    let sql = evt.value().to_string();
                    if sql.is_empty() {
                        *status.write() = "".to_string();
                        clear();
                        return
                    };

                    *input_sql.write() = sql.clone();

                    let mut ast = match parse(sql.clone()) {
                        Ok(res) => res,
                        Err(e) => {
                            *status.write() = format!("Invalid query: {e:#?}");
                            clear();
                            return
                        }
                    };

                    if ast.is_empty() {
                        *status.write() = format!("Empty AST");
                        clear();
                        return
                    }

                    let ast = ast.pop().unwrap();
                    *sqlite.write() = ast.to_sql(&SQLiteDialect{}).unwrap();
                    *postgresql.write() = ast.to_sql(&PostgreSqlDialect{}).unwrap();
                    *mysql.write() = ast.to_sql(&MySqlDialect {}).unwrap();
                    *status.write() = "".to_string();

                    let hidden_ast = match HiddenParser::parse_sql(&GenericDialect {}, &sql) {
                        Ok(a) => format!("{:?}", a),
                        Err(_) => "Failed".to_string(),
                    };
                    *ast_details.write() = format!("Sqlx Repo AST: {:?} \n\nSql Parser AST: {}", ast, hidden_ast);
                }
            }
            div {
                class: "whitespace-pre-wrap break-words break-all label text-sm text-red-800",
                {status}
            }
            if !ast_details().is_empty() {
                div {
                    class: "collapse collapse-arrow",
                    input {
                        type: "checkbox"
                    }
                    div {
                        class: "collapse-title text-sm p-0 after:start-17 after:end-auto after:top-3 after:translate-y-0",
                        "AST details"
                    }
                    div {
                        class: "collapse-content text-xs p-0 whitespace-pre-wrap break-words break-all tracking-wide",
                        "{ast_details}"
                    }
                }
            }
        }
        div {
            class: "bg-base-200 border border-base-300 p-4 space-y-2",
            div {
                class: "flex space-x-2",
                img {
                    class: "h-6 object-scale-down",
                    src: local_asset!("sqlite-logo.png"),
                }
                div {
                    class: "font-semibold",
                    "SQLite"
                }
            }
            div {
                class: "font-sans border bg-base-100 border-base-300 rounded-sm px-3 py-2",
                "{sqlite}"
            }
        }
        div {
            class: "bg-base-200 border border-base-300 p-4 space-y-2",
            div {
                class: "flex space-x-2",
                img {
                    class: "h-6 object-scale-down",
                    src: local_asset!("postgresql-logo.png"),
                }
                div {
                    class: "font-semibold",
                    "PostgreSQL"
                }
            }
            div {
                class: "font-sans border bg-base-100 border-base-300 rounded-sm px-3 py-2",
                "{postgresql}"
            }
        }
        div {
            class: "bg-base-200 border border-base-300 p-4 space-y-2",
            div {
                class: "flex space-x-2",
                img {
                    class: "h-6 object-scale-down",
                    src: local_asset!("mysql-logo.png"),
                }
                div {
                    class: "font-semibold",
                    "MySQL"
                }
            }
            div {
                class: "font-sans border bg-base-100 border-base-300 rounded-sm px-3 py-2",
                "{mysql}"
            }
        }
    }
}

pub fn Info() -> Element {
    rsx! {
        div {
            iframe {
                src: "https://docs.rs/sqlx-repo/latest/sqlx_repo/#supported-queries",
                class: "w-full h-[calc(100vh-140px)] border border-base-300 rounded-sm",
                referrerpolicy: "no-referrer",
                frame_border: "0",
            }
        }
    }
}
