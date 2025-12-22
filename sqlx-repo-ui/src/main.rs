#![allow(non_snake_case)]
use std::io::{Read, Write};

use dioxus::prelude::*;
use dioxus_logger::tracing::Level;

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use flate2::Compression;
use flate2::{read::GzDecoder, write::GzEncoder};
use gloo_timers::callback::Timeout;
use url::{Url, form_urlencoded::byte_serialize};

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

pub fn copy_to_clipboard(text: &str) {
    let navigator = match web_sys::window() {
        Some(window) => window.navigator(),
        None => {
            error!("window object is not accessible");
            return;
        }
    };
    let _ = navigator.clipboard().write_text(text);
}

pub fn location() -> Url {
    let location: String = web_sys::window()
        .expect("failed to get access to window")
        .location()
        .to_string()
        .into();
    let url: Url = location.parse().expect("failed to parse location");
    url
}

pub fn base_url() -> Url {
    let mut url: Url = location();
    // Reset path and fragments
    url.set_path("");
    url.set_fragment(None);
    url
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
        document::Title { "Sqlx Repo Playground" }
        Router::<Route> {}
    }
}

#[component]
pub fn Home(route: Vec<String>) -> Element {
    let url = location();
    if let Some(fragment) = url.fragment() {
        let decoded_fragment = match URL_SAFE.decode(fragment.as_bytes()) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to decode a fragment {fragment}, {e}");
                return rsx! {
                    Main { }
                };
            }
        };

        let mut decoder = GzDecoder::new(decoded_fragment.as_slice());
        let mut ungzipped = String::new();
        if let Err(e) = decoder.read_to_string(&mut ungzipped) {
            error!("Failed to ungzip a fragment {fragment}, {e}");
            return rsx! {
                Main { }
            };
        };

        rsx! {
            Main { sql: Some(ungzipped) }
        }
    } else {
        rsx! {
            Main { }
        }
    }
}

#[component]
pub fn Main(sql: Option<String>) -> Element {
    rsx! {
        div {
            class: "flex w-full px-8 py-4",
            div {
                class: "w-full",
                Header { }
                Body { sql: sql}
            }
        }
    }
}

pub fn Header() -> Element {
    rsx! {
        div {
            class: "flex pb-4",
            div {
                class: "flex text-3xl tracking-tighter font-semibold",
                "SQLx Repo Playground"
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

#[component]
pub fn Body(sql: Option<String>) -> Element {
    rsx! {
        div {
            class: "flex h-full text-gray-600 space-x-4",
            div {
                class: "flex-1/2 space-y-4 tracking-tighter",
                Playground { sql },
            }
            div {
                class: "flex-1/2",
                Info { },
            }
        }
    }
}

#[component]
pub fn Playground(sql: Option<String>) -> Element {
    let message = "Waiting for SQL ...";
    let base_url = base_url();

    let mut input_sql = use_signal(|| String::new());
    let mut sqlite = use_signal(|| String::from(message));
    let mut postgresql = use_signal(|| String::from(message));
    let mut mysql = use_signal(|| String::from(message));
    let mut ast_details = use_signal(|| String::new());
    let mut status = use_signal(|| String::new());
    let mut encoded_query = use_signal(|| String::new());

    let mut clear = move || {
        *sqlite.write() = message.to_string();
        *postgresql.write() = message.to_string();
        *mysql.write() = message.to_string();
        *ast_details.write() = "".to_string();
    };

    let encode_query = move || -> String {
        let mut e = GzEncoder::new(Vec::new(), Compression::best());
        let _ = e.write_all(input_sql().as_bytes());
        let compressed = e.finish().expect("Failed to gzip query");
        URL_SAFE.encode(compressed.as_slice())
    };

    let url_with_fragment = move |fragment: &str| -> String {
        let mut url = base_url.clone();
        url.set_fragment(Some(&fragment));
        url.to_string()
    };

    let mut handle_query = move |sql: String| {
        if sql.is_empty() {
            *status.write() = "".to_string();
            *encoded_query.write() = "".to_string();
            clear();
            return;
        };

        *input_sql.write() = sql.clone();
        *encoded_query.write() = encode_query();

        let mut ast = match parse(sql.clone()) {
            Ok(res) => res,
            Err(e) => {
                *status.write() = format!("Invalid query: {e:#?}");
                clear();
                return;
            }
        };

        let ast = match ast.pop() {
            Some(a) => a,
            None => {
                *status.write() = format!("Empty AST");
                clear();
                return;
            }
        };

        *sqlite.write() = ast
            .to_sql(&SQLiteDialect {})
            .expect("SQLite dialect failed.");
        *postgresql.write() = ast
            .to_sql(&PostgreSqlDialect {})
            .expect("Postgres dialect failed.");
        *mysql.write() = ast.to_sql(&MySqlDialect {}).expect("MySql dialect failed");
        *status.write() = "".to_string();

        let hidden_ast = match HiddenParser::parse_sql(&GenericDialect {}, &sql) {
            Ok(a) => format!("{:?}", a),
            Err(_) => "Failed".to_string(),
        };
        *ast_details.write() = format!(
            "Sqlx Repo AST: {:?} \n\nSql Parser AST: {}",
            ast, hidden_ast
        );
    };

    let mut parsed = use_signal(|| false);
    if !*parsed.read() {
        *parsed.write() = true;
        if let Some(s) = &sql {
            handle_query(s.to_string())
        }
    }

    rsx! {
        fieldset {
            class: "fieldset bg-base-200 border-base-300 border p-4",
            div {
                class: "text-base",
                "Insert your query below to check it against SQLite, PostgreSQL and MySQL"
            }
            div {
                style: "position: relative;",
                textarea {
                    class: "textarea w-full",
                    style: "padding-top: 34px",
                    placeholder: "Your SQL query",
                    value: "{input_sql()}",
                    oninput: move |evt| {
                        let sql = evt.value().to_string();
                        handle_query(sql);
                    }
                }
                div {
                    class: "toolbar",
                    SharePlayground { url: url_with_fragment(&encoded_query()) }
                    CopyToClipboard { text: input_sql() }
                    OpenIssue { url: url_with_fragment(&encoded_query()) }
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
                div {
                    class: "ml-auto",
                    CopyToClipboard { text: sqlite() }
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
                div {
                    class: "ml-auto",
                    CopyToClipboard { text: postgresql() }
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
                div {
                    class: "ml-auto",
                    CopyToClipboard { text: mysql() }
                }
            }
            div {
                class: "font-sans border bg-base-100 border-base-300 rounded-sm px-3 py-2",
                "{mysql}"
            }
        }
    }
}

#[component]
pub fn SharePlayground(url: String) -> Element {
    let mut open_dialog = use_signal(|| false);
    rsx! {
        div {
            class: "tooltip",
            "data-tip": "Share Playground",
            div {
                class: "btn btn-xs btn-ghost btn-square",
                onclick: move |_| open_dialog.set(true),
                svg {
                    view_box: "0 0 24 24",
                    width: "16",
                    height: "16",
                    fill: "none",
                    stroke: "#707070",
                    path {
                        d: "M8.68439 10.6578L15.3124 7.34378 M15.3156 16.6578L8.69379 13.3469 M21 6C21 7.65685 19.6569 9 18 9C16.3431 9 15 7.65685 15 6C15 4.34315 16.3431 3 18 3C19.6569 3 21 4.34315 21 6ZM9 12C9 13.6569 7.65685 15 6 15C4.34315 15 3 13.6569 3 12C3 10.3431 4.34315 9 6 9C7.65685 9 9 10.3431 9 12ZM21 18C21 19.6569 19.6569 21 18 21C16.3431 21 15 19.6569 15 18C15 16.3431 16.3431 15 18 15C19.6569 15 21 16.3431 21 18Z",
                        stroke_width: "1.5",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                    }
                }
            }
        }
        if open_dialog() {
            dialog {
                open: true,
                class: "modal",
                onclick: move |_| open_dialog.set(false),
                div {
                    class: "modal-box",
                    onclick: |e| e.stop_propagation(),
                    h3 { class: "text-lg", "Shared link" }
                    SharedInfo {url: url }
                    form {
                        method: "dialog",
                        class: "modal-action",
                        button {
                            class: "btn",
                            onclick: move |_| open_dialog.set(false),
                            "Close"
                        }
                    }
                }
                form {
                    method: "dialog",
                    class: "modal-backdrop",
                    button {
                        onclick: move |_| open_dialog.set(false),
                    }
                }
            }
        }
    }
}

#[component]
pub fn CopyToClipboard(text: String) -> Element {
    rsx! {
        div {
            class: "tooltip",
            "data-tip": "Copy to clipboard",
            div {
                class: "btn btn-xs btn-ghost btn-square",
                onclick: move |_| copy_to_clipboard(&text),
                svg {
                    view_box: "0 0 24 24",
                    width: "16",
                    height: "16",
                    fill: "#707070",
                    stroke: "#707070",
                    path {
                        d: "M23 15H11.707l2.646 2.646-.707.707L9.793 14.5l3.854-3.854.707.707L11.707 14H23zm-13-5H6v1h4zm-4 5h2v-1H6zM3 4h3V3h3a2 2 0 0 1 4 0h3v1h3v9h-1V5h-2v2H6V5H4v16h14v-5h1v6H3zm4 2h8V4h-3V2.615A.615.615 0 0 0 11.386 2h-.771a.615.615 0 0 0-.615.615V4H7zM6 19h4v-1H6z",
                        stroke_width: "0.5",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                    }
                }
            }
        }
    }
}

#[component]
pub fn OpenIssue(url: String) -> Element {
    let description = format!(
        r#"Describe what you observed in the playground: expected vs actual behavior.

### Expectation

I expected ... but got ...

### Related Playground

For more details follow the [link]({})
"#,
        url
    );
    let title: String = byte_serialize(b"Finding from the Playground").collect();
    let body: String = byte_serialize(description.as_bytes()).collect();
    let github_url = format!(
        "https://github.com/vidicas/sqlx-repo/issues/new?title={}&body={}",
        title, body
    );
    rsx! {
        div {
            class: "tooltip",
            "data-tip": "Open Issue",
            a {
                class: "btn btn-xs btn-ghost btn-square",
                href: "{github_url}",
                rel: "nofollow noopener noreferrer",
                svg {
                    view_box: "0 0 24 24",
                    width: "16",
                    height: "16",
                    fill: "none",
                    stroke: "#707070",
                    path {
                        d: "M8 21H20.4C20.7314 21 21 20.7314 21 20.4V3.6C21 3.26863 20.7314 3 20.4 3H3.6C3.26863 3 3 3.26863 3 3.6V16",
                        stroke_width: "1.5",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                    }
                    path {
                        d: "M3.5 20.5L12 12M12 12V16M12 12H8",
                        stroke_width: "1.5",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                    }
                }
            }
        }
    }
}

#[component]
pub fn SharedInfo(url: String) -> Element {
    let mut copied = use_signal(|| false);
    rsx! {
        div {
            div {
                class: "bg-green-50 p-4 border-t border-green-200",
                div {
                    class: "flex items-center gap-2 text-green-700 text-base font-medium mb-1",
                    "Copy link below"
                }

                p {
                    class: "text-sm text-gray-700 mb-3",
                    "Anyone with the link can view this playground"
                }

                div {
                    class: "flex items-center bg-white border border-gray-300 rounded-sm overflow-hidden text-sm",
                    input {
                        readonly: true,
                        value: "{url}",
                        class: "flex-1 px-3 py-2 text-gray-800 font-mono outline-none bg-white"
                    }
                    button {
                        class: if copied() {
                            "px-3 py-2 bg-gray-200 text-green-700 border-l border-gray-300 text-sm font-medium"
                        } else {
                            "px-3 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 border-l border-gray-300 text-sm"
                        },
                        onclick: move |_| {
                            copy_to_clipboard(&url);
                            copied.set(true);
                            // Reset the button
                            Timeout::new(3000, move || copied.set(false)).forget();
                        },
                        if copied() { "Copied!" } else { "Copy" }
                    }
                }
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
