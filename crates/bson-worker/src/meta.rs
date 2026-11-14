//! Shared helpers for the per-object discovery/description metadata that the
//! `vgi-lint` strict profile expects on every function and table:
//! `vgi.title`, `vgi.doc_llm`, `vgi.doc_md`, and `vgi.keywords`.
//!
//! Per-object `vgi.source_url` is intentionally NOT emitted here: it belongs on
//! the catalog object only (VGI139). The catalog's `source_url` field already
//! points at the repo.

/// Encode comma-separated keywords as the JSON array of strings `vgi.keywords`
/// requires (VGI138).
pub fn keywords_json(keywords: &str) -> String {
    let items: Vec<String> = keywords
        .split(',')
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(|k| {
            let escaped = k.replace('\\', "\\\\").replace('"', "\\\"");
            format!("\"{escaped}\"")
        })
        .collect();
    format!("[{}]", items.join(","))
}

/// Build the standard per-object discovery tags. `category` names one of the
/// schema's `vgi.categories` (VGI413), so listings and navigation can group this
/// object.
pub fn object_tags(
    title: &str,
    category: &str,
    description_llm: &str,
    description_md: &str,
    keywords: &str,
) -> Vec<(String, String)> {
    vec![
        ("vgi.title".to_string(), title.to_string()),
        ("vgi.category".to_string(), category.to_string()),
        ("vgi.doc_llm".to_string(), description_llm.to_string()),
        ("vgi.doc_md".to_string(), description_md.to_string()),
        ("vgi.keywords".to_string(), keywords_json(keywords)),
    ]
}

/// One `vgi.example_queries` entry: a human description and its SQL.
pub type Example = (&'static str, &'static str);

/// Encode examples as the JSON list `vgi.example_queries` expects
/// (`[{"description": …, "sql": …}, …]`), escaping each string for JSON so
/// SQL can be written once, as plain Rust text, and shared between lists.
pub fn examples_json(examples: &[Example]) -> String {
    let items: Vec<String> = examples
        .iter()
        .map(|(description, sql)| {
            format!(
                "{{\"description\":{},\"sql\":{}}}",
                json_string(description),
                json_string(sql)
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::examples_json;

    #[test]
    fn examples_json_escapes_quotes_and_backslashes() {
        let got = examples_json(&[("say \"hi\"", "SELECT '{\"a\":1}', 'C:\\x'")]);
        assert_eq!(
            got,
            r#"[{"description":"say \"hi\"","sql":"SELECT '{\"a\":1}', 'C:\\x'"}]"#
        );
    }

    #[test]
    fn examples_json_lists_entries_in_order() {
        let got = examples_json(&[("a", "SELECT 1"), ("b", "SELECT 2")]);
        assert_eq!(
            got,
            r#"[{"description":"a","sql":"SELECT 1"},{"description":"b","sql":"SELECT 2"}]"#
        );
    }
}
