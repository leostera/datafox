use crate::{Atom, Clause, Query, Term, Value};

const CONTINUATION_INDENT: &str = "  ";

pub fn format_query(query: &Query) -> String {
    match query {
        Query::Single(atom) => format_atom(atom),
        Query::Multi(clauses) => clauses
            .iter()
            .enumerate()
            .map(|(index, clause)| {
                let prefix = if index == 0 { "" } else { CONTINUATION_INDENT };
                let suffix = if index + 1 == clauses.len() { "" } else { "," };
                format!("{prefix}{}{suffix}", format_clause(clause))
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

pub fn format_queries(queries: &[Query]) -> String {
    queries
        .iter()
        .map(format_query)
        .collect::<Vec<_>>()
        .join(";\n\n")
}

fn format_clause(clause: &Clause) -> String {
    match clause {
        Clause::Atom(atom) => format_atom(atom),
        Clause::Negated(atom) => format!("!{}", format_atom(atom)),
        Clause::Builtin { name, args } if is_infix_relation(name) && args.len() == 2 => {
            format!("{} {name} {}", format_term(&args[0]), format_term(&args[1]))
        }
        Clause::Builtin { name, args } => format_call(name, args),
    }
}

fn format_atom(atom: &Atom) -> String {
    format_call(&atom.predicate, &atom.args)
}

fn format_call(name: &str, args: &[Term]) -> String {
    let args = args.iter().map(format_term).collect::<Vec<_>>().join(", ");
    format!("{}({args})", format_predicate(name))
}

fn format_term(term: &Term) -> String {
    match term {
        Term::Var(name) => name.clone(),
        Term::Const(value) => format_value(value),
        Term::Call { name, args } if is_infix_operator(name) && args.len() == 2 => {
            format!(
                "({} {name} {})",
                format_term(&args[0]),
                format_term(&args[1])
            )
        }
        Term::Call { name, args } => format_call(name, args),
        Term::Wildcard => "_".to_string(),
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Integer(value) => value.to_string(),
        Value::String(value) => format!("\"{}\"", escape_double_quoted(value)),
    }
}

fn format_predicate(name: &str) -> String {
    if is_identifier(name) {
        name.to_string()
    } else {
        format!("'{}'", name)
    }
}

fn escape_double_quoted(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect(),
            _ => vec![ch],
        })
        .collect()
}

fn is_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    chars.next().is_some_and(|ch| ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '_' | '-' | '?'))
}

fn is_infix_relation(name: &str) -> bool {
    matches!(name, "=" | "<" | "<=" | ">" | ">=")
}

fn is_infix_operator(name: &str) -> bool {
    matches!(name, "+" | "-" | "*" | "/")
}

#[cfg(test)]
mod tests {
    use crate::{format_queries, parse_queries, parse_query};

    #[test]
    fn formats_multi_clause_queries_one_clause_per_line() {
        let query = parse_query(
            r#"node(Node, "macro_invocation", _, _, _, _), text(Node, Text), contains(Text, "dbg!")"#,
        )
        .expect("query");

        assert_eq!(
            crate::format_query(&query),
            "node(Node, \"macro_invocation\", _, _, _, _),\n  text(Node, Text),\n  contains(Text, \"dbg!\")"
        );
    }

    #[test]
    fn formats_query_sets_with_semicolon_separators() {
        let queries = parse_queries(
            r#"node(Node, "call_expression", _, _, _, _), text(Node, Text); !skip(Node), X >= 1"#,
        )
        .expect("queries");

        assert_eq!(
            format_queries(&queries),
            "node(Node, \"call_expression\", _, _, _, _),\n  text(Node, Text);\n\n!skip(Node),\n  X >= 1"
        );
    }

    #[test]
    fn preserves_regex_backslashes() {
        let query = parse_query(r#"text(Text, "_async\s*\(")"#).expect("query");

        assert_eq!(crate::format_query(&query), r#"text(Text, "_async\s*\(")"#);
    }
}
