# datafox

`datafox` is a small Datalog parser and streaming query engine for querying caller-owned facts.

It was built for lintbook rule evaluation, but the crate is standalone: provide facts through a store, parse read-only queries, and evaluate substitutions through a `DatafoxClient`.

```toml
[dependencies]
datafox = "0.1"
```

```rust
use datafox::{DatafoxClient, DatafoxConfig, InMemoryStorage, Value, parse_query};

fn main() -> datafox::Result<()> {
    let storage = InMemoryStorage::from_facts([(
        "edge".to_string(),
        vec![
            vec![Value::integer(1), Value::integer(2)],
            vec![Value::integer(2), Value::integer(3)],
        ],
    )]);

    let query = parse_query("edge(From, 2)")?;
    let datafox = DatafoxClient::new(DatafoxConfig::new(&storage))?;
    let results = datafox.eval(&query)?.collect::<Vec<_>>();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].lookup("From"), Some(&Value::integer(1)));
    Ok(())
}
```

## Query Syntax

| Form | Example |
| --- | --- |
| Atom | `edge(From, To)` |
| Variable | `Name` |
| String constant | `"dbg!"` |
| Integer constant | `42` |
| Wildcard | `_` |
| Conjunction | `node(Node), text(Node, Text)` |
| Negation | `node(Node), !test(Node)` |
| Query set | `node(Node); edge(From, To)` |
| Quoted predicate | `'local://schema/name'(Entity, Value)` |
| Binary expression | `(Line + 1) = 42` |

Builtins are available as clauses:

| Builtin | Example |
| --- | --- |
| Equality and order | `Start < End`, `A = B` |
| String matching | `startsWith(Name, "lint")` |
| Negative string matching | `notContains(Text, "dbg!")` |
| Regex matching | `matchesRegex(Text, "^dbg!")` |
| Temporal aliases | `before(Start, End)`, `after(End, Start)` |
| Arithmetic operators | `(X + 1) = Y`, `(X * 2) > 10`, `(X - 1) = 0`, `(X / 2) = 4` |

Negated atoms and builtin arguments must be grounded by earlier clauses. Evaluation is read-only and snapshot-oriented; facts are supplied by the caller.

Configure the runtime profile up front:

```rust
let datafox = DatafoxClient::new(DatafoxConfig::new(&storage)
    .parallel()
    .threads(4)
    .seed_threshold(1024))?;

for substitution in datafox.eval(&query)? {
    println!("{substitution}");
}
```

For hot paths, plan once and evaluate the validated plan repeatedly:

```rust
let plan = datafox.plan(&query)?;
for substitution in datafox.eval_plan(&plan)? {
    println!("{substitution}");
}
```

Add a prelude when the evaluator should see ambient facts, custom relations, or custom expression operators:

```rust
use datafox::{BinaryOperator, DatafoxClient, DatafoxConfig, Prelude, Value};

let prelude = Prelude::new()
    .with_fact("threshold", vec![Value::integer(10)])
    .with_operator(BinaryOperator::from_option("plusTen", |left, right| match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(Value::integer(left + right + 10)),
        _ => None,
    }));

let datafox = DatafoxClient::new(DatafoxConfig::new(&storage).with_prelude(prelude))?;
```
