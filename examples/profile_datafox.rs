use std::time::Instant;

use datafox::{Evaluator, InMemoryStorage, Result, Value, parse_query};

fn main() -> Result<()> {
    let nodes = std::env::var("DATAFOX_PROFILE_NODES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20_000);
    let mut edges = Vec::with_capacity(nodes);
    let mut labels = Vec::with_capacity(nodes + 1);

    for node in 0..nodes {
        edges.push(vec![
            Value::integer(node as i64),
            Value::integer((node + 1) as i64),
        ]);
        labels.push(vec![
            Value::integer(node as i64),
            Value::string(format!("node-{node}")),
        ]);
    }
    labels.push(vec![
        Value::integer(nodes as i64),
        Value::string(format!("node-{nodes}")),
    ]);

    let storage =
        InMemoryStorage::from_facts([("edge".to_string(), edges), ("label".to_string(), labels)]);
    let query = parse_query(r#"edge(From, To), label(To, Name), contains(Name, "999")"#)?;

    let iterations = std::env::var("DATAFOX_PROFILE_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(200);
    let mode = std::env::var("DATAFOX_PROFILE_MODE").unwrap_or_else(|_| "serial".to_string());
    let evaluator = match mode.as_str() {
        "parallel" => Evaluator::builder()
            .with_store(&storage)
            .parallel()
            .build()?,
        "serial" => Evaluator::builder().with_store(&storage).build()?,
        other => {
            eprintln!("unknown DATAFOX_PROFILE_MODE={other:?}; use serial or parallel");
            std::process::exit(2);
        }
    };

    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..iterations {
        total += evaluator.eval(&query)?.count();
    }

    let elapsed = start.elapsed();
    println!(
        "mode={mode} nodes={nodes} iterations={iterations} total_matches={total} elapsed_ms={:.3}",
        elapsed.as_secs_f64() * 1_000.0
    );

    Ok(())
}
