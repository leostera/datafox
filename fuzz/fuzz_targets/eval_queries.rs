#![no_main]

use datafox::{DatafoxClient, DatafoxConfig, InMemoryStorage, Value};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 512 {
        return;
    }

    let Ok(source) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(queries) = datafox::parse_queries(source) else {
        return;
    };

    let storage = InMemoryStorage::from_facts([
        (
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(2)],
                vec![Value::integer(2), Value::integer(3)],
                vec![Value::integer(3), Value::integer(3)],
            ],
        ),
        (
            "displayName".to_string(),
            vec![
                vec![Value::string("rush"), Value::string("Rush")],
                vec![Value::string("yes"), Value::string("Yes")],
            ],
        ),
        (
            "text".to_string(),
            vec![
                vec![Value::string("node-1"), Value::string("dbg!")],
                vec![Value::string("node-2"), Value::string("println!")],
            ],
        ),
    ]);

    let Ok(datafox) = DatafoxClient::new(DatafoxConfig::new(&storage)) else {
        return;
    };
    let Ok(parallel_datafox) =
        DatafoxClient::new(DatafoxConfig::new(&storage).parallel().seed_threshold(1))
    else {
        return;
    };
    for query in queries.into_iter().take(16) {
        let Ok(plan) = datafox.plan(&query) else {
            continue;
        };
        let _ = datafox.eval_plan(&plan);
        let _ = parallel_datafox.eval_plan(&plan);
    }
});
