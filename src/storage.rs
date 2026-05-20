use std::collections::BTreeMap;
use std::slice;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;

use crate::{Result, Value};

pub type FactTuple = Vec<Value>;
pub type TupleStream = mpsc::Receiver<Result<FactTuple>>;

const DEFAULT_STREAM_BUFFER: usize = 64;

/// Snapshot-oriented read-only storage interface for Datalog queries.
#[async_trait]
pub trait Storage {
    async fn get_facts_matching(
        &self,
        predicate: &str,
        pattern: Vec<Option<Value>>,
    ) -> Result<TupleStream>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactEstimate {
    pub rows: usize,
    pub exact: bool,
}

impl FactEstimate {
    pub fn exact(rows: usize) -> Self {
        Self { rows, exact: true }
    }

    pub fn upper_bound(rows: usize) -> Self {
        Self { rows, exact: false }
    }
}

pub trait FactStore {
    type Scan<'store, 'pattern>: Iterator<Item = &'store FactTuple>
    where
        Self: 'store;

    fn scan<'store, 'pattern>(
        &'store self,
        predicate: &str,
        pattern: &'pattern [Option<Value>],
    ) -> Self::Scan<'store, 'pattern>;

    fn estimate(&self, predicate: &str, pattern: &[Option<Value>]) -> FactEstimate;
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InMemoryStorage {
    facts: BTreeMap<String, Vec<FactTuple>>,
    indexes: BTreeMap<String, BTreeMap<(usize, Value), Vec<usize>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_facts(facts: impl IntoIterator<Item = (String, Vec<FactTuple>)>) -> Self {
        let mut storage = Self::new();
        for (predicate, tuples) in facts {
            for tuple in tuples {
                storage.insert(predicate.clone(), tuple);
            }
        }
        storage
    }

    pub fn insert(&mut self, predicate: impl Into<String>, tuple: FactTuple) {
        let predicate = predicate.into();
        let tuple_index = self.facts.get(&predicate).map_or(0, Vec::len);

        for (value_index, value) in tuple.iter().cloned().enumerate() {
            self.indexes
                .entry(predicate.clone())
                .or_default()
                .entry((value_index, value))
                .or_default()
                .push(tuple_index);
        }

        self.facts.entry(predicate).or_default().push(tuple);
    }

    pub fn facts_matching<'a>(
        &'a self,
        predicate: &str,
        pattern: &[Option<Value>],
    ) -> Vec<&'a FactTuple> {
        self.scan(predicate, pattern).collect()
    }

    pub fn predicates(&self) -> impl Iterator<Item = &str> {
        self.facts.keys().map(String::as_str)
    }
}

pub enum FactScan<'store, 'pattern> {
    Empty,
    All {
        iter: slice::Iter<'store, FactTuple>,
        pattern: &'pattern [Option<Value>],
    },
    Indexed {
        facts: &'store [FactTuple],
        tuple_indexes: slice::Iter<'store, usize>,
        pattern: &'pattern [Option<Value>],
    },
}

impl<'store> Iterator for FactScan<'store, '_> {
    type Item = &'store FactTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::All { iter, pattern } => {
                iter.find(|tuple| matches_pattern(pattern, tuple.as_slice()))
            }
            Self::Indexed {
                facts,
                tuple_indexes,
                pattern,
            } => tuple_indexes.find_map(|tuple_index| {
                facts
                    .get(*tuple_index)
                    .filter(|tuple| matches_pattern(pattern, tuple))
            }),
        }
    }
}

impl FactStore for InMemoryStorage {
    type Scan<'store, 'pattern>
        = FactScan<'store, 'pattern>
    where
        Self: 'store;

    fn scan<'store, 'pattern>(
        &'store self,
        predicate: &str,
        pattern: &'pattern [Option<Value>],
    ) -> Self::Scan<'store, 'pattern> {
        let Some(facts) = self.facts.get(predicate) else {
            return FactScan::Empty;
        };

        let best_index = best_index(self, predicate, pattern);

        if let Some(tuple_indexes) = best_index {
            return FactScan::Indexed {
                facts,
                tuple_indexes: tuple_indexes.iter(),
                pattern,
            };
        }

        FactScan::All {
            iter: facts.iter(),
            pattern,
        }
    }

    fn estimate(&self, predicate: &str, pattern: &[Option<Value>]) -> FactEstimate {
        let Some(facts) = self.facts.get(predicate) else {
            return FactEstimate::exact(0);
        };

        if let Some(tuple_indexes) = best_index(self, predicate, pattern) {
            return FactEstimate::upper_bound(tuple_indexes.len());
        }

        FactEstimate::upper_bound(facts.len())
    }
}

fn best_index<'a>(
    storage: &'a InMemoryStorage,
    predicate: &str,
    pattern: &[Option<Value>],
) -> Option<&'a Vec<usize>> {
    pattern
        .iter()
        .enumerate()
        .filter_map(|(value_index, value)| {
            let value = value.as_ref()?;
            let tuple_indexes = storage
                .indexes
                .get(predicate)?
                .get(&(value_index, value.clone()))?;
            Some(tuple_indexes)
        })
        .min_by_key(|tuple_indexes| tuple_indexes.len())
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get_facts_matching(
        &self,
        predicate: &str,
        pattern: Vec<Option<Value>>,
    ) -> Result<TupleStream> {
        let tuples = self
            .facts_matching(predicate, &pattern)
            .into_iter()
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        debug!(match_count = tuples.len(), "filtered in-memory tuples");

        let (tx, rx) = mpsc::channel(tuples.len().max(DEFAULT_STREAM_BUFFER));
        tokio::spawn(async move {
            for tuple in tuples {
                if tx.send(tuple).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

pub fn matches_pattern(pattern: &[Option<Value>], tuple: &[Value]) -> bool {
    pattern.len() == tuple.len()
        && pattern
            .iter()
            .zip(tuple)
            .all(|(pattern, value)| match pattern {
                Some(pattern) => pattern == value,
                None => true,
            })
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use crate::{FactStore, InMemoryStorage, Storage, Value, matches_pattern};

    #[test]
    fn matches_pattern_treats_none_as_wildcard() {
        assert!(matches_pattern(
            &[Some(Value::integer(1)), None],
            &[Value::integer(1), Value::integer(2)],
        ));
        assert!(!matches_pattern(
            &[Some(Value::integer(1)), None],
            &[Value::integer(2), Value::integer(3)],
        ));
    }

    #[test]
    fn in_memory_storage_filters_matching_tuples() {
        let storage = InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(2)],
                vec![Value::integer(2), Value::integer(3)],
            ],
        )]);

        let runtime = Runtime::new().expect("runtime");
        let tuples = runtime.block_on(async {
            let mut tuples = storage
                .get_facts_matching("edge", vec![Some(Value::integer(1)), None])
                .await
                .expect("tuples");
            let mut results = Vec::new();
            while let Some(tuple) = tuples.recv().await {
                results.push(tuple.expect("tuple result"));
            }
            results
        });

        assert_eq!(tuples, vec![vec![Value::integer(1), Value::integer(2)]]);
    }

    #[test]
    fn in_memory_storage_scans_without_collecting_first() {
        let storage = InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(2)],
                vec![Value::integer(2), Value::integer(3)],
            ],
        )]);

        let pattern = vec![Some(Value::integer(2)), None];
        let tuples = storage.scan("edge", &pattern).collect::<Vec<_>>();

        assert_eq!(tuples, vec![&vec![Value::integer(2), Value::integer(3)]]);
        assert_eq!(storage.estimate("edge", &pattern).rows, 1);
    }

    #[test]
    fn in_memory_storage_can_round_trip_through_serde() {
        let storage = InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(2)],
                vec![Value::integer(2), Value::integer(3)],
            ],
        )]);

        let encoded = bincode::serialize(&storage).expect("encoded storage");
        let decoded: InMemoryStorage = bincode::deserialize(&encoded).expect("decoded storage");
        let pattern = vec![Some(Value::integer(2)), None];

        assert_eq!(
            decoded.scan("edge", &pattern).collect::<Vec<_>>(),
            vec![&vec![Value::integer(2), Value::integer(3)]]
        );
        assert_eq!(decoded.estimate("edge", &pattern).rows, 1);
    }
}
