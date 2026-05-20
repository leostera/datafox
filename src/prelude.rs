use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use regex::Regex;

use crate::{FactTuple, InMemoryStorage, Value};

type BinaryRelationFn = dyn Fn(&Value, &Value) -> bool + Send + Sync;
type BinaryOperatorFn = dyn Fn(&Value, &Value) -> Option<Value> + Send + Sync;

#[derive(Clone)]
pub struct BinaryRelation {
    name: String,
    relation: Arc<BinaryRelationFn>,
}

impl BinaryRelation {
    pub fn new(
        name: impl Into<String>,
        relation: impl Fn(&Value, &Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            relation: Arc::new(relation),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn evaluate(&self, left: &Value, right: &Value) -> bool {
        (self.relation)(left, right)
    }
}

#[derive(Clone)]
pub struct BinaryOperator {
    name: String,
    operator: Arc<BinaryOperatorFn>,
}

impl BinaryOperator {
    pub fn new(
        name: impl Into<String>,
        operator: impl Fn(&Value, &Value) -> Option<Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            operator: Arc::new(operator),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn evaluate(&self, left: &Value, right: &Value) -> Option<Value> {
        (self.operator)(left, right)
    }
}

#[derive(Clone)]
pub struct Prelude {
    facts: InMemoryStorage,
    relations: BTreeMap<String, BinaryRelation>,
    operators: BTreeMap<String, BinaryOperator>,
}

impl Prelude {
    pub fn new() -> Self {
        let mut prelude = Self::empty();

        for relation in default_relations() {
            prelude = prelude.with_relation(relation);
        }
        for operator in default_operators() {
            prelude = prelude.with_operator(operator);
        }

        prelude
    }

    pub fn empty() -> Self {
        Self {
            facts: InMemoryStorage::new(),
            relations: BTreeMap::new(),
            operators: BTreeMap::new(),
        }
    }

    pub fn with_fact(
        mut self,
        predicate: impl Into<String>,
        tuple: impl IntoIterator<Item = Value>,
    ) -> Self {
        self.facts.insert(predicate, tuple.into_iter().collect());
        self
    }

    pub fn with_facts(mut self, facts: impl IntoIterator<Item = (String, Vec<FactTuple>)>) -> Self {
        for (predicate, tuples) in facts {
            for tuple in tuples {
                self.facts.insert(predicate.clone(), tuple);
            }
        }
        self
    }

    pub fn with_relation(mut self, relation: BinaryRelation) -> Self {
        self.relations.insert(relation.name().to_string(), relation);
        self
    }

    pub fn with_relations(mut self, relations: impl IntoIterator<Item = BinaryRelation>) -> Self {
        for relation in relations {
            self = self.with_relation(relation);
        }
        self
    }

    pub fn with_operator(mut self, operator: BinaryOperator) -> Self {
        self.operators.insert(operator.name().to_string(), operator);
        self
    }

    pub fn with_operators(mut self, operators: impl IntoIterator<Item = BinaryOperator>) -> Self {
        for operator in operators {
            self = self.with_operator(operator);
        }
        self
    }

    pub(crate) fn facts(&self) -> &InMemoryStorage {
        &self.facts
    }

    pub(crate) fn relation(&self, name: &str) -> Option<&BinaryRelation> {
        self.relations.get(name)
    }

    pub(crate) fn operator(&self, name: &str) -> Option<&BinaryOperator> {
        self.operators.get(name)
    }
}

impl Default for Prelude {
    fn default() -> Self {
        Self::new()
    }
}

fn default_relations() -> Vec<BinaryRelation> {
    let regex_cache = Arc::new(Mutex::new(HashMap::new()));

    vec![
        BinaryRelation::new("=", |left, right| left == right),
        BinaryRelation::new("eq", |left, right| left == right),
        BinaryRelation::new("<", |left, right| {
            values_are_ordered_compatibly(left, right) && left < right
        }),
        BinaryRelation::new("lt", |left, right| {
            values_are_ordered_compatibly(left, right) && left < right
        }),
        BinaryRelation::new("<=", |left, right| {
            values_are_ordered_compatibly(left, right) && left <= right
        }),
        BinaryRelation::new("lte", |left, right| {
            values_are_ordered_compatibly(left, right) && left <= right
        }),
        BinaryRelation::new(">", |left, right| {
            values_are_ordered_compatibly(left, right) && left > right
        }),
        BinaryRelation::new("gt", |left, right| {
            values_are_ordered_compatibly(left, right) && left > right
        }),
        BinaryRelation::new(">=", |left, right| {
            values_are_ordered_compatibly(left, right) && left >= right
        }),
        BinaryRelation::new("gte", |left, right| {
            values_are_ordered_compatibly(left, right) && left >= right
        }),
        BinaryRelation::new("before", |left, right| {
            values_are_ordered_compatibly(left, right) && left < right
        }),
        BinaryRelation::new("after", |left, right| {
            values_are_ordered_compatibly(left, right) && left > right
        }),
        BinaryRelation::new("startsWith", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| haystack.starts_with(needle))
        }),
        BinaryRelation::new("endsWith", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| haystack.ends_with(needle))
        }),
        BinaryRelation::new("contains", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| haystack.contains(needle))
        }),
        BinaryRelation::new("notStartsWith", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| !haystack.starts_with(needle))
        }),
        BinaryRelation::new("notEndsWith", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| !haystack.ends_with(needle))
        }),
        BinaryRelation::new("notContains", |left, right| {
            string_args(left, right).is_some_and(|(haystack, needle)| !haystack.contains(needle))
        }),
        {
            let regex_cache = Arc::clone(&regex_cache);
            BinaryRelation::new("matchesRegex", move |left, right| {
                string_args(left, right).is_some_and(|(haystack, pattern)| {
                    regex_is_match(&regex_cache, haystack, pattern)
                })
            })
        },
        {
            let regex_cache = Arc::clone(&regex_cache);
            BinaryRelation::new("notMatchesRegex", move |left, right| {
                string_args(left, right).is_some_and(|(haystack, pattern)| {
                    !regex_is_match(&regex_cache, haystack, pattern)
                })
            })
        },
    ]
}

fn default_operators() -> Vec<BinaryOperator> {
    vec![
        BinaryOperator::new("+", |left, right| {
            let (left, right) = integer_args(left, right)?;
            left.checked_add(right).map(Value::integer)
        }),
        BinaryOperator::new("-", |left, right| {
            let (left, right) = integer_args(left, right)?;
            left.checked_sub(right).map(Value::integer)
        }),
        BinaryOperator::new("*", |left, right| {
            let (left, right) = integer_args(left, right)?;
            left.checked_mul(right).map(Value::integer)
        }),
        BinaryOperator::new("/", |left, right| {
            let (left, right) = integer_args(left, right)?;
            left.checked_div(right).map(Value::integer)
        }),
    ]
}

fn values_are_ordered_compatibly(left: &Value, right: &Value) -> bool {
    matches!(
        (left, right),
        (Value::Integer(_), Value::Integer(_)) | (Value::String(_), Value::String(_))
    )
}

fn integer_args(left: &Value, right: &Value) -> Option<(i64, i64)> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some((*left, *right)),
        _ => None,
    }
}

fn string_args<'a>(left: &'a Value, right: &'a Value) -> Option<(&'a str, &'a str)> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => Some((left, right)),
        _ => None,
    }
}

fn regex_is_match(
    cache: &Mutex<HashMap<String, Option<Regex>>>,
    haystack: &str,
    pattern: &str,
) -> bool {
    let regex = match cache.lock() {
        Ok(mut cache) => cache
            .entry(pattern.to_string())
            .or_insert_with(|| Regex::new(pattern).ok())
            .clone(),
        Err(_) => Regex::new(pattern).ok(),
    };

    regex.is_some_and(|regex| regex.is_match(haystack))
}
