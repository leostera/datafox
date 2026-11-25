//! What one evaluation has asked its store. A body probes a relation once per binding of
//! the clauses before it, and many bindings ask the same lookup: in
//! `within(To, App), parent(App, "apps")` each of thousands of `To`s asks `parent` about the
//! same few directories. A store that answers each with a query (SQLite) spends its time
//! there, so a lookup asked again is answered from what was kept.
//!
//! An evaluation's store doesn't change while it runs, so what it answered stays true.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::Value;
use crate::storage::FactTuple;

/// A clause's answers, by the values its pattern binds: a clause's requests differ only by
/// these, as mode and equality groups are the clause's.
type Answers = FxHashMap<Vec<Option<Value>>, Arc<[FactTuple]>>;

/// The answers kept, by clause (its index in the plan).
#[derive(Default)]
pub(crate) struct Lookups {
    answers: FxHashMap<usize, Answers>,
    tuples: usize,
}

impl Lookups {
    /// The most tuples kept at once: past it, all are forgotten and keeping starts again.
    /// Lookups (each at least one, even an empty answer) hold a few MB at most.
    pub(crate) const KEPT: usize = 1 << 16;

    /// A lookup answered by more tuples isn't kept: it's a scan, which a store spends more
    /// on than on the query, and which would fill what's kept.
    pub(crate) const KEPT_PER_LOOKUP: usize = 64;

    /// What `clause` was answered for `pattern`, if it was kept.
    pub(crate) fn get(&self, clause: usize, pattern: &[Option<Value>]) -> Option<Arc<[FactTuple]>> {
        self.answers.get(&clause)?.get(pattern).map(Arc::clone)
    }

    /// Keeps `tuples`, what `clause` was answered for `pattern`.
    pub(crate) fn keep(
        &mut self,
        clause: usize,
        pattern: Vec<Option<Value>>,
        tuples: Vec<FactTuple>,
    ) {
        debug_assert!(tuples.len() <= Self::KEPT_PER_LOOKUP);
        let cost = tuples.len().max(1);
        if self.tuples + cost > Self::KEPT {
            self.answers.clear();
            self.tuples = 0;
        }
        self.tuples += cost;
        self.answers
            .entry(clause)
            .or_default()
            .insert(pattern, tuples.into());
    }
}

/// The tuples one request was answered with, while it's answered: `None` once there are
/// more than [`Lookups::KEPT_PER_LOOKUP`], as it won't be kept.
pub(crate) struct Answering(Option<Vec<FactTuple>>);

impl Answering {
    pub(crate) fn new() -> Self {
        Self(Some(Vec::new()))
    }

    pub(crate) fn push(&mut self, tuple: &FactTuple) {
        if let Some(tuples) = &mut self.0 {
            if tuples.len() == Lookups::KEPT_PER_LOOKUP {
                self.0 = None;
            } else {
                tuples.push(tuple.clone());
            }
        }
    }

    /// The whole answer, if it's to be kept.
    pub(crate) fn finish(self) -> Option<Vec<FactTuple>> {
        self.0
    }
}
