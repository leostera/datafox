//! Programs: facts and rules evaluated over a store's facts to a fixpoint, then queries.
//!
//! A program's relations are the least fixpoint of its rules over its facts and the
//! store's. Rules may be recursive; a rule may negate, or aggregate over, only relations
//! that don't depend on it (stratification). Evaluation is semi-naive: each round, a rule
//! is evaluated once per recursive atom in its body, that atom reading only the tuples the
//! previous round added.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};

use rustc_hash::{FxBuildHasher, FxHashMap};
use std::slice;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use async_trait::async_trait;

use crate::{
    Atom, Clause, DatafoxClient, DatafoxConfig, Error, FactEstimate, FactRequest, FactRequestMode,
    FactStore, FactTuple, Query, Result, Storage, Substitution, TaggedTupleStream, Term,
    TupleStream, Value, finished_stream, matches_pattern,
};

/// A program: facts, rules, and the queries to answer over them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Program {
    /// Ground atoms.
    pub facts: Vec<Atom>,
    pub rules: Vec<Rule>,
    pub queries: Vec<Query>,
}

/// `head :- body.`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub head: Head,
    pub body: Vec<Clause>,
}

/// A rule's head: an atom whose terms may aggregate a body variable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub predicate: String,
    pub args: Vec<HeadTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeadTerm {
    Term(Term),
    /// `count(X)`: over the body's distinct solutions (its named variables' bindings; `_`
    /// isn't one), grouped by the head's other terms.
    Aggregate {
        aggregate: Aggregate,
        variable: String,
    },
}

/// What an aggregate computes over a group's distinct solutions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggregate {
    /// How many distinct values the variable takes.
    Count,
    /// The sum of the variable's integer values, one per solution.
    Sum,
    Min,
    Max,
}

impl Aggregate {
    pub(crate) fn named(name: &str) -> Option<Self> {
        match name {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

impl Head {
    /// The head as a fact: every term a constant.
    pub(crate) fn into_fact(self) -> std::result::Result<Atom, String> {
        let mut args = Vec::with_capacity(self.args.len());
        for term in self.args {
            match term {
                HeadTerm::Term(term @ Term::Const(_)) => args.push(term),
                _ => return Err(format!("a fact of `{}` must be ground", self.predicate)),
            }
        }
        Atom::new(self.predicate, args).map_err(|err| err.to_string())
    }

    fn is_aggregate(&self) -> bool {
        self.args
            .iter()
            .any(|term| matches!(term, HeadTerm::Aggregate { .. }))
    }
}

impl Rule {
    fn positive_atoms(&self) -> impl Iterator<Item = &Atom> {
        self.body.iter().filter_map(|clause| match clause {
            Clause::Atom(atom) => Some(atom),
            _ => None,
        })
    }

    /// Every predicate the body reads, and whether through a negation.
    fn dependencies(&self) -> impl Iterator<Item = (&str, bool)> {
        self.body.iter().filter_map(|clause| match clause {
            Clause::Atom(atom) => Some((atom.predicate.as_str(), false)),
            Clause::Negated(atom) => Some((atom.predicate.as_str(), true)),
            Clause::Builtin { .. } => None,
        })
    }
}

impl Program {
    /// Every predicate the program defines, by its facts or its rules' heads.
    pub fn defined(&self) -> BTreeSet<&str> {
        self.facts
            .iter()
            .map(|fact| fact.predicate.as_str())
            .chain(self.rules.iter().map(|rule| rule.head.predicate.as_str()))
            .collect()
    }

    /// Every predicate the program's rules and queries read, defined by it or not.
    pub fn used(&self) -> BTreeSet<String> {
        let rules = self
            .rules
            .iter()
            .flat_map(Rule::dependencies)
            .map(|(p, _)| p.to_owned());
        let queries =
            self.queries
                .iter()
                .flat_map(Query::clauses)
                .filter_map(|clause| match clause {
                    Clause::Atom(atom) | Clause::Negated(atom) => Some(atom.predicate),
                    Clause::Builtin { .. } => None,
                });
        rules.chain(queries).collect()
    }

    /// Adds `other`'s facts, rules and queries to this program's.
    pub fn extend(&mut self, other: Self) {
        self.facts.extend(other.facts);
        self.rules.extend(other.rules);
        self.queries.extend(other.queries);
    }

    /// The arity of each relation the program defines or reads, as it first uses it.
    ///
    /// # Errors
    /// If a relation is used with two arities.
    pub fn arities(&self) -> Result<BTreeMap<String, usize>> {
        let mut arities = BTreeMap::new();
        let mut note = |predicate: &str, arity: usize| -> Result<()> {
            match arities.get(predicate) {
                Some(&expected) if expected != arity => Err(Error::ArityMismatch {
                    predicate: predicate.to_owned(),
                    expected,
                    found: arity,
                }),
                Some(_) => Ok(()),
                None => {
                    arities.insert(predicate.to_owned(), arity);
                    Ok(())
                }
            }
        };
        for fact in &self.facts {
            note(&fact.predicate, fact.args.len())?;
        }
        for rule in &self.rules {
            note(&rule.head.predicate, rule.head.args.len())?;
        }
        let bodies = self.rules.iter().flat_map(|rule| rule.body.iter());
        let queries: Vec<Clause> = self.queries.iter().flat_map(Query::clauses).collect();
        for clause in bodies.chain(queries.iter()) {
            if let Clause::Atom(atom) | Clause::Negated(atom) = clause {
                note(&atom.predicate, atom.args.len())?;
            }
        }
        Ok(arities)
    }

    /// Checks that every rule is safe and the program stratifies, and returns its strata:
    /// groups of mutually recursive predicates, each after every group it depends on.
    ///
    /// # Errors
    /// If a head or negated variable isn't bound by a positive atom, a head isn't a
    /// variable, a constant or an aggregate, a predicate is defined both by an aggregate
    /// rule and otherwise, or a predicate depends on itself through a negation or an
    /// aggregate.
    pub fn strata(&self) -> Result<Vec<Vec<String>>> {
        self.arities()?;
        for rule in &self.rules {
            check_safe(rule)?;
        }
        let mut aggregated = HashSet::new();
        let mut plain = HashSet::new();
        for rule in &self.rules {
            if rule.head.is_aggregate() {
                aggregated.insert(rule.head.predicate.as_str());
            } else {
                plain.insert(rule.head.predicate.as_str());
            }
        }
        for fact in &self.facts {
            plain.insert(fact.predicate.as_str());
        }
        if let Some(both) = aggregated.intersection(&plain).next() {
            return Err(Error::InvalidProgram {
                message: format!(
                    "`{both}` is defined both by a rule that aggregates and by other rules or facts"
                ),
            });
        }

        let defined = self.defined();
        let names: Vec<&str> = defined.iter().copied().collect();
        let index: HashMap<&str, usize> = names.iter().enumerate().map(|(i, n)| (*n, i)).collect();
        // Edges from a head to what its body reads, among defined predicates; `true` when
        // the dependency can't be in the head's own stratum.
        let mut edges: Vec<Vec<(usize, bool)>> = vec![Vec::new(); names.len()];
        for rule in &self.rules {
            let head = index[rule.head.predicate.as_str()];
            let aggregate = rule.head.is_aggregate();
            for (predicate, negated) in rule.dependencies() {
                if let Some(&to) = index.get(predicate) {
                    edges[head].push((to, negated || aggregate));
                }
            }
        }
        let components = tarjan(&edges);
        let mut component_of = vec![0; names.len()];
        for (c, members) in components.iter().enumerate() {
            for &member in members {
                component_of[member] = c;
            }
        }
        for (from, targets) in edges.iter().enumerate() {
            for &(to, strict) in targets {
                if strict && component_of[from] == component_of[to] {
                    return Err(Error::NotStratifiable {
                        predicate: names[from].to_owned(),
                        depends_on: names[to].to_owned(),
                    });
                }
            }
        }
        // Tarjan returns components with every dependency before its dependents.
        Ok(components
            .into_iter()
            .map(|members| members.into_iter().map(|m| names[m].to_owned()).collect())
            .collect())
    }

    /// The predicates `wanted` depend on, themselves included, through the program's rules.
    pub fn needed<'a>(&self, wanted: impl IntoIterator<Item = &'a str>) -> BTreeSet<String> {
        let mut needed = BTreeSet::new();
        let mut stack: Vec<String> = wanted.into_iter().map(str::to_owned).collect();
        while let Some(predicate) = stack.pop() {
            if !needed.insert(predicate.clone()) {
                continue;
            }
            for rule in self
                .rules
                .iter()
                .filter(|rule| rule.head.predicate == predicate)
            {
                stack.extend(rule.dependencies().map(|(p, _)| p.to_owned()));
            }
        }
        needed
    }
}

fn check_safe(rule: &Rule) -> Result<()> {
    let bound: HashSet<&str> = rule.positive_atoms().flat_map(Atom::variables).collect();
    let unsafe_variable = |variable: &str| Error::UnsafeRule {
        predicate: rule.head.predicate.clone(),
        variable: variable.to_owned(),
    };
    for term in &rule.head.args {
        match term {
            HeadTerm::Term(Term::Var(variable)) | HeadTerm::Aggregate { variable, .. } => {
                if !bound.contains(variable.as_str()) {
                    return Err(unsafe_variable(variable));
                }
            }
            HeadTerm::Term(Term::Const(_)) => {}
            HeadTerm::Term(other) => {
                return Err(Error::InvalidProgram {
                    message: format!(
                        "a head of `{}` has `{other}`: a head's terms are variables, constants or aggregates",
                        rule.head.predicate
                    ),
                });
            }
        }
    }
    for clause in &rule.body {
        if let Clause::Negated(atom) = clause
            && let Some(variable) = atom.variables().into_iter().find(|v| !bound.contains(v))
        {
            return Err(unsafe_variable(variable));
        }
    }
    Ok(())
}

/// Strongly connected components, each after every component it has an edge to.
fn tarjan(edges: &[Vec<(usize, bool)>]) -> Vec<Vec<usize>> {
    struct State<'a> {
        edges: &'a [Vec<(usize, bool)>],
        index: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        next: usize,
        components: Vec<Vec<usize>>,
    }
    fn visit(state: &mut State<'_>, node: usize) {
        state.index[node] = Some(state.next);
        state.low[node] = state.next;
        state.next += 1;
        state.stack.push(node);
        state.on_stack[node] = true;
        for &(to, _) in &state.edges[node] {
            match state.index[to] {
                None => {
                    visit(state, to);
                    state.low[node] = state.low[node].min(state.low[to]);
                }
                Some(index) if state.on_stack[to] => {
                    state.low[node] = state.low[node].min(index);
                }
                Some(_) => {}
            }
        }
        if Some(state.low[node]) == state.index[node] {
            let mut component = Vec::new();
            loop {
                let member = state.stack.pop().expect("on the stack");
                state.on_stack[member] = false;
                component.push(member);
                if member == node {
                    break;
                }
            }
            component.sort_unstable();
            state.components.push(component);
        }
    }
    let mut state = State {
        edges,
        index: vec![None; edges.len()],
        low: vec![0; edges.len()],
        on_stack: vec![false; edges.len()],
        stack: Vec::new(),
        next: 0,
        components: Vec::new(),
    };
    for node in 0..edges.len() {
        if state.index[node].is_none() {
            visit(&mut state, node);
        }
    }
    state.components
}

/// Derived relations are hashed only to find tuples, never to resist an adversary, so with
/// a fast hasher.
fn hash_of(value: &impl Hash) -> u64 {
    FxBuildHasher.hash_one(value)
}

type Index = FxHashMap<u64, Vec<u32>>;

/// One derived relation: its tuples, each once, and an index per column from a value's
/// hash to the tuples with that value there, built the first time a scan binds the column
/// and kept up to date as tuples are added. Every scan checks each tuple against the whole
/// pattern, so a collision costs a comparison, never a wrong answer.
#[derive(Debug, Default)]
struct Relation {
    tuples: Vec<FactTuple>,
    /// Tuples by their hash.
    members: Index,
    columns: Vec<OnceLock<Index>>,
}

impl Relation {
    /// Adds `tuple`; returns whether it's new.
    fn insert(&mut self, tuple: FactTuple) -> bool {
        let hash = hash_of(&tuple);
        let same = self.members.entry(hash).or_default();
        if same.iter().any(|&ix| self.tuples[ix as usize] == tuple) {
            return false;
        }
        let ix = u32::try_from(self.tuples.len()).expect("fewer than 2^32 tuples in a relation");
        same.push(ix);
        if self.columns.len() < tuple.len() {
            self.columns.resize_with(tuple.len(), OnceLock::new);
        }
        for (column, value) in tuple.iter().enumerate() {
            if let Some(index) = self.columns[column].get_mut() {
                index.entry(hash_of(value)).or_default().push(ix);
            }
        }
        self.tuples.push(tuple);
        true
    }

    fn is_empty(&self) -> bool {
        self.tuples.is_empty()
    }

    fn column(&self, column: usize) -> Option<&Index> {
        let index = self.columns.get(column)?;
        Some(index.get_or_init(|| {
            let mut index = Index::default();
            for (ix, tuple) in self.tuples.iter().enumerate() {
                if let Some(value) = tuple.get(column) {
                    let ix = u32::try_from(ix).expect("fewer than 2^32 tuples in a relation");
                    index.entry(hash_of(value)).or_default().push(ix);
                }
            }
            index
        }))
    }

    /// The tuples that can match `pattern`: those with the rarest of its bound values, or
    /// every tuple (`Some(None)`) if it binds nothing; `None` if a bound value is in no
    /// tuple.
    fn candidates(&self, pattern: &[Option<Value>]) -> Option<Option<&[u32]>> {
        let mut best: Option<&[u32]> = None;
        for (column, value) in pattern.iter().enumerate() {
            let Some(value) = value else { continue };
            let ixs = self.column(column)?.get(&hash_of(value))?;
            if best.is_none_or(|best| ixs.len() < best.len()) {
                best = Some(ixs);
            }
        }
        Some(best)
    }

    fn scan<'store, 'pattern>(
        &'store self,
        pattern: &'pattern [Option<Value>],
    ) -> RelationScan<'store, 'pattern> {
        match self.candidates(pattern) {
            None => RelationScan::Empty,
            Some(None) => RelationScan::All {
                tuples: self.tuples.iter(),
                pattern,
            },
            Some(Some(ixs)) => RelationScan::Indexed {
                tuples: &self.tuples,
                ixs: ixs.iter(),
                pattern,
            },
        }
    }

    fn estimate(&self, pattern: &[Option<Value>]) -> FactEstimate {
        match self.candidates(pattern) {
            None => FactEstimate::exact(0),
            Some(None) => FactEstimate::upper_bound(self.tuples.len()),
            Some(Some(ixs)) => FactEstimate::upper_bound(ixs.len()),
        }
    }
}

/// The tuples of a derived relation that match a pattern.
pub enum RelationScan<'store, 'pattern> {
    Empty,
    All {
        tuples: slice::Iter<'store, FactTuple>,
        pattern: &'pattern [Option<Value>],
    },
    Indexed {
        tuples: &'store [FactTuple],
        ixs: slice::Iter<'store, u32>,
        pattern: &'pattern [Option<Value>],
    },
}

impl<'store> Iterator for RelationScan<'store, '_> {
    type Item = &'store FactTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::All { tuples, pattern } => tuples.find(|tuple| matches_pattern(pattern, tuple)),
            Self::Indexed {
                tuples,
                ixs,
                pattern,
            } => ixs.find_map(|&ix| {
                let tuple = &tuples[ix as usize];
                matches_pattern(pattern, tuple).then_some(tuple)
            }),
        }
    }
}

/// Derived relations by name.
#[derive(Debug, Default)]
struct Relations(FxHashMap<String, Relation>);

impl Relations {
    fn scan<'store, 'pattern>(
        &'store self,
        predicate: &str,
        pattern: &'pattern [Option<Value>],
    ) -> RelationScan<'store, 'pattern> {
        self.0
            .get(predicate)
            .map_or(RelationScan::Empty, |relation| relation.scan(pattern))
    }

    fn estimate(&self, predicate: &str, pattern: &[Option<Value>]) -> FactEstimate {
        self.0
            .get(predicate)
            .map_or(FactEstimate::exact(0), |relation| {
                relation.estimate(pattern)
            })
    }

    fn is_empty(&self) -> bool {
        self.0.values().all(Relation::is_empty)
    }
}

/// The relations a program derived, over the store it was evaluated on.
#[derive(Debug, Default)]
pub struct Derived {
    relations: Relations,
    /// The tuples the last round added, under their delta names, while a stratum is
    /// evaluated.
    delta: Relations,
}

/// The name a recursive atom reads the last round's tuples under.
fn delta_name(predicate: &str) -> String {
    format!("\u{0}delta\u{0}{predicate}")
}

impl Derived {
    /// Whether the program defines `predicate`.
    pub fn defines(&self, predicate: &str) -> bool {
        self.relations.0.contains_key(predicate)
    }

    /// The tuples of `predicate`, if the program defines it.
    pub fn tuples(&self, predicate: &str) -> &[FactTuple] {
        self.relations
            .0
            .get(predicate)
            .map_or(&[], |relation| relation.tuples.as_slice())
    }

    /// How many tuples `predicate` has.
    pub fn len(&self, predicate: &str) -> usize {
        self.tuples(predicate).len()
    }

    fn define(&mut self, predicate: &str) {
        self.relations.0.entry(predicate.to_owned()).or_default();
    }

    /// Defines the relations of `stratum`, with the program's facts of them.
    fn start(&mut self, program: &Program, stratum: &[String]) {
        for predicate in stratum {
            self.define(predicate);
        }
        for fact in program
            .facts
            .iter()
            .filter(|f| stratum.contains(&f.predicate))
        {
            let tuple = fact
                .args
                .iter()
                .filter_map(Term::const_value)
                .cloned()
                .collect();
            self.insert(&fact.predicate, tuple);
        }
    }

    /// Adds `tuple` to `predicate`; returns whether it's new.
    fn insert(&mut self, predicate: &str, tuple: FactTuple) -> bool {
        self.relations
            .0
            .entry(predicate.to_owned())
            .or_default()
            .insert(tuple)
    }
}

/// A store of derived relations over another store: a predicate the program defines is
/// read from what it derived, any other from the store below.
pub struct Layered<'a, S: FactStore + ?Sized> {
    below: &'a S,
    derived: &'a Derived,
}

impl<'a, S: FactStore + ?Sized> Layered<'a, S> {
    pub fn new(below: &'a S, derived: &'a Derived) -> Self {
        Self { below, derived }
    }
}

/// A scan of a layered store: from what was derived, or from the store below.
pub enum LayeredScan<'store, 'pattern, B> {
    Derived(RelationScan<'store, 'pattern>),
    Below(B),
}

impl<'store, B: Iterator<Item = &'store FactTuple>> Iterator for LayeredScan<'store, '_, B> {
    type Item = &'store FactTuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Derived(scan) => scan.next(),
            Self::Below(scan) => scan.next(),
        }
    }
}

impl<S: FactStore + ?Sized> FactStore for Layered<'_, S> {
    type Scan<'store, 'pattern>
        = LayeredScan<'store, 'pattern, S::Scan<'store, 'pattern>>
    where
        Self: 'store;

    fn scan<'store, 'pattern>(
        &'store self,
        predicate: &str,
        pattern: &'pattern [Option<Value>],
    ) -> Self::Scan<'store, 'pattern> {
        if predicate.starts_with('\u{0}') {
            LayeredScan::Derived(self.derived.delta.scan(predicate, pattern))
        } else if self.derived.defines(predicate) {
            LayeredScan::Derived(self.derived.relations.scan(predicate, pattern))
        } else {
            LayeredScan::Below(self.below.scan(predicate, pattern))
        }
    }

    fn estimate(&self, predicate: &str, pattern: &[Option<Value>]) -> FactEstimate {
        if predicate.starts_with('\u{0}') {
            self.derived.delta.estimate(predicate, pattern)
        } else if self.derived.defines(predicate) {
            self.derived.relations.estimate(predicate, pattern)
        } else {
            self.below.estimate(predicate, pattern)
        }
    }
}

/// [`Layered`], as a [`Storage`]: what was derived from memory, the rest asked of the
/// storage below.
pub struct LayeredStorage<'a, S: Storage + ?Sized> {
    below: &'a S,
    derived: &'a Derived,
}

impl<'a, S: Storage + ?Sized> LayeredStorage<'a, S> {
    pub fn new(below: &'a S, derived: &'a Derived) -> Self {
        Self { below, derived }
    }

    /// The relations in memory that hold `predicate`, if they do.
    fn in_memory(&self, predicate: &str) -> Option<&Relations> {
        if predicate.starts_with('\u{0}') {
            Some(&self.derived.delta)
        } else if self.derived.defines(predicate) {
            Some(&self.derived.relations)
        } else {
            None
        }
    }
}

/// The tuples of `relations` a request asks for.
fn answer(relations: &Relations, request: &FactRequest) -> Vec<FactTuple> {
    let pattern = request.pattern_options();
    let limit = match request.mode {
        FactRequestMode::Tuples => request.hints.limit,
        FactRequestMode::Exists => Some(request.hints.limit.unwrap_or(1)),
    };
    relations
        .scan(&request.predicate, &pattern)
        .take(limit.unwrap_or(usize::MAX))
        .cloned()
        .collect()
}

#[async_trait]
impl<S: Storage + ?Sized> Storage for LayeredStorage<'_, S> {
    async fn get_facts(&self, request: FactRequest) -> Result<TupleStream> {
        match self.in_memory(&request.predicate) {
            Some(relations) => Ok(finished_stream(
                answer(relations, &request).into_iter().map(Ok).collect(),
            )),
            None => self.below.get_facts(request).await,
        }
    }

    async fn get_facts_many(&self, requests: Vec<FactRequest>) -> Result<TaggedTupleStream> {
        let mut found = Vec::new();
        let mut below = Vec::new();
        for (index, request) in requests.into_iter().enumerate() {
            match self.in_memory(&request.predicate) {
                Some(relations) => found.extend(
                    answer(relations, &request)
                        .into_iter()
                        .map(|tuple| Ok((index, tuple))),
                ),
                None => below.push((index, request)),
            }
        }
        if below.is_empty() {
            return Ok(finished_stream(found));
        }
        let (indexes, requests): (Vec<usize>, Vec<FactRequest>) = below.into_iter().unzip();
        let mut streamed = self.below.get_facts_many(requests).await?;
        if found.is_empty() && indexes.iter().enumerate().all(|(at, index)| at == *index) {
            return Ok(streamed);
        }
        // Some of the batch came from memory: the rest is renumbered as it streams.
        let (tx, rx) = tokio::sync::mpsc::channel(found.len().max(64));
        tokio::spawn(async move {
            for item in found {
                if tx.send(item).await.is_err() {
                    return;
                }
            }
            while let Some(item) = streamed.recv().await {
                let item = item.map(|(at, tuple)| (indexes[at], tuple));
                if tx.send(item).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }
}

/// Evaluates the relations `wanted` need (every one the program defines, if `None`) over
/// `store`, to a fixpoint.
///
/// # Errors
/// As [`Program::strata`], or if a rule's body can't be evaluated.
pub fn evaluate<S: FactStore + ?Sized>(
    program: &Program,
    store: &S,
    wanted: Option<&[&str]>,
) -> Result<Derived> {
    let strata = program.strata()?;
    let needed = wanted.map(|wanted| program.needed(wanted.iter().copied()));
    let mut derived = Derived::default();
    for stratum in strata {
        if needed
            .as_ref()
            .is_some_and(|needed| !stratum.iter().any(|p| needed.contains(p)))
        {
            continue;
        }
        derived.start(program, &stratum);
        evaluate_stratum(program, store, &stratum, &mut derived)?;
    }
    Ok(derived)
}

fn evaluate_stratum<S: FactStore + ?Sized>(
    program: &Program,
    store: &S,
    stratum: &[String],
    derived: &mut Derived,
) -> Result<()> {
    let rules = stratum_rules(program, stratum);
    // The first round reads every relation whole.
    let mut added: Vec<(&str, Vec<FactTuple>)> = Vec::new();
    for rule in &rules {
        added.push((
            rule.head.predicate.as_str(),
            solve(rule, &rule.body, store, derived)?,
        ));
    }
    let mut delta = commit(derived, added);
    while !delta.is_empty() {
        derived.delta = delta;
        let mut added = Vec::new();
        for (rule, body) in delta_bodies(&rules, stratum) {
            added.push((
                rule.head.predicate.as_str(),
                solve(rule, &body, store, derived)?,
            ));
        }
        delta = commit(derived, added);
    }
    derived.delta = Relations::default();
    Ok(())
}

/// [`evaluate`], over a [`Storage`]: the store's relations are asked for by what each
/// rule's body binds, a batch at a time, and streamed; only what the program derives is
/// held in memory.
///
/// # Errors
/// As [`evaluate`], or if the storage fails.
pub async fn evaluate_streaming<S: Storage + ?Sized>(
    program: &Program,
    storage: &S,
    wanted: Option<&[&str]>,
) -> Result<Derived> {
    let strata = program.strata()?;
    let needed = wanted.map(|wanted| program.needed(wanted.iter().copied()));
    let mut derived = Derived::default();
    for stratum in strata {
        if needed
            .as_ref()
            .is_some_and(|needed| !stratum.iter().any(|p| needed.contains(p)))
        {
            continue;
        }
        derived.start(program, &stratum);
        let rules = stratum_rules(program, &stratum);
        let mut added = Vec::new();
        for rule in &rules {
            added.push((
                rule.head.predicate.as_str(),
                solve_streaming(rule, &rule.body, storage, &derived).await?,
            ));
        }
        let mut delta = commit(&mut derived, added);
        while !delta.is_empty() {
            derived.delta = delta;
            let mut added = Vec::new();
            for (rule, body) in delta_bodies(&rules, &stratum) {
                added.push((
                    rule.head.predicate.as_str(),
                    solve_streaming(rule, &body, storage, &derived).await?,
                ));
            }
            delta = commit(&mut derived, added);
        }
        derived.delta = Relations::default();
    }
    Ok(derived)
}

/// The rules that define the relations of `stratum`.
fn stratum_rules<'p>(program: &'p Program, stratum: &[String]) -> Vec<&'p Rule> {
    program
        .rules
        .iter()
        .filter(|rule| stratum.contains(&rule.head.predicate))
        .collect()
}

/// Each rule of a recursive stratum once per recursive atom in its body, that atom reading
/// the last round's tuples: a semi-naive round. Empty if no rule is recursive.
fn delta_bodies<'p>(rules: &[&'p Rule], stratum: &[String]) -> Vec<(&'p Rule, Vec<Clause>)> {
    let mut bodies = Vec::new();
    for rule in rules {
        for (position, clause) in rule.body.iter().enumerate() {
            let Clause::Atom(atom) = clause else {
                continue;
            };
            if !stratum.contains(&atom.predicate) {
                continue;
            }
            let mut body = rule.body.clone();
            body[position] = Clause::Atom(Atom {
                predicate: delta_name(&atom.predicate),
                args: atom.args.clone(),
            });
            bodies.push((*rule, body));
        }
    }
    bodies
}

/// Adds tuples to what was derived; returns the ones that were new, under their delta names.
fn commit(derived: &mut Derived, added: Vec<(&str, Vec<FactTuple>)>) -> Relations {
    let mut delta = Relations::default();
    for (predicate, tuples) in added {
        let name = delta_name(predicate);
        for tuple in tuples {
            if derived.insert(predicate, tuple.clone()) {
                delta.0.entry(name.clone()).or_default().insert(tuple);
            }
        }
    }
    delta
}

/// The head tuples `body` gives `rule`, aggregated if its head aggregates.
fn solve<S: FactStore + ?Sized>(
    rule: &Rule,
    body: &[Clause],
    store: &S,
    derived: &Derived,
) -> Result<Vec<FactTuple>> {
    let layered = Layered::new(store, derived);
    let client = DatafoxClient::new(DatafoxConfig::new(&layered).serial())?;
    let mut heads = Heads::new(rule);
    for solution in client.eval(&body_query(body)?)? {
        heads.add(&solution);
    }
    heads.finish()
}

/// [`solve`], over a [`Storage`]: the body's solutions are streamed into the heads.
async fn solve_streaming<S: Storage + ?Sized>(
    rule: &Rule,
    body: &[Clause],
    storage: &S,
    derived: &Derived,
) -> Result<Vec<FactTuple>> {
    let layered = LayeredStorage::new(storage, derived);
    let client = DatafoxClient::new(DatafoxConfig::new(&layered).serial())?;
    let mut heads = Heads::new(rule);
    client
        .stream(&body_query(body)?, &mut |solution| {
            heads.add(&solution);
            Ok(())
        })
        .await?;
    heads.finish()
}

fn body_query(body: &[Clause]) -> Result<Query> {
    match body {
        [Clause::Atom(atom)] => Ok(Query::single(atom.clone())),
        _ => Query::multi(body.to_vec()),
    }
}

/// A rule's head tuples, from its body's solutions one at a time. An aggregating head
/// groups the body's distinct solutions by the head's other terms; each aggregate gets its
/// variable's value in every solution of the group.
struct Heads<'r> {
    rule: &'r Rule,
    /// The variables of the body's positive atoms: what makes two solutions distinct.
    variables: Vec<&'r str>,
    tuples: Vec<FactTuple>,
    distinct: HashSet<Vec<Value>>,
    groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>>,
}

impl<'r> Heads<'r> {
    fn new(rule: &'r Rule) -> Self {
        let variables: BTreeSet<&str> = rule.positive_atoms().flat_map(Atom::variables).collect();
        Self {
            rule,
            variables: variables.into_iter().collect(),
            tuples: Vec::new(),
            distinct: HashSet::new(),
            groups: BTreeMap::new(),
        }
    }

    fn add(&mut self, solution: &Substitution) {
        let bound = |variable: &str| {
            solution
                .lookup(variable)
                .cloned()
                .expect("a safe rule binds its head")
        };
        if !self.rule.head.is_aggregate() {
            let tuple = self
                .rule
                .head
                .args
                .iter()
                .map(|term| match term {
                    HeadTerm::Term(Term::Var(variable)) => bound(variable),
                    HeadTerm::Term(term) => term.const_value().cloned().expect("checked in strata"),
                    HeadTerm::Aggregate { .. } => unreachable!("not an aggregate"),
                })
                .collect();
            self.tuples.push(tuple);
            return;
        }
        if !self
            .distinct
            .insert(self.variables.iter().map(|v| bound(v)).collect())
        {
            return;
        }
        let mut key = Vec::new();
        let mut values = Vec::new();
        for term in &self.rule.head.args {
            match term {
                HeadTerm::Term(Term::Var(variable)) => key.push(bound(variable)),
                HeadTerm::Term(term) => key.push(term.const_value().cloned().expect("checked")),
                HeadTerm::Aggregate { variable, .. } => values.push(bound(variable)),
            }
        }
        let group = self
            .groups
            .entry(key)
            .or_insert_with(|| vec![Vec::new(); values.len()]);
        for (slot, value) in group.iter_mut().zip(values) {
            slot.push(value);
        }
    }

    fn finish(self) -> Result<Vec<FactTuple>> {
        if !self.rule.head.is_aggregate() {
            return Ok(self.tuples);
        }
        let mut tuples = Vec::new();
        for (key, group) in self.groups {
            let mut key = key.into_iter();
            let mut aggregates = group.into_iter();
            let mut tuple = Vec::with_capacity(self.rule.head.args.len());
            for term in &self.rule.head.args {
                match term {
                    HeadTerm::Aggregate { aggregate, .. } => {
                        let values = aggregates.next().expect("one per aggregate");
                        tuple.push(fold(*aggregate, &self.rule.head.predicate, values)?);
                    }
                    HeadTerm::Term(_) => tuple.push(key.next().expect("one per term")),
                }
            }
            tuples.push(tuple);
        }
        Ok(tuples)
    }
}

/// An aggregate of a group's values, one per solution.
fn fold(aggregate: Aggregate, predicate: &str, values: Vec<Value>) -> Result<Value> {
    if aggregate == Aggregate::Count {
        let distinct: BTreeSet<Value> = values.into_iter().collect();
        return Ok(Value::integer(
            i64::try_from(distinct.len()).unwrap_or(i64::MAX),
        ));
    }
    let integers = values
        .into_iter()
        .map(|value| match value {
            Value::Integer(n) => Ok(n),
            Value::String(_) => Err(Error::InvalidProgram {
                message: format!(
                    "`{}` in a head of `{predicate}` reads integers",
                    aggregate.as_str()
                ),
            }),
        })
        .collect::<Result<Vec<i64>>>()?;
    let folded = match aggregate {
        Aggregate::Sum => integers.iter().sum(),
        Aggregate::Min => integers.iter().copied().min().unwrap_or(0),
        Aggregate::Max => integers.iter().copied().max().unwrap_or(0),
        Aggregate::Count => unreachable!("counted above"),
    };
    Ok(Value::integer(folded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryStorage, Substitution, parse_program};

    fn store() -> InMemoryStorage {
        let edge = |a: i64, b: i64| vec![Value::integer(a), Value::integer(b)];
        InMemoryStorage::from_facts([(
            "edge".to_owned(),
            vec![edge(1, 2), edge(2, 3), edge(3, 4), edge(4, 2), edge(5, 6)],
        )])
    }

    /// The answers to the program's queries, over `store` and what it derives, as text.
    /// The program's answers over `store`, which must be the same evaluated in memory and
    /// streamed from a [`Storage`].
    fn answers(source: &str, store: &InMemoryStorage) -> Result<Vec<String>> {
        let program = parse_program(source)?;
        let derived = evaluate(&program, store, None)?;
        let layered = Layered::new(store, &derived);
        let client = DatafoxClient::new(DatafoxConfig::new(&layered))?;
        let mut found = Vec::new();
        for query in &program.queries {
            for solution in client.eval(query)? {
                found.push(show(&solution));
            }
        }
        found.sort();
        let streamed = tokio::runtime::Runtime::new()
            .expect("a runtime")
            .block_on(streamed_answers(&program, store))?;
        assert_eq!(streamed, found, "streamed from a Storage");
        Ok(found)
    }

    async fn streamed_answers(program: &Program, store: &InMemoryStorage) -> Result<Vec<String>> {
        let derived = evaluate_streaming(program, store, None).await?;
        let layered = LayeredStorage::new(store, &derived);
        let client = DatafoxClient::new(DatafoxConfig::new(&layered))?;
        let mut found = Vec::new();
        for query in &program.queries {
            client
                .stream(query, &mut |solution| {
                    found.push(show(&solution));
                    Ok(())
                })
                .await?;
        }
        found.sort();
        Ok(found)
    }

    fn show(solution: &Substitution) -> String {
        let mut bindings: Vec<String> = solution
            .iter()
            .map(|(name, value)| match value {
                Value::String(text) => format!("{name}={text}"),
                Value::Integer(number) => format!("{name}={number}"),
            })
            .collect();
        bindings.sort();
        bindings.join(" ")
    }

    #[test]
    fn recursion_reaches_a_fixpoint() -> Result<()> {
        let found = answers(
            "path(X, Y) :- edge(X, Y).\n\
             path(X, Z) :- path(X, Y), edge(Y, Z).\n\
             path(1, Y)",
            &store(),
        )?;
        assert_eq!(found, ["Y=2", "Y=3", "Y=4"]);
        Ok(())
    }

    #[test]
    fn facts_and_rules_come_before_the_queries() -> Result<()> {
        let program = parse_program(
            "% layers\nlayer(\"http\", 3).\nlayer(\"store\", 1).\n\
             above(A, B) :- layer(A, X), layer(B, Y), X > Y.\n\
             above(A, B); layer(A, 3)",
        )?;
        assert_eq!(program.facts.len(), 2);
        assert_eq!(program.rules.len(), 1);
        assert_eq!(program.queries.len(), 2);
        let found = answers(
            "layer(\"http\", 3). layer(\"store\", 1).\n\
             above(A, B) :- layer(A, X), layer(B, Y), X > Y.\n\
             above(A, B)",
            &store(),
        )?;
        assert_eq!(found, ["A=http B=store"]);
        Ok(())
    }

    #[test]
    fn a_query_alone_is_still_a_program() -> Result<()> {
        let program = parse_program("edge(X, Y), !edge(Y, X), X > 1; edge(5, Y)")?;
        assert!(program.facts.is_empty() && program.rules.is_empty());
        assert_eq!(program.queries.len(), 2);
        Ok(())
    }

    #[test]
    fn negation_reads_a_lower_stratum() -> Result<()> {
        let found = answers(
            "path(X, Y) :- edge(X, Y).\n\
             path(X, Z) :- path(X, Y), edge(Y, Z).\n\
             node(X) :- edge(X, _).\n\
             node(Y) :- edge(_, Y).\n\
             unreachable(Y) :- node(Y), !path(1, Y).\n\
             unreachable(Y)",
            &store(),
        )?;
        assert_eq!(found, ["Y=1", "Y=5", "Y=6"]);
        Ok(())
    }

    #[test]
    fn a_negation_in_a_cycle_is_an_error_that_names_it() {
        let program = parse_program("p(X) :- edge(X, _), !q(X).\nq(X) :- p(X).\np(X)").unwrap();
        let err = evaluate(&program, &store(), None).unwrap_err();
        assert!(
            matches!(&err, Error::NotStratifiable { predicate, depends_on } if predicate == "p" && depends_on == "q"),
            "{err}"
        );
    }

    #[test]
    fn a_head_variable_the_body_does_not_bind_is_unsafe() {
        let program = parse_program("p(X, Y) :- edge(X, _).\np(X, Y)").unwrap();
        assert!(matches!(
            evaluate(&program, &store(), None),
            Err(Error::UnsafeRule { variable, .. }) if variable == "Y"
        ));
        let negated = parse_program("p(X) :- edge(X, _), !edge(Z, X).\np(X)").unwrap();
        assert!(matches!(
            evaluate(&negated, &store(), None),
            Err(Error::UnsafeRule { variable, .. }) if variable == "Z"
        ));
    }

    #[test]
    fn a_relation_used_with_two_arities_is_an_error() {
        let program = parse_program("p(X) :- edge(X, _).\np(X, Y)").unwrap();
        assert!(matches!(program.strata(), Err(Error::ArityMismatch { .. })));
    }

    #[test]
    fn aggregates_group_by_the_other_terms() -> Result<()> {
        let found = answers(
            "out(X, count(Y)) :- edge(X, Y).\n\
             per_edge(sum(Y)) :- edge(X, Y).\n\
             per_target(sum(Y)) :- edge(_, Y).\n\
             low(min(Y)) :- edge(_, Y).\n\
             high(max(Y)) :- edge(_, Y).\n\
             out(X, N), N > 0; per_edge(E); per_target(T); low(L); high(H)",
            &store(),
        )?;
        // An aggregate reads the body's distinct solutions, over its named variables: each
        // edge is a solution of `edge(X, Y)` (2 + 3 + 4 + 2 + 6), but `edge(_, Y)` has one
        // solution per target (2 + 3 + 4 + 6).
        assert_eq!(
            found,
            [
                "E=17", "H=6", "L=2", "N=1 X=1", "N=1 X=2", "N=1 X=3", "N=1 X=4", "N=1 X=5", "T=15"
            ]
        );
        Ok(())
    }

    #[test]
    fn an_aggregate_over_its_own_relation_is_not_stratifiable() {
        let program = parse_program("p(X, count(Y)) :- edge(X, Y), p(Y, _).\np(X, N)").unwrap();
        assert!(matches!(
            program.strata(),
            Err(Error::NotStratifiable { .. })
        ));
    }

    #[test]
    fn only_what_is_wanted_is_evaluated() -> Result<()> {
        let program = parse_program(
            "path(X, Y) :- edge(X, Y).\n\
             path(X, Z) :- path(X, Y), edge(Y, Z).\n\
             out(X, count(Y)) :- edge(X, Y).",
        )?;
        let derived = evaluate(&program, &store(), Some(&["out"]))?;
        assert!(derived.defines("out"));
        assert!(!derived.defines("path"));
        Ok(())
    }

    #[test]
    fn programs_layer() -> Result<()> {
        let base = store();
        let lib =
            parse_program("path(X, Y) :- edge(X, Y).\npath(X, Z) :- path(X, Y), edge(Y, Z).")?;
        let lib_derived = evaluate(&lib, &base, None)?;
        let over = Layered::new(&base, &lib_derived);
        let rule = parse_program("cyclic(X) :- path(X, X).\ncyclic(X)")?;
        let derived = evaluate(&rule, &over, None)?;
        let top = Layered::new(&over, &derived);
        let client = DatafoxClient::new(DatafoxConfig::new(&top))?;
        let mut found: Vec<String> = client.eval(&rule.queries[0])?.map(|s| show(&s)).collect();
        found.sort();
        assert_eq!(found, ["X=2", "X=3", "X=4"]);
        Ok(())
    }
}
