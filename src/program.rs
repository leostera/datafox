//! Programs: facts and rules evaluated over a store's facts to a fixpoint, then queries.
//!
//! A program's relations are the least fixpoint of its rules over its facts and the
//! store's. Rules may be recursive; a rule may negate, or aggregate over, only relations
//! that don't depend on it (stratification). Evaluation is semi-naive: each round, a rule
//! is evaluated once per recursive atom in its body, that atom reading only the tuples the
//! previous round added.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{
    Atom, Clause, DatafoxClient, DatafoxConfig, Error, FactEstimate, FactScan, FactStore,
    FactTuple, InMemoryStorage, Query, Result, Term, Value,
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

/// The relations a program derived, over the store it was evaluated on.
#[derive(Debug, Default)]
pub struct Derived {
    relations: InMemoryStorage,
    seen: HashMap<String, HashSet<FactTuple>>,
    /// The tuples the last round added, by predicate, while a stratum is evaluated.
    delta: InMemoryStorage,
}

/// The name a recursive atom reads the last round's tuples under.
fn delta_name(predicate: &str) -> String {
    format!("\u{0}delta\u{0}{predicate}")
}

impl Derived {
    /// Whether the program defines `predicate`.
    pub fn defines(&self, predicate: &str) -> bool {
        self.seen.contains_key(predicate)
    }

    /// The tuples of `predicate`, if the program defines it.
    pub fn tuples(&self, predicate: &str) -> Vec<&FactTuple> {
        self.relations.scan(predicate, &[]).collect()
    }

    /// How many tuples `predicate` has.
    pub fn len(&self, predicate: &str) -> usize {
        self.seen.get(predicate).map_or(0, HashSet::len)
    }

    fn define(&mut self, predicate: &str) {
        self.seen.entry(predicate.to_owned()).or_default();
    }

    /// Adds `tuple` to `predicate`; returns whether it's new.
    fn insert(&mut self, predicate: &str, tuple: FactTuple) -> bool {
        let seen = self.seen.entry(predicate.to_owned()).or_default();
        if seen.insert(tuple.clone()) {
            self.relations.insert(predicate.to_owned(), tuple);
            true
        } else {
            false
        }
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
    Derived(FactScan<'store, 'pattern>),
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
        for predicate in &stratum {
            derived.define(predicate);
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
            derived.insert(&fact.predicate, tuple);
        }
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
    let rules: Vec<&Rule> = program
        .rules
        .iter()
        .filter(|rule| stratum.contains(&rule.head.predicate))
        .collect();
    let recursive = |atom: &Atom| stratum.contains(&atom.predicate);

    // The first round reads every relation whole.
    let mut added: Vec<(String, FactTuple)> = Vec::new();
    for rule in &rules {
        added.extend(solve(rule, &rule.body, store, derived)?);
    }
    let mut delta = commit(derived, added);

    let any_recursive = rules
        .iter()
        .any(|rule| rule.positive_atoms().any(recursive));
    while any_recursive && !delta.is_empty() {
        derived.delta = InMemoryStorage::new();
        for (predicate, tuple) in delta {
            derived.delta.insert(delta_name(&predicate), tuple);
        }
        let mut added = Vec::new();
        for rule in &rules {
            for (position, clause) in rule.body.iter().enumerate() {
                let Clause::Atom(atom) = clause else {
                    continue;
                };
                if !recursive(atom) {
                    continue;
                }
                let mut body = rule.body.clone();
                body[position] = Clause::Atom(Atom {
                    predicate: delta_name(&atom.predicate),
                    args: atom.args.clone(),
                });
                added.extend(solve(rule, &body, store, derived)?);
            }
        }
        delta = commit(derived, added);
    }
    derived.delta = InMemoryStorage::new();
    Ok(())
}

/// Adds tuples to what was derived; returns the ones that were new.
fn commit(derived: &mut Derived, added: Vec<(String, FactTuple)>) -> Vec<(String, FactTuple)> {
    added
        .into_iter()
        .filter(|(predicate, tuple)| derived.insert(predicate, tuple.clone()))
        .collect()
}

/// The head tuples `body` gives `rule`, aggregated if its head aggregates.
fn solve<S: FactStore + ?Sized>(
    rule: &Rule,
    body: &[Clause],
    store: &S,
    derived: &Derived,
) -> Result<Vec<(String, FactTuple)>> {
    let layered = Layered::new(store, derived);
    let query = match body {
        [Clause::Atom(atom)] => Query::single(atom.clone()),
        _ => Query::multi(body.to_vec())?,
    };
    let client = DatafoxClient::new(DatafoxConfig::new(&layered).serial())?;
    let solutions = client.eval(&query)?;
    let predicate = rule.head.predicate.clone();
    if !rule.head.is_aggregate() {
        return Ok(solutions
            .map(|solution| {
                let tuple = rule
                    .head
                    .args
                    .iter()
                    .map(|term| match term {
                        HeadTerm::Term(Term::Var(variable)) => solution
                            .lookup(variable)
                            .cloned()
                            .expect("a safe rule binds its head"),
                        HeadTerm::Term(term) => {
                            term.const_value().cloned().expect("checked in strata")
                        }
                        HeadTerm::Aggregate { .. } => unreachable!("not an aggregate"),
                    })
                    .collect();
                (predicate.clone(), tuple)
            })
            .collect());
    }

    // Group the body's distinct solutions by the head's other terms; each aggregate gets
    // its variable's value in every solution of the group.
    let variables: BTreeSet<&str> = rule.positive_atoms().flat_map(Atom::variables).collect();
    let mut distinct: HashSet<Vec<Value>> = HashSet::new();
    let mut groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>> = BTreeMap::new();
    for solution in solutions {
        let bound = |variable: &str| solution.lookup(variable).cloned().expect("safe");
        if !distinct.insert(variables.iter().map(|v| bound(v)).collect()) {
            continue;
        }
        let mut key = Vec::new();
        let mut values = Vec::new();
        for term in &rule.head.args {
            match term {
                HeadTerm::Term(Term::Var(variable)) => key.push(bound(variable)),
                HeadTerm::Term(term) => key.push(term.const_value().cloned().expect("checked")),
                HeadTerm::Aggregate { variable, .. } => values.push(bound(variable)),
            }
        }
        let group = groups
            .entry(key)
            .or_insert_with(|| vec![Vec::new(); values.len()]);
        for (slot, value) in group.iter_mut().zip(values) {
            slot.push(value);
        }
    }
    let mut tuples = Vec::new();
    for (key, group) in groups {
        let mut key = key.into_iter();
        let mut aggregates = group.into_iter();
        let mut tuple = Vec::with_capacity(rule.head.args.len());
        for term in &rule.head.args {
            match term {
                HeadTerm::Aggregate { aggregate, .. } => {
                    let values = aggregates.next().expect("one per aggregate");
                    tuple.push(fold(*aggregate, &rule.head.predicate, values)?);
                }
                HeadTerm::Term(_) => tuple.push(key.next().expect("one per term")),
            }
        }
        tuples.push((predicate.clone(), tuple));
    }
    Ok(tuples)
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
    use crate::{Substitution, parse_program};

    fn store() -> InMemoryStorage {
        let edge = |a: i64, b: i64| vec![Value::integer(a), Value::integer(b)];
        InMemoryStorage::from_facts([(
            "edge".to_owned(),
            vec![edge(1, 2), edge(2, 3), edge(3, 4), edge(4, 2), edge(5, 6)],
        )])
    }

    /// The answers to the program's queries, over `store` and what it derives, as text.
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
