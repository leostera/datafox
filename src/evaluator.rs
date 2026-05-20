use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use std::sync::Arc;
use std::vec::IntoIter;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    Atom, Clause, Error, InMemoryStorage, Prelude, Query, Result, Storage, Substitution, Term,
    Universe, Value,
};

pub type SubstitutionStream = mpsc::Receiver<Result<Substitution>>;

const DEFAULT_STREAM_BUFFER: usize = 64;
const DEFAULT_PARALLEL_SEED_THRESHOLD: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationStrategy {
    Serial,
    Parallel { seed_threshold: usize },
}

/// Iterator over substitutions produced by an evaluator run.
pub struct Evaluation {
    substitutions: IntoIter<Substitution>,
}

impl Evaluation {
    fn new(substitutions: Vec<Substitution>) -> Self {
        Self {
            substitutions: substitutions.into_iter(),
        }
    }
}

impl Iterator for Evaluation {
    type Item = Substitution;

    fn next(&mut self) -> Option<Self::Item> {
        self.substitutions.next()
    }
}

/// Query evaluator bound to a fact store and runtime strategy.
#[derive(Clone)]
pub struct Evaluator<'store> {
    storage: &'store InMemoryStorage,
    prelude: Prelude,
    strategy: EvaluationStrategy,
    pool: Option<Arc<ThreadPool>>,
}

/// Builder for configuring an [`Evaluator`].
pub struct EvaluatorBuilder<'store> {
    storage: Option<&'store InMemoryStorage>,
    prelude: Prelude,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store> EvaluatorBuilder<'store> {
    pub fn with_store(mut self, storage: &'store InMemoryStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_prelude(mut self, prelude: Prelude) -> Self {
        self.prelude = prelude;
        self
    }

    pub fn serial(mut self) -> Self {
        self.strategy = EvaluationStrategy::Serial;
        self
    }

    pub fn parallel(mut self) -> Self {
        self.strategy = EvaluationStrategy::Parallel {
            seed_threshold: DEFAULT_PARALLEL_SEED_THRESHOLD,
        };
        self
    }

    pub fn seed_threshold(mut self, seed_threshold: usize) -> Self {
        if matches!(self.strategy, EvaluationStrategy::Serial) {
            self = self.parallel();
        }
        self.strategy = EvaluationStrategy::Parallel { seed_threshold };
        self
    }

    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    pub fn build(self) -> Result<Evaluator<'store>> {
        let storage = self.storage.ok_or_else(|| Error::EvaluatorBuild {
            message: "missing storage; call `with_store` before `build`".to_string(),
        })?;

        match self.strategy {
            EvaluationStrategy::Serial => Ok(Evaluator {
                storage,
                prelude: self.prelude,
                strategy: self.strategy,
                pool: None,
            }),
            EvaluationStrategy::Parallel { .. } => {
                let mut builder = ThreadPoolBuilder::new();
                if let Some(threads) = self.threads {
                    builder = builder.num_threads(threads);
                }
                let pool = builder.build().map_err(|error| Error::EvaluatorBuild {
                    message: error.to_string(),
                })?;
                Ok(Evaluator {
                    storage,
                    prelude: self.prelude,
                    strategy: self.strategy,
                    pool: Some(Arc::new(pool)),
                })
            }
        }
    }
}

impl Default for EvaluatorBuilder<'_> {
    fn default() -> Self {
        Self {
            storage: None,
            prelude: Prelude::new(),
            strategy: EvaluationStrategy::Serial,
            threads: None,
        }
    }
}

impl<'store> Evaluator<'store> {
    pub fn builder() -> EvaluatorBuilder<'store> {
        EvaluatorBuilder::default()
    }

    pub fn eval(&self, query: &Query) -> Result<Evaluation> {
        let substitutions = match self.strategy {
            EvaluationStrategy::Serial => {
                Self::evaluate_clauses_serial(self.storage, &self.prelude, query.clauses())?
            }
            EvaluationStrategy::Parallel { seed_threshold } => {
                let Some(pool) = &self.pool else {
                    return Err(Error::EvaluatorBuild {
                        message: "parallel strategy was configured without a worker pool"
                            .to_string(),
                    });
                };
                pool.install(|| {
                    Self::evaluate_clauses_parallel(
                        self.storage,
                        &self.prelude,
                        query.clauses(),
                        seed_threshold,
                    )
                })?
            }
        };

        Ok(Evaluation::new(substitutions))
    }

    pub fn strategy(&self) -> EvaluationStrategy {
        self.strategy
    }

    pub async fn query<S>(universe: &Universe<S>, atom: &Atom) -> Result<SubstitutionStream>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        Self::evaluate_query(universe.clone(), Query::single(atom.clone())).await
    }

    pub async fn evaluate<S>(universe: &Universe<S>, query: &Query) -> Result<SubstitutionStream>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        Self::evaluate_query(universe.clone(), query.clone()).await
    }

    async fn evaluate_query<S>(universe: Universe<S>, query: Query) -> Result<SubstitutionStream>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        match &query {
            Query::Single(_) | Query::Multi(_) => {}
        }

        let (tx, rx) = mpsc::channel(DEFAULT_STREAM_BUFFER);
        tokio::spawn(async move {
            debug!("starting query evaluation task");
            let result = match query {
                Query::Single(atom) => {
                    Self::evaluate_positive_clauses(&universe, vec![Clause::atom(atom)]).await
                }
                Query::Multi(clauses) => Self::evaluate_positive_clauses(&universe, clauses).await,
            };

            match result {
                Ok(substitutions) => {
                    for substitution in substitutions {
                        if tx.send(Ok(substitution)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                }
            }
        });

        Ok(rx)
    }

    async fn evaluate_positive_clauses<S>(
        universe: &Universe<S>,
        clauses: Vec<Clause>,
    ) -> Result<Vec<Substitution>>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        debug!(clause_count = clauses.len(), "evaluating positive clauses");
        let prelude = Prelude::new();
        let mut seeds = vec![Substitution::new()];

        for clause in clauses {
            let atom = match clause {
                Clause::Atom(atom) => atom,
                Clause::Negated(atom) => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        for variable in atom.variables() {
                            if !seed.contains(variable) {
                                return Err(Error::UngroundedBuiltin {
                                    name: format!("!{}", atom.predicate),
                                });
                            }
                        }

                        let matches =
                            Self::query_atom_matches(universe, &prelude, &atom, &seed).await?;
                        if matches.is_empty() {
                            next_seeds.push(seed);
                        }
                    }
                    debug!(
                        predicate = %atom.predicate,
                        seed_count = next_seeds.len(),
                        "advanced negated clause evaluation"
                    );
                    seeds = next_seeds;
                    continue;
                }
                Clause::Builtin { name, args } => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        if Self::evaluate_builtin_clause(&name, &args, &seed, &prelude)? {
                            next_seeds.push(seed);
                        }
                    }
                    debug!(
                        builtin = %name,
                        seed_count = next_seeds.len(),
                        "advanced builtin clause evaluation"
                    );
                    seeds = next_seeds;
                    continue;
                }
            };

            let mut next_seeds = Vec::new();
            for seed in seeds {
                let mut matches =
                    Self::query_atom_matches(universe, &prelude, &atom, &seed).await?;
                next_seeds.append(&mut matches);
            }
            debug!(seed_count = next_seeds.len(), predicate = %atom.predicate, "advanced clause evaluation");
            seeds = next_seeds;
        }

        Ok(seeds)
    }

    fn evaluate_clauses_serial(
        storage: &InMemoryStorage,
        prelude: &Prelude,
        clauses: Vec<Clause>,
    ) -> Result<Vec<Substitution>> {
        let mut seeds = vec![Substitution::new()];

        for clause in clauses {
            let atom = match clause {
                Clause::Atom(atom) => atom,
                Clause::Negated(atom) => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        for variable in atom.variables() {
                            if !seed.contains(variable) {
                                return Err(Error::UngroundedBuiltin {
                                    name: format!("!{}", atom.predicate),
                                });
                            }
                        }

                        let matches =
                            Self::query_atom_matches_in_memory(storage, prelude, &atom, &seed)?;
                        if matches.is_empty() {
                            next_seeds.push(seed);
                        }
                    }
                    seeds = next_seeds;
                    continue;
                }
                Clause::Builtin { name, args } => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        if Self::evaluate_builtin_clause(&name, &args, &seed, prelude)? {
                            next_seeds.push(seed);
                        }
                    }
                    seeds = next_seeds;
                    continue;
                }
            };

            let mut next_seeds = Vec::new();
            for seed in seeds {
                next_seeds.extend(Self::query_atom_matches_in_memory(
                    storage, prelude, &atom, &seed,
                )?);
            }
            seeds = next_seeds;
        }

        Ok(seeds)
    }

    fn evaluate_clauses_parallel(
        storage: &InMemoryStorage,
        prelude: &Prelude,
        clauses: Vec<Clause>,
        seed_threshold: usize,
    ) -> Result<Vec<Substitution>> {
        let mut seeds = vec![Substitution::new()];

        for clause in clauses {
            if seeds.len() < seed_threshold {
                seeds = Self::advance_clause_in_memory(storage, prelude, clause, seeds)?;
                continue;
            }

            seeds = Self::advance_clause_in_memory_parallel(storage, prelude, clause, seeds)?;
        }

        Ok(seeds)
    }

    fn advance_clause_in_memory(
        storage: &InMemoryStorage,
        prelude: &Prelude,
        clause: Clause,
        seeds: Vec<Substitution>,
    ) -> Result<Vec<Substitution>> {
        let atom = match clause {
            Clause::Atom(atom) => atom,
            Clause::Negated(atom) => {
                let mut next_seeds = Vec::new();
                for seed in seeds {
                    ensure_negation_is_grounded(&atom, &seed)?;

                    let matches =
                        Self::query_atom_matches_in_memory(storage, prelude, &atom, &seed)?;
                    if matches.is_empty() {
                        next_seeds.push(seed);
                    }
                }
                return Ok(next_seeds);
            }
            Clause::Builtin { name, args } => {
                let mut next_seeds = Vec::new();
                for seed in seeds {
                    if Self::evaluate_builtin_clause(&name, &args, &seed, prelude)? {
                        next_seeds.push(seed);
                    }
                }
                return Ok(next_seeds);
            }
        };

        let mut next_seeds = Vec::new();
        for seed in seeds {
            next_seeds.extend(Self::query_atom_matches_in_memory(
                storage, prelude, &atom, &seed,
            )?);
        }
        Ok(next_seeds)
    }

    fn advance_clause_in_memory_parallel(
        storage: &InMemoryStorage,
        prelude: &Prelude,
        clause: Clause,
        seeds: Vec<Substitution>,
    ) -> Result<Vec<Substitution>> {
        match clause {
            Clause::Atom(atom) => seeds
                .into_par_iter()
                .map(|seed| Self::query_atom_matches_in_memory(storage, prelude, &atom, &seed))
                .collect::<Result<Vec<_>>>()
                .map(flatten_chunks),
            Clause::Negated(atom) => seeds
                .into_par_iter()
                .map(|seed| {
                    ensure_negation_is_grounded(&atom, &seed)?;

                    let matches =
                        Self::query_atom_matches_in_memory(storage, prelude, &atom, &seed)?;
                    Ok(matches.is_empty().then_some(seed))
                })
                .collect::<Result<Vec<_>>>()
                .map(flatten_options),
            Clause::Builtin { name, args } => seeds
                .into_par_iter()
                .map(|seed| {
                    Self::evaluate_builtin_clause(&name, &args, &seed, prelude)
                        .map(|keep| keep.then_some(seed))
                })
                .collect::<Result<Vec<_>>>()
                .map(flatten_options),
        }
    }

    fn evaluate_builtin_clause(
        name: &str,
        args: &[Term],
        seed: &Substitution,
        prelude: &Prelude,
    ) -> Result<bool> {
        let [left, right] = args else {
            return Err(Error::BuiltinArityMismatch {
                name: name.to_string(),
                expected: 2,
                found: args.len(),
            });
        };

        let Some(relation) = prelude.relation(name) else {
            return Err(Error::UnsupportedBuiltin {
                name: name.to_string(),
            });
        };

        let left = evaluate_term(left, seed, prelude)?;
        let right = evaluate_term(right, seed, prelude)?;

        match (left, right) {
            (EvaluatedTerm::Value(left), EvaluatedTerm::Value(right)) => {
                Ok(relation.evaluate(&left, &right))
            }
            (EvaluatedTerm::NoResult, _) | (_, EvaluatedTerm::NoResult) => Ok(false),
            (EvaluatedTerm::Ungrounded, _) | (_, EvaluatedTerm::Ungrounded) => {
                Err(Error::UngroundedBuiltin {
                    name: name.to_string(),
                })
            }
        }
    }

    async fn query_atom_matches<S>(
        universe: &Universe<S>,
        prelude: &Prelude,
        atom: &Atom,
        seed: &Substitution,
    ) -> Result<Vec<Substitution>>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let pattern = atom_to_pattern(atom, seed, prelude)?;
        let mut tuples = universe
            .get_facts_matching(&atom.predicate, pattern)
            .await?;
        let mut substitutions = Vec::new();

        while let Some(tuple) = tuples.recv().await {
            let tuple = tuple?;
            if let Some(substitution) = match_atom(seed, atom, &tuple, prelude)? {
                substitutions.push(substitution);
            }
        }

        debug!(
            match_count = substitutions.len(),
            "matched atom against storage"
        );
        Ok(substitutions)
    }

    fn query_atom_matches_in_memory(
        storage: &InMemoryStorage,
        prelude: &Prelude,
        atom: &Atom,
        seed: &Substitution,
    ) -> Result<Vec<Substitution>> {
        let pattern = atom_to_pattern(atom, seed, prelude)?;
        let mut substitutions = Vec::new();

        for tuple in storage.facts_matching(&atom.predicate, &pattern) {
            if let Some(substitution) = match_atom(seed, atom, tuple, prelude)? {
                substitutions.push(substitution);
            }
        }

        for tuple in prelude.facts().facts_matching(&atom.predicate, &pattern) {
            if let Some(substitution) = match_atom(seed, atom, tuple, prelude)? {
                substitutions.push(substitution);
            }
        }

        Ok(substitutions)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EvaluatedTerm {
    Value(Value),
    Ungrounded,
    NoResult,
}

fn atom_to_pattern(
    atom: &Atom,
    seed: &Substitution,
    prelude: &Prelude,
) -> Result<Vec<Option<Value>>> {
    atom.args
        .iter()
        .map(|term| match evaluate_term(term, seed, prelude)? {
            EvaluatedTerm::Value(value) => Ok(Some(value)),
            EvaluatedTerm::Ungrounded | EvaluatedTerm::NoResult => Ok(None),
        })
        .collect()
}

fn match_atom(
    seed: &Substitution,
    atom: &Atom,
    tuple: &[Value],
    prelude: &Prelude,
) -> Result<Option<Substitution>> {
    if atom.args.len() != tuple.len() {
        return Err(Error::ArityMismatch {
            predicate: atom.predicate.clone(),
            expected: atom.args.len(),
            found: tuple.len(),
        });
    }

    let mut current = seed.clone();
    for (term, value) in atom.args.iter().zip(tuple) {
        match term {
            Term::Const(expected) if expected != value => return Ok(None),
            Term::Const(_) | Term::Wildcard => {}
            Term::Var(variable) => match current.lookup(variable) {
                Some(existing) if existing != value => return Ok(None),
                Some(_) => {}
                None => current = current.bind(variable.clone(), value.clone()),
            },
            Term::Call { .. } => match evaluate_term(term, &current, prelude)? {
                EvaluatedTerm::Value(expected) if expected != *value => return Ok(None),
                EvaluatedTerm::Value(_) => {}
                EvaluatedTerm::Ungrounded | EvaluatedTerm::NoResult => return Ok(None),
            },
        }
    }

    Ok(Some(current))
}

fn evaluate_term(term: &Term, seed: &Substitution, prelude: &Prelude) -> Result<EvaluatedTerm> {
    match term {
        Term::Const(value) => Ok(EvaluatedTerm::Value(value.clone())),
        Term::Var(variable) => Ok(seed
            .lookup(variable)
            .cloned()
            .map(EvaluatedTerm::Value)
            .unwrap_or(EvaluatedTerm::Ungrounded)),
        Term::Wildcard => Ok(EvaluatedTerm::Ungrounded),
        Term::Call { name, args } => {
            let [left, right] = args.as_slice() else {
                return Err(Error::BuiltinArityMismatch {
                    name: name.clone(),
                    expected: 2,
                    found: args.len(),
                });
            };
            let left = evaluate_term(left, seed, prelude)?;
            let right = evaluate_term(right, seed, prelude)?;

            match (left, right) {
                (EvaluatedTerm::NoResult, _) | (_, EvaluatedTerm::NoResult) => {
                    Ok(EvaluatedTerm::NoResult)
                }
                (EvaluatedTerm::Ungrounded, _) | (_, EvaluatedTerm::Ungrounded) => {
                    Ok(EvaluatedTerm::Ungrounded)
                }
                (EvaluatedTerm::Value(left), EvaluatedTerm::Value(right)) => {
                    let Some(operator) = prelude.operator(name) else {
                        return Err(Error::UnsupportedBuiltin { name: name.clone() });
                    };
                    Ok(operator
                        .evaluate(&left, &right)
                        .map(EvaluatedTerm::Value)
                        .unwrap_or(EvaluatedTerm::NoResult))
                }
            }
        }
    }
}

fn ensure_negation_is_grounded(atom: &Atom, seed: &Substitution) -> Result<()> {
    for variable in atom.variables() {
        if !seed.contains(variable) {
            return Err(Error::UngroundedBuiltin {
                name: format!("!{}", atom.predicate),
            });
        }
    }

    Ok(())
}

fn flatten_chunks<T>(chunks: Vec<Vec<T>>) -> Vec<T> {
    let total_len = chunks.iter().map(Vec::len).sum();
    let mut values = Vec::with_capacity(total_len);
    for mut chunk in chunks {
        values.append(&mut chunk);
    }
    values
}

fn flatten_options<T>(values: Vec<Option<T>>) -> Vec<T> {
    values.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::{
        BinaryOperator, Clause, Evaluator, InMemoryStorage, Prelude, Query, Result, Universe,
        Value, parse_query,
    };

    async fn collect_results(
        mut stream: crate::SubstitutionStream,
    ) -> crate::Result<Vec<crate::Substitution>> {
        let mut results = Vec::new();
        while let Some(result) = stream.recv().await {
            results.push(result?);
        }
        Ok(results)
    }

    #[tokio::test]
    async fn evaluator_streams_single_goal_matches() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "spotify:displayName".to_string(),
            vec![
                vec![Value::from("spotify:album:2112"), Value::from("2112")],
                vec![Value::from("spotify:album:signals"), Value::from("Signals")],
            ],
        )]));
        let atom = crate::atom!(
            "spotify:displayName",
            vec![crate::var!("Album"), crate::lit!(Value::from("2112"))]
        );

        let results = collect_results(Evaluator::query(&universe, &atom).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup("Album"),
            Some(&Value::from("spotify:album:2112"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_can_run_parsed_single_goal_queries() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![vec![Value::integer(1), Value::integer(2)]],
        )]));
        let query = parse_query("edge(X, 2)")?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(1)));
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_streams_multi_goal_conjunctive_matches() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([
            (
                "spotify:byArtist".to_string(),
                vec![
                    vec![
                        Value::from("spotify:album:2112"),
                        Value::from("spotify:artist:rush"),
                    ],
                    vec![
                        Value::from("spotify:album:fragile"),
                        Value::from("spotify:artist:yes"),
                    ],
                ],
            ),
            (
                "spotify:displayName".to_string(),
                vec![
                    vec![Value::from("spotify:artist:rush"), Value::from("Rush")],
                    vec![Value::from("spotify:artist:yes"), Value::from("Yes")],
                ],
            ),
        ]));
        let query = Query::multi(vec![
            Clause::atom(crate::atom!(
                "spotify:byArtist",
                vec![crate::var!("Album"), crate::var!("Artist")]
            )),
            Clause::atom(crate::atom!(
                "spotify:displayName",
                vec![crate::var!("Artist"), crate::lit!(Value::from("Rush"))]
            )),
        ])?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup("Album"),
            Some(&Value::from("spotify:album:2112"))
        );
        assert_eq!(
            results[0].lookup("Artist"),
            Some(&Value::from("spotify:artist:rush"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_streams_empty_results_for_unsatisfied_multi_goal_queries() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(2)],
                vec![Value::integer(2), Value::integer(3)],
            ],
        )]));
        let query = Query::multi(vec![
            Clause::atom(crate::atom!(
                "edge",
                vec![crate::var!("X"), crate::var!("Y")]
            )),
            Clause::atom(crate::atom!(
                "edge",
                vec![crate::var!("Y"), crate::lit!(Value::integer(99))]
            )),
        ])?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_supports_safe_negation() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([
            (
                "person".to_string(),
                vec![vec![Value::string("geddy")], vec![Value::string("alex")]],
            ),
            ("bassist".to_string(), vec![vec![Value::string("geddy")]]),
        ]));
        let query = parse_query(r#"person(Name), !bassist(Name)"#)?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("Name"), Some(&Value::string("alex")));
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_requires_negated_variables_to_be_bound() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::new());
        let query = Query::multi(vec![Clause::negated(crate::atom!(
            "edge",
            vec![crate::var!("X"), crate::var!("Y")]
        ))])?;

        let error = match Evaluator::evaluate(&universe, &query).await {
            Ok(mut stream) => match stream.recv().await {
                Some(Err(error)) => error,
                other => panic!("expected ungrounded negation, got {other:?}"),
            },
            Err(error) => error,
        };

        assert_eq!(
            error,
            crate::Error::UngroundedBuiltin {
                name: "!edge".to_string(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_filters_results_with_infix_comparison_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "gcal:startedAt".to_string(),
            vec![
                vec![
                    Value::string("gcal:event:one"),
                    Value::string("2026-01-01 22:00:00"),
                ],
                vec![
                    Value::string("gcal:event:two"),
                    Value::string("2026-01-03 08:00:00"),
                ],
            ],
        )]));
        let query = parse_query(
            "gcal:startedAt(Event, Start), Start > \"2026-01-01\", Start < \"2026-01-02\"",
        )?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup("Event"),
            Some(&Value::string("gcal:event:one"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_supports_equality_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "edge".to_string(),
            vec![
                vec![Value::integer(1), Value::integer(1)],
                vec![Value::integer(1), Value::integer(2)],
            ],
        )]));
        let query = parse_query("edge(X, Y), X = Y")?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(1)));
        assert_eq!(results[0].lookup("Y"), Some(&Value::integer(1)));
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_supports_named_string_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "spotify:displayName".to_string(),
            vec![
                vec![Value::string("spotify:artist:rush"), Value::string("Rush")],
                vec![Value::string("spotify:artist:yes"), Value::string("Yes")],
            ],
        )]));
        let query = parse_query(
            r#"spotify:displayName(Artist, Name), startsWith(Name, "Ru"), endsWith(Name, "sh"), contains(Name, "us"), matchesRegex(Name, "^R.*h$")"#,
        )?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup("Artist"),
            Some(&Value::string("spotify:artist:rush"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_supports_negative_named_string_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "displayName".to_string(),
            vec![
                vec![Value::string("rush"), Value::string("Rush")],
                vec![Value::string("yes"), Value::string("Yes")],
            ],
        )]));
        let query = parse_query(
            r#"displayName(Artist, Name), notStartsWith(Name, "Ru"), notEndsWith(Name, "sh"), notContains(Name, "us"), notMatchesRegex(Name, "^R")"#,
        )?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("Artist"), Some(&Value::string("yes")));
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_supports_temporal_alias_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::from_facts([(
            "gcal:startedAt".to_string(),
            vec![
                vec![
                    Value::string("gcal:event:one"),
                    Value::string("2026-01-01 22:00:00"),
                ],
                vec![
                    Value::string("gcal:event:two"),
                    Value::string("2026-01-03 08:00:00"),
                ],
            ],
        )]));
        let query = parse_query(
            r#"gcal:startedAt(Event, Start), after(Start, "2026-01-01"), before(Start, "2026-01-02")"#,
        )?;

        let results = collect_results(Evaluator::evaluate(&universe, &query).await?).await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].lookup("Event"),
            Some(&Value::string("gcal:event:one"))
        );
        Ok(())
    }

    #[test]
    fn evaluator_supports_default_arithmetic_operators() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![
                vec![Value::integer(1)],
                vec![Value::integer(2)],
                vec![Value::integer(3)],
            ],
        )]);
        let query = parse_query("value(X), (X + 1) = 4, (X * 2) > 5, (X - 1) = 2, (X / 1) = 3")?;

        let results = Evaluator::builder()
            .with_store(&storage)
            .build()?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(3)));
        Ok(())
    }

    #[test]
    fn evaluator_can_read_facts_from_the_prelude() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![
                vec![Value::integer(1)],
                vec![Value::integer(2)],
                vec![Value::integer(3)],
            ],
        )]);
        let prelude = Prelude::new().with_fact("threshold", vec![Value::integer(2)]);
        let query = parse_query("value(X), threshold(T), X > T")?;

        let results = Evaluator::builder()
            .with_store(&storage)
            .with_prelude(prelude)
            .build()?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(3)));
        assert_eq!(results[0].lookup("T"), Some(&Value::integer(2)));
        Ok(())
    }

    #[test]
    fn evaluator_can_use_custom_prelude_operators() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(2)], vec![Value::integer(3)]],
        )]);
        let prelude =
            Prelude::new().with_operator(BinaryOperator::new("plusTen", |left, right| {
                match (left, right) {
                    (Value::Integer(left), Value::Integer(right)) => {
                        Some(Value::integer(left + right + 10))
                    }
                    _ => None,
                }
            }));
        let query = parse_query("value(X), (X plusTen 1) = 14")?;

        let results = Evaluator::builder()
            .with_store(&storage)
            .with_prelude(prelude)
            .build()?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(3)));
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_requires_ground_builtin_arguments() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::new());
        let query = Query::multi(vec![Clause::builtin(
            "gt",
            vec![
                crate::var!("Start"),
                crate::lit!(Value::string("2026-01-01")),
            ],
        )])?;

        let error = match Evaluator::evaluate(&universe, &query).await {
            Ok(mut stream) => match stream.recv().await {
                Some(Err(error)) => error,
                other => panic!("expected ungrounded builtin error, got {other:?}"),
            },
            Err(error) => error,
        };

        assert_eq!(
            error,
            crate::Error::UngroundedBuiltin {
                name: "gt".to_string(),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn evaluator_rejects_unknown_builtins() -> Result<()> {
        let universe = Universe::new(InMemoryStorage::new());
        let query = Query::multi(vec![Clause::builtin(
            "bogusBuiltin",
            vec![
                crate::lit!(Value::string("hello")),
                crate::lit!(Value::string("ell")),
            ],
        )])?;

        let error = match Evaluator::evaluate(&universe, &query).await {
            Ok(mut stream) => match stream.recv().await {
                Some(Err(error)) => error,
                other => panic!("expected unsupported builtin error, got {other:?}"),
            },
            Err(error) => error,
        };

        assert_eq!(
            error,
            crate::Error::UnsupportedBuiltin {
                name: "bogusBuiltin".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn parallel_evaluator_matches_serial_for_atoms_negation_and_builtins() -> Result<()> {
        let persons = (0..3_000)
            .map(|id| vec![Value::integer(id), Value::string(format!("node-{id}"))])
            .collect::<Vec<_>>();
        let banned = (0..3_000)
            .filter(|id| id % 2 == 0)
            .map(|id| vec![Value::integer(id)])
            .collect::<Vec<_>>();
        let storage = InMemoryStorage::from_facts([
            ("person".to_string(), persons),
            ("banned".to_string(), banned),
        ]);
        let query = parse_query(r#"person(Id, Name), !banned(Id), contains(Name, "1")"#)?;

        let sequential = Evaluator::builder()
            .with_store(&storage)
            .build()?
            .eval(&query)?
            .collect::<Vec<_>>();
        let parallel = Evaluator::builder()
            .with_store(&storage)
            .parallel()
            .build()?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert!(!parallel.is_empty());
        assert_eq!(
            normalize_substitutions(sequential),
            normalize_substitutions(parallel)
        );
        Ok(())
    }

    fn normalize_substitutions(
        mut substitutions: Vec<crate::Substitution>,
    ) -> Vec<Vec<(String, Value)>> {
        let mut normalized = substitutions
            .drain(..)
            .map(|substitution| {
                substitution
                    .bindings()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        normalized.sort();
        normalized
    }

    proptest! {
        #[test]
        fn evaluator_never_panics_on_parser_output(source in "[A-Za-z0-9_():,;!<>=\\\"' ?.-]{0,256}") {
            if let Ok(queries) = crate::parse_queries(&source) {
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

                let evaluator = Evaluator::builder().with_store(&storage).build().expect("evaluator");
                for query in queries.into_iter().take(16) {
                    let _ = evaluator.eval(&query);
                }
            }
        }
    }
}
