use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use std::sync::Arc;
use std::vec::IntoIter;
#[cfg(test)]
use tokio::sync::mpsc;
#[cfg(test)]
use tracing::debug;

use crate::plan::{
    ExecutablePlan, PlannedAtom, PlannedClause, PlannedRelation, PlannedTerm, VariableId,
};
use crate::{
    AtomRole, Error, FactRequest, FactRequestMode, FactStore, OperatorOutcome, Plan, Prelude,
    Result, Storage, Substitution, Value,
};

#[cfg(test)]
use crate::{Atom, Clause, Query, Term, Universe};
#[cfg(test)]
pub type SubstitutionStream = mpsc::Receiver<Result<Substitution>>;

#[cfg(test)]
const DEFAULT_STREAM_BUFFER: usize = 64;
const DEFAULT_PARALLEL_SEED_THRESHOLD: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationStrategy {
    Serial,
    Parallel { seed_threshold: usize },
}

impl EvaluationStrategy {
    pub(crate) fn parallel_default() -> Self {
        Self::Parallel {
            seed_threshold: DEFAULT_PARALLEL_SEED_THRESHOLD,
        }
    }
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

#[derive(Clone)]
struct PlanSeed {
    bindings: Vec<Option<Value>>,
}

impl PlanSeed {
    fn new(variable_count: usize) -> Self {
        Self {
            bindings: vec![None; variable_count],
        }
    }

    fn lookup(&self, variable: VariableId) -> Option<&Value> {
        self.bindings.get(variable.index())?.as_ref()
    }

    fn bind(&mut self, variable: VariableId, value: Value) -> bool {
        let slot = &mut self.bindings[variable.index()];
        match slot {
            Some(existing) => existing == &value,
            None => {
                *slot = Some(value);
                true
            }
        }
    }

    fn into_substitution(self, plan: &ExecutablePlan<'_>) -> Substitution {
        Substitution::from_bindings(self.bindings.into_iter().enumerate().filter_map(
            |(index, value)| {
                value.map(|value| {
                    (
                        plan.variable_name(VariableId::from_index(index))
                            .to_string(),
                        value,
                    )
                })
            },
        ))
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
pub(crate) struct Evaluator<'store, S: FactStore + ?Sized> {
    storage: &'store S,
    prelude: Prelude,
    strategy: EvaluationStrategy,
    pool: Option<Arc<ThreadPool>>,
}

/// Builder for configuring an [`Evaluator`].
pub(crate) struct EvaluatorBuilder<'store, S: FactStore + ?Sized> {
    storage: Option<&'store S>,
    prelude: Prelude,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store, S: FactStore + ?Sized> EvaluatorBuilder<'store, S> {
    pub(crate) fn with_store(mut self, storage: &'store S) -> Self {
        self.storage = Some(storage);
        self
    }

    pub(crate) fn with_prelude(mut self, prelude: Prelude) -> Self {
        self.prelude = prelude;
        self
    }

    pub(crate) fn serial(mut self) -> Self {
        self.strategy = EvaluationStrategy::Serial;
        self
    }

    pub(crate) fn parallel(mut self) -> Self {
        self.strategy = EvaluationStrategy::parallel_default();
        self
    }

    pub(crate) fn seed_threshold(mut self, seed_threshold: usize) -> Self {
        if matches!(self.strategy, EvaluationStrategy::Serial) {
            self = self.parallel();
        }
        self.strategy = EvaluationStrategy::Parallel { seed_threshold };
        self
    }

    pub(crate) fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    pub(crate) fn build(self) -> Result<Evaluator<'store, S>> {
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

impl<S: FactStore + ?Sized> Default for EvaluatorBuilder<'_, S> {
    fn default() -> Self {
        Self {
            storage: None,
            prelude: Prelude::new(),
            strategy: EvaluationStrategy::Serial,
            threads: None,
        }
    }
}

impl<'store, S: FactStore + ?Sized> Evaluator<'store, S> {
    pub(crate) fn builder() -> EvaluatorBuilder<'store, S> {
        EvaluatorBuilder::default()
    }

    pub(crate) fn new(
        storage: &'store S,
        prelude: Prelude,
        strategy: EvaluationStrategy,
        threads: Option<usize>,
    ) -> Result<Self> {
        let mut builder = Self::builder().with_store(storage).with_prelude(prelude);
        builder = match strategy {
            EvaluationStrategy::Serial => builder.serial(),
            EvaluationStrategy::Parallel { seed_threshold } => {
                builder.parallel().seed_threshold(seed_threshold)
            }
        };
        if let Some(threads) = threads {
            builder = builder.threads(threads);
        }
        builder.build()
    }

    pub(crate) fn eval_plan(&self, plan: &Plan) -> Result<Evaluation> {
        let executable = plan.bind(&self.prelude)?;
        let seeds = match self.strategy {
            EvaluationStrategy::Serial => {
                Self::evaluate_plan_serial(self.storage, &self.prelude, &executable)?
            }
            EvaluationStrategy::Parallel { seed_threshold } => {
                let Some(pool) = &self.pool else {
                    return Err(Error::EvaluatorBuild {
                        message: "parallel strategy was configured without a worker pool"
                            .to_string(),
                    });
                };
                pool.install(|| {
                    Self::evaluate_plan_parallel(
                        self.storage,
                        &self.prelude,
                        &executable,
                        seed_threshold,
                    )
                })?
            }
        };

        let substitutions = seeds
            .into_iter()
            .map(|seed| seed.into_substitution(&executable))
            .collect();
        Ok(Evaluation::new(substitutions))
    }

    #[cfg(test)]
    pub(crate) async fn query<T>(universe: &Universe<T>, atom: &Atom) -> Result<SubstitutionStream>
    where
        T: Storage + Clone + Send + Sync + 'static,
    {
        Self::evaluate_query(universe.clone(), Query::single(atom.clone())).await
    }

    #[cfg(test)]
    pub(crate) async fn evaluate<T>(
        universe: &Universe<T>,
        query: &Query,
    ) -> Result<SubstitutionStream>
    where
        T: Storage + Clone + Send + Sync + 'static,
    {
        Self::evaluate_query(universe.clone(), query.clone()).await
    }

    #[cfg(test)]
    async fn evaluate_query<T>(universe: Universe<T>, query: Query) -> Result<SubstitutionStream>
    where
        T: Storage + Clone + Send + Sync + 'static,
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

    #[cfg(test)]
    async fn evaluate_positive_clauses<T>(
        universe: &Universe<T>,
        clauses: Vec<Clause>,
    ) -> Result<Vec<Substitution>>
    where
        T: Storage + Clone + Send + Sync + 'static,
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

    fn evaluate_plan_serial(
        storage: &(impl FactStore + ?Sized),
        prelude: &Prelude,
        plan: &ExecutablePlan<'_>,
    ) -> Result<Vec<PlanSeed>> {
        let mut seeds = vec![PlanSeed::new(plan.variable_count())];

        for clause in plan.clauses() {
            match clause {
                PlannedClause::Atom(atom) => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        next_seeds.extend(Self::query_planned_atom_matches(
                            storage, prelude, plan, atom, &seed,
                        )?);
                    }
                    seeds = next_seeds;
                }
                PlannedClause::Negated(atom) => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        let matches =
                            Self::query_planned_atom_matches(storage, prelude, plan, atom, &seed)?;
                        if matches.is_empty() {
                            next_seeds.push(seed);
                        }
                    }
                    seeds = next_seeds;
                }
                PlannedClause::Relation(relation) => {
                    let mut next_seeds = Vec::new();
                    for seed in seeds {
                        if evaluate_planned_relation(relation, &seed, plan)? {
                            next_seeds.push(seed);
                        }
                    }
                    seeds = next_seeds;
                }
            }
        }

        Ok(seeds)
    }

    fn evaluate_plan_parallel(
        storage: &(impl FactStore + ?Sized),
        prelude: &Prelude,
        plan: &ExecutablePlan<'_>,
        seed_threshold: usize,
    ) -> Result<Vec<PlanSeed>> {
        let mut seeds = vec![PlanSeed::new(plan.variable_count())];

        for clause in plan.clauses() {
            if seeds.len() < seed_threshold {
                seeds = Self::advance_planned_clause(storage, prelude, plan, clause, seeds)?;
                continue;
            }

            seeds = Self::advance_planned_clause_parallel(storage, prelude, plan, clause, seeds)?;
        }

        Ok(seeds)
    }

    fn advance_planned_clause(
        storage: &(impl FactStore + ?Sized),
        prelude: &Prelude,
        plan: &ExecutablePlan<'_>,
        clause: &PlannedClause,
        seeds: Vec<PlanSeed>,
    ) -> Result<Vec<PlanSeed>> {
        match clause {
            PlannedClause::Atom(atom) => {
                let mut next_seeds = Vec::new();
                for seed in seeds {
                    next_seeds.extend(Self::query_planned_atom_matches(
                        storage, prelude, plan, atom, &seed,
                    )?);
                }
                Ok(next_seeds)
            }
            PlannedClause::Negated(atom) => {
                let mut next_seeds = Vec::new();
                for seed in seeds {
                    let matches =
                        Self::query_planned_atom_matches(storage, prelude, plan, atom, &seed)?;
                    if matches.is_empty() {
                        next_seeds.push(seed);
                    }
                }
                Ok(next_seeds)
            }
            PlannedClause::Relation(relation) => {
                let mut next_seeds = Vec::new();
                for seed in seeds {
                    if evaluate_planned_relation(relation, &seed, plan)? {
                        next_seeds.push(seed);
                    }
                }
                Ok(next_seeds)
            }
        }
    }

    fn advance_planned_clause_parallel(
        storage: &(impl FactStore + ?Sized),
        prelude: &Prelude,
        plan: &ExecutablePlan<'_>,
        clause: &PlannedClause,
        seeds: Vec<PlanSeed>,
    ) -> Result<Vec<PlanSeed>> {
        match clause {
            PlannedClause::Atom(atom) => seeds
                .into_par_iter()
                .map(|seed| Self::query_planned_atom_matches(storage, prelude, plan, atom, &seed))
                .collect::<Result<Vec<_>>>()
                .map(flatten_chunks),
            PlannedClause::Negated(atom) => seeds
                .into_par_iter()
                .map(|seed| {
                    let matches =
                        Self::query_planned_atom_matches(storage, prelude, plan, atom, &seed)?;
                    Ok(matches.is_empty().then_some(seed))
                })
                .collect::<Result<Vec<_>>>()
                .map(flatten_options),
            PlannedClause::Relation(relation) => seeds
                .into_par_iter()
                .map(|seed| {
                    evaluate_planned_relation(relation, &seed, plan)
                        .map(|keep| keep.then_some(seed))
                })
                .collect::<Result<Vec<_>>>()
                .map(flatten_options),
        }
    }

    fn query_planned_atom_matches(
        storage: &(impl FactStore + ?Sized),
        prelude: &Prelude,
        plan: &ExecutablePlan<'_>,
        atom: &PlannedAtom,
        seed: &PlanSeed,
    ) -> Result<Vec<PlanSeed>> {
        let predicate = plan.predicate_name(atom.predicate);
        let pattern = planned_atom_to_pattern(atom, seed, plan)?;
        let mut substitutions = Vec::new();

        for tuple in storage.scan(predicate, &pattern) {
            if let Some(substitution) = match_planned_atom(seed, atom, tuple, plan)? {
                substitutions.push(substitution);
            }
        }

        for tuple in prelude.facts().scan(predicate, &pattern) {
            if let Some(substitution) = match_planned_atom(seed, atom, tuple, plan)? {
                substitutions.push(substitution);
            }
        }

        Ok(substitutions)
    }

    #[cfg(test)]
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
                Ok(relation.evaluate(&left, &right).is_match())
            }
            (EvaluatedTerm::NoResult, _) | (_, EvaluatedTerm::NoResult) => Ok(false),
            (EvaluatedTerm::Ungrounded, _) | (_, EvaluatedTerm::Ungrounded) => {
                Err(Error::UngroundedBuiltin {
                    name: name.to_string(),
                })
            }
        }
    }

    #[cfg(test)]
    async fn query_atom_matches<T>(
        universe: &Universe<T>,
        prelude: &Prelude,
        atom: &Atom,
        seed: &Substitution,
    ) -> Result<Vec<Substitution>>
    where
        T: Storage + Clone + Send + Sync + 'static,
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
}

pub(crate) async fn eval_plan_streaming<S>(
    storage: &S,
    prelude: &Prelude,
    plan: &Plan,
) -> Result<Evaluation>
where
    S: Storage + ?Sized,
{
    let executable = plan.bind(prelude)?;
    let mut seeds = vec![PlanSeed::new(executable.variable_count())];

    for clause in executable.clauses() {
        let mut next_seeds = Vec::new();
        match clause {
            PlannedClause::Atom(atom) => {
                for seed in seeds {
                    next_seeds.extend(
                        query_streaming_planned_atom_matches(
                            storage,
                            prelude,
                            &executable,
                            atom,
                            &seed,
                            AtomRole::Positive,
                        )
                        .await?,
                    );
                }
            }
            PlannedClause::Negated(atom) => {
                for seed in seeds {
                    let matches = query_streaming_planned_atom_matches(
                        storage,
                        prelude,
                        &executable,
                        atom,
                        &seed,
                        AtomRole::Negated,
                    )
                    .await?;
                    if matches.is_empty() {
                        next_seeds.push(seed);
                    }
                }
            }
            PlannedClause::Relation(relation) => {
                for seed in seeds {
                    if evaluate_planned_relation(relation, &seed, &executable)? {
                        next_seeds.push(seed);
                    }
                }
            }
        }
        seeds = next_seeds;
    }

    let substitutions = seeds
        .into_iter()
        .map(|seed| seed.into_substitution(&executable))
        .collect();
    Ok(Evaluation::new(substitutions))
}

async fn query_streaming_planned_atom_matches<S>(
    storage: &S,
    prelude: &Prelude,
    plan: &ExecutablePlan<'_>,
    atom: &PlannedAtom,
    seed: &PlanSeed,
    role: AtomRole,
) -> Result<Vec<PlanSeed>>
where
    S: Storage + ?Sized,
{
    let predicate = plan.predicate_name(atom.predicate);
    let pattern = planned_atom_to_pattern(atom, seed, plan)?;
    let request = FactRequest::matching(predicate, pattern.clone())
        .with_role(role)
        .with_equality_groups(planned_atom_equality_groups(atom))
        .with_mode(match role {
            AtomRole::Positive => FactRequestMode::Tuples,
            AtomRole::Negated => FactRequestMode::Exists,
        });
    let mut substitutions = Vec::new();
    let mut tuples = storage.get_facts(request).await?;

    while let Some(tuple) = tuples.recv().await {
        let tuple = tuple?;
        if let Some(substitution) = match_planned_atom(seed, atom, &tuple, plan)? {
            substitutions.push(substitution);
        }
    }

    for tuple in prelude.facts().scan(predicate, &pattern) {
        if let Some(substitution) = match_planned_atom(seed, atom, tuple, plan)? {
            substitutions.push(substitution);
        }
    }

    Ok(substitutions)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EvaluatedTerm {
    Value(Value),
    Ungrounded,
    NoResult,
}

fn planned_atom_equality_groups(atom: &PlannedAtom) -> Vec<Vec<usize>> {
    let mut groups = Vec::new();
    for (index, term) in atom.args.iter().enumerate() {
        let PlannedTerm::Var(variable) = term else {
            continue;
        };

        if atom.args[..index]
            .iter()
            .any(|term| matches!(term, PlannedTerm::Var(other) if other == variable))
        {
            continue;
        }

        let group = atom
            .args
            .iter()
            .enumerate()
            .filter_map(|(candidate_index, term)| {
                matches!(term, PlannedTerm::Var(other) if other == variable)
                    .then_some(candidate_index)
            })
            .collect::<Vec<_>>();
        if group.len() > 1 {
            groups.push(group);
        }
    }
    groups
}

fn planned_atom_to_pattern(
    atom: &PlannedAtom,
    seed: &PlanSeed,
    plan: &ExecutablePlan<'_>,
) -> Result<Vec<Option<Value>>> {
    atom.args
        .iter()
        .map(|term| match evaluate_planned_term(term, seed, plan)? {
            EvaluatedTerm::Value(value) => Ok(Some(value)),
            EvaluatedTerm::Ungrounded | EvaluatedTerm::NoResult => Ok(None),
        })
        .collect()
}

fn match_planned_atom(
    seed: &PlanSeed,
    atom: &PlannedAtom,
    tuple: &[Value],
    plan: &ExecutablePlan<'_>,
) -> Result<Option<PlanSeed>> {
    if atom.args.len() != tuple.len() {
        return Err(Error::ArityMismatch {
            predicate: plan.predicate_name(atom.predicate).to_string(),
            expected: atom.args.len(),
            found: tuple.len(),
        });
    }

    let mut current = seed.clone();
    for (term, value) in atom.args.iter().zip(tuple) {
        match term {
            PlannedTerm::Const(expected) if plan.value(*expected) != value => return Ok(None),
            PlannedTerm::Const(_) | PlannedTerm::Wildcard => {}
            PlannedTerm::Var(variable) => {
                if !current.bind(*variable, value.clone()) {
                    return Ok(None);
                }
            }
            PlannedTerm::Call { .. } => match evaluate_planned_term(term, &current, plan)? {
                EvaluatedTerm::Value(expected) if expected != *value => return Ok(None),
                EvaluatedTerm::Value(_) => {}
                EvaluatedTerm::Ungrounded | EvaluatedTerm::NoResult => return Ok(None),
            },
        }
    }

    Ok(Some(current))
}

fn evaluate_planned_relation(
    relation: &PlannedRelation,
    seed: &PlanSeed,
    plan: &ExecutablePlan<'_>,
) -> Result<bool> {
    let [left, right] = relation.args.as_slice() else {
        return Err(Error::BuiltinArityMismatch {
            name: relation.name.clone(),
            expected: 2,
            found: relation.args.len(),
        });
    };

    let left = evaluate_planned_term(left, seed, plan)?;
    let right = evaluate_planned_term(right, seed, plan)?;

    match (left, right) {
        (EvaluatedTerm::Value(left), EvaluatedTerm::Value(right)) => {
            Ok(relation.relation.evaluate(&left, &right).is_match())
        }
        (EvaluatedTerm::NoResult, _) | (_, EvaluatedTerm::NoResult) => Ok(false),
        (EvaluatedTerm::Ungrounded, _) | (_, EvaluatedTerm::Ungrounded) => {
            Err(Error::UngroundedBuiltin {
                name: relation.name.clone(),
            })
        }
    }
}

fn evaluate_planned_term(
    term: &PlannedTerm,
    seed: &PlanSeed,
    plan: &ExecutablePlan<'_>,
) -> Result<EvaluatedTerm> {
    match term {
        PlannedTerm::Const(value) => Ok(EvaluatedTerm::Value(plan.value(*value).clone())),
        PlannedTerm::Var(variable) => Ok(seed
            .lookup(*variable)
            .cloned()
            .map(EvaluatedTerm::Value)
            .unwrap_or(EvaluatedTerm::Ungrounded)),
        PlannedTerm::Wildcard => Ok(EvaluatedTerm::Ungrounded),
        PlannedTerm::Call {
            name,
            operator,
            args,
        } => {
            let [left, right] = args.as_slice() else {
                return Err(Error::BuiltinArityMismatch {
                    name: name.clone(),
                    expected: 2,
                    found: args.len(),
                });
            };
            let left = evaluate_planned_term(left, seed, plan)?;
            let right = evaluate_planned_term(right, seed, plan)?;

            match (left, right) {
                (EvaluatedTerm::NoResult, _) | (_, EvaluatedTerm::NoResult) => {
                    Ok(EvaluatedTerm::NoResult)
                }
                (EvaluatedTerm::Ungrounded, _) | (_, EvaluatedTerm::Ungrounded) => {
                    Ok(EvaluatedTerm::Ungrounded)
                }
                (EvaluatedTerm::Value(left), EvaluatedTerm::Value(right)) => {
                    match operator.evaluate(&left, &right) {
                        OperatorOutcome::Value(value) => Ok(EvaluatedTerm::Value(value)),
                        OperatorOutcome::NoResult => Ok(EvaluatedTerm::NoResult),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
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
                    match operator.evaluate(&left, &right) {
                        OperatorOutcome::Value(value) => Ok(EvaluatedTerm::Value(value)),
                        OperatorOutcome::NoResult => Ok(EvaluatedTerm::NoResult),
                    }
                }
            }
        }
    }
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

    use super::{Evaluator, SubstitutionStream};
    use crate::{
        BinaryOperator, Clause, DatafoxClient, DatafoxConfig, InMemoryStorage, OperatorOutcome,
        Prelude, Query, Result, Universe, Value, parse_query,
    };

    async fn collect_results(
        mut stream: SubstitutionStream,
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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::query(&universe, &atom).await?).await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let error = match Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await {
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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results =
            collect_results(Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await?)
                .await?;

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

        let results = DatafoxClient::new(DatafoxConfig::new(&storage))?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(3)));
        Ok(())
    }

    #[test]
    fn evaluator_exposes_plan_and_eval_plan() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(1)], vec![Value::integer(2)]],
        )]);
        let datafox = DatafoxClient::new(DatafoxConfig::new(&storage))?;
        let query = parse_query("value(X), X > 1")?;
        let plan = datafox.plan(&query)?;

        let results = datafox.eval_plan(&plan)?.collect::<Vec<_>>();

        assert_eq!(plan.variable_names().collect::<Vec<_>>(), vec!["X"]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(2)));
        Ok(())
    }

    #[test]
    fn planner_orders_relations_after_their_required_facts() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(1)], vec![Value::integer(2)]],
        )]);
        let query = parse_query("X > 1, value(X)")?;

        let results = DatafoxClient::new(DatafoxConfig::new(&storage))?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(2)));
        Ok(())
    }

    #[test]
    fn planner_rejects_queries_with_no_safe_ordering() -> Result<()> {
        let storage = InMemoryStorage::new();
        let query = Query::multi(vec![Clause::builtin(
            ">",
            vec![crate::var!("X"), crate::lit!(Value::integer(1))],
        )])?;

        let error = match DatafoxClient::new(DatafoxConfig::new(&storage))?.plan(&query) {
            Ok(_) => panic!("unsafe query"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            crate::Error::UngroundedBuiltin {
                name: ">".to_string(),
            }
        );
        Ok(())
    }

    #[test]
    fn arithmetic_operators_treat_divide_by_zero_as_no_result() -> Result<()> {
        let storage =
            InMemoryStorage::from_facts([("value".to_string(), vec![vec![Value::integer(2)]])]);
        let query = parse_query("value(X), (X / 0) = 1")?;

        let results = DatafoxClient::new(DatafoxConfig::new(&storage))?
            .eval(&query)?
            .collect::<Vec<_>>();

        assert!(results.is_empty());
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

        let results = DatafoxClient::new(DatafoxConfig::new(&storage).with_prelude(prelude))?
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
                        OperatorOutcome::Value(Value::integer(left + right + 10))
                    }
                    _ => OperatorOutcome::NoResult,
                }
            }));
        let query = parse_query("value(X), (X plusTen 1) = 14")?;

        let results = DatafoxClient::new(DatafoxConfig::new(&storage).with_prelude(prelude))?
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

        let error = match Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await {
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
    async fn planned_evaluator_matches_source_order_reference_for_safe_queries() -> Result<()> {
        let storage = InMemoryStorage::from_facts([
            (
                "edge".to_string(),
                vec![
                    vec![Value::integer(1), Value::integer(2)],
                    vec![Value::integer(2), Value::integer(3)],
                    vec![Value::integer(3), Value::integer(4)],
                ],
            ),
            (
                "name".to_string(),
                vec![
                    vec![Value::integer(2), Value::string("rush")],
                    vec![Value::integer(3), Value::string("yes")],
                ],
            ),
        ]);
        let query = parse_query(r#"edge(X, Y), name(Y, Name), contains(Name, "s"), Y > 1"#)?;

        let planned = DatafoxClient::new(DatafoxConfig::new(&storage))?
            .eval(&query)?
            .collect::<Vec<_>>();
        let reference = collect_results(
            Evaluator::<InMemoryStorage>::evaluate(&Universe::new(storage), &query).await?,
        )
        .await?;

        assert_eq!(
            normalize_substitutions(planned),
            normalize_substitutions(reference)
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

        let error = match Evaluator::<InMemoryStorage>::evaluate(&universe, &query).await {
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

        let sequential = DatafoxClient::new(DatafoxConfig::new(&storage))?
            .eval(&query)?
            .collect::<Vec<_>>();
        let parallel = DatafoxClient::new(DatafoxConfig::new(&storage).parallel())?
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

                let datafox = DatafoxClient::new(DatafoxConfig::new(&storage)).expect("datafox");
                for query in queries.into_iter().take(16) {
                    let _ = datafox.eval(&query);
                }
            }
        }
    }
}
