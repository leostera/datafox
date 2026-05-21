use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    Atom, BinaryOperator, BinaryRelation, Clause, Error, FactStore, Prelude, Query, Result, Term,
    Value,
};

pub const PREPARED_QUERY_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VariableId(usize);

impl VariableId {
    pub(crate) fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PredicateId(usize);

impl PredicateId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ValueId(usize);

impl ValueId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedQuery {
    format_version: u32,
    clauses: Vec<PreparedClause>,
    variables: Vec<String>,
    predicates: Vec<String>,
    values: Vec<Value>,
    required_relations: BTreeSet<String>,
    required_operators: BTreeSet<String>,
}

pub type Plan = PreparedQuery;

impl PreparedQuery {
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn variable_names(&self) -> impl Iterator<Item = &str> {
        self.variables.iter().map(String::as_str)
    }

    pub fn required_relations(&self) -> impl Iterator<Item = &str> {
        self.required_relations.iter().map(String::as_str)
    }

    pub fn required_operators(&self) -> impl Iterator<Item = &str> {
        self.required_operators.iter().map(String::as_str)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != PREPARED_QUERY_FORMAT_VERSION {
            return Err(Error::PreparedQueryFormat {
                expected: PREPARED_QUERY_FORMAT_VERSION,
                found: self.format_version,
            });
        }

        for clause in &self.clauses {
            validate_clause(clause, self)?;
        }

        Ok(())
    }

    pub fn validate_for_prelude(&self, prelude: &Prelude) -> Result<()> {
        self.validate()?;
        self.validate_prelude(prelude)
    }

    pub(crate) fn bind<'a>(&'a self, prelude: &Prelude) -> Result<ExecutablePlan<'a>> {
        self.validate()?;
        self.validate_prelude(prelude)?;

        let clauses = self
            .clauses
            .iter()
            .map(|clause| bind_clause(clause, prelude))
            .collect::<Result<_>>()?;

        Ok(ExecutablePlan {
            prepared: self,
            clauses,
        })
    }

    pub(crate) fn validate_prelude(&self, prelude: &Prelude) -> Result<()> {
        for relation in &self.required_relations {
            if prelude.relation(relation).is_none() {
                return Err(Error::UnsupportedBuiltin {
                    name: relation.clone(),
                });
            }
        }

        for operator in &self.required_operators {
            if prelude.operator(operator).is_none() {
                return Err(Error::UnsupportedBuiltin {
                    name: operator.clone(),
                });
            }
        }

        Ok(())
    }

    fn variable_count(&self) -> usize {
        self.variables.len()
    }

    fn variable_name(&self, variable: VariableId) -> &str {
        &self.variables[variable.index()]
    }

    fn predicate_name(&self, predicate: PredicateId) -> &str {
        &self.predicates[predicate.index()]
    }

    fn value(&self, value: ValueId) -> &Value {
        &self.values[value.index()]
    }
}

pub(crate) struct ExecutablePlan<'a> {
    prepared: &'a PreparedQuery,
    clauses: Vec<PlannedClause>,
}

impl ExecutablePlan<'_> {
    pub(crate) fn clauses(&self) -> &[PlannedClause] {
        &self.clauses
    }

    pub(crate) fn variable_count(&self) -> usize {
        self.prepared.variable_count()
    }

    pub(crate) fn variable_name(&self, variable: VariableId) -> &str {
        self.prepared.variable_name(variable)
    }

    pub(crate) fn predicate_name(&self, predicate: PredicateId) -> &str {
        self.prepared.predicate_name(predicate)
    }

    pub(crate) fn value(&self, value: ValueId) -> &Value {
        self.prepared.value(value)
    }
}

pub struct Planner<'a, S: FactStore + ?Sized = crate::InMemoryStorage> {
    storage: Option<&'a S>,
    prelude: &'a Prelude,
}

impl<'a, S: FactStore + ?Sized> Planner<'a, S> {
    pub fn new(storage: &'a S, prelude: &'a Prelude) -> Self {
        Self {
            storage: Some(storage),
            prelude,
        }
    }

    pub fn plan(&self, query: &Query) -> Result<PreparedQuery> {
        let mut context = PlanContext::default();
        let mut clauses = Vec::new();

        for (original_index, clause) in query.clauses().iter().enumerate() {
            clauses.push(self.lower_clause(&mut context, original_index, clause)?);
        }

        let ordered = self.order_clauses(&context, clauses)?;
        Ok(PreparedQuery {
            format_version: PREPARED_QUERY_FORMAT_VERSION,
            clauses: ordered.into_iter().map(|clause| clause.clause).collect(),
            variables: context.variables,
            predicates: context.predicates,
            values: context.values,
            required_relations: context.required_relations,
            required_operators: context.required_operators,
        })
    }

    fn lower_clause(
        &self,
        context: &mut PlanContext,
        original_index: usize,
        clause: &Clause,
    ) -> Result<CandidateClause> {
        let clause = match clause {
            Clause::Atom(atom) => {
                let atom = self.lower_atom(context, atom)?;
                let binds = atom.bound_direct_variables();
                let requires = atom.expression_variables();
                CandidateClause {
                    original_index,
                    requires,
                    binds,
                    clause: PreparedClause::Atom(atom),
                }
            }
            Clause::Negated(atom) => {
                let atom = self.lower_atom(context, atom)?;
                CandidateClause {
                    original_index,
                    requires: atom.variables(),
                    binds: BTreeSet::new(),
                    clause: PreparedClause::Negated(atom),
                }
            }
            Clause::Builtin { name, args } => {
                if self.prelude.relation(name).is_none() {
                    return Err(Error::UnsupportedBuiltin { name: name.clone() });
                }
                if args.len() != 2 {
                    return Err(Error::BuiltinArityMismatch {
                        name: name.clone(),
                        expected: 2,
                        found: args.len(),
                    });
                }

                let args = args
                    .iter()
                    .map(|term| self.lower_term(context, term))
                    .collect::<Result<Vec<_>>>()?;
                let requires = args.iter().flat_map(PreparedTerm::variables).collect();
                context.required_relations.insert(name.clone());
                CandidateClause {
                    original_index,
                    requires,
                    binds: BTreeSet::new(),
                    clause: PreparedClause::Relation(PreparedRelation {
                        name: name.clone(),
                        args,
                    }),
                }
            }
        };

        Ok(clause)
    }

    fn lower_atom(&self, context: &mut PlanContext, atom: &Atom) -> Result<PreparedAtom> {
        Ok(PreparedAtom {
            predicate: context.intern_predicate(&atom.predicate),
            args: atom
                .args
                .iter()
                .map(|term| self.lower_term(context, term))
                .collect::<Result<_>>()?,
        })
    }

    fn lower_term(&self, context: &mut PlanContext, term: &Term) -> Result<PreparedTerm> {
        match term {
            Term::Var(name) => Ok(PreparedTerm::Var(context.intern_variable(name))),
            Term::Const(value) => Ok(PreparedTerm::Const(context.intern_value(value.clone()))),
            Term::Wildcard => Ok(PreparedTerm::Wildcard),
            Term::Call { name, args } => {
                if self.prelude.operator(name).is_none() {
                    return Err(Error::UnsupportedBuiltin { name: name.clone() });
                }
                if args.len() != 2 {
                    return Err(Error::BuiltinArityMismatch {
                        name: name.clone(),
                        expected: 2,
                        found: args.len(),
                    });
                }

                context.required_operators.insert(name.clone());
                Ok(PreparedTerm::Call {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|term| self.lower_term(context, term))
                        .collect::<Result<_>>()?,
                })
            }
        }
    }

    fn order_clauses(
        &self,
        context: &PlanContext,
        mut clauses: Vec<CandidateClause>,
    ) -> Result<Vec<CandidateClause>> {
        let mut ordered = Vec::with_capacity(clauses.len());
        let mut bound = BTreeSet::new();

        while !clauses.is_empty() {
            let Some((index, _)) = clauses
                .iter()
                .enumerate()
                .filter(|(_, clause)| clause.requires.is_subset(&bound))
                .min_by_key(|(_, clause)| {
                    clause.order_key(context, self.storage, self.prelude, &bound)
                })
            else {
                return Err(blocked_clause_error(&clauses[0], context));
            };

            let clause = clauses.remove(index);
            bound.extend(clause.binds.iter().copied());
            ordered.push(clause);
        }

        Ok(ordered)
    }
}

impl<'a> Planner<'a, crate::InMemoryStorage> {
    pub fn for_prelude(prelude: &'a Prelude) -> Self {
        Self {
            storage: None,
            prelude,
        }
    }
}

#[derive(Default)]
struct PlanContext {
    variables_by_name: BTreeMap<String, VariableId>,
    variables: Vec<String>,
    predicates_by_name: BTreeMap<String, PredicateId>,
    predicates: Vec<String>,
    values_by_value: BTreeMap<Value, ValueId>,
    values: Vec<Value>,
    required_relations: BTreeSet<String>,
    required_operators: BTreeSet<String>,
}

impl PlanContext {
    fn intern_variable(&mut self, name: &str) -> VariableId {
        if let Some(variable) = self.variables_by_name.get(name) {
            return *variable;
        }

        let variable = VariableId(self.variables.len());
        self.variables.push(name.to_string());
        self.variables_by_name.insert(name.to_string(), variable);
        variable
    }

    fn intern_predicate(&mut self, name: &str) -> PredicateId {
        if let Some(predicate) = self.predicates_by_name.get(name) {
            return *predicate;
        }

        let predicate = PredicateId(self.predicates.len());
        self.predicates.push(name.to_string());
        self.predicates_by_name.insert(name.to_string(), predicate);
        predicate
    }

    fn intern_value(&mut self, value: Value) -> ValueId {
        if let Some(value_id) = self.values_by_value.get(&value) {
            return *value_id;
        }

        let value_id = ValueId(self.values.len());
        self.values.push(value.clone());
        self.values_by_value.insert(value, value_id);
        value_id
    }

    fn predicate_name(&self, predicate: PredicateId) -> &str {
        &self.predicates[predicate.index()]
    }
}

#[derive(Clone)]
struct CandidateClause {
    original_index: usize,
    requires: BTreeSet<VariableId>,
    binds: BTreeSet<VariableId>,
    clause: PreparedClause,
}

impl CandidateClause {
    fn order_key(
        &self,
        context: &PlanContext,
        storage: Option<&(impl FactStore + ?Sized)>,
        prelude: &Prelude,
        bound: &BTreeSet<VariableId>,
    ) -> (bool, Reverse<usize>, usize, usize) {
        let bound_variable_count = self.bound_variable_count(bound);
        let variables = self.variables();
        let disconnected = !bound.is_empty() && !variables.is_empty() && bound_variable_count == 0;

        (
            disconnected,
            Reverse(bound_variable_count),
            self.estimated_rows(context, storage, prelude, bound),
            self.original_index,
        )
    }

    fn bound_variable_count(&self, bound: &BTreeSet<VariableId>) -> usize {
        self.variables()
            .iter()
            .filter(|variable| bound.contains(variable))
            .count()
    }

    fn variables(&self) -> BTreeSet<VariableId> {
        match &self.clause {
            PreparedClause::Atom(atom) | PreparedClause::Negated(atom) => atom.variables(),
            PreparedClause::Relation(relation) => relation
                .args
                .iter()
                .flat_map(PreparedTerm::variables)
                .collect(),
        }
    }

    fn estimated_rows(
        &self,
        context: &PlanContext,
        storage: Option<&(impl FactStore + ?Sized)>,
        prelude: &Prelude,
        bound: &BTreeSet<VariableId>,
    ) -> usize {
        match &self.clause {
            PreparedClause::Atom(atom) | PreparedClause::Negated(atom) => {
                let predicate = context.predicate_name(atom.predicate);
                let pattern = atom.estimate_pattern(context, bound);
                storage.map_or(0, |storage| storage.estimate(predicate, &pattern).rows)
                    + prelude.facts().estimate(predicate, &pattern).rows
            }
            PreparedClause::Relation(_) => 0,
        }
    }
}

fn blocked_clause_error(clause: &CandidateClause, context: &PlanContext) -> Error {
    match &clause.clause {
        PreparedClause::Atom(atom) => Error::UngroundedBuiltin {
            name: context.predicate_name(atom.predicate).to_string(),
        },
        PreparedClause::Negated(atom) => Error::UngroundedBuiltin {
            name: format!("!{}", context.predicate_name(atom.predicate)),
        },
        PreparedClause::Relation(relation) => Error::UngroundedBuiltin {
            name: relation.name.clone(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum PreparedClause {
    Atom(PreparedAtom),
    Negated(PreparedAtom),
    Relation(PreparedRelation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PreparedAtom {
    predicate: PredicateId,
    args: Vec<PreparedTerm>,
}

impl PreparedAtom {
    fn bound_direct_variables(&self) -> BTreeSet<VariableId> {
        self.args
            .iter()
            .filter_map(|term| match term {
                PreparedTerm::Var(variable) => Some(*variable),
                _ => None,
            })
            .collect()
    }

    fn expression_variables(&self) -> BTreeSet<VariableId> {
        self.args
            .iter()
            .filter(|term| matches!(term, PreparedTerm::Call { .. }))
            .flat_map(PreparedTerm::variables)
            .collect()
    }

    fn variables(&self) -> BTreeSet<VariableId> {
        self.args.iter().flat_map(PreparedTerm::variables).collect()
    }

    fn estimate_pattern(
        &self,
        context: &PlanContext,
        bound: &BTreeSet<VariableId>,
    ) -> Vec<Option<Value>> {
        self.args
            .iter()
            .map(|term| match term {
                PreparedTerm::Const(value) => Some(context.values[value.index()].clone()),
                PreparedTerm::Var(variable) if bound.contains(variable) => None,
                PreparedTerm::Var(_) | PreparedTerm::Call { .. } | PreparedTerm::Wildcard => None,
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PreparedRelation {
    name: String,
    args: Vec<PreparedTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum PreparedTerm {
    Var(VariableId),
    Const(ValueId),
    Call {
        name: String,
        args: Vec<PreparedTerm>,
    },
    Wildcard,
}

impl PreparedTerm {
    fn variables(&self) -> BTreeSet<VariableId> {
        match self {
            Self::Var(variable) => BTreeSet::from([*variable]),
            Self::Const(_) | Self::Wildcard => BTreeSet::new(),
            Self::Call { args, .. } => args.iter().flat_map(Self::variables).collect(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum PlannedClause {
    Atom(PlannedAtom),
    Negated(PlannedAtom),
    Relation(PlannedRelation),
}

#[derive(Clone)]
pub(crate) struct PlannedAtom {
    pub(crate) predicate: PredicateId,
    pub(crate) args: Vec<PlannedTerm>,
}

#[derive(Clone)]
pub(crate) struct PlannedRelation {
    pub(crate) name: String,
    pub(crate) relation: BinaryRelation,
    pub(crate) args: Vec<PlannedTerm>,
}

#[derive(Clone)]
pub(crate) enum PlannedTerm {
    Var(VariableId),
    Const(ValueId),
    Call {
        name: String,
        operator: BinaryOperator,
        args: Vec<PlannedTerm>,
    },
    Wildcard,
}

fn bind_clause(clause: &PreparedClause, prelude: &Prelude) -> Result<PlannedClause> {
    Ok(match clause {
        PreparedClause::Atom(atom) => PlannedClause::Atom(bind_atom(atom, prelude)?),
        PreparedClause::Negated(atom) => PlannedClause::Negated(bind_atom(atom, prelude)?),
        PreparedClause::Relation(relation) => {
            let Some(binary_relation) = prelude.relation(&relation.name).cloned() else {
                return Err(Error::UnsupportedBuiltin {
                    name: relation.name.clone(),
                });
            };

            PlannedClause::Relation(PlannedRelation {
                name: relation.name.clone(),
                relation: binary_relation,
                args: bind_terms(&relation.args, prelude)?,
            })
        }
    })
}

fn bind_atom(atom: &PreparedAtom, prelude: &Prelude) -> Result<PlannedAtom> {
    Ok(PlannedAtom {
        predicate: atom.predicate,
        args: bind_terms(&atom.args, prelude)?,
    })
}

fn bind_terms(terms: &[PreparedTerm], prelude: &Prelude) -> Result<Vec<PlannedTerm>> {
    terms.iter().map(|term| bind_term(term, prelude)).collect()
}

fn bind_term(term: &PreparedTerm, prelude: &Prelude) -> Result<PlannedTerm> {
    Ok(match term {
        PreparedTerm::Var(variable) => PlannedTerm::Var(*variable),
        PreparedTerm::Const(value) => PlannedTerm::Const(*value),
        PreparedTerm::Wildcard => PlannedTerm::Wildcard,
        PreparedTerm::Call { name, args } => {
            let Some(operator) = prelude.operator(name).cloned() else {
                return Err(Error::UnsupportedBuiltin { name: name.clone() });
            };
            PlannedTerm::Call {
                name: name.clone(),
                operator,
                args: bind_terms(args, prelude)?,
            }
        }
    })
}

fn validate_clause(clause: &PreparedClause, prepared: &PreparedQuery) -> Result<()> {
    match clause {
        PreparedClause::Atom(atom) | PreparedClause::Negated(atom) => validate_atom(atom, prepared),
        PreparedClause::Relation(relation) => validate_terms(&relation.args, prepared),
    }
}

fn validate_atom(atom: &PreparedAtom, prepared: &PreparedQuery) -> Result<()> {
    if atom.predicate.index() >= prepared.predicates.len() {
        return Err(Error::InvalidPreparedQuery {
            message: format!("predicate id {} is out of bounds", atom.predicate.index()),
        });
    }

    validate_terms(&atom.args, prepared)
}

fn validate_terms(terms: &[PreparedTerm], prepared: &PreparedQuery) -> Result<()> {
    for term in terms {
        validate_term(term, prepared)?;
    }
    Ok(())
}

fn validate_term(term: &PreparedTerm, prepared: &PreparedQuery) -> Result<()> {
    match term {
        PreparedTerm::Var(variable) if variable.index() >= prepared.variables.len() => {
            Err(Error::InvalidPreparedQuery {
                message: format!("variable id {} is out of bounds", variable.index()),
            })
        }
        PreparedTerm::Const(value) if value.index() >= prepared.values.len() => {
            Err(Error::InvalidPreparedQuery {
                message: format!("value id {} is out of bounds", value.index()),
            })
        }
        PreparedTerm::Call { args, .. } => validate_terms(args, prepared),
        PreparedTerm::Var(_) | PreparedTerm::Const(_) | PreparedTerm::Wildcard => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryStorage, Planner, Prelude, Result, Value, parse_query};

    use super::{PREPARED_QUERY_FORMAT_VERSION, PreparedClause};

    #[test]
    fn planner_prefers_connected_clauses_after_binding_variables() -> Result<()> {
        let child_facts = (0..100)
            .map(|index| vec![Value::integer(1), Value::integer(index)])
            .collect::<Vec<_>>();
        let storage = InMemoryStorage::from_facts([
            ("root".to_string(), vec![vec![Value::integer(1)]]),
            ("child".to_string(), child_facts),
            ("small".to_string(), vec![vec![Value::integer(9)]]),
        ]);
        let prelude = Prelude::new();
        let query = parse_query("root(A), child(A, B), small(C)")?;

        let plan = Planner::new(&storage, &prelude).plan(&query)?;

        let predicates = plan
            .clauses
            .iter()
            .filter_map(|clause| match clause {
                PreparedClause::Atom(atom) => Some(plan.predicate_name(atom.predicate)),
                PreparedClause::Negated(_) | PreparedClause::Relation(_) => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(predicates, vec!["root", "child", "small"]);
        Ok(())
    }

    #[test]
    fn prepared_queries_track_required_builtins() -> Result<()> {
        let prelude = Prelude::new();
        let query = parse_query("value(X), (X + 1) > 3")?;

        let plan = Planner::for_prelude(&prelude).plan(&query)?;

        assert_eq!(plan.format_version(), PREPARED_QUERY_FORMAT_VERSION);
        assert_eq!(plan.required_relations().collect::<Vec<_>>(), vec![">"]);
        assert_eq!(plan.required_operators().collect::<Vec<_>>(), vec!["+"]);
        Ok(())
    }

    #[test]
    fn prepared_queries_can_be_serialized_and_reloaded() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(1)], vec![Value::integer(2)]],
        )]);
        let prelude = Prelude::new();
        let query = parse_query("value(X), X > 1")?;
        let prepared = Planner::for_prelude(&prelude).plan(&query)?;

        let encoded = serde_json::to_string(&prepared).expect("prepared query json");
        let decoded = serde_json::from_str(&encoded).expect("decoded prepared query");
        let datafox = crate::DatafoxClient::new(crate::DatafoxConfig::new(&storage))?;

        let results = datafox.eval_prepared(&decoded)?.collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(2)));
        Ok(())
    }
}
