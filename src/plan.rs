use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Atom, BinaryOperator, BinaryRelation, Clause, Error, FactStore, InMemoryStorage, Prelude,
    Query, Result, Term, Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VariableId(usize);

impl VariableId {
    pub(crate) fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PredicateId(usize);

impl PredicateId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId(usize);

impl ValueId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone)]
pub struct Plan {
    clauses: Vec<PlannedClause>,
    variables: Vec<String>,
    predicates: Vec<String>,
    values: Vec<Value>,
}

impl Plan {
    pub fn variable_names(&self) -> impl Iterator<Item = &str> {
        self.variables.iter().map(String::as_str)
    }

    pub(crate) fn clauses(&self) -> &[PlannedClause] {
        &self.clauses
    }

    pub(crate) fn variable_count(&self) -> usize {
        self.variables.len()
    }

    pub(crate) fn variable_name(&self, variable: VariableId) -> &str {
        &self.variables[variable.index()]
    }

    pub(crate) fn predicate_name(&self, predicate: PredicateId) -> &str {
        &self.predicates[predicate.index()]
    }

    pub(crate) fn value(&self, value: ValueId) -> &Value {
        &self.values[value.index()]
    }
}

pub struct Planner<'a> {
    storage: &'a InMemoryStorage,
    prelude: &'a Prelude,
}

impl<'a> Planner<'a> {
    pub fn new(storage: &'a InMemoryStorage, prelude: &'a Prelude) -> Self {
        Self { storage, prelude }
    }

    pub fn plan(&self, query: &Query) -> Result<Plan> {
        let mut context = PlanContext::default();
        let mut clauses = Vec::new();

        for (original_index, clause) in query.clauses().iter().enumerate() {
            clauses.push(self.lower_clause(&mut context, original_index, clause)?);
        }

        let ordered = self.order_clauses(&context, clauses)?;
        Ok(Plan {
            clauses: ordered.into_iter().map(|clause| clause.clause).collect(),
            variables: context.variables,
            predicates: context.predicates,
            values: context.values,
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
                    clause: PlannedClause::Atom(atom),
                }
            }
            Clause::Negated(atom) => {
                let atom = self.lower_atom(context, atom)?;
                CandidateClause {
                    original_index,
                    requires: atom.variables(),
                    binds: BTreeSet::new(),
                    clause: PlannedClause::Negated(atom),
                }
            }
            Clause::Builtin { name, args } => {
                let Some(relation) = self.prelude.relation(name).cloned() else {
                    return Err(Error::UnsupportedBuiltin { name: name.clone() });
                };
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
                let requires = args.iter().flat_map(PlannedTerm::variables).collect();
                CandidateClause {
                    original_index,
                    requires,
                    binds: BTreeSet::new(),
                    clause: PlannedClause::Relation(PlannedRelation {
                        name: name.clone(),
                        relation,
                        args,
                    }),
                }
            }
        };

        Ok(clause)
    }

    fn lower_atom(&self, context: &mut PlanContext, atom: &Atom) -> Result<PlannedAtom> {
        Ok(PlannedAtom {
            predicate: context.intern_predicate(&atom.predicate),
            args: atom
                .args
                .iter()
                .map(|term| self.lower_term(context, term))
                .collect::<Result<_>>()?,
        })
    }

    fn lower_term(&self, context: &mut PlanContext, term: &Term) -> Result<PlannedTerm> {
        match term {
            Term::Var(name) => Ok(PlannedTerm::Var(context.intern_variable(name))),
            Term::Const(value) => Ok(PlannedTerm::Const(context.intern_value(value.clone()))),
            Term::Wildcard => Ok(PlannedTerm::Wildcard),
            Term::Call { name, args } => {
                let Some(operator) = self.prelude.operator(name).cloned() else {
                    return Err(Error::UnsupportedBuiltin { name: name.clone() });
                };
                if args.len() != 2 {
                    return Err(Error::BuiltinArityMismatch {
                        name: name.clone(),
                        expected: 2,
                        found: args.len(),
                    });
                }

                Ok(PlannedTerm::Call {
                    name: name.clone(),
                    operator,
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
                    (
                        clause.estimated_rows(context, self.storage, self.prelude, &bound),
                        clause.original_index,
                    )
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

#[derive(Default)]
struct PlanContext {
    variables_by_name: BTreeMap<String, VariableId>,
    variables: Vec<String>,
    predicates_by_name: BTreeMap<String, PredicateId>,
    predicates: Vec<String>,
    values_by_value: BTreeMap<Value, ValueId>,
    values: Vec<Value>,
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
    clause: PlannedClause,
}

impl CandidateClause {
    fn estimated_rows(
        &self,
        context: &PlanContext,
        storage: &InMemoryStorage,
        prelude: &Prelude,
        bound: &BTreeSet<VariableId>,
    ) -> usize {
        match &self.clause {
            PlannedClause::Atom(atom) | PlannedClause::Negated(atom) => {
                let predicate = context.predicate_name(atom.predicate);
                let pattern = atom.estimate_pattern(context, bound);
                storage.estimate(predicate, &pattern).rows
                    + prelude.facts().estimate(predicate, &pattern).rows
            }
            PlannedClause::Relation(_) => 0,
        }
    }
}

fn blocked_clause_error(clause: &CandidateClause, context: &PlanContext) -> Error {
    match &clause.clause {
        PlannedClause::Atom(atom) => Error::UngroundedBuiltin {
            name: context.predicate_name(atom.predicate).to_string(),
        },
        PlannedClause::Negated(atom) => Error::UngroundedBuiltin {
            name: format!("!{}", context.predicate_name(atom.predicate)),
        },
        PlannedClause::Relation(relation) => Error::UngroundedBuiltin {
            name: relation.name.clone(),
        },
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

impl PlannedAtom {
    fn bound_direct_variables(&self) -> BTreeSet<VariableId> {
        self.args
            .iter()
            .filter_map(|term| match term {
                PlannedTerm::Var(variable) => Some(*variable),
                _ => None,
            })
            .collect()
    }

    fn expression_variables(&self) -> BTreeSet<VariableId> {
        self.args
            .iter()
            .filter(|term| matches!(term, PlannedTerm::Call { .. }))
            .flat_map(PlannedTerm::variables)
            .collect()
    }

    fn variables(&self) -> BTreeSet<VariableId> {
        self.args.iter().flat_map(PlannedTerm::variables).collect()
    }

    fn estimate_pattern(
        &self,
        context: &PlanContext,
        bound: &BTreeSet<VariableId>,
    ) -> Vec<Option<Value>> {
        self.args
            .iter()
            .map(|term| match term {
                PlannedTerm::Const(value) => Some(context.values[value.index()].clone()),
                PlannedTerm::Var(variable) if bound.contains(variable) => None,
                PlannedTerm::Var(_) | PlannedTerm::Call { .. } | PlannedTerm::Wildcard => None,
            })
            .collect()
    }
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

impl PlannedTerm {
    pub(crate) fn variables(&self) -> BTreeSet<VariableId> {
        match self {
            Self::Var(variable) => BTreeSet::from([*variable]),
            Self::Const(_) | Self::Wildcard => BTreeSet::new(),
            Self::Call { args, .. } => args.iter().flat_map(Self::variables).collect(),
        }
    }
}
