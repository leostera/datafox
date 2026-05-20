use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::evaluator::Evaluator;
use crate::{
    Error, Evaluation, EvaluationStrategy, InMemoryStorage, Plan, Planner, Prelude, PreparedQuery,
    Query, Result,
};

#[derive(Clone)]
pub struct DatafoxEnvironment {
    prelude: Prelude,
    planning_cache: Option<PlanningCache>,
}

impl DatafoxEnvironment {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn builder() -> DatafoxEnvironmentBuilder {
        DatafoxEnvironmentBuilder::default()
    }

    pub fn prepare(&self, query: &Query) -> Result<Arc<PreparedQuery>> {
        if let Some(cache) = &self.planning_cache {
            return cache.prepare(query, &self.prelude);
        }

        Ok(Arc::new(self.prepare_uncached(query)?))
    }

    pub fn client<'store>(
        &self,
        mut config: DatafoxConfig<'store>,
    ) -> Result<DatafoxClient<'store>> {
        config.environment = self.clone();
        DatafoxClient::new(config)
    }

    pub fn prelude(&self) -> &Prelude {
        &self.prelude
    }

    fn prepare_uncached(&self, query: &Query) -> Result<PreparedQuery> {
        Planner::for_prelude(&self.prelude).plan(query)
    }
}

impl Default for DatafoxEnvironment {
    fn default() -> Self {
        Self {
            prelude: Prelude::new(),
            planning_cache: None,
        }
    }
}

#[derive(Default)]
pub struct DatafoxEnvironmentBuilder {
    environment: DatafoxEnvironment,
}

impl DatafoxEnvironmentBuilder {
    pub fn with_prelude(mut self, prelude: Prelude) -> Self {
        self.environment.prelude = prelude;
        self
    }

    pub fn with_planning_cache(mut self, planning_cache: PlanningCache) -> Self {
        self.environment.planning_cache = Some(planning_cache);
        self
    }

    pub fn build(self) -> DatafoxEnvironment {
        self.environment
    }
}

#[derive(Clone, Default)]
pub struct PlanningCache {
    inner: Arc<Mutex<BTreeMap<Query, Arc<PreparedQuery>>>>,
}

impl PlanningCache {
    pub fn unbounded() -> Self {
        Self::default()
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.lock()?.is_empty())
    }

    fn prepare(&self, query: &Query, prelude: &Prelude) -> Result<Arc<PreparedQuery>> {
        if let Some(prepared) = self.lock()?.get(query).cloned() {
            prepared.validate_prelude(prelude)?;
            return Ok(prepared);
        }

        let prepared = Arc::new(Planner::for_prelude(prelude).plan(query)?);
        let mut cache = self.lock()?;
        Ok(cache
            .entry(query.clone())
            .or_insert_with(|| Arc::clone(&prepared))
            .clone())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<Query, Arc<PreparedQuery>>>> {
        self.inner.lock().map_err(|error| Error::EvaluatorBuild {
            message: format!("planning cache lock poisoned: {error}"),
        })
    }
}

pub struct DatafoxConfig<'store> {
    storage: &'store InMemoryStorage,
    environment: DatafoxEnvironment,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store> DatafoxConfig<'store> {
    pub fn new(storage: &'store InMemoryStorage) -> Self {
        Self {
            storage,
            environment: DatafoxEnvironment::new(),
            strategy: EvaluationStrategy::Serial,
            threads: None,
        }
    }

    pub fn with_environment(mut self, environment: DatafoxEnvironment) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_prelude(mut self, prelude: Prelude) -> Self {
        self.environment.prelude = prelude;
        self
    }

    pub fn with_planning_cache(mut self, planning_cache: PlanningCache) -> Self {
        self.environment.planning_cache = Some(planning_cache);
        self
    }

    pub fn serial(mut self) -> Self {
        self.strategy = EvaluationStrategy::Serial;
        self
    }

    pub fn parallel(mut self) -> Self {
        self.strategy = EvaluationStrategy::parallel_default();
        self
    }

    pub fn seed_threshold(mut self, seed_threshold: usize) -> Self {
        self.strategy = EvaluationStrategy::Parallel { seed_threshold };
        self
    }

    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }
}

pub struct DatafoxClient<'store> {
    storage: &'store InMemoryStorage,
    environment: DatafoxEnvironment,
    evaluator: Evaluator<'store>,
}

impl<'store> DatafoxClient<'store> {
    pub fn new(config: DatafoxConfig<'store>) -> Result<Self> {
        let evaluator = Evaluator::new(
            config.storage,
            config.environment.prelude.clone(),
            config.strategy,
            config.threads,
        )?;

        Ok(Self {
            storage: config.storage,
            environment: config.environment,
            evaluator,
        })
    }

    pub fn from_store(storage: &'store InMemoryStorage) -> Result<Self> {
        Self::new(DatafoxConfig::new(storage))
    }

    pub fn planner(&self) -> Planner<'_> {
        Planner::new(self.storage, &self.environment.prelude)
    }

    pub fn prepare(&self, query: &Query) -> Result<Arc<PreparedQuery>> {
        self.environment.prepare(query)
    }

    pub fn plan(&self, query: &Query) -> Result<Plan> {
        Ok((*self.prepare(query)?).clone())
    }

    pub fn eval(&self, query: &Query) -> Result<Evaluation> {
        let prepared = self.prepare(query)?;
        self.eval_prepared(&prepared)
    }

    pub fn eval_prepared(&self, prepared: &PreparedQuery) -> Result<Evaluation> {
        self.evaluator.eval_plan(prepared)
    }

    pub fn eval_plan(&self, plan: &Plan) -> Result<Evaluation> {
        self.eval_prepared(plan)
    }

    pub fn environment(&self) -> &DatafoxEnvironment {
        &self.environment
    }

    pub fn strategy(&self) -> EvaluationStrategy {
        self.evaluator.strategy()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        DatafoxClient, DatafoxConfig, DatafoxEnvironment, InMemoryStorage, PlanningCache, Result,
        Value, parse_query,
    };

    #[test]
    fn planning_cache_reuses_prepared_queries() -> Result<()> {
        let cache = PlanningCache::unbounded();
        let environment = DatafoxEnvironment::builder()
            .with_planning_cache(cache.clone())
            .build();
        let query = parse_query("value(X), X > 1")?;

        let first = environment.prepare(&query)?;
        let second = environment.prepare(&query)?;

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len()?, 1);
        Ok(())
    }

    #[test]
    fn eval_and_eval_prepared_match() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(1)], vec![Value::integer(2)]],
        )]);
        let environment = DatafoxEnvironment::builder()
            .with_planning_cache(PlanningCache::unbounded())
            .build();
        let datafox =
            DatafoxClient::new(DatafoxConfig::new(&storage).with_environment(environment))?;
        let query = parse_query("value(X), X > 1")?;
        let prepared = datafox.prepare(&query)?;

        let direct = datafox.eval(&query)?.collect::<Vec<_>>();
        let prepared = datafox.eval_prepared(&prepared)?.collect::<Vec<_>>();

        assert_eq!(direct, prepared);
        Ok(())
    }
}
