use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::evaluator::Evaluator;
use crate::{
    Error, Evaluation, EvaluationStrategy, InMemoryStorage, PREPARED_QUERY_FORMAT_VERSION, Plan,
    Planner, Prelude, PreparedQuery, Query, Result,
};

#[derive(Clone)]
pub struct DatafoxEnvironment {
    prelude: Prelude,
    prepared_query_storage: Option<Arc<dyn PreparedQueryStorage>>,
}

impl DatafoxEnvironment {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn builder() -> DatafoxEnvironmentBuilder {
        DatafoxEnvironmentBuilder::default()
    }

    pub fn prepare(&self, query: &Query) -> Result<Arc<PreparedQuery>> {
        if let Some(storage) = &self.prepared_query_storage {
            let key = PreparedQueryKey::new(query.clone());
            if let Some(prepared) = storage.get(&key)? {
                prepared.validate_for_prelude(&self.prelude)?;
                return Ok(prepared);
            }

            let prepared = Arc::new(self.prepare_uncached(query)?);
            storage.insert(key, Arc::clone(&prepared))?;
            return Ok(prepared);
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
            prepared_query_storage: None,
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

    pub fn with_prepared_query_storage<S>(mut self, storage: S) -> Self
    where
        S: PreparedQueryStorage + 'static,
    {
        self.environment.prepared_query_storage = Some(Arc::new(storage));
        self
    }

    pub fn with_planning_cache(self, planning_cache: PlanningCache) -> Self {
        self.with_prepared_query_storage(planning_cache)
    }

    pub fn build(self) -> DatafoxEnvironment {
        self.environment
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PreparedQueryKey {
    format_version: u32,
    query: Query,
}

impl PreparedQueryKey {
    pub fn new(query: Query) -> Self {
        Self {
            format_version: PREPARED_QUERY_FORMAT_VERSION,
            query,
        }
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn query(&self) -> &Query {
        &self.query
    }
}

pub trait PreparedQueryStorage: Send + Sync {
    fn get(&self, key: &PreparedQueryKey) -> Result<Option<Arc<PreparedQuery>>>;

    fn insert(&self, key: PreparedQueryKey, prepared: Arc<PreparedQuery>) -> Result<()>;
}

#[derive(Clone, Default)]
pub struct InMemoryPreparedQueryStorage {
    inner: Arc<Mutex<BTreeMap<PreparedQueryKey, Arc<PreparedQuery>>>>,
}

pub type PlanningCache = InMemoryPreparedQueryStorage;

impl InMemoryPreparedQueryStorage {
    pub fn unbounded() -> Self {
        Self::default()
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<PreparedQueryKey, Arc<PreparedQuery>>>> {
        self.inner
            .lock()
            .map_err(|error| Error::PreparedQueryStorage {
                message: format!("prepared query storage lock poisoned: {error}"),
            })
    }
}

impl PreparedQueryStorage for InMemoryPreparedQueryStorage {
    fn get(&self, key: &PreparedQueryKey) -> Result<Option<Arc<PreparedQuery>>> {
        Ok(self.lock()?.get(key).cloned())
    }

    fn insert(&self, key: PreparedQueryKey, prepared: Arc<PreparedQuery>) -> Result<()> {
        self.lock()?.entry(key).or_insert(prepared);
        Ok(())
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

    pub fn with_prepared_query_storage<S>(mut self, storage: S) -> Self
    where
        S: PreparedQueryStorage + 'static,
    {
        self.environment.prepared_query_storage = Some(Arc::new(storage));
        self
    }

    pub fn with_planning_cache(self, planning_cache: PlanningCache) -> Self {
        self.with_prepared_query_storage(planning_cache)
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
        DatafoxClient, DatafoxConfig, DatafoxEnvironment, InMemoryPreparedQueryStorage,
        InMemoryStorage, PreparedQueryKey, Result, Value, parse_query,
    };

    #[test]
    fn prepared_query_storage_reuses_prepared_queries() -> Result<()> {
        let prepared_query_storage = InMemoryPreparedQueryStorage::unbounded();
        let environment = DatafoxEnvironment::builder()
            .with_prepared_query_storage(prepared_query_storage.clone())
            .build();
        let query = parse_query("value(X), X > 1")?;

        let first = environment.prepare(&query)?;
        let second = environment.prepare(&query)?;

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(prepared_query_storage.len()?, 1);
        Ok(())
    }

    #[test]
    fn prepared_query_keys_are_serializable() -> Result<()> {
        let query = parse_query("value(X), X > 1")?;
        let key = PreparedQueryKey::new(query);

        let encoded = serde_json::to_string(&key).expect("encoded key");
        let decoded: PreparedQueryKey = serde_json::from_str(&encoded).expect("decoded key");

        assert_eq!(decoded, key);
        Ok(())
    }

    #[test]
    fn eval_and_eval_prepared_match() -> Result<()> {
        let storage = InMemoryStorage::from_facts([(
            "value".to_string(),
            vec![vec![Value::integer(1)], vec![Value::integer(2)]],
        )]);
        let environment = DatafoxEnvironment::builder()
            .with_prepared_query_storage(InMemoryPreparedQueryStorage::unbounded())
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
