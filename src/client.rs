use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::evaluator::{Evaluator, eval_plan_streaming};
use crate::{
    Error, Evaluation, EvaluationStrategy, FactStore, PREPARED_QUERY_FORMAT_VERSION, Plan, Planner,
    Prelude, PreparedQuery, Query, Result, Storage,
};

#[derive(Clone, Default)]
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

    pub fn client<'store, S: ?Sized>(
        &self,
        mut config: DatafoxConfig<'store, S>,
    ) -> Result<DatafoxClient<'store, S>> {
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

pub struct DatafoxConfig<'store, S: ?Sized = crate::InMemoryStorage> {
    storage: &'store S,
    environment: DatafoxEnvironment,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store, S: ?Sized> DatafoxConfig<'store, S> {
    pub fn new(storage: &'store S) -> Self {
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

    pub fn with_prepared_query_storage<P>(mut self, storage: P) -> Self
    where
        P: PreparedQueryStorage + 'static,
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

pub struct DatafoxClient<'store, S: ?Sized = crate::InMemoryStorage> {
    storage: &'store S,
    environment: DatafoxEnvironment,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store, S: ?Sized> DatafoxClient<'store, S> {
    pub fn new(config: DatafoxConfig<'store, S>) -> Result<Self> {
        Ok(Self {
            storage: config.storage,
            environment: config.environment,
            strategy: config.strategy,
            threads: config.threads,
        })
    }

    pub fn from_store(storage: &'store S) -> Result<Self> {
        Self::new(DatafoxConfig::new(storage))
    }

    pub fn planner(&self) -> Planner<'_, S>
    where
        S: FactStore,
    {
        Planner::new(self.storage, &self.environment.prelude)
    }

    pub fn prepare(&self, query: &Query) -> Result<Arc<PreparedQuery>> {
        self.environment.prepare(query)
    }

    pub fn plan(&self, query: &Query) -> Result<Plan> {
        Ok((*self.prepare(query)?).clone())
    }

    pub fn eval(&self, query: &Query) -> Result<Evaluation>
    where
        S: FactStore,
    {
        let prepared = self.prepare(query)?;
        self.eval_prepared(&prepared)
    }

    pub fn eval_prepared(&self, prepared: &PreparedQuery) -> Result<Evaluation>
    where
        S: FactStore,
    {
        Evaluator::new(
            self.storage,
            self.environment.prelude.clone(),
            self.strategy,
            self.threads,
        )?
        .eval_plan(prepared)
    }

    pub fn eval_plan(&self, plan: &Plan) -> Result<Evaluation>
    where
        S: FactStore,
    {
        self.eval_prepared(plan)
    }

    pub async fn eval_streaming(&self, query: &Query) -> Result<Evaluation>
    where
        S: Storage,
    {
        let prepared = self.prepare(query)?;
        self.eval_prepared_streaming(&prepared).await
    }

    pub async fn eval_prepared_streaming(&self, prepared: &PreparedQuery) -> Result<Evaluation>
    where
        S: Storage,
    {
        eval_plan_streaming(self.storage, &self.environment.prelude, prepared).await
    }

    pub async fn eval_plan_streaming(&self, plan: &Plan) -> Result<Evaluation>
    where
        S: Storage,
    {
        self.eval_prepared_streaming(plan).await
    }

    pub fn environment(&self) -> &DatafoxEnvironment {
        &self.environment
    }

    pub fn strategy(&self) -> EvaluationStrategy {
        self.strategy
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use crate::{
        DatafoxClient, DatafoxConfig, DatafoxEnvironment, FactRequest, FactTuple,
        InMemoryPreparedQueryStorage, InMemoryStorage, PreparedQueryKey, Result, Storage,
        TupleStream, Value, matches_pattern, parse_query,
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

    #[derive(Clone)]
    struct StreamingOnlyStorage {
        facts: Vec<FactTuple>,
        requests: Arc<Mutex<Vec<FactRequest>>>,
    }

    #[async_trait]
    impl Storage for StreamingOnlyStorage {
        async fn get_facts(&self, request: FactRequest) -> Result<TupleStream> {
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.clone());
            let pattern = request.pattern_options();
            let (tx, rx) = mpsc::channel(4);
            if request.predicate == "value" {
                for tuple in &self.facts {
                    if matches_pattern(&pattern, tuple) {
                        tx.send(Ok(tuple.clone())).await.expect("receiver is open");
                    }
                }
            }
            Ok(rx)
        }
    }

    #[tokio::test]
    async fn eval_streaming_uses_storage_trait_without_fact_store() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let storage = StreamingOnlyStorage {
            facts: vec![vec![Value::integer(1)], vec![Value::integer(2)]],
            requests: Arc::clone(&requests),
        };
        let datafox = DatafoxClient::new(DatafoxConfig::new(&storage))?;
        let query = parse_query("value(X), X > 1")?;

        let results = datafox.eval_streaming(&query).await?.collect::<Vec<_>>();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup("X"), Some(&Value::integer(2)));
        let requests = requests.lock().expect("requests lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].predicate, "value");
        assert_eq!(requests[0].pattern_options(), vec![None]);
        assert_eq!(requests[0].hints.role, crate::AtomRole::Positive);
        Ok(())
    }
}
