use crate::evaluator::Evaluator;
use crate::{
    Evaluation, EvaluationStrategy, InMemoryStorage, Plan, Planner, Prelude, Query, Result,
};

pub struct DatafoxConfig<'store> {
    storage: &'store InMemoryStorage,
    prelude: Prelude,
    strategy: EvaluationStrategy,
    threads: Option<usize>,
}

impl<'store> DatafoxConfig<'store> {
    pub fn new(storage: &'store InMemoryStorage) -> Self {
        Self {
            storage,
            prelude: Prelude::new(),
            strategy: EvaluationStrategy::Serial,
            threads: None,
        }
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
    prelude: Prelude,
    evaluator: Evaluator<'store>,
}

impl<'store> DatafoxClient<'store> {
    pub fn new(config: DatafoxConfig<'store>) -> Result<Self> {
        let evaluator = Evaluator::new(
            config.storage,
            config.prelude.clone(),
            config.strategy,
            config.threads,
        )?;

        Ok(Self {
            storage: config.storage,
            prelude: config.prelude,
            evaluator,
        })
    }

    pub fn from_store(storage: &'store InMemoryStorage) -> Result<Self> {
        Self::new(DatafoxConfig::new(storage))
    }

    pub fn planner(&self) -> Planner<'_> {
        Planner::new(self.storage, &self.prelude)
    }

    pub fn plan(&self, query: &Query) -> Result<Plan> {
        self.planner().plan(query)
    }

    pub fn eval(&self, query: &Query) -> Result<Evaluation> {
        let plan = self.plan(query)?;
        self.eval_plan(&plan)
    }

    pub fn eval_plan(&self, plan: &Plan) -> Result<Evaluation> {
        self.evaluator.eval_plan(plan)
    }

    pub fn strategy(&self) -> EvaluationStrategy {
        self.evaluator.strategy()
    }
}
