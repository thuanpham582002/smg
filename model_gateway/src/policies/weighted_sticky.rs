//! Weighted sticky routing policy.
//!
//! Uses stable weighted hashing when a sticky key is present and weighted
//! random selection for anonymous traffic.

use std::sync::Arc;

use rand::Rng;

use super::{get_healthy_worker_indices, LoadBalancingPolicy, SelectWorkerInfo};
use crate::{
    observability::metrics::Metrics, routers::common::header_utils::extract_routing_key,
    worker::Worker,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Branch {
    NoHealthyWorkers,
    Sticky,
    RandomFallback,
}

impl Branch {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NoHealthyWorkers => "no_healthy_workers",
            Self::Sticky => "sticky",
            Self::RandomFallback => "random_fallback",
        }
    }
}

#[derive(Debug, Default)]
pub struct WeightedStickyPolicy;

impl WeightedStickyPolicy {
    pub fn new() -> Self {
        Self
    }

    fn sticky_key(info: &SelectWorkerInfo) -> Option<String> {
        let headers = info.headers?;

        if let Some(key) = extract_routing_key(Some(headers)) {
            return Some(key.to_string());
        }

        for name in ["x-session-id", "x-user-id", "cookie", "authorization"] {
            if let Some(value) = headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
            {
                return Some(value.to_string());
            }
        }

        None
    }

    fn worker_weight(worker: &Arc<dyn Worker>) -> u32 {
        worker.metadata().spec.routing_weight
    }

    fn select_weighted_random(
        workers: &[Arc<dyn Worker>],
        healthy_indices: &[usize],
    ) -> Option<usize> {
        let total_weight: u64 = healthy_indices
            .iter()
            .map(|idx| u64::from(Self::worker_weight(&workers[*idx])))
            .sum();

        if total_weight == 0 {
            return None;
        }

        let mut remaining = rand::rng().random_range(0..total_weight);
        for idx in healthy_indices {
            let weight = u64::from(Self::worker_weight(&workers[*idx]));
            if remaining < weight {
                return Some(*idx);
            }
            remaining -= weight;
        }

        healthy_indices.last().copied()
    }

    fn select_weighted_sticky(
        workers: &[Arc<dyn Worker>],
        healthy_indices: &[usize],
        key: &str,
    ) -> Option<usize> {
        healthy_indices
            .iter()
            .filter_map(|idx| {
                let weight = Self::worker_weight(&workers[*idx]);
                if weight == 0 {
                    return None;
                }

                let input = format!("{key}\0{}", workers[*idx].url());
                let hash = blake3::hash(input.as_bytes());
                let hash_bytes: [u8; 8] = hash.as_bytes()[..8].try_into().ok()?;
                let unit = (u64::from_le_bytes(hash_bytes) as f64 + 1.0) / (u64::MAX as f64 + 1.0);
                let score = unit.ln() / f64::from(weight);
                Some((*idx, score))
            })
            .max_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(idx, _)| idx)
    }

    fn select_worker_impl(
        &self,
        workers: &[Arc<dyn Worker>],
        info: &SelectWorkerInfo,
    ) -> (Option<usize>, Branch) {
        let healthy_indices = get_healthy_worker_indices(workers);
        if healthy_indices.is_empty() {
            return (None, Branch::NoHealthyWorkers);
        }

        if let Some(key) = Self::sticky_key(info) {
            return (
                Self::select_weighted_sticky(workers, &healthy_indices, &key),
                Branch::Sticky,
            );
        }

        (
            Self::select_weighted_random(workers, &healthy_indices),
            Branch::RandomFallback,
        )
    }
}

impl LoadBalancingPolicy for WeightedStickyPolicy {
    fn select_worker(&self, workers: &[Arc<dyn Worker>], info: &SelectWorkerInfo) -> Option<usize> {
        let (result, branch) = self.select_worker_impl(workers, info);
        Metrics::record_worker_weighted_sticky_policy_branch(branch.as_str());
        result
    }

    fn name(&self) -> &'static str {
        "weighted_sticky"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use openai_protocol::worker::{HealthCheckConfig, WorkerStatus};

    use super::*;
    use crate::worker::{BasicWorkerBuilder, WorkerType};

    fn no_health_check() -> HealthCheckConfig {
        HealthCheckConfig {
            disable_health_check: true,
            ..Default::default()
        }
    }

    fn workers() -> Vec<Arc<dyn Worker>> {
        vec![
            Arc::new(
                BasicWorkerBuilder::new("http://w1:8000")
                    .worker_type(WorkerType::Regular)
                    .routing_weight(8)
                    .health_config(no_health_check())
                    .build(),
            ),
            Arc::new(
                BasicWorkerBuilder::new("http://w2:8000")
                    .worker_type(WorkerType::Regular)
                    .routing_weight(2)
                    .health_config(no_health_check())
                    .build(),
            ),
        ]
    }

    fn headers_with_routing_key(key: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-smg-routing-key", key.parse().unwrap());
        headers
    }

    #[test]
    fn sticky_key_is_stable() {
        let policy = WeightedStickyPolicy::new();
        let workers = workers();
        let headers = headers_with_routing_key("tenant-a");
        let info = SelectWorkerInfo {
            headers: Some(&headers),
            ..Default::default()
        };

        let selected = policy.select_worker(&workers, &info);
        for _ in 0..100 {
            assert_eq!(policy.select_worker(&workers, &info), selected);
        }
    }

    #[test]
    fn sticky_keys_follow_weights_over_many_keys() {
        let policy = WeightedStickyPolicy::new();
        let workers = workers();
        let mut counts = HashMap::new();

        for i in 0..5000 {
            let headers = headers_with_routing_key(&format!("tenant-{i}"));
            let info = SelectWorkerInfo {
                headers: Some(&headers),
                ..Default::default()
            };
            let idx = policy.select_worker(&workers, &info).unwrap();
            *counts.entry(idx).or_insert(0usize) += 1;
        }

        let first = *counts.get(&0).unwrap_or(&0) as f64 / 5000.0;
        assert!((0.74..0.86).contains(&first), "first worker share: {first}");
    }

    #[test]
    fn random_fallback_follows_weights() {
        let policy = WeightedStickyPolicy::new();
        let workers = workers();
        let mut counts = HashMap::new();

        for _ in 0..5000 {
            let idx = policy
                .select_worker(&workers, &SelectWorkerInfo::default())
                .unwrap();
            *counts.entry(idx).or_insert(0usize) += 1;
        }

        let first = *counts.get(&0).unwrap_or(&0) as f64 / 5000.0;
        assert!((0.74..0.86).contains(&first), "first worker share: {first}");
    }

    #[test]
    fn unhealthy_worker_is_excluded() {
        let policy = WeightedStickyPolicy::new();
        let workers = workers();
        workers[0].set_status(WorkerStatus::NotReady);

        for i in 0..100 {
            let headers = headers_with_routing_key(&format!("tenant-{i}"));
            let info = SelectWorkerInfo {
                headers: Some(&headers),
                ..Default::default()
            };
            assert_eq!(policy.select_worker(&workers, &info), Some(1));
        }
    }
}
