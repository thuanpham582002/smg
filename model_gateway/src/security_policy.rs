use std::{sync::Arc, time::Duration};

use futures::TryStreamExt;
use kube::{
    api::{Api, ListParams},
    runtime::watcher::{watcher, Config, Event},
    Client, CustomResource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::{sync::RwLock, task, time};
use tracing::{debug, error, info, warn};

use crate::middleware::ExtAuthConfig;

#[derive(Debug, Clone)]
pub struct SecurityPolicyConfig {
    pub enabled: bool,
    pub namespace: Option<String>,
    pub target_name: Option<String>,
    pub check_interval: Duration,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "smg.lightseek.io",
    version = "v1alpha1",
    kind = "SmgSecurityPolicy",
    plural = "smgsecuritypolicies",
    namespaced,
    status = "SmgSecurityPolicyStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct SmgSecurityPolicySpec {
    pub target_refs: Vec<SecurityPolicyTargetRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext_auth: Option<SecurityPolicyExtAuth>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicyTargetRef {
    #[serde(default = "default_smg_group")]
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicyExtAuth {
    #[serde(default)]
    pub fail_open: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_to_ext_auth: Option<SecurityPolicyBodyToExtAuth>,
    #[serde(default)]
    pub headers_to_ext_auth: Vec<String>,
    pub http: SecurityPolicyHttpExtAuth,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicyBodyToExtAuth {
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicyHttpExtAuth {
    #[serde(default = "default_ext_auth_path")]
    pub path: String,
    pub backend_ref: SecurityPolicyBackendRef,
    #[serde(default)]
    pub headers_to_backend: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecurityPolicyBackendRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub port: u16,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SmgSecurityPolicyStatus {
    #[serde(default)]
    pub observed_generation: i64,
    #[serde(default)]
    pub ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

fn default_smg_group() -> String {
    "smg.lightseek.io".to_string()
}

fn default_ext_auth_path() -> String {
    "/ext-auth".to_string()
}

fn default_max_request_bytes() -> u32 {
    262_144
}

pub async fn start_security_policy_reconciliation(
    config: SecurityPolicyConfig,
    ext_auth_config: Arc<RwLock<ExtAuthConfig>>,
    baseline_ext_auth_config: ExtAuthConfig,
) -> Result<task::JoinHandle<()>, kube::Error> {
    if !config.enabled {
        return Err(kube::Error::Api(
            kube::core::Status::failure(
                "Security policy reconciliation is disabled",
                "ConfigurationError",
            )
            .with_code(400)
            .boxed(),
        ));
    }

    let client = Client::try_default().await?;
    let policies: Api<SmgSecurityPolicy> = if let Some(namespace) = &config.namespace {
        Api::namespaced(client, namespace)
    } else {
        Api::all(client)
    };

    let handle = task::spawn(async move {
        start_security_policy_watcher(policies, config, ext_auth_config, baseline_ext_auth_config)
            .await;
    });
    Ok(handle)
}

async fn start_security_policy_watcher(
    policies: Api<SmgSecurityPolicy>,
    config: SecurityPolicyConfig,
    ext_auth_config: Arc<RwLock<ExtAuthConfig>>,
    baseline_ext_auth_config: ExtAuthConfig,
) {
    info!(
        namespace = config.namespace.as_deref().unwrap_or("<all>"),
        target_name = config.target_name.as_deref().unwrap_or("<any>"),
        "Starting SmgSecurityPolicy reconciliation"
    );

    reconcile_security_policies(
        &policies,
        &config,
        &ext_auth_config,
        &baseline_ext_auth_config,
    )
    .await;

    {
        let policies = policies.clone();
        let config = config.clone();
        let ext_auth_config = Arc::clone(&ext_auth_config);
        let baseline_ext_auth_config = baseline_ext_auth_config.clone();
        task::spawn(async move {
            let start = time::Instant::now() + config.check_interval;
            let mut interval = time::interval_at(start, config.check_interval);
            loop {
                interval.tick().await;
                reconcile_security_policies(
                    &policies,
                    &config,
                    &ext_auth_config,
                    &baseline_ext_auth_config,
                )
                .await;
            }
        });
    }

    let mut retry_delay = Duration::from_secs(1);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(300);

    loop {
        let watcher_ok = watcher(policies.clone(), Config::default())
            .try_for_each(|event| {
                let policies = policies.clone();
                let config = config.clone();
                let ext_auth_config = Arc::clone(&ext_auth_config);
                let baseline_ext_auth_config = baseline_ext_auth_config.clone();
                async move {
                    match event {
                        Event::Apply(_)
                        | Event::Delete(_)
                        | Event::InitApply(_)
                        | Event::InitDone => {
                            reconcile_security_policies(
                                &policies,
                                &config,
                                &ext_auth_config,
                                &baseline_ext_auth_config,
                            )
                            .await;
                        }
                        Event::Init => {}
                    }
                    Ok(())
                }
            })
            .await;

        match watcher_ok {
            Ok(()) => retry_delay = Duration::from_secs(1),
            Err(err) => {
                error!(error = %err, "Error in SmgSecurityPolicy watcher");
                warn!(
                    seconds = retry_delay.as_secs(),
                    "Retrying SmgSecurityPolicy watcher"
                );
                time::sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
            }
        }

        warn!("SmgSecurityPolicy watcher exited, restarting");
        time::sleep(retry_delay).await;
    }
}

async fn reconcile_security_policies(
    policies: &Api<SmgSecurityPolicy>,
    config: &SecurityPolicyConfig,
    ext_auth_config: &Arc<RwLock<ExtAuthConfig>>,
    baseline_ext_auth_config: &ExtAuthConfig,
) {
    let list = match policies.list(&ListParams::default()).await {
        Ok(list) => list,
        Err(err) => {
            error!(error = %err, "SmgSecurityPolicy reconcile: list failed");
            return;
        }
    };

    let mut candidates: Vec<_> = list
        .items
        .into_iter()
        .filter(|policy| policy.metadata.deletion_timestamp.is_none())
        .filter(|policy| policy_matches_target(policy, config.target_name.as_deref()))
        .collect();
    candidates.sort_by(|a, b| {
        let a_key = format!(
            "{}/{}",
            a.metadata.namespace.as_deref().unwrap_or(""),
            a.metadata.name.as_deref().unwrap_or("")
        );
        let b_key = format!(
            "{}/{}",
            b.metadata.namespace.as_deref().unwrap_or(""),
            b.metadata.name.as_deref().unwrap_or("")
        );
        a_key.cmp(&b_key)
    });

    let next = candidates.first().and_then(policy_to_ext_auth_config);
    let mut guard = ext_auth_config.write().await;
    match next {
        Some(cfg) => {
            let enabled_url = cfg.url.clone();
            *guard = cfg;
            info!(
                ext_auth_url = enabled_url.as_deref().unwrap_or(""),
                "SmgSecurityPolicy applied"
            );
        }
        None => {
            if baseline_ext_auth_config.is_enabled() {
                info!("No matching SmgSecurityPolicy found; restoring baseline ext-auth config");
            } else if guard.url.is_some() {
                info!("No matching SmgSecurityPolicy found; disabling CRD-managed ext-auth");
            } else {
                debug!("No matching SmgSecurityPolicy found; ext-auth already disabled");
            }
            *guard = baseline_ext_auth_config.clone();
        }
    }
}

fn policy_matches_target(policy: &SmgSecurityPolicy, target_name: Option<&str>) -> bool {
    policy.spec.target_refs.iter().any(|target| {
        target.group == "smg.lightseek.io"
            && target.kind == "SmgGateway"
            && target_name.is_none_or(|name| target.name == name)
    })
}

fn policy_to_ext_auth_config(policy: &SmgSecurityPolicy) -> Option<ExtAuthConfig> {
    let ext_auth = policy.spec.ext_auth.as_ref()?;
    let namespace = ext_auth
        .http
        .backend_ref
        .namespace
        .as_deref()
        .or(policy.metadata.namespace.as_deref())?;
    let path = if ext_auth.http.path.starts_with('/') {
        ext_auth.http.path.clone()
    } else {
        format!("/{}", ext_auth.http.path)
    };
    let url = format!(
        "http://{}.{}.svc.cluster.local:{}{}",
        ext_auth.http.backend_ref.name, namespace, ext_auth.http.backend_ref.port, path
    );
    Some(
        ExtAuthConfig::new(Some(url))
            .with_timeout_ms(500)
            .with_fail_open_on_transport_error(ext_auth.fail_open),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(name: &str, target: &str) -> SmgSecurityPolicy {
        policy_with_backend_namespace(name, target, None, "/ext-auth")
    }

    fn policy_with_backend_namespace(
        name: &str,
        target: &str,
        backend_namespace: Option<&str>,
        path: &str,
    ) -> SmgSecurityPolicy {
        SmgSecurityPolicy {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("tenant-ns".to_string()),
                ..Default::default()
            },
            spec: SmgSecurityPolicySpec {
                target_refs: vec![SecurityPolicyTargetRef {
                    group: "smg.lightseek.io".to_string(),
                    kind: "SmgGateway".to_string(),
                    name: target.to_string(),
                }],
                ext_auth: Some(SecurityPolicyExtAuth {
                    fail_open: false,
                    body_to_ext_auth: None,
                    headers_to_ext_auth: vec![],
                    http: SecurityPolicyHttpExtAuth {
                        path: path.to_string(),
                        backend_ref: SecurityPolicyBackendRef {
                            name: "regional-auth-backend".to_string(),
                            namespace: backend_namespace.map(str::to_string),
                            port: 8080,
                        },
                        headers_to_backend: vec![],
                    },
                }),
            },
            status: None,
        }
    }

    #[test]
    fn matches_target_when_gateway_name_matches() {
        let policy = policy("inference-auth", "regional");
        assert!(policy_matches_target(&policy, Some("regional")));
        assert!(!policy_matches_target(&policy, Some("other")));
    }

    #[test]
    fn converts_policy_to_ext_auth_config() {
        let policy = policy("inference-auth", "regional");
        let cfg = policy_to_ext_auth_config(&policy).expect("policy should configure ext-auth");
        assert_eq!(
            cfg.url.as_deref(),
            Some("http://regional-auth-backend.tenant-ns.svc.cluster.local:8080/ext-auth")
        );
        assert!(!cfg.fail_open_on_transport_error);
    }

    #[test]
    fn converts_backend_namespace_override_to_ext_auth_config() {
        let policy = policy_with_backend_namespace(
            "inference-auth",
            "regional",
            Some("auth-system"),
            "ext-auth",
        );
        let cfg = policy_to_ext_auth_config(&policy).expect("policy should configure ext-auth");
        assert_eq!(
            cfg.url.as_deref(),
            Some("http://regional-auth-backend.auth-system.svc.cluster.local:8080/ext-auth")
        );
    }

    #[test]
    fn ignores_policy_without_ext_auth() {
        let mut policy = policy("inference-auth", "regional");
        policy.spec.ext_auth = None;
        assert!(policy_to_ext_auth_config(&policy).is_none());
    }
}
