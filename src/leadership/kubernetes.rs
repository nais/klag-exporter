//! Kubernetes leader election using the Lease API.
//!
//! This module implements leader election using Kubernetes Lease objects
//! from the coordination.k8s.io/v1 API. Only the leader instance collects
//! metrics; standbys wait until they acquire the lease.

use super::{LeadershipProvider, LeadershipState, LeadershipStateUpdater, LeadershipStatus};
use crate::error::{KlagError, Result};
use kube::Client;
use kube_lease_manager::{LeaseCreateMode, LeaseManagerBuilder};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Configuration for Kubernetes leader election.
#[derive(Debug, Clone)]
pub struct KubernetesLeaderConfig {
    /// Name of the Kubernetes Lease resource.
    pub lease_name: String,
    /// Namespace for the Lease resource.
    pub namespace: String,
    /// Identity of this instance (typically pod name).
    pub identity: String,
    /// Duration the lease is valid in seconds.
    pub lease_duration_secs: u32,
    /// Grace period for lease renewal in seconds.
    pub grace_period_secs: u32,
}

impl Default for KubernetesLeaderConfig {
    fn default() -> Self {
        Self {
            lease_name: "klag-exporter".to_string(),
            namespace: "default".to_string(),
            identity: Self::default_identity(),
            lease_duration_secs: 15,
            grace_period_secs: 5,
        }
    }
}

impl KubernetesLeaderConfig {
    /// Generate a default identity from hostname or random string.
    fn default_identity() -> String {
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("POD_NAME"))
            .unwrap_or_else(|_| format!("klag-{}", &uuid_simple()[..8]))
    }
}

/// Simple UUID-like random string generator.
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", nanos)
}

/// Kubernetes-based leader election provider.
pub struct KubernetesLeader {
    config: KubernetesLeaderConfig,
    /// Handle to the background lease management task.
    task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Sender to signal shutdown to the background task.
    shutdown_tx: Arc<Mutex<Option<watch::Sender<bool>>>>,
}

impl KubernetesLeader {
    /// Create a new Kubernetes leader election provider.
    pub fn new(config: KubernetesLeaderConfig) -> Self {
        Self {
            config,
            task_handle: Arc::new(Mutex::new(None)),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Run the leader election loop in the background.
    async fn run_election(
        config: KubernetesLeaderConfig,
        updater: LeadershipStateUpdater,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        info!(
            lease = %config.lease_name,
            namespace = %config.namespace,
            identity = %config.identity,
            "Starting Kubernetes leader election"
        );

        // Create Kubernetes client
        let client = match Client::try_default().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to create Kubernetes client");
                return;
            }
        };

        // Build lease manager
        let manager = match LeaseManagerBuilder::new(client, &config.lease_name)
            .with_namespace(&config.namespace)
            .with_identity(&config.identity)
            .with_duration(u64::from(config.lease_duration_secs))
            .with_grace(u64::from(config.grace_period_secs))
            .with_create_mode(LeaseCreateMode::AutoCreate)
            .build()
            .await
        {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "Failed to create lease manager");
                return;
            }
        };

        // Start watching for leadership changes
        let (mut lease_rx, lease_task) = manager.watch().await;

        info!("Lease manager started, watching for leadership changes");

        loop {
            // Check current leadership status
            let is_leader = *lease_rx.borrow_and_update();
            let new_state = if is_leader {
                LeadershipState::Leader
            } else {
                LeadershipState::Standby
            };

            debug!(is_leader, "Leadership status update");
            updater.set_state(new_state);

            if is_leader {
                info!(identity = %config.identity, "Acquired leadership");
            } else {
                info!(identity = %config.identity, "On standby, waiting for leadership");
            }

            tokio::select! {
                // Wait for leadership change
                result = lease_rx.changed() => {
                    if result.is_err() {
                        warn!("Lease channel closed, stopping election");
                        break;
                    }
                }
                // Check for shutdown signal
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received, releasing leadership");
                        break;
                    }
                }
            }
        }

        // Clean up: drop the lease channel to release leadership
        drop(lease_rx);

        // Wait for the lease task to complete
        match lease_task.await {
            Ok(Ok(_manager)) => {
                info!("Lease manager stopped cleanly");
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Lease manager stopped with error");
            }
            Err(e) => {
                error!(error = %e, "Lease task panicked");
            }
        }

        // Ensure we're marked as standby when stopping
        updater.set_state(LeadershipState::Standby);
    }
}

#[async_trait::async_trait]
impl LeadershipProvider for KubernetesLeader {
    async fn start(&self) -> Result<LeadershipStatus> {
        // Create the status and updater
        let (status, updater) = LeadershipStatus::new(LeadershipState::Standby);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Store shutdown sender
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        // Spawn background task
        let config = self.config.clone();
        let handle = tokio::spawn(async move {
            Self::run_election(config, updater, shutdown_rx).await;
        });

        // Store task handle
        *self.task_handle.lock().await = Some(handle);

        Ok(status)
    }

    async fn stop(&self) {
        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(true);
        }

        // Wait for task to complete
        if let Some(handle) = self.task_handle.lock().await.take() {
            let _ = handle.await;
        }
    }
}

/// Create a Kubernetes leader provider from configuration.
pub fn create_kubernetes_leader(
    lease_name: &str,
    namespace: &str,
    identity: Option<&str>,
    lease_duration_secs: Option<u32>,
    grace_period_secs: Option<u32>,
) -> Result<KubernetesLeader> {
    let config = KubernetesLeaderConfig {
        lease_name: lease_name.to_string(),
        namespace: namespace.to_string(),
        identity: identity
            .map(String::from)
            .unwrap_or_else(KubernetesLeaderConfig::default_identity),
        lease_duration_secs: lease_duration_secs.unwrap_or(15),
        grace_period_secs: grace_period_secs.unwrap_or(5),
    };

    // Validate config
    if config.lease_duration_secs <= config.grace_period_secs {
        return Err(KlagError::Config(
            "lease_duration must be greater than grace_period".to_string(),
        ));
    }

    if config.grace_period_secs == 0 {
        return Err(KlagError::Config(
            "grace_period must be greater than 0".to_string(),
        ));
    }

    Ok(KubernetesLeader::new(config))
}
