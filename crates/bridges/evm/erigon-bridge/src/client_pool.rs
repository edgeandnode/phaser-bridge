/// Resilient connection pool for BlockDataClient with automatic retry and health checking
use crate::blockdata_client::BlockDataClient;
use crate::error::ErigonBridgeError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Configuration for the connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of independent connections to maintain
    pub pool_size: usize,

    /// Maximum retry attempts for connection establishment
    pub max_retries: u32,

    /// Initial backoff duration (doubled on each retry)
    pub initial_backoff: Duration,

    /// Maximum backoff duration
    pub max_backoff: Duration,

    /// Health check interval (how often to verify connections are alive)
    pub health_check_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 8,
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(60),
        }
    }
}

/// A connection wrapper with health tracking
struct PooledConnection {
    client: RwLock<Option<BlockDataClient>>,
    healthy: AtomicBool,
    endpoint: String,
    config: PoolConfig,
}

impl PooledConnection {
    fn new(endpoint: String, config: PoolConfig) -> Self {
        Self {
            client: RwLock::new(None),
            healthy: AtomicBool::new(false),
            endpoint,
            config,
        }
    }

    /// Attempt to establish connection with exponential backoff
    async fn connect(&self) -> Result<(), ErigonBridgeError> {
        let mut backoff = self.config.initial_backoff;

        for attempt in 1..=self.config.max_retries {
            match BlockDataClient::connect(self.endpoint.clone()).await {
                Ok(client) => {
                    // Test the connection before marking as healthy
                    let mut test_client = client;
                    match test_client.test_connection().await {
                        Ok(_) => {
                            info!(
                                "Connection to {} established (attempt {}/{})",
                                self.endpoint, attempt, self.config.max_retries
                            );
                            *self.client.write().await = Some(test_client);
                            self.healthy.store(true, Ordering::Release);
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                "Connection test failed for {}: {} (attempt {}/{})",
                                self.endpoint, e, attempt, self.config.max_retries
                            );
                            if attempt < self.config.max_retries {
                                sleep(backoff).await;
                                backoff = (backoff * 2).min(self.config.max_backoff);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to {}: {} (attempt {}/{})",
                        self.endpoint, e, attempt, self.config.max_retries
                    );
                    if attempt < self.config.max_retries {
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(self.config.max_backoff);
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(ErigonBridgeError::ConnectionFailed(format!(
            "Failed to connect to {} after {} attempts",
            self.endpoint, self.config.max_retries
        )))
    }

    /// Reconnect if unhealthy
    async fn ensure_healthy(&self) -> Result<(), ErigonBridgeError> {
        if !self.healthy.load(Ordering::Acquire) {
            warn!(
                "Connection to {} is unhealthy, attempting reconnection",
                self.endpoint
            );
            self.connect().await?;
        }
        Ok(())
    }

    /// Get a reference to the client, reconnecting if necessary
    async fn get_client(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, Option<BlockDataClient>>, ErigonBridgeError> {
        self.ensure_healthy().await?;
        Ok(self.client.read().await)
    }

    /// Mark connection as unhealthy (called when errors occur)
    fn mark_unhealthy(&self) {
        if self.healthy.swap(false, Ordering::Release) {
            error!("Marking connection to {} as unhealthy", self.endpoint);
        }
    }
}

/// Connection pool that maintains multiple independent gRPC connections
pub struct ClientPool {
    connections: Vec<Arc<PooledConnection>>,
    next_index: AtomicUsize,
    config: PoolConfig,
}

impl ClientPool {
    /// Create a new connection pool
    ///
    /// This will establish `pool_size` independent connections to the same endpoint.
    /// Each connection runs over its own HTTP/2 connection with independent flow control.
    pub async fn new(endpoint: String, config: PoolConfig) -> Result<Self, ErigonBridgeError> {
        info!(
            "Creating connection pool with {} connections to {}",
            config.pool_size, endpoint
        );

        let mut connections = Vec::with_capacity(config.pool_size);
        let mut errors = Vec::new();

        // Create all connections, tracking failures
        for i in 0..config.pool_size {
            let pooled = Arc::new(PooledConnection::new(endpoint.clone(), config.clone()));

            match pooled.connect().await {
                Ok(_) => {
                    info!(
                        "Connection {}/{} established successfully",
                        i + 1,
                        config.pool_size
                    );
                    connections.push(pooled);
                }
                Err(e) => {
                    error!(
                        "Failed to establish connection {}/{}: {}",
                        i + 1,
                        config.pool_size,
                        e
                    );
                    errors.push(e);
                }
            }
        }

        // Require at least 50% of connections to succeed
        if connections.len() < config.pool_size / 2 {
            return Err(ErigonBridgeError::ConnectionFailed(format!(
                "Failed to establish minimum connections: {}/{} succeeded",
                connections.len(),
                config.pool_size
            )));
        }

        if !errors.is_empty() {
            warn!(
                "Connection pool created with {}/{} connections (some failed)",
                connections.len(),
                config.pool_size
            );
        } else {
            info!(
                "Connection pool created successfully with all {} connections",
                config.pool_size
            );
        }

        let pool = Self {
            connections,
            next_index: AtomicUsize::new(0),
            config: config.clone(),
        };

        // Start background health checker
        pool.start_health_checker();

        Ok(pool)
    }

    /// Get the next client using round-robin selection
    ///
    /// This distributes load evenly across all connections in the pool.
    /// If a connection is unhealthy, it will automatically attempt to reconnect.
    pub async fn get(&self) -> Result<ClientHandle, ErigonBridgeError> {
        if self.connections.is_empty() {
            return Err(ErigonBridgeError::ConnectionFailed(
                "No healthy connections in pool".to_string(),
            ));
        }

        let start_idx = self.next_index.fetch_add(1, Ordering::Relaxed);

        // Try each connection once, starting from round-robin position
        for offset in 0..self.connections.len() {
            let idx = (start_idx + offset) % self.connections.len();
            let conn = &self.connections[idx];

            match conn.ensure_healthy().await {
                Ok(_) => {
                    debug!("Using connection {} from pool", idx);
                    return Ok(ClientHandle {
                        connection: conn.clone(),
                        index: idx,
                    });
                }
                Err(e) => {
                    warn!("Connection {} is unavailable: {}, trying next", idx, e);
                    continue;
                }
            }
        }

        Err(ErigonBridgeError::ConnectionFailed(
            "No healthy connections available in pool".to_string(),
        ))
    }

    /// Start background task to periodically check connection health
    fn start_health_checker(&self) {
        let connections = self.connections.clone();
        let interval = self.config.health_check_interval;

        tokio::spawn(async move {
            loop {
                sleep(interval).await;

                debug!("Running connection pool health check");
                let mut healthy = 0;
                let mut unhealthy = 0;

                for (i, conn) in connections.iter().enumerate() {
                    if conn.healthy.load(Ordering::Acquire) {
                        healthy += 1;
                    } else {
                        unhealthy += 1;
                        debug!("Connection {} is unhealthy, attempting recovery", i);

                        // Attempt to reconnect in background
                        let conn = conn.clone();
                        tokio::spawn(async move {
                            if let Err(e) = conn.connect().await {
                                error!("Failed to recover connection: {}", e);
                            }
                        });
                    }
                }

                if unhealthy > 0 {
                    warn!(
                        "Pool health: {}/{} healthy, {}/{} unhealthy",
                        healthy,
                        connections.len(),
                        unhealthy,
                        connections.len()
                    );
                } else {
                    debug!("Pool health: all {} connections healthy", connections.len());
                }
            }
        });
    }
}

/// Handle to a client from the pool
///
/// When this handle is used and encounters errors, it can mark the underlying
/// connection as unhealthy to trigger automatic reconnection.
pub struct ClientHandle {
    connection: Arc<PooledConnection>,
    index: usize,
}

impl ClientHandle {
    /// Get the underlying BlockDataClient
    ///
    /// Note: You must call `mark_error()` if this client returns gRPC errors
    /// so the pool can track connection health.
    pub async fn client(
        &self,
    ) -> Result<impl std::ops::Deref<Target = Option<BlockDataClient>> + '_, ErigonBridgeError>
    {
        self.connection.get_client().await
    }

    /// Mark this connection as having encountered an error
    ///
    /// This triggers automatic reconnection in the background.
    pub fn mark_error(&self) {
        warn!(
            "Connection {} encountered error, marking unhealthy",
            self.index
        );
        self.connection.mark_unhealthy();
    }

    /// Get the pool index of this connection (for debugging)
    pub fn index(&self) -> usize {
        self.index
    }
}
