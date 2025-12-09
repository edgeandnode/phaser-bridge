use crate::error::ErigonBridgeError;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::proto::remote::{LogsFilterRequest, SubscribeLogsReply, SyncingReply};
use crate::proto::{BlockReply, BlockRequest, EthbackendClient, Event, SubscribeRequest};

/// Client for connecting to Erigon's gRPC interface
#[derive(Clone)]
pub struct ErigonClient {
    client: EthbackendClient<Channel>,
}

impl ErigonClient {
    /// Create a new ErigonClient and connect to the given endpoint (TCP or IPC)
    pub async fn connect(endpoint: String) -> Result<Self, ErigonBridgeError> {
        info!("Connecting to Erigon gRPC at {}", endpoint);

        // Determine if this is an IPC or TCP connection
        let channel = if endpoint.starts_with("/") || endpoint.starts_with("./") {
            // Unix domain socket (IPC) connection
            info!("Using IPC connection to Erigon");
            info!(
                "Note: Erigon must be running with --private.api.addr=unix://{}",
                endpoint
            );

            // For Unix domain sockets, we need a dummy URI
            let uri = "http://[::]:50051".to_string();

            #[cfg(unix)]
            {
                use tokio::net::UnixStream;
                use tonic::transport::Uri;
                use tower::service_fn;

                let path = endpoint.clone();
                Channel::from_shared(uri)?
                    .connect_with_connector(service_fn(move |_: Uri| {
                        let path = path.clone();
                        async move {
                            UnixStream::connect(path).await.map(|stream| {
                                use hyper_util::rt::tokio::TokioIo;
                                TokioIo::new(stream)
                            })
                        }
                    }))
                    .await
                    .map_err(|e| {
                        error!("Failed to connect to Erigon IPC at {}: {}", endpoint, e);
                        error!(
                            "Make sure Erigon is running with --private.api.addr=unix://{}",
                            endpoint
                        );
                        e
                    })?
            }

            #[cfg(not(unix))]
            {
                return Err(anyhow::anyhow!(
                    "Unix domain sockets are not supported on this platform"
                ));
            }
        } else {
            // TCP connection (existing logic)
            info!("Using TCP connection to Erigon");
            info!(
                "Note: Erigon must be running with --private.api.addr={}",
                endpoint
            );

            Channel::from_shared(format!("http://{endpoint}"))?
                .connect()
                .await
                .map_err(|e| {
                    error!("Failed to connect to Erigon at {}: {}", endpoint, e);
                    error!("Make sure Erigon is running with --private.api.addr flag");
                    e
                })?
        };

        let mut client = EthbackendClient::new(channel);

        // Test the connection
        let request = tonic::Request::new(crate::proto::remote::ClientVersionRequest {});
        let response = client.client_version(request).await?;
        info!(
            "Connected to Erigon node: {}",
            response.into_inner().node_name
        );

        Ok(Self { client })
    }

    /// Get the current syncing status
    pub async fn syncing_status(&mut self) -> Result<SyncingReply, ErigonBridgeError> {
        let request = tonic::Request::new(());
        let response = self.client.syncing(request).await?;
        Ok(response.into_inner())
    }

    /// Subscribe to block headers
    pub async fn subscribe_headers(
        &mut self,
    ) -> Result<tonic::Streaming<crate::proto::remote::SubscribeReply>, ErigonBridgeError> {
        info!("Subscribing to block headers...");

        let request = tonic::Request::new(SubscribeRequest {
            r#type: Event::Header as i32,
        });

        let stream = self.client.subscribe(request).await?.into_inner();
        info!("Subscription established");

        Ok(stream)
    }

    /// Subscribe to logs with optional filters
    pub async fn subscribe_logs(
        &mut self,
        all_addresses: bool,
        addresses: Vec<[u8; 20]>,
        all_topics: bool,
        topics: Vec<[u8; 32]>,
    ) -> Result<tonic::Streaming<SubscribeLogsReply>, ErigonBridgeError> {
        info!("Subscribing to logs...");

        // Convert addresses to H160 format
        let addresses = addresses
            .into_iter()
            .map(|addr| {
                // H160 is 20 bytes: hi is H128 (16 bytes), lo is u32 (4 bytes)
                let hi_bytes = &addr[0..16];
                let lo_bytes = &addr[16..20];

                crate::proto::types::H160 {
                    hi: Some(crate::proto::types::H128 {
                        hi: u64::from_be_bytes(
                            hi_bytes[0..8]
                                .try_into()
                                .expect("20-byte address should split into 8-byte chunks"),
                        ),
                        lo: u64::from_be_bytes(
                            hi_bytes[8..16]
                                .try_into()
                                .expect("20-byte address should split into 8-byte chunks"),
                        ),
                    }),
                    lo: u32::from_be_bytes(
                        lo_bytes
                            .try_into()
                            .expect("20-byte address should have 4-byte remainder"),
                    ),
                }
            })
            .collect();

        // Convert topics to H256 format
        let topics = topics
            .into_iter()
            .map(|topic| {
                let hi_bytes = &topic[0..16];
                let lo_bytes = &topic[16..32];

                crate::proto::types::H256 {
                    hi: Some(crate::proto::types::H128 {
                        hi: u64::from_be_bytes(
                            hi_bytes[0..8]
                                .try_into()
                                .expect("32-byte topic should split into 8-byte chunks"),
                        ),
                        lo: u64::from_be_bytes(
                            hi_bytes[8..16]
                                .try_into()
                                .expect("32-byte topic should split into 8-byte chunks"),
                        ),
                    }),
                    lo: Some(crate::proto::types::H128 {
                        hi: u64::from_be_bytes(
                            lo_bytes[0..8]
                                .try_into()
                                .expect("32-byte topic should split into 8-byte chunks"),
                        ),
                        lo: u64::from_be_bytes(
                            lo_bytes[8..16]
                                .try_into()
                                .expect("32-byte topic should split into 8-byte chunks"),
                        ),
                    }),
                }
            })
            .collect();

        // Create a stream that sends the initial filter and then keeps the stream alive
        let request_stream = async_stream::stream! {
            // Send initial filter
            yield LogsFilterRequest {
                all_addresses,
                addresses,
                all_topics,
                topics,
            };

            // Keep the stream alive by never ending
            // The server needs the request stream to stay open
            futures::future::pending::<()>().await;
        };

        let request = tonic::Request::new(request_stream);

        let stream = self.client.subscribe_logs(request).await?.into_inner();
        info!("Log subscription established");

        Ok(stream)
    }

    /// Get the latest block number
    pub async fn get_latest_block(&mut self) -> Result<u64, ErigonBridgeError> {
        let sync_info = self.syncing_status().await?;
        Ok(sync_info.current_block)
    }

    /// Get a full block by number and optionally hash
    pub async fn get_block(
        &mut self,
        block_number: u64,
        block_hash: Option<&[u8; 32]>,
    ) -> Result<BlockReply, ErigonBridgeError> {
        debug!("Fetching full block #{}", block_number);

        // Convert the block hash bytes to the protobuf H256 format if provided
        let block_hash_proto = block_hash.map(|hash_bytes| {
            // Split the 32 bytes into two 16-byte halves for H256 format
            let hi_bytes = &hash_bytes[0..16];
            let lo_bytes = &hash_bytes[16..32];

            crate::proto::types::H256 {
                hi: Some(crate::proto::types::H128 {
                    hi: u64::from_be_bytes(
                        hi_bytes[0..8]
                            .try_into()
                            .expect("32-byte hash should split into 8-byte chunks"),
                    ),
                    lo: u64::from_be_bytes(
                        hi_bytes[8..16]
                            .try_into()
                            .expect("32-byte hash should split into 8-byte chunks"),
                    ),
                }),
                lo: Some(crate::proto::types::H128 {
                    hi: u64::from_be_bytes(
                        lo_bytes[0..8]
                            .try_into()
                            .expect("32-byte hash should split into 8-byte chunks"),
                    ),
                    lo: u64::from_be_bytes(
                        lo_bytes[8..16]
                            .try_into()
                            .expect("32-byte hash should split into 8-byte chunks"),
                    ),
                }),
            }
        });

        // While erigon's interfaces say: " it's ok to request block only by hash or only by number"
        // in practice, requesting a new block by number only fails with a nil ptr error.
        let request = tonic::Request::new(BlockRequest {
            block_height: block_number,
            block_hash: block_hash_proto,
        });

        let response = self.client.block(request).await?;
        let block = response.into_inner();

        debug!(
            "Received block #{} - RLP size: {} bytes, senders: {} bytes",
            block_number,
            block.block_rlp.len(),
            block.senders.len()
        );

        Ok(block)
    }
}
