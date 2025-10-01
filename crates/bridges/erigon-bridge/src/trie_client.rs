/// Client for Erigon's custom TrieBackend service
use crate::error::ErigonBridgeError;
use crate::proto::custom::{
    trie_backend_client::TrieBackendClient, CommitmentNodeBatch, GetNodesByHashReply,
    GetNodesByHashRequest, GetStateRootReply, GetStateRootRequest, StreamCommitmentRequest,
};
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, error, info, warn};

pub struct TrieClient {
    client: TrieBackendClient<Channel>,
    endpoint: String,
}

impl TrieClient {
    /// Connect to Erigon's TrieBackend service
    pub async fn connect(endpoint: String) -> Result<Self, ErigonBridgeError> {
        info!("Connecting to Erigon TrieBackend at {}", endpoint);

        // Parse endpoint to determine if it's IPC or TCP
        let channel = if endpoint.starts_with('/') || endpoint.starts_with("./") {
            // Unix socket (IPC)
            info!("Using IPC connection to Erigon TrieBackend");

            // We need to use a dummy URI for IPC
            let uri = "http://[::]:50051".to_string();
            let path = endpoint.clone();

            Channel::builder(uri.parse()?)
                .connect_with_connector(tower::service_fn(move |_| {
                    let socket_path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(socket_path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await?
        } else {
            // TCP connection
            info!("Using TCP connection to Erigon TrieBackend");
            info!("Note: Custom Erigon must be running with TrieBackend service enabled");

            let uri = if !endpoint.starts_with("http://") {
                format!("http://{}", endpoint)
            } else {
                endpoint.clone()
            };

            Channel::builder(uri.parse()?).connect().await?
        };

        let client = TrieBackendClient::new(channel);

        Ok(Self {
            client,
            endpoint: endpoint.clone(),
        })
    }

    /// Get the state root for a specific block
    pub async fn get_state_root(
        &mut self,
        block_number: Option<u64>,
    ) -> Result<GetStateRootReply, ErigonBridgeError> {
        use crate::proto::custom::get_state_root_request::BlockId;

        let request = GetStateRootRequest {
            block_id: if let Some(num) = block_number {
                Some(BlockId::BlockNumber(num))
            } else {
                Some(BlockId::BlockTag("latest".to_string()))
            },
        };

        debug!("Requesting state root for block {:?}", block_number);

        let response = self.client.get_state_root(request).await?;
        let reply = response.into_inner();

        info!(
            "Got state root for block #{}: 0x{}",
            reply.block_number,
            hex::encode(&reply.state_root)
        );

        Ok(reply)
    }

    /// Stream commitment nodes for trie reconstruction
    pub async fn stream_commitment_nodes(
        &mut self,
        state_root: Option<Vec<u8>>,
        from_step: u64,
        to_step: u64,
        batch_size: u32,
    ) -> Result<Streaming<CommitmentNodeBatch>, ErigonBridgeError> {
        let request = StreamCommitmentRequest {
            state_root: state_root.unwrap_or_default(),
            from_step,
            to_step,
            bloom_filter: vec![],
            bloom_bits: 0,
            bloom_hashes: 0,
            batch_size: if batch_size > 0 { batch_size } else { 1000 },
            include_proofs: false,
        };

        info!(
            "Starting commitment node stream (steps {}-{}, batch_size: {})",
            from_step, to_step, batch_size
        );

        let response = self.client.stream_commitment_nodes(request).await?;
        Ok(response.into_inner())
    }

    /// Get specific nodes by their hashes
    pub async fn get_nodes_by_hash(
        &mut self,
        hashes: Vec<Vec<u8>>,
        as_of_step: Option<u64>,
    ) -> Result<GetNodesByHashReply, ErigonBridgeError> {
        let request = GetNodesByHashRequest {
            hashes,
            as_of_step: as_of_step.unwrap_or(0),
        };

        debug!("Requesting {} specific nodes by hash", request.hashes.len());

        let response = self.client.get_nodes_by_hash(request).await?;
        let reply = response.into_inner();

        if !reply.missing_hashes.is_empty() {
            warn!("{} nodes were not found", reply.missing_hashes.len());
        }

        Ok(reply)
    }

    /// Test if the TrieBackend service is available
    pub async fn test_connection(&mut self) -> Result<(), ErigonBridgeError> {
        info!("Testing TrieBackend service connection...");

        // Try to get the latest state root
        match self.get_state_root(None).await {
            Ok(reply) => {
                info!(
                    "TrieBackend service is available! Latest state root at block #{}: 0x{}",
                    reply.block_number,
                    hex::encode(&reply.state_root)
                );
                Ok(())
            }
            Err(e) => {
                // Check if it's an "Unimplemented" error
                if let ErigonBridgeError::ErigonClient(status) = &e {
                    if status.code() == tonic::Code::Unimplemented {
                        warn!(
                            "TrieBackend service is NOT implemented in this Erigon instance. \
                            You need to run a custom Erigon build with TrieBackend support in order for this service to work."
                        );
                    }
                }
                Err(e)
            }
        }
    }
}

impl Clone for TrieClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}
