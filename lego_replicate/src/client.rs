//! Client interface for sending commands to the replicated service.
//!
//! Provides [`ReplicatedClient`], an async TCP client that connects to replicas
//! and sends commands using bincode serialization with length-prefixed framing.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::ReplicableService;

/// Errors that can occur during client operations.
#[derive(Debug)]
pub enum ClientError {
    AllReplicasDown,
    Io(io::Error),
    Codec(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::AllReplicasDown => write!(f, "all replicas are unreachable"),
            ClientError::Io(e) => write!(f, "IO error: {}", e),
            ClientError::Codec(msg) => write!(f, "codec error: {}", msg),
        }
    }
}

impl std::error::Error for ClientError {}
impl From<io::Error> for ClientError { fn from(e: io::Error) -> Self { ClientError::Io(e) } }

/// A client for sending commands to a replicated service cluster.
pub struct ReplicatedClient<S: ReplicableService> {
    addrs: Vec<SocketAddr>,
    current: usize,
    conn: Option<TcpStream>,
    _phantom: PhantomData<S>,
}

impl<S: ReplicableService> ReplicatedClient<S>
where
    S::Command: serde::Serialize + serde::de::DeserializeOwned,
    S::Response: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Create a new client connected to the first available replica.
    pub async fn new(addrs: Vec<SocketAddr>) -> Result<Self, ClientError> {
        let mut client = Self { addrs, current: 0, conn: None, _phantom: PhantomData };
        client.connect().await?;
        Ok(client)
    }

    async fn connect(&mut self) -> Result<(), ClientError> {
        for i in 0..self.addrs.len() {
            let idx = (self.current + i) % self.addrs.len();
            match TcpStream::connect(self.addrs[idx]).await {
                Ok(stream) => {
                    self.conn = Some(stream);
                    self.current = idx;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
        Err(ClientError::AllReplicasDown)
    }

    /// Execute a command against the replicated service.
    pub async fn execute(&mut self, cmd: S::Command) -> Result<S::Response, ClientError> {
        let payload = bincode::serialize(&cmd)
            .map_err(|e| ClientError::Codec(e.to_string()))?;

        for _ in 0..self.addrs.len() {
            match self.send_recv(&payload).await {
                Ok(resp_bytes) => {
                    return bincode::deserialize(&resp_bytes)
                        .map_err(|e| ClientError::Codec(e.to_string()));
                }
                Err(_) => {
                    self.conn = None;
                    self.current = (self.current + 1) % self.addrs.len();
                    if self.connect().await.is_err() {
                        return Err(ClientError::AllReplicasDown);
                    }
                }
            }
        }
        Err(ClientError::AllReplicasDown)
    }

    async fn send_recv(&mut self, payload: &[u8]) -> Result<Vec<u8>, io::Error> {
        let stream = self.conn.as_mut().ok_or(io::Error::new(io::ErrorKind::NotConnected, "no connection"))?;
        let len = payload.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(payload).await?;
        stream.flush().await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;
        Ok(resp_buf)
    }
}
