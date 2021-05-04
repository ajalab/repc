mod error;
use self::error::ToChannelError;
use http::Uri;
use repc_common::types::NodeId;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::IpAddr};
use tonic::transport::Channel;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(default)]
    pub nodes: HashMap<NodeId, NodeConfiguration>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeConfiguration {
    pub ip: IpAddr,
    pub repc_port: u16,
}

impl NodeConfiguration {
    pub fn new<I: Into<IpAddr>>(ip: I, repc_port: u16) -> Self {
        Self {
            ip: ip.into(),
            repc_port,
        }
    }

    pub fn to_channel_lazy(self) -> Result<Channel, ToChannelError> {
        let authority = format!("{}:{}", self.ip, self.repc_port);
        let uri = Uri::builder()
            .scheme("http")
            .authority(authority.as_bytes())
            .path_and_query("/")
            .build()
            .map_err(ToChannelError::HttpError)?;
        Channel::builder(uri)
            .connect_lazy()
            .map_err(ToChannelError::TransportError)
    }
}
