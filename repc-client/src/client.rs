use crate::{
    configuration::{Configuration, NodeConfiguration},
    error::ToChannelError,
    session::Session,
};
use bytes::{Bytes, BytesMut};
use http::Uri;
use http_body::Body as HttpBody;
use repc_common::{
    metadata::{request::RequestMetadata, status::StatusMetadata},
    pb::repc::{
        repc_client::RepcClient as TonicRepcClient, CommandRequest, CommandResponse,
        RegisterRequest, RegisterResponse,
    },
    types::NodeId,
    util::clone_request,
};
use std::collections::HashMap;
use tonic::{
    body::{Body, BoxBody},
    client::GrpcService,
    transport::Channel,
    IntoRequest, Request, Response, Status,
};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct RetryingRepcClient<T> {
    id: NodeId,
    clients: HashMap<NodeId, TonicRepcClient<T>>,
}

impl<T> RetryingRepcClient<T>
where
    T: GrpcService<BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
{
    pub fn new<I>(services: I) -> Self
    where
        I: IntoIterator<Item = (NodeId, T)>,
    {
        let clients = services
            .into_iter()
            .map(|(id, service)| (id, TonicRepcClient::new(service)))
            .collect::<HashMap<_, _>>();
        let id = clients.keys().next().unwrap().clone();

        Self { id, clients }
    }

    async fn register(&mut self) -> Result<Response<RegisterResponse>, Status> {
        let mut candidates = self.clients.keys().cloned().collect::<Vec<_>>();
        // TODO: Extract the common retry logic into RetryPolicy
        let mut n = 10;
        while n > 0 {
            let id = self.id;
            let client = self.clients.get_mut(&id).ok_or_else(|| {
                Status::internal(format!("failed to get client for node id {}", id))
            })?;

            match client.register(RegisterRequest {}).await {
                Ok(res) => {
                    return Ok(res);
                }
                Err(status) => match StatusMetadata::decode_retry(status.metadata()) {
                    Ok(Some(Some(i))) => {
                        self.id = i;
                    }
                    Ok(Some(None)) => match candidates.pop() {
                        Some(i) => {
                            self.id = i;
                        }
                        None => {
                            return Err(status);
                        }
                    },
                    _ => {
                        return Err(status);
                    }
                },
            }
            n -= 1;
        }

        Err(Status::unknown("retry exhausted"))
    }

    async fn unary(
        &mut self,
        req: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let mut candidates = self.clients.keys().cloned().collect::<Vec<_>>();
        // TODO: Extract the common retry logic into RetryPolicy
        let mut n = 10;
        while n > 0 {
            let id = self.id;
            let client = self.clients.get_mut(&id).ok_or_else(|| {
                Status::internal(format!("failed to get client for node id {}", id))
            })?;

            let r = clone_request(&req);
            match client.unary_command(r).await {
                Ok(res) => return Ok(res),
                Err(status) => match StatusMetadata::decode_retry(status.metadata()) {
                    Ok(Some(Some(i))) => {
                        self.id = i;
                    }
                    Ok(Some(None)) => match candidates.pop() {
                        Some(i) => {
                            self.id = i;
                        }
                        None => {
                            return Err(status);
                        }
                    },
                    _ => {
                        return Err(status);
                    }
                },
            }
            n -= 1;
        }

        Err(Status::unknown("retry exhausted"))
    }
}

pub struct RepcClient<T> {
    client: RetryingRepcClient<T>,
    session: Option<Session>,
}

impl RepcClient<Channel> {
    pub fn from_conf(conf: Configuration) -> Result<Self, ToChannelError> {
        let services: Vec<_> = conf
            .nodes
            .iter()
            .map(|(&id, node_conf)| match to_channel_lazy(node_conf) {
                Ok(c) => Ok((id, c)),
                Err(e) => Err(e),
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            client: RetryingRepcClient::new(services),
            session: None,
        })
    }
}

impl<T> RepcClient<T>
where
    T: GrpcService<BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
{
    pub fn new<I>(services: I) -> Self
    where
        I: IntoIterator<Item = (NodeId, T)>,
    {
        Self {
            client: RetryingRepcClient::new(services),
            session: None,
        }
    }

    pub async fn register(&mut self) -> Result<(), Status> {
        let session = self
            .client
            .register()
            .await
            .map(|r| Session::new(r.into_inner().id))?;
        self.session = Some(session);
        Ok(())
    }

    pub async fn unary<P, Req, Res>(
        &mut self,
        path: P,
        req: impl IntoRequest<Req>,
    ) -> Result<Response<Res>, Status>
    where
        P: AsRef<str>,
        Req: prost::Message + Clone,
        Res: prost::Message + Default,
    {
        if self.session.is_none() {
            self.register().await?;
        }
        let session = self.session.as_ref().unwrap();
        let request = Self::encode_request(path, req, session);

        self.client
            .unary(clone_request(&request))
            .await
            .and_then(|res| {
                let res = Self::decode_response::<Res>(res);
                res
            })
    }

    fn encode_request<P, Req>(
        path: P,
        req: impl IntoRequest<Req>,
        session: &Session,
    ) -> Request<CommandRequest>
    where
        P: AsRef<str>,
        Req: prost::Message,
    {
        let mut req = req.into_request();
        let md = std::mem::take(req.metadata_mut());

        let mut body = BytesMut::new();
        req.into_inner().encode(&mut body).unwrap();

        let mut request = Request::new(CommandRequest {
            path: path.as_ref().to_string(),
            body: body.to_vec(),
        });
        *request.metadata_mut() = md;

        let metadata = RequestMetadata {
            client_id: session.client_id,
            sequence: session.sequence,
        };

        let _ = metadata.encode(request.metadata_mut());
        request
    }

    fn decode_response<Res>(mut res: Response<CommandResponse>) -> Result<Response<Res>, Status>
    where
        Res: prost::Message + Default,
    {
        let metadata = std::mem::take(res.metadata_mut());
        let res_message_inner = Res::decode(Bytes::from(res.into_inner().body))
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }
}

fn to_channel_lazy(node_conf: &NodeConfiguration) -> Result<Channel, ToChannelError> {
    let authority = format!("{}:{}", node_conf.ip, node_conf.repc_port);
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
