pub mod error;

use self::error::Error;
use bytes::{Bytes, BytesMut};
use http_body::Body as HttpBody;
use repc_proto::{
    repc_client::RepcClient as TonicRepcClient, CommandRequest, RegisterRequest,
    METADATA_REPC_CLIENT_ID_KEY, METADATA_REPC_SEQUENCE_KEY,
};
use tonic::{body::Body, metadata::MetadataValue, Request, Response, Status};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

const ID_UNREGISTERED: u64 = 0;

pub struct RepcClient<T> {
    inner: TonicRepcClient<T>,
    id: u64,
    sequence: u64,
}

impl<T> RepcClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
{
    pub fn new(service: T) -> Self {
        Self {
            inner: TonicRepcClient::new(service),
            id: ID_UNREGISTERED,
            sequence: 0,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub async fn register(&mut self) -> Result<(), Error> {
        let res = self
            .inner
            .register(RegisterRequest {})
            .await
            .map_err(|e| Error::RegisterError(e))?
            .into_inner();

        self.id = res.id;
        self.sequence = 0;

        Ok(())
    }

    pub async fn unary<P, T1, T2>(&mut self, path: P, req: T1) -> Result<Response<T2>, Status>
    where
        P: AsRef<str>,
        T1: prost::Message,
        T2: prost::Message + Default,
    {
        let mut body = BytesMut::new();
        req.encode(&mut body).unwrap();
        let mut request = Request::new(CommandRequest {
            path: path.as_ref().to_string(),
            body: body.to_vec(),
            sequence: 0,
        });
        self.embed_request_metadata(&mut request);

        let mut response = self.inner.unary_command(request).await?;
        let metadata = std::mem::take(response.metadata_mut());

        let res_message_inner = T2::decode(Bytes::from(response.into_inner().body))
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }

    fn embed_request_metadata<R>(&self, request: &mut Request<R>) {
        let metadata = request.metadata_mut();

        metadata.insert(METADATA_REPC_CLIENT_ID_KEY, MetadataValue::from(self.id));
        metadata.insert(
            METADATA_REPC_SEQUENCE_KEY,
            MetadataValue::from(self.sequence),
        );
    }
}
