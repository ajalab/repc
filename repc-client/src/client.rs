use crate::session::{
    encoder::{MetadataEncoder, SessionMetadataEncoder},
    Session,
};

use super::error::RegisterError;
use bytes::{Bytes, BytesMut};
use http_body::Body as HttpBody;
use repc_proto::{repc_client::RepcClient as TonicRepcClient, CommandRequest, RegisterRequest};
use tonic::{body::Body, IntoRequest, Request, Response, Status};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct RepcClient<T> {
    client: TonicRepcClient<T>,
    session: Session,
}

impl<T> RepcClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
{
    pub async fn register(service: T) -> Result<Self, RegisterError> {
        let mut client = TonicRepcClient::new(service);
        let res = client
            .register(RegisterRequest {})
            .await
            .map_err(RegisterError::new)?
            .into_inner();

        let session = Session::new(res.id);

        Ok(Self { client, session })
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub async fn unary<P, T1, T2>(
        &mut self,
        path: P,
        req: impl IntoRequest<T1>,
    ) -> Result<Response<T2>, Status>
    where
        P: AsRef<str>,
        T1: prost::Message,
        T2: prost::Message + Default,
    {
        let mut req = req.into_request();
        let metadata = std::mem::take(req.metadata_mut());

        let mut body = BytesMut::new();
        req.into_inner().encode(&mut body).unwrap();

        let mut request = Request::new(CommandRequest {
            path: path.as_ref().to_string(),
            body: body.to_vec(),
            sequence: self.session.sequence(),
        });
        *request.metadata_mut() = metadata;

        let _ = SessionMetadataEncoder.encode(&self.session, request.metadata_mut());

        let mut response = self.client.unary_command(request).await?;

        let metadata = std::mem::take(response.metadata_mut());
        let res_message_inner = T2::decode(Bytes::from(response.into_inner().body))
            .map_err(|e| Status::internal(format!("failed to decode: {}", e)))?;

        let mut res_message = Response::new(res_message_inner);
        *res_message.metadata_mut() = metadata;
        Ok(res_message)
    }
}
