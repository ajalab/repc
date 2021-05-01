use crate::session::Session;

use super::error::RegisterError;
use bytes::{Bytes, BytesMut};
use http_body::Body as HttpBody;
use repc_common::repc::{
    metadata::RequestMetadata, repc_client::RepcClient as TonicRepcClient, CommandRequest,
    CommandResponse, RegisterRequest,
};
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

    pub async fn unary<P, Req, Res>(
        &mut self,
        path: P,
        req: impl IntoRequest<Req>,
    ) -> Result<Response<Res>, Status>
    where
        P: AsRef<str>,
        Req: prost::Message,
        Res: prost::Message + Default,
    {
        let request = self.encode_request(path, req);

        let res = self.client.unary_command(request).await?;
        let response = Self::decode_response::<Res>(res);
        response
    }

    fn encode_request<P, Req>(&self, path: P, req: impl IntoRequest<Req>) -> Request<CommandRequest>
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
            client_id: self.session.client_id(),
            sequence: self.session.sequence(),
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
