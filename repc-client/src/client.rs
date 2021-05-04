use crate::session::Session;
use bytes::{Bytes, BytesMut};
use http_body::Body as HttpBody;
use repc_common::{
    metadata::request::RequestMetadata,
    pb::repc::{
        repc_client::RepcClient as TonicRepcClient, CommandRequest, CommandResponse,
        RegisterRequest,
    },
    util::clone_request,
};
use tonic::{
    body::{Body, BoxBody},
    client::GrpcService,
    IntoRequest, Request, Response, Status,
};

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct RepcClient<T> {
    client: TonicRepcClient<T>,
    session: Option<Session>,
}

impl<T> RepcClient<T>
where
    T: GrpcService<BoxBody>,
    T::ResponseBody: Body + HttpBody + Send + 'static,
    T::Error: Into<StdError>,
    <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
{
    pub fn new(service: T) -> Self {
        Self {
            client: TonicRepcClient::new(service),
            session: None,
        }
    }

    pub async fn register(&mut self) -> Result<(), Status> {
        let session = self
            .client
            .register(RegisterRequest {})
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
            .unary_command(clone_request(&request))
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
