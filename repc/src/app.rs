use futures_util::TryStreamExt;
use tonic::codec::{Codec, ProstCodec, Streaming};
use tonic::codegen::*;
use tonic::{Request, Response, Status};

mod pb {
    tonic::include_proto!("kvs");
}

pub struct Sender {}

pub struct KeyValueStoreService {
    sender: Sender,
}

impl KeyValueStoreService {
    fn create() -> impl FnOnce(Sender) -> Self {
        |sender| KeyValueStoreService { sender }
    }
}

#[tonic::async_trait]
impl pb::key_value_store_server::KeyValueStore for KeyValueStoreService {
    async fn insert(
        &self,
        request: Request<pb::InsertRequest>,
    ) -> Result<Response<pb::InsertResponse>, Status> {
        Err(Status::unimplemented("message"))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        Err(Status::unimplemented("message"))
    }
}

pub async fn map_request<B>(
    req: http::Request<B>,
) -> Result<tonic::Request<Box<dyn prost::Message>>, Status>
where
    B: HttpBody + Send + Sync + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    let (mut parts, body) = req.into_parts();
    let (message, trailers): (Box<dyn prost::Message>, _) = match parts.uri.path() {
        "/kvs.KeyValueStore/Insert" => {
            let mut codec = ProstCodec::<(), pb::InsertRequest>::default();
            let stream = Streaming::new_request(codec.decoder(), body);

            futures::pin_mut!(stream);

            let message = stream
                .try_next()
                .await?
                .ok_or_else(|| Status::internal("request message is missing"))?;

            (Box::new(message), stream.trailers().await?)
        }

        "/kvs.KeyValueStore/Delete" => {
            let mut codec = ProstCodec::<(), pb::DeleteRequest>::default();
            let stream = Streaming::new_request(codec.decoder(), body);

            futures::pin_mut!(stream);

            let message = stream
                .try_next()
                .await?
                .ok_or_else(|| Status::internal("request message is missing"))?;

            (Box::new(message), stream.trailers().await?)
        }
        _ => Err(Status::not_found("not found"))?,
    };

    let mut request = tonic::Request::new(message);
    if let Some(trailers) = trailers {
        parts.headers.extend(trailers.into_headers());
    }
    let metadata = tonic::metadata::MetadataMap::from_headers(parts.headers);
    *request.metadata_mut() = metadata;

    Ok(request)
}
