use super::error::RepcServiceError;
use super::RepcService;
use bytes::{Bytes, BytesMut};
use prost::Message;
use tonic::transport::Body;
use tower_service::Service;

#[derive(Clone)]
pub struct RepcServicePeer {
    service: RepcService,
}

impl RepcServicePeer {
    pub fn new(service: RepcService) -> Self {
        Self { service }
    }

    async fn send<T, U>(&mut self, req: T)
    where
        T: Message + 'static,
        U: Message + Default + 'static,
    {
        let mut body = BytesMut::new();
        req.encode(&mut body).unwrap();
        let req = http::Request::new(Body::from(Bytes::from(body)));
        let res = self.service.call(req).await.unwrap();
    }
}
