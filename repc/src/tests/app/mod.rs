use crate::state::error::StateMachineError;
use crate::state::StateMachine;
use bytes::{Bytes, BytesMut};
use prost::Message as ProstMessage;

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrRequest {
    #[prost(uint32, tag = "1")]
    pub i: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrResponse {
    #[prost(uint32, tag = "1")]
    pub n: u32,
}

pub trait Incr {
    fn incr(&mut self, req: IncrRequest) -> Result<tonic::Response<IncrResponse>, tonic::Status>;
}

impl<S> StateMachine for S
where
    S: Incr,
{
    fn apply(
        &mut self,
        path: &str,
        body: &[u8],
    ) -> Result<tonic::Response<Bytes>, StateMachineError> {
        match path {
            "/incr.Incr/Incr" => {
                let req = IncrRequest::decode(body)
                    .map_err(|e| StateMachineError::DecodeRequestFailed(e))?;
                let mut res = self.incr(req).map_err(StateMachineError::ApplyFailed)?;
                let mut res_bytes = tonic::Response::new(BytesMut::new());
                std::mem::swap(res.metadata_mut(), res_bytes.metadata_mut());
                res.into_inner()
                    .encode(res_bytes.get_mut())
                    .map_err(StateMachineError::EncodeResponseFailed)?;
                Ok(res_bytes.map(Bytes::from))
            }
            _ => Err(StateMachineError::UnknownPath(path.into())),
        }
    }
}

#[derive(Default, Clone)]
pub struct IncrState {
    n: u32,
}

impl Incr for IncrState {
    fn incr(&mut self, req: IncrRequest) -> Result<tonic::Response<IncrResponse>, tonic::Status> {
        self.n += req.i;
        Ok(tonic::Response::new(IncrResponse { n: self.n }))
    }
}
