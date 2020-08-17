use crate::state_machine::{ApplyError, StateMachine, StateMachineError};
use bytes::{Bytes, BytesMut};
use prost::Message;
use tonic::transport::NamedService;

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrRequest {
    #[prost(uint32, tag = "1")]
    i: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct IncrResponse {
    #[prost(uint32, tag = "1")]
    n: u32,
}

pub trait Incr {
    fn incr(&mut self, req: IncrRequest) -> Result<IncrResponse, ApplyError>;
}

#[derive(Default, Clone)]
pub struct IncrStateMachine<S> {
    server: S,
}

impl<S> NamedService for IncrStateMachine<S> {
    const NAME: &'static str = "incr.Incr";
}

impl<S> StateMachine for IncrStateMachine<S>
where
    S: Incr,
{
    fn apply<P: AsRef<str>>(
        &mut self,
        path: P,
        _command: Bytes,
    ) -> Result<Bytes, StateMachineError> {
        let path = path.as_ref();
        match path {
            "/incr.Incr/Incr" => {
                let req = IncrRequest::decode(_command)
                    .map_err(|e| StateMachineError::DecodeRequestFailed(e))?;
                let res = self
                    .server
                    .incr(req)
                    .map_err(StateMachineError::ApplyFailed)?;
                let mut buf = BytesMut::new();
                res.encode(&mut buf)
                    .map_err(StateMachineError::EncodeResponseFailed)?;
                Ok(buf.into())
            }
            _ => Err(StateMachineError::UnknownPath(path.to_string())),
        }
    }
}

#[derive(Default, Clone)]
pub struct IncrServer {
    n: u32,
}

impl Incr for IncrServer {
    fn incr(&mut self, req: IncrRequest) -> Result<IncrResponse, ApplyError> {
        self.n += req.i;
        Ok(IncrResponse { n: self.n })
    }
}
