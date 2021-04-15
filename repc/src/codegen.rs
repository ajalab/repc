pub use crate::state_machine::{error::StateMachineError, StateMachine};

/// Handles a byte-array request body with a given handler.
pub fn handle_request<Req, Res, H>(
    request_body: &[u8],
    handler: H,
) -> Result<tonic::Response<bytes::Bytes>, StateMachineError>
where
    Req: Default + prost::Message,
    Res: prost::Message,
    H: FnOnce(Req) -> Result<tonic::Response<Res>, tonic::Status>,
{
    let req = Req::decode(request_body).map_err(|e| StateMachineError::DecodeRequestFailed(e))?;
    let mut res = handler(req).map_err(StateMachineError::ApplyFailed)?;
    let mut res_bytes = tonic::Response::new(bytes::BytesMut::new());
    std::mem::swap(res.metadata_mut(), res_bytes.metadata_mut());
    res.into_inner()
        .encode(res_bytes.get_mut())
        .map_err(StateMachineError::EncodeResponseFailed)?;
    Ok(res_bytes.map(bytes::Bytes::from))
}
